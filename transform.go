// Package skytf implements dataset transformations using the skylark programming dialect
// For more info on skylark check github.com/google/skylark
package skytf

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/google/skylark"
	"github.com/google/skylark/resolve"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/qri/p2p"
	skyctx "github.com/qri-io/skytf/context"
	skyds "github.com/qri-io/skytf/ds"
	skyqri "github.com/qri-io/skytf/qri"
	"github.com/qri-io/starlib"
)

// ExecOpts defines options for execution
type ExecOpts struct {
	Node           *p2p.QriNode
	AllowFloat     bool                   // allow floating-point numbers
	AllowSet       bool                   // allow set data type
	AllowLambda    bool                   // allow lambda expressions
	AllowNestedDef bool                   // allow nested def statements
	Secrets        map[string]interface{} // passed-in secrets (eg: API keys)
	Globals        skylark.StringDict
}

// AddQriNodeOpt adds a qri node to execution options
func AddQriNodeOpt(node *p2p.QriNode) func(o *ExecOpts) {
	return func(o *ExecOpts) {
		o.Node = node
	}
}

// DefaultExecOpts applies default options to an ExecOpts pointer
func DefaultExecOpts(o *ExecOpts) {
	o.AllowFloat = true
	o.AllowSet = true
	o.AllowLambda = true
	o.Globals = skylark.StringDict{}
}

type transform struct {
	node    *p2p.QriNode
	ds      *dataset.Dataset
	skyqri  *skyqri.Module
	globals skylark.StringDict
	secrets map[string]interface{}
	infile  cafs.File

	download skylark.Iterable
}

func newTransform(node *p2p.QriNode, ds *dataset.Dataset, secrets map[string]interface{}, infile cafs.File) *transform {
	return &transform{
		node:    node,
		ds:      ds,
		skyqri:  skyqri.NewModule(node, ds, secrets, infile),
		secrets: secrets,
		infile:  infile,
	}
}

// ExecFile executes a transformation against a skylark file located at filepath, giving back an EntryReader of resulting data
// ExecFile modifies the given dataset pointer. At bare minimum it will set transformation details, but skylark scripts can modify
// many parts of the dataset pointer, including meta, structure, and transform
func ExecFile(ds *dataset.Dataset, filename string, bodyFile cafs.File, opts ...func(o *ExecOpts)) (cafs.File, error) {
	var (
		scriptdata []byte
		err        error
	)

	o := &ExecOpts{}
	DefaultExecOpts(o)
	for _, opt := range opts {
		opt(o)
	}

	// hoist execution settings to resolve package settings
	resolve.AllowFloat = o.AllowFloat
	resolve.AllowSet = o.AllowSet
	resolve.AllowLambda = o.AllowLambda
	resolve.AllowNestedDef = o.AllowNestedDef

	// add error func to skylark environment
	skylark.Universe["error"] = skylark.NewBuiltin("error", Error)
	for key, val := range o.Globals {
		skylark.Universe[key] = val
	}

	// set transform details
	if ds.Transform == nil {
		ds.Transform = &dataset.Transform{}
	}
	ds.Transform.Syntax = "skylark"
	ds.Transform.SyntaxVersion = Version

	// create a reader of script bytes
	scriptdata, err = ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	ds.Transform.Script = bytes.NewReader(scriptdata)

	t := newTransform(o.Node, ds, o.Secrets, bodyFile)

	thread := &skylark.Thread{Load: t.Loader}
	if o.Node != nil {
		thread.Print = func(thread *skylark.Thread, msg string) {
			// note we're ignoring a returned error here
			_, _ = o.Node.LocalStreams.Out.Write([]byte(msg))
		}
	}

	ctx := skyctx.NewContext()

	// execute the transformation
	t.globals, err = skylark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		if evalErr, ok := err.(*skylark.EvalError); ok {
			return nil, fmt.Errorf(evalErr.Backtrace())
		}
		return nil, err
	}

	// set infile to be an empty array for now
	// TODO - this default assumption may mess with things. we shoulda attempt to
	// infer more from the input dataset & make smarter assumption about empty data
	// this implies that all datasets must have a body, which isn't true :/
	t.infile = cafs.NewMemfileBytes("data.json", []byte("[]"))

	funcs, err := t.specialFuncs()
	if err != nil {
		return nil, err
	}

	for name, fn := range funcs {
		val, err := fn(t, thread, ctx)

		if err != nil {
			if evalErr, ok := err.(*skylark.EvalError); ok {
				return nil, fmt.Errorf(evalErr.Backtrace())
			}
			return nil, err
		}

		ctx.SetResult(name, val)
	}

	err = callTransformFunc(t, thread, ctx)

	return t.infile, err
}

// Error halts program execution with an error
func Error(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var msg skylark.Value
	if err := skylark.UnpackPositionalArgs("error", args, kwargs, 1, &msg); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("transform error: %s", msg)
}

// ErrNotDefined is for when a skylark value is not defined or does not exist
var ErrNotDefined = fmt.Errorf("not defined")

// globalFunc checks if a global function is defined
func (t *transform) globalFunc(name string) (fn *skylark.Function, err error) {
	x, ok := t.globals[name]
	if !ok {
		return fn, ErrNotDefined
	}
	if x.Type() != "function" {
		return fn, fmt.Errorf("'%s' is not a function", name)
	}
	return x.(*skylark.Function), nil
}

func confirmIterable(x skylark.Value) (skylark.Iterable, error) {
	v, ok := x.(skylark.Iterable)
	if !ok {
		return nil, fmt.Errorf("did not return structured data")
	}
	return v, nil
}

func (t *transform) specialFuncs() (defined map[string]specialFunc, err error) {
	specialFuncs := map[string]specialFunc{
		"download": callDownloadFunc,
	}

	defined = map[string]specialFunc{}

	for name, fn := range specialFuncs {
		if _, err = t.globalFunc(name); err != nil {
			if err == ErrNotDefined {
				err = nil
				continue
			}
			return nil, err
		}
		defined[name] = fn
	}

	return
}

type specialFunc func(t *transform, thread *skylark.Thread, ctx *skyctx.Context) (result skylark.Value, err error)

func callDownloadFunc(t *transform, thread *skylark.Thread, ctx *skyctx.Context) (result skylark.Value, err error) {
	httpGuard.EnableNtwk()
	defer httpGuard.DisableNtwk()
	t.print("📡 running download...\n")

	var download *skylark.Function
	if download, err = t.globalFunc("download"); err != nil {
		if err == ErrNotDefined {
			return skylark.None, nil
		}
		return skylark.None, err
	}

	return download.Call(thread, skylark.Tuple{ctx.Struct()}, nil)
}

func callTransformFunc(t *transform, thread *skylark.Thread, ctx *skyctx.Context) (err error) {
	var transform *skylark.Function
	if transform, err = t.globalFunc("transform"); err != nil {
		if err == ErrNotDefined {
			return nil
		}
		return err
	}
	t.print("⚙️  running transform...\n")

	d := skyds.NewDataset(t.ds, t.infile)
	if _, err = transform.Call(thread, skylark.Tuple{d.Methods(), ctx.Struct()}, nil); err != nil {
		return err
	}
	t.infile = d.Infile()
	return nil
}

func (t *transform) setSpinnerMsg(msg string) {
	if t.node != nil {
		t.node.LocalStreams.SpinnerMsg(msg)
	}
}

// print writes output only if a node is specified
func (t *transform) print(msg string) {
	if t.node != nil {
		t.node.LocalStreams.Print(msg)
	}
}

func (t *transform) Loader(thread *skylark.Thread, module string) (dict skylark.StringDict, err error) {
	if module == skyqri.ModuleName {
		return t.skyqri.Namespace(), nil
	}
	return starlib.Loader(thread, module)
}

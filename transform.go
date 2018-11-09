// Package startf implements dataset transformations using the starlark programming dialect
// For more info on starlark check github.com/google/starlark
package startf

import (
	"bytes"
	"fmt"
	"io"

	starlark "github.com/google/skylark"
	"github.com/google/skylark/resolve"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/qri/p2p"
	"github.com/qri-io/starlib"
	skyctx "github.com/qri-io/startf/context"
	skyds "github.com/qri-io/startf/ds"
	skyqri "github.com/qri-io/startf/qri"
)

// ExecOpts defines options for execution
type ExecOpts struct {
	Node             *p2p.QriNode
	AllowFloat       bool                   // allow floating-point numbers
	AllowSet         bool                   // allow set data type
	AllowLambda      bool                   // allow lambda expressions
	AllowNestedDef   bool                   // allow nested def statements
	Secrets          map[string]interface{} // passed-in secrets (eg: API keys)
	Globals          starlark.StringDict
	MutateFieldCheck func(path ...string) error
}

// AddQriNodeOpt adds a qri node to execution options
func AddQriNodeOpt(node *p2p.QriNode) func(o *ExecOpts) {
	return func(o *ExecOpts) {
		o.Node = node
	}
}

// AddMutateFieldCheck provides a checkFunc to ExecScript
func AddMutateFieldCheck(check func(path ...string) error) func(o *ExecOpts) {
	return func(o *ExecOpts) {
		o.MutateFieldCheck = check
	}
}

// DefaultExecOpts applies default options to an ExecOpts pointer
func DefaultExecOpts(o *ExecOpts) {
	o.AllowFloat = true
	o.AllowSet = true
	o.AllowLambda = true
	o.Globals = starlark.StringDict{}
}

type transform struct {
	node      *p2p.QriNode
	ds        *dataset.Dataset
	skyqri    *skyqri.Module
	checkFunc func(path ...string) error
	globals   starlark.StringDict
	bodyFile  cafs.File

	download starlark.Iterable
}

// ExecScript executes a transformation against a starlark script file, giving back an EntryReader of resulting data
// ExecScript modifies the given dataset pointer. At bare minimum it will set transformation details, but starlark scripts can modify
// many parts of the dataset pointer, including meta, structure, and transform
func ExecScript(ds *dataset.Dataset, script, bodyFile cafs.File, opts ...func(o *ExecOpts)) (cafs.File, error) {
	var err error

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

	// add error func to starlark environment
	starlark.Universe["error"] = starlark.NewBuiltin("error", Error)
	for key, val := range o.Globals {
		starlark.Universe[key] = val
	}

	// set transform details
	if ds.Transform == nil {
		ds.Transform = &dataset.Transform{}
	}
	ds.Transform.Syntax = "starlark"
	ds.Transform.SyntaxVersion = Version

	// "tee" the script reader to avoid losing script data, as starlark.ExecFile
	// reads, data will be copied to buf, which is re-set to the transform script
	buf := &bytes.Buffer{}
	ds.Transform.Script = buf
	tr := io.TeeReader(script, buf)
	pipeScript := cafs.NewMemfileReader(script.FileName(), tr)

	t := &transform{
		node:      o.Node,
		ds:        ds,
		skyqri:    skyqri.NewModule(o.Node, ds),
		bodyFile:  bodyFile,
		checkFunc: o.MutateFieldCheck,
	}

	ctx := skyctx.NewContext(ds.Transform.Config, o.Secrets)

	thread := &starlark.Thread{Load: t.Loader}
	if o.Node != nil {
		thread.Print = func(thread *starlark.Thread, msg string) {
			// note we're ignoring a returned error here
			_, _ = o.Node.LocalStreams.Out.Write([]byte(msg))
		}
	}

	// execute the transformation
	t.globals, err = starlark.ExecFile(thread, pipeScript.FileName(), pipeScript, nil)
	if err != nil {
		if evalErr, ok := err.(*starlark.EvalError); ok {
			return nil, fmt.Errorf(evalErr.Backtrace())
		}
		return nil, err
	}

	funcs, err := t.specialFuncs()
	if err != nil {
		return nil, err
	}

	for name, fn := range funcs {
		val, err := fn(t, thread, ctx)

		if err != nil {
			if evalErr, ok := err.(*starlark.EvalError); ok {
				return nil, fmt.Errorf(evalErr.Backtrace())
			}
			return nil, err
		}

		ctx.SetResult(name, val)
	}

	err = callTransformFunc(t, thread, ctx)

	return t.bodyFile, err
}

// Error halts program execution with an error
func Error(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var msg starlark.Value
	if err := starlark.UnpackPositionalArgs("error", args, kwargs, 1, &msg); err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("transform error: %s", msg)
}

// ErrNotDefined is for when a starlark value is not defined or does not exist
var ErrNotDefined = fmt.Errorf("not defined")

// globalFunc checks if a global function is defined
func (t *transform) globalFunc(name string) (fn *starlark.Function, err error) {
	x, ok := t.globals[name]
	if !ok {
		return fn, ErrNotDefined
	}
	if x.Type() != "function" {
		return fn, fmt.Errorf("'%s' is not a function", name)
	}
	return x.(*starlark.Function), nil
}

func confirmIterable(x starlark.Value) (starlark.Iterable, error) {
	v, ok := x.(starlark.Iterable)
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

type specialFunc func(t *transform, thread *starlark.Thread, ctx *skyctx.Context) (result starlark.Value, err error)

func callDownloadFunc(t *transform, thread *starlark.Thread, ctx *skyctx.Context) (result starlark.Value, err error) {
	httpGuard.EnableNtwk()
	defer httpGuard.DisableNtwk()
	t.print("📡 running download...\n")

	var download *starlark.Function
	if download, err = t.globalFunc("download"); err != nil {
		if err == ErrNotDefined {
			return starlark.None, nil
		}
		return starlark.None, err
	}

	return starlark.Call(thread, download, starlark.Tuple{ctx.Struct()}, nil)
}

func callTransformFunc(t *transform, thread *starlark.Thread, ctx *skyctx.Context) (err error) {
	var transform *starlark.Function
	if transform, err = t.globalFunc("transform"); err != nil {
		if err == ErrNotDefined {
			return nil
		}
		return err
	}
	t.print("🤖  running transform...\n")

	d := skyds.NewDataset(t.ds, t.bodyFile, t.checkFunc)
	if _, err = starlark.Call(thread, transform, starlark.Tuple{d.Methods(), ctx.Struct()}, nil); err != nil {
		return err
	}
	t.bodyFile = d.BodyFile()
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

func (t *transform) Loader(thread *starlark.Thread, module string) (dict starlark.StringDict, err error) {
	if module == skyqri.ModuleName {
		return t.skyqri.Namespace(), nil
	}
	return starlib.Loader(thread, module)
}

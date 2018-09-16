// Package skytf implements dataset transformations using the skylark programming dialect
// For more info on skylark check github.com/google/skylark
package skytf

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/google/skylark"
	"github.com/google/skylark/resolve"
	"github.com/google/skylark/skylarkstruct"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qri/p2p"
	"github.com/qri-io/skytf/lib"

	skyhtml "github.com/qri-io/skytf/lib/html"
	skyhttp "github.com/qri-io/skytf/lib/http"
	skyqri "github.com/qri-io/skytf/lib/qri"
	skyxlsx "github.com/qri-io/skytf/lib/xlsx"
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
	ds      *dataset.Dataset
	globals skylark.StringDict
	secrets map[string]interface{}
	infile  cafs.File

	download skylark.Iterable
}

func newTransform(ds *dataset.Dataset, secrets map[string]interface{}, infile cafs.File) *transform {
	return &transform{
		ds:      ds,
		secrets: secrets,
		infile:  infile,
	}
}

// ExecFile executes a transformation against a skylark file located at filepath, giving back an EntryReader of resulting data
// ExecFile modifies the given dataset pointer. At bare minimum it will set transformation details, but skylark scripts can modify
// many parts of the dataset pointer, including meta, structure, and transform
func ExecFile(ds *dataset.Dataset, filename string, bodyFile cafs.File, opts ...func(o *ExecOpts)) (dsio.EntryReader, error) {
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

	t := newTransform(ds, o.Secrets, bodyFile)

	thread := &skylark.Thread{Load: loader}
	if o.Node != nil {
		thread.Print = func(thread *skylark.Thread, msg string) {
			// note we're ignoring a returned error here
			_, _ = o.Node.LocalStreams.Out.Write([]byte(msg))
		}
	}

	skyhttp.DisableNtwk()
	// execute the transformation
	t.globals, err = skylark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		return nil, err
	}

	var data skylark.Iterable
	funcs, err := t.transformFuncs()
	if err != nil {
		return nil, err
	}
	for _, fn := range funcs {
		if data, err = fn(t, thread, data); err != nil {
			return nil, err
		}
	}

	sch := dataset.BaseSchemaArray
	if data.Type() == "dict" {
		sch = dataset.BaseSchemaObject
	}

	st := &dataset.Structure{
		Format: dataset.UnknownDataFormat,
		Schema: sch,
	}

	if ds.Structure == nil {
		ds.Structure = st
	}

	r := NewEntryReader(st, data)
	return r, nil
}

// Error halts program execution with an error
func Error(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var msg skylark.Value
	if err := skylark.UnpackPositionalArgs("error", args, kwargs, 1, &msg); err != nil {
		return nil, err
	}
	if str, err := lib.AsString(msg); err == nil {
		return nil, fmt.Errorf("transform error: %s", str)
	}
	return nil, fmt.Errorf("tranform errored (no valid message provided)")
}

// loader is currently not in use
func loader(thread *skylark.Thread, module string) (dict skylark.StringDict, err error) {
	return nil, fmt.Errorf("load is not permitted when executing a qri transformation")
}

// ErrNotDefined is for when a skylark value is not defined or does not exist
var ErrNotDefined = fmt.Errorf("not defined")

// globalFunc checks global function is defined
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

func (t *transform) transformFuncs() (funcs []transformFunc, err error) {
	cascade := []struct {
		name string
		fn   transformFunc
	}{
		{"download", callDownloadFunc},
		{"transform", callTransformFunc},
	}

	for _, s := range cascade {
		if _, err = t.globalFunc(s.name); err != nil {
			if err == ErrNotDefined {
				continue
			}
			return nil, err
		}
		funcs = append(funcs, s.fn)
	}
	if len(funcs) == 0 {
		err = fmt.Errorf("no transform functions defined. please define at least either a 'download' or a 'transform' function")
	}
	return
}

type transformFunc func(t *transform, thread *skylark.Thread, prev skylark.Iterable) (data skylark.Iterable, err error)

func callDownloadFunc(t *transform, thread *skylark.Thread, prev skylark.Iterable) (data skylark.Iterable, err error) {
	skyhttp.EnableNtwk()
	defer skyhttp.DisableNtwk()

	var download *skylark.Function
	if download, err = t.globalFunc("download"); err != nil {
		if err == ErrNotDefined {
			return prev, nil
		}
		return prev, err
	}

	qm := skyqri.NewModule(t.ds, t.secrets, t.infile)

	qri := skylarkstruct.FromStringDict(skylarkstruct.Default, skylark.StringDict{
		"get_config": skylark.NewBuiltin("get_config", qm.GetConfig),
		"get_secret": skylark.NewBuiltin("get_secret", qm.GetSecret),
		"http":       skyhttp.NewModule(t.ds).Struct(),
		"html":       skylark.NewBuiltin("html", skyhtml.NewDocument),
		"xlsx":       skyxlsx.NewModule().Struct(),
	})

	x, err := download.Call(thread, skylark.Tuple{qri}, nil)
	if err != nil {
		return nil, err
	}
	data, err = confirmIterable(x)
	if err != nil {
		return nil, err
	}
	t.download = data

	return data, err
}

func callTransformFunc(t *transform, thread *skylark.Thread, prev skylark.Iterable) (data skylark.Iterable, err error) {
	var transform *skylark.Function
	if transform, err = t.globalFunc("transform"); err != nil {
		if err == ErrNotDefined {
			return prev, nil
		}
		return prev, err
	}

	qm := skyqri.NewModule(t.ds, t.secrets, t.infile)
	qri := skylarkstruct.FromStringDict(skylark.String("qri"), qm.AddAllMethods(skylark.StringDict{
		"download": t.download,
		"html":     skylark.NewBuiltin("html", skyhtml.NewDocument),
	}))

	x, err := transform.Call(thread, skylark.Tuple{qri}, nil)
	if err != nil {
		return nil, err
	}
	return confirmIterable(x)
}

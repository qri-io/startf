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
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/skytf/lib"
	skyhttp "github.com/qri-io/skytf/lib/http"
	skyqri "github.com/qri-io/skytf/lib/qri"
)

// ExecOpts defines options for exection
type ExecOpts struct {
	AllowFloat     bool                   // allow floating-point numbers
	AllowSet       bool                   // allow set data type
	AllowLambda    bool                   // allow lambda expressions
	AllowNestedDef bool                   // allow nested def statements
	Secrets        map[string]interface{} // passed-in secrets (eg: API keys)
}

// DefaultExecOpts applies default options to an ExecOpts pointer
func DefaultExecOpts(o *ExecOpts) {
	o.AllowFloat = true
	o.AllowSet = true
}

type transform struct {
	ds      *dataset.Dataset
	secrets map[string]interface{}
	rules   map[string]*Protector
	infile  cafs.File
}

func newTransform(ds *dataset.Dataset, secrets map[string]interface{}, infile cafs.File) *transform {
	rules := map[string]*Protector{
		skyqri.ModuleName: &Protector{
			Module: "qri",
			Rules: []Rule{
				{"download", "", false},
				{"download", "get_config", true},
				{"download", "get_secret", true},
			},
		},
		skyhttp.ModuleName: &Protector{
			Module: "http",
			Rules: []Rule{
				{"", "", false},
				{"download", "", true},
			},
		},
	}

	return &transform{
		ds:      ds,
		secrets: secrets,
		rules:   rules,
		infile:  infile,
	}
}

func (t *transform) setStep(step string) {
	for _, p := range t.rules {
		p.SetStep(step)
	}
}

func (t *transform) Loader(thread *skylark.Thread, module string) (dict skylark.StringDict, err error) {
	switch module {
	case skyqri.ModuleName:
		dict, err = skyqri.NewModule(t.ds, t.secrets, t.infile)
		if err != nil {
			return nil, err
		}
	case skyhttp.ModuleName:
		dict, err = skyhttp.NewModule(t.ds)
		if err != nil {
			return nil, err
		}
	}

	if dict == nil {
		return nil, fmt.Errorf("invalid module")
	}

	t.rules[module].ProtectMethods(dict)
	return
}

// ExecFile executes a transformation against a skylark file located at filepath, giving back an EntryReader of resulting data
// ExecFile modifies the given dataset pointer. At bare minimum it will set transformation details, but skylark scripts can modify
// many parts of the dataset pointer, including meta, structure, and transform
func ExecFile(ds *dataset.Dataset, filename string, infile cafs.File, opts ...func(o *ExecOpts)) (dsio.EntryReader, error) {
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

	t := newTransform(ds, o.Secrets, infile)

	var globals skylark.StringDict
	thread := &skylark.Thread{Load: t.Loader}

	// execute the transformation
	globals, err = skylark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		return nil, err
	}

	data, err := t.execTransformSteps(globals, thread, "download", "map", "reduce", "transform")
	if err != nil {
		return nil, err
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

// ErrNotDefined is for when a skylark value is not defined or does not exist
var ErrNotDefined = fmt.Errorf("not defined")

func (t *transform) execTransformSteps(globals skylark.StringDict, thread *skylark.Thread, chain ...string) (data skylark.Iterable, err error) {
	var (
		called bool
		fn     *skylark.Function
	)

	for _, step := range chain {
		t.setStep(step)
		if fn, err = isDictFunc(globals, step); err != nil {
			if err == ErrNotDefined {
				err = nil
				continue
			}
			return
		}

		called = true
		data, err = callDataFunc(fn, thread, data)
		if err != nil {
			return
		}
	}
	if !called {
		return nil, fmt.Errorf("no data functions were defined")
	}

	return
}

// isDictFunc checks if a skylark string dictionary value is a function
func isDictFunc(globals skylark.StringDict, name string) (fn *skylark.Function, err error) {
	x, ok := globals[name]
	if !ok {
		return fn, ErrNotDefined
	}
	if x.Type() != "function" {
		return fn, fmt.Errorf("'%s' is not a function", name)
	}
	return x.(*skylark.Function), nil
}

func callDataFunc(fn *skylark.Function, thread *skylark.Thread, data skylark.Iterable) (skylark.Iterable, error) {
	x, err := fn.Call(thread, skylark.Tuple{data}, nil)
	if err != nil {
		return nil, err
	}

	v, ok := x.(skylark.Iterable)
	if !ok {
		return nil, fmt.Errorf("did not return structured data")
	}
	return v, nil
}

// Package skytf implements dataset transformations using the skylark programming dialect
// For more info on skylark check github.com/google/skylark
package skytf

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/google/skylark"
	"github.com/google/skylark/resolve"
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

// ExecFile executes a transformation against a skylark file located at filepath, giving back an EntryReader of resulting data
// ExecFile modifies the given dataset pointer. At bare minimum it will set transformation details, but skylark scripts can modify
// many parts of the dataset pointer, including meta, structure, and transform
func ExecFile(ds *dataset.Dataset, filename string, opts ...func(o *ExecOpts)) (dsio.EntryReader, error) {
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

	// allocate skyqri module here so we can get data back post-execution
	m := skyqri.NewModule(ds, o.Secrets)

	thread := &skylark.Thread{Load: newLoader(ds, m)}

	// execute the transformation
	_, err = skylark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		return nil, err
	}

	data, err := m.Data()
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

// newLoader implements the 'load' skylark loader function, allowing only modules specified here to be loaded (so, no filesystem access)
func newLoader(ds *dataset.Dataset, m *skyqri.Module) func(thread *skylark.Thread, module string) (skylark.StringDict, error) {
	return func(thread *skylark.Thread, module string) (skylark.StringDict, error) {
		switch module {
		case skyqri.ModuleName:
			return m.Load()
		case skyhttp.ModuleName:
			return skyhttp.NewModule(ds)
		}

		return nil, fmt.Errorf("invalid module")
	}
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

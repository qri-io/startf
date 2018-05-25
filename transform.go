// Package skytf implements dataset transformations using the skylark programming dialect
// For more info on skylark check github.com/google/skylark
package skytf

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/google/skylark"
	"github.com/google/skylark/repl"
	"github.com/google/skylark/resolve"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

// ExecOpts defines options for exection
type ExecOpts struct {
	AllowFloat     bool // allow floating-point numbers
	AllowSet       bool // allow set data type
	AllowLambda    bool // allow lambda expressions
	AllowNestedDef bool // allow nested def statements
	Secrets        map[string]interface{}
}

// DefaultExecOpts applies default options to an ExecOpts pointer
func DefaultExecOpts(o *ExecOpts) {
	o.AllowFloat = true
	o.AllowSet = true
}

// ExecFile executes a transformation against a filepath
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

	resolve.AllowFloat = o.AllowFloat
	resolve.AllowSet = o.AllowSet
	resolve.AllowLambda = o.AllowLambda
	resolve.AllowNestedDef = o.AllowNestedDef

	scriptdata, err = ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if ds.Transform == nil {
		ds.Transform = &dataset.Transform{}
	}
	ds.Transform.Syntax = "skylark"
	ds.Transform.SyntaxVersion = Version
	ds.Transform.Script = bytes.NewReader(scriptdata)

	skylark.Universe["error"] = skylark.NewBuiltin("error", Error)

	cm := commit{}
	skylark.Universe["commit"] = skylark.NewBuiltin("commit", cm.Do)

	dsb := newDatasetBuiltins(ds)
	skylark.Universe["get_config"] = skylark.NewBuiltin("get_config", dsb.GetConfig)
	skylark.Universe["set_meta"] = skylark.NewBuiltin("set_meta", dsb.SetMeta)

	hr := newHTTPRequests(ds)
	skylark.Universe["fetch_json_url"] = skylark.NewBuiltin("fetch_json_url", hr.FetchJSONUrl)

	thread := &skylark.Thread{Load: repl.MakeLoad()}

	// Execute specified file.
	_, err = skylark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		return nil, err
	}

	if !cm.called {
		return nil, fmt.Errorf("commit must be called once to add data")
	}

	sch := dataset.BaseSchemaArray
	if cm.data.Type() == "dict" {
		sch = dataset.BaseSchemaObject
	}

	st := &dataset.Structure{
		Format: dataset.UnknownDataFormat,
		Schema: sch,
	}

	if ds.Structure == nil {
		ds.Structure = st
	}

	// fmt.Printf("%v", cm.data)

	r := NewEntryReader(st, cm.data)
	return r, nil
}

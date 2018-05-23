// Package skytf implements dataset transformations using the skylark programming dialect
// For more info on skylark check github.com/google/skylark
package skytf

import (
	"fmt"
	"log"

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
}

// DefaultExecOpts applies default options to an ExecOpts pointer
func DefaultExecOpts(o *ExecOpts) {
	o.AllowFloat = true
	o.AllowSet = true
}

// ExecFile executes a transformation against a filepath
func ExecFile(filename string, opts ...func(o *ExecOpts)) (*dataset.Dataset, dsio.EntryReader, error) {
	o := &ExecOpts{}
	DefaultExecOpts(o)
	for _, opt := range opts {
		opt(o)
	}

	resolve.AllowFloat = o.AllowFloat
	resolve.AllowSet = o.AllowSet
	resolve.AllowLambda = o.AllowLambda
	resolve.AllowNestedDef = o.AllowNestedDef

	cm := commit{}
	skylark.Universe["commit"] = skylark.NewBuiltin("commit", cm.Do)

	thread := &skylark.Thread{Load: repl.MakeLoad()}
	// globals := make(skylark.StringDict)

	// Execute specified file.
	var err error
	_, err = skylark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		log.Print(err.Error())
		return nil, nil, err
	}

	if !cm.called {
		return nil, nil, fmt.Errorf("commit must be called once to add data")
	}

	// Print the global environment.
	// var names []string
	// for name := range globals {
	// 	if !strings.HasPrefix(name, "_") {
	// 		names = append(names, name)
	// 	}
	// }
	// sort.Strings(names)
	// for _, name := range names {
	// 	fmt.Fprintf(os.Stderr, "%s = %s\n", name, globals[name])
	// }

	ds := &dataset.Dataset{
		Structure: &dataset.Structure{
			Format: dataset.UnknownDataFormat,
			Schema: dataset.BaseSchemaArray,
		},
		Transform: &dataset.Transform{
			Syntax: "skylark",
		},
	}

	r := NewEntryReader(ds.Structure, cm.data)
	return ds, r, nil
}

type commit struct {
	called bool
	data   skylark.Iterable
}

// Do executes a commit. must be called exactly once per transformation
func (c *commit) Do(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if c.called {
		return skylark.False, fmt.Errorf("commit can only be called once per transformation")
	}

	if err := skylark.UnpackPositionalArgs("foo", args, kwargs, 1, &c.data); err != nil {
		return nil, err
	}

	// iter := iterable.Iterate()
	// defer iter.Done()
	// var x skylark.Value
	// for iter.Next(&x) {
	// 	if x.Truth() {
	// 		return skylark.True, nil
	// 	}
	// }

	c.called = true
	return skylark.True, nil
}

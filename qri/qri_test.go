package qri

import (
	"fmt"
	"testing"

	"github.com/google/skylark"
	"github.com/qri-io/dataset"
)

func TestNewModule(t *testing.T) {
	ds := &dataset.Dataset{
		Transform: &dataset.Transform{
			Syntax: "skylark",
			Config: map[string]interface{}{
				"foo": "bar",
			},
		},
	}

	thread := &skylark.Thread{Load: newLoader(ds)}

	// Execute test file
	_, err := skylark.ExecFile(thread, "testdata/test.sky", nil, nil)
	if err != nil {
		t.Error(err)
	}
}

// load implements the 'load' operation as used in the evaluator tests.
func newLoader(ds *dataset.Dataset) func(thread *skylark.Thread, module string) (skylark.StringDict, error) {
	return func(thread *skylark.Thread, module string) (skylark.StringDict, error) {
		if module == ModuleName {
			return skylark.StringDict{"qri": NewModule(nil, ds, nil, nil).Struct()}, nil
		}

		return nil, fmt.Errorf("invalid module")
	}
}

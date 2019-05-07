package ds

import (
	"fmt"
	"testing"

	"github.com/qri-io/dataset"
	"go.starlark.net/resolve"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarktest"
)

func TestCheckFields(t *testing.T) {
	fieldErr := fmt.Errorf("can't mutate this field")
	allErrCheck := func(fields ...string) error {
		return fieldErr
	}
	ds := NewDataset(nil, allErrCheck)
	ds.SetMutable(&dataset.Dataset{})
	thread := &starlark.Thread{}

	if _, err := ds.SetBody(thread, nil, starlark.Tuple{starlark.String("data")}, nil); err != fieldErr {
		t.Errorf("expected fieldErr, got: %s", err)
	}

	if _, err := ds.SetMeta(thread, nil, starlark.Tuple{starlark.String("key"), starlark.String("value")}, nil); err != fieldErr {
		t.Errorf("expected fieldErr, got: %s", err)
	}

	if _, err := ds.SetStructure(thread, nil, starlark.Tuple{starlark.String("wut")}, nil); err != fieldErr {
		t.Errorf("expected fieldErr, got: %s", err)
	}
}

func TestFile(t *testing.T) {
	resolve.AllowFloat = true
	thread := &starlark.Thread{Load: newLoader()}
	starlarktest.SetReporter(thread, t)

	// Execute test file
	_, err := starlark.ExecFile(thread, "testdata/test.star", nil, nil)
	if err != nil {
		if ee, ok := err.(*starlark.EvalError); ok {
			t.Error(ee.Backtrace())
		} else {
			t.Error(err)
		}
	}
}

// load implements the 'load' operation as used in the evaluator tests.
func newLoader() func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
	return func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
		switch module {
		case ModuleName:
			return LoadModule()
		case "assert.star":
			return starlarktest.LoadAssertModule()
		}

		return nil, fmt.Errorf("invalid module")
	}
}

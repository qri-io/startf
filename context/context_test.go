package context

import (
	"fmt"
	"testing"

	starlark "github.com/google/skylark"
	starlarktest "github.com/google/skylark/skylarktest"
)

func TestContext(t *testing.T) {
	thread := &starlark.Thread{Load: newLoader()}

	dlCtx := NewContext()
	dlCtx.SetResult("download", starlark.Bool(true))

	// Execute test file
	_, err := starlark.ExecFile(thread, "testdata/test.sky", nil, starlark.StringDict{
		"ctx":    NewContext().Struct(),
		"dl_ctx": dlCtx.Struct(),
	})
	if err != nil {
		t.Error(err)
	}
}

func TestMissingValue(t *testing.T) {
	thread := &starlark.Thread{Load: newLoader()}

	ctx := NewContext()
	val, err := ctx.getValue(thread, nil, starlark.Tuple{starlark.String("foo")}, nil)
	if val != starlark.None {
		t.Errorf("expected none return value")
	}

	if err.Error() != "value foo not set in context" {
		t.Errorf("error message mismatch. expected: %s, got: %s", "value foo not set in context", err.Error())
	}
}

// load implements the 'load' operation as used in the evaluator tests.
func newLoader() func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
	return func(thread *starlark.Thread, module string) (starlark.StringDict, error) {
		switch module {
		case "assert.sky":
			return starlarktest.LoadAssertModule()
		}

		return nil, fmt.Errorf("invalid module")
	}
}

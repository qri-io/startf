package ds

import (
	"fmt"
	"testing"

	starlark "github.com/google/skylark"
)

func TestCheckFields(t *testing.T) {
	fieldErr := fmt.Errorf("can't mutate this field")
	allErrCheck := func(fields ...string) error {
		return fieldErr
	}
	ds := NewDataset(nil, nil, allErrCheck)
	thread := &starlark.Thread{}

	if _, err := ds.SetBody(thread, nil, starlark.Tuple{starlark.String("data")}, nil); err != fieldErr {
		t.Errorf("expected fieldErr, got: %s", err)
	}

	if _, err := ds.SetMeta(thread, nil, starlark.Tuple{starlark.String("key"), starlark.String("value")}, nil); err != fieldErr {
		t.Errorf("expected fieldErr, got: %s", err)
	}

	if _, err := ds.SetSchema(thread, nil, starlark.Tuple{starlark.String("wut")}, nil); err != fieldErr {
		t.Errorf("expected fieldErr, got: %s", err)
	}
}

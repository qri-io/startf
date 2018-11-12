package qri

import (
	"fmt"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
	"github.com/qri-io/starlib/util"
	starlark "go.starlark.net/starlark"
)

// StarlarkEntryWriter creates a starlark.Value as an EntryWriter
type StarlarkEntryWriter struct {
	IsDict bool
	Struct *dataset.Structure
	Object starlark.Value
}

// WriteEntry adds an entry to the underlying starlark.Value
func (w *StarlarkEntryWriter) WriteEntry(ent dsio.Entry) error {
	if w.IsDict {
		dict := w.Object.(*starlark.Dict)
		key, err := util.Marshal(ent.Key)
		if err != nil {
			return err
		}
		val, err := util.Marshal(ent.Value)
		if err != nil {
			return err
		}
		dict.SetKey(key, val)
	} else {
		list := w.Object.(*starlark.List)
		val, err := util.Marshal(ent.Value)
		if err != nil {
			return err
		}
		list.Append(val)
	}
	return nil
}

// Structure returns the EntryWriter's dataset structure
func (w *StarlarkEntryWriter) Structure() *dataset.Structure {
	return w.Struct
}

// Close is a no-op
func (w *StarlarkEntryWriter) Close() error {
	return nil
}

// Value returns the underlying starlark.Value
func (w *StarlarkEntryWriter) Value() starlark.Value {
	return w.Object
}

// NewStarlarkEntryWriter returns a new StarlarkEntryWriter
func NewStarlarkEntryWriter(st *dataset.Structure) (*StarlarkEntryWriter, error) {
	mode, err := schemaScanMode(st.Schema)
	if err != nil {
		return nil, err
	}
	if mode == smObject {
		return &StarlarkEntryWriter{IsDict: true, Struct: st, Object: &starlark.Dict{}}, nil
	}
	return &StarlarkEntryWriter{IsDict: false, Struct: st, Object: &starlark.List{}}, nil
}

// TODO: Refactor everything below this so that jsonschema returns this in a simple way
type scanMode int

const (
	smArray scanMode = iota
	smObject
)

// schemaScanMode determines weather the top level is an array or object
func schemaScanMode(sc *jsonschema.RootSchema) (scanMode, error) {
	if vt, ok := sc.Validators["type"]; ok {
		// TODO - lol go PR jsonschema to export access to this instead of this
		// silly validation hack
		obj := []jsonschema.ValError{}
		arr := []jsonschema.ValError{}
		vt.Validate("", map[string]interface{}{}, &obj)
		vt.Validate("", []interface{}{}, &arr)
		if len(obj) == 0 {
			return smObject, nil
		} else if len(arr) == 0 {
			return smArray, nil
		}
	}
	err := fmt.Errorf("invalid schema. root must be either an array or object type")
	return smArray, err
}

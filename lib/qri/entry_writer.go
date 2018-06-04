package qri

import (
	"fmt"

	"github.com/google/skylark"
	"github.com/qri-io/skytf/lib"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
)

// SkylarkEntryWriter creates a skylark.Value as an EntryWriter
type SkylarkEntryWriter struct {
	IsDict bool
	Struct *dataset.Structure
	Object skylark.Value
}

// WriteEntry adds an entry to the underlying skylark.Value
func (w *SkylarkEntryWriter) WriteEntry(ent dsio.Entry) error {
	if w.IsDict {
		dict := w.Object.(*skylark.Dict)
		key, err := lib.Marshal(ent.Key)
		if err != nil {
			return err
		}
		val, err := lib.Marshal(ent.Value)
		if err != nil {
			return err
		}
		dict.Set(key, val)
	} else {
		list := w.Object.(*skylark.List)
		val, err := lib.Marshal(ent.Value)
		if err != nil {
			return err
		}
		list.Append(val)
	}
	return nil
}

// Structure returns the EntryWriter's dataset structure
func (w *SkylarkEntryWriter) Structure() *dataset.Structure {
	return w.Struct
}

// Close is a no-op
func (w *SkylarkEntryWriter) Close() error {
	return nil
}

// Value returns the underlying skylark.Value
func (w *SkylarkEntryWriter) Value() skylark.Value {
	return w.Object
}

// NewSkylarkEntryWriter returns a new SkylarkEntryWriter
func NewSkylarkEntryWriter(st *dataset.Structure) (*SkylarkEntryWriter, error) {
	mode, err := schemaScanMode(st.Schema)
	if err != nil {
		return nil, err
	}
	if mode == smObject {
		return &SkylarkEntryWriter{IsDict: true, Struct: st, Object: &skylark.Dict{}}, nil
	}
	return &SkylarkEntryWriter{IsDict: false, Struct: st, Object: &skylark.List{}}, nil
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

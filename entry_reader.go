package sltf

import (
	"fmt"
	"io"

	"github.com/google/skylark"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

// EntryReader implements the dsio.EntryReader interface for skylark.Iterable's
type EntryReader struct {
	i    int
	st   *dataset.Structure
	iter skylark.Iterator
}

// NewEntryReader creates a new Entry Reader
func NewEntryReader(st *dataset.Structure, data skylark.Iterable) *EntryReader {
	return &EntryReader{
		st:   st,
		iter: data.Iterate(),
	}
}

// Structure gives this reader's structure
func (r *EntryReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one entry from the reader
func (r *EntryReader) ReadEntry() (e dsio.Entry, err error) {
	defer func() { r.i++ }()

	var v skylark.Value

	if !r.iter.Next(&v) {
		r.iter.Done()
		return e, io.EOF
	}

	switch v.Type() {
	case "NoneType":
		e.Value = nil
	case "bool":
		e.Value = v.Truth() == skylark.True
	case "float":
		if f, ok := skylark.AsFloat(v); ok {
			e.Value = f
		} else {
			err = fmt.Errorf("couldn't coerce float")
		}
	case "dict":
		err = fmt.Errorf("dicts aren't yet supported")
	case "list":
		err = fmt.Errorf("lists aren't yet supported")
	case "string":
		e.Value = v.String()
	case "tuple":
		err = fmt.Errorf("tuples aren't yet supported")
	case "set":
		err = fmt.Errorf("sets aren't yet supported")
	case "int":
		e.Value, err = skylark.AsInt32(v)
	default:
		err = fmt.Errorf("unrecognized skylark type: %s", v.Type())
	}

	return
}

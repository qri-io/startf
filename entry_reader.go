package startf

import (
	"fmt"
	"io"

	"github.com/google/skylark"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/starlib/util"
)

// EntryReader implements the dsio.EntryReader interface for skylark.Iterable's
type EntryReader struct {
	i    int
	st   *dataset.Structure
	iter skylark.Iterator
	data skylark.Value
}

// NewEntryReader creates a new Entry Reader
func NewEntryReader(st *dataset.Structure, iter skylark.Iterable) *EntryReader {
	return &EntryReader{
		st:   st,
		data: iter.(skylark.Value),
		iter: iter.Iterate(),
	}
}

// Structure gives this reader's structure
func (r *EntryReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one entry from the reader
func (r *EntryReader) ReadEntry() (e dsio.Entry, err error) {
	// Read next element (key for object, value for array).
	var next skylark.Value
	if !r.iter.Next(&next) {
		r.iter.Done()
		return e, io.EOF
	}

	// Handle array entry.
	if r.st.Schema.TopLevelType() == "array" {
		e.Index = r.i
		r.i++
		e.Value, err = util.Unmarshal(next)
		if err != nil {
			fmt.Printf("reading error: %s\n", err.Error())
		}
		return
	}

	// Handle object entry. Assume key is a string.
	var ok bool
	e.Key, ok = skylark.AsString(next)
	if !ok {
		fmt.Printf("key error: %s\n", next)
	}
	// Lookup the corresponding value for the key.
	dict := r.data.(*skylark.Dict)
	value, ok, err := dict.Get(next)
	if err != nil {
		fmt.Printf("reading error: %s\n", err.Error())
	}
	e.Value, err = util.Unmarshal(value)
	if err != nil {
		fmt.Printf("reading error: %s\n", err.Error())
	}
	return
}

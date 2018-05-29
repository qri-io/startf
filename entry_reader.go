package skytf

import (
	"fmt"
	"io"
	"strconv"

	"github.com/google/skylark"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/skytf/lib"
)

// EntryReader implements the dsio.EntryReader interface for skylark.Iterable's
type EntryReader struct {
	i    int
	st   *dataset.Structure
	iter skylark.Iterator
	keys []skylark.Value
}

// NewEntryReader creates a new Entry Reader
func NewEntryReader(st *dataset.Structure, data skylark.Iterable) *EntryReader {
	r := &EntryReader{
		st:   st,
		iter: data.Iterate(),
	}

	// TODO - better base objet / map detection
	if dict, ok := data.(*skylark.Dict); ok {
		r.keys = dict.Keys()
	}

	return r
}

// Structure gives this reader's structure
func (r *EntryReader) Structure() *dataset.Structure {
	return r.st
}

// ReadEntry reads one entry from the reader
func (r *EntryReader) ReadEntry() (e dsio.Entry, err error) {

	defer func() { r.i++ }()

	var x skylark.Value
	if !r.iter.Next(&x) {
		r.iter.Done()
		return e, io.EOF
	}

	if r.keys != nil {
		key, err := strconv.Unquote(r.keys[r.i].String())
		if err != nil {
			return e, err
		}
		e.Key = key
	} else {
		e.Index = r.i
	}

	e.Value, err = lib.Unmarshal(x)
	if err != nil {
		fmt.Printf("reading error: %s\n", err.Error())
	}
	return
}

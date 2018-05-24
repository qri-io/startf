package skytf

import (
	"fmt"
	"io"
	"strconv"

	"github.com/google/skylark"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
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

	e.Value, err = parse(x)
	if err != nil {
		fmt.Printf("reading error: %s\n", err.Error())
	}
	return
}

func parse(x skylark.Value) (val interface{}, err error) {
	switch x.Type() {
	case "NoneType":
		val = nil
	case "bool":
		val = x.Truth() == skylark.True
	case "int":
		val, err = skylark.AsInt32(x)
	case "float":
		if f, ok := skylark.AsFloat(x); ok {
			val = f
		} else {
			err = fmt.Errorf("couldn't parse float")
		}
	case "string":
		val, err = asString(x)
		// val = x.String()
	case "dict":
		if dict, ok := x.(*skylark.Dict); ok {
			var (
				v     skylark.Value
				pval  interface{}
				value = map[string]interface{}{}
			)

			for _, k := range dict.Keys() {
				v, ok, err = dict.Get(k)
				if err != nil {
					return
				}

				pval, err = parse(v)
				if err != nil {
					return
				}

				var str string
				str, err = asString(k)
				if err != nil {
					return
				}

				value[str] = pval
			}
			val = value
		} else {
			err = fmt.Errorf("error parsing dict. invalid type: %v", x)
		}
	case "list":
		if list, ok := x.(*skylark.List); ok {
			var (
				i     int
				v     skylark.Value
				iter  = list.Iterate()
				value = make([]interface{}, list.Len())
			)

			for iter.Next(&v) {
				value[i], err = parse(v)
				if err != nil {
					return
				}
				i++
			}
			iter.Done()
			val = value
		} else {
			err = fmt.Errorf("error parsing list. invalid type: %v", x)
		}
	case "tuple":
		if tuple, ok := x.(skylark.Tuple); ok {
			var (
				i     int
				v     skylark.Value
				iter  = tuple.Iterate()
				value = make([]interface{}, tuple.Len())
			)

			for iter.Next(&v) {
				value[i], err = parse(v)
				if err != nil {
					return
				}
				i++
			}
			iter.Done()
			val = value
		} else {
			err = fmt.Errorf("error parsing dict. invalid type: %v", x)
		}
	case "set":
		fmt.Println("errnotdone: SET")
		err = fmt.Errorf("sets aren't yet supported")
	default:
		fmt.Println("errbadtype:", x.Type())
		err = fmt.Errorf("unrecognized skylark type: %s", x.Type())
	}
	return
}

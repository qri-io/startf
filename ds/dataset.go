// Package ds exposes the qri dataset document model into starlark
package ds

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	starlark "github.com/google/skylark"
	starlarkstruct "github.com/google/skylark/skylarkstruct"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
	"github.com/qri-io/starlib/util"
)

// Dataset is a qri dataset starlark type
type Dataset struct {
	ds     *dataset.Dataset
	infile cafs.File
	body   starlark.Iterable
}

// NewDataset creates a dataset object
func NewDataset(ds *dataset.Dataset, infile cafs.File) *Dataset {
	return &Dataset{ds: ds, infile: infile}
}

// Dataset returns the underlying dataset
func (d *Dataset) Dataset() *dataset.Dataset {
	return d.ds
}

// Infile gives access to the private infile
func (d *Dataset) Infile() cafs.File {
	return d.infile
}

// Methods exposes dataset methods as starlark values
func (d *Dataset) Methods() *starlarkstruct.Struct {
	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"set_meta":   starlark.NewBuiltin("set_meta", d.SetMeta),
		"set_schema": starlark.NewBuiltin("set_schema", d.SetSchema),
		"get_body":   starlark.NewBuiltin("get_body", d.GetBody),
		"set_body":   starlark.NewBuiltin("set_body", d.SetBody),
	})
}

// SetMeta sets a dataset meta field
func (d *Dataset) SetMeta(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var keyx, valx starlark.Value
	if err := starlark.UnpackPositionalArgs("set_meta", args, kwargs, 2, &keyx, &valx); err != nil {
		return nil, err
	}

	if keyx.Type() != "string" {
		return nil, fmt.Errorf("expected key to be a string")
	}

	key := string(keyx.(starlark.String))

	val, err := util.Unmarshal(valx)
	if err != nil {
		return nil, err
	}

	if d.ds.Meta == nil {
		d.ds.Meta = &dataset.Meta{}
	}

	return starlark.None, d.ds.Meta.Set(key, val)
}

// SetSchema sets the dataset schema field
func (d *Dataset) SetSchema(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var valx starlark.Value
	if err := starlark.UnpackPositionalArgs("set_schema", args, kwargs, 1, &valx); err != nil {
		return nil, err
	}

	rs := &jsonschema.RootSchema{}
	if err := json.Unmarshal([]byte(valx.String()), rs); err != nil {
		return starlark.None, err
	}

	if d.ds.Structure == nil {
		d.ds.Structure = &dataset.Structure{
			Format: dataset.JSONDataFormat,
		}
	}
	d.ds.Structure.Schema = rs
	return starlark.None, nil
}

// GetBody returns the body of the dataset we're transforming
func (d *Dataset) GetBody(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if d.body != nil {
		return d.body, nil
	}

	if d.infile == nil {
		return starlark.None, fmt.Errorf("no DataFile")
	}
	if d.ds.Structure == nil {
		return starlark.None, fmt.Errorf("error: no structure for previous dataset")
	}

	// TODO - this is bad. make not bad.
	data, err := ioutil.ReadAll(d.infile)
	if err != nil {
		return starlark.None, err
	}
	d.infile = cafs.NewMemfileBytes("data.json", data)

	rr, err := dsio.NewEntryReader(d.ds.Structure, cafs.NewMemfileBytes("data.json", data))
	if err != nil {
		return starlark.None, fmt.Errorf("error allocating data reader: %s", err)
	}
	w, err := NewStarlarkEntryWriter(d.ds.Structure)
	if err != nil {
		return starlark.None, fmt.Errorf("error allocating starlark entry writer: %s", err)
	}

	err = dsio.Copy(rr, w)
	if err != nil {
		return starlark.None, err
	}
	if err = w.Close(); err != nil {
		return starlark.None, err
	}

	if iter, ok := w.Value().(starlark.Iterable); ok {
		d.body = iter
		return d.body, nil
	}
	return starlark.None, fmt.Errorf("value is not iterable")
}

// SetBody assigns the dataset body
func (d *Dataset) SetBody(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		data starlark.Value
		raw  starlark.Bool
	)

	if err := starlark.UnpackArgs("set_body", args, kwargs, "data", &data, "raw?", &raw); err != nil {
		return starlark.None, err
	}

	if raw {
		if str, ok := data.(starlark.String); ok {
			d.infile = cafs.NewMemfileBytes("data", []byte(string(str)))
			return starlark.None, nil
		}

		return starlark.None, fmt.Errorf("expected raw data for body to be a string")
	}

	iter, ok := data.(starlark.Iterable)
	if !ok {
		return starlark.None, fmt.Errorf("expected body to be iterable")
	}

	sch := dataset.BaseSchemaArray
	if data.Type() == "dict" {
		sch = dataset.BaseSchemaObject
	}

	st := &dataset.Structure{
		Format: dataset.JSONDataFormat,
		Schema: sch,
	}

	if d.ds.Structure == nil {
		d.ds.Structure = st
	}
	w, err := dsio.NewEntryBuffer(st)
	if err != nil {
		return starlark.None, err
	}

	r := NewEntryReader(st, iter)
	if err := dsio.Copy(r, w); err != nil {
		return starlark.None, err
	}
	if err := w.Close(); err != nil {
		return starlark.None, err
	}
	d.infile = cafs.NewMemfileBytes("data.json", w.Bytes())

	return starlark.None, nil
}

// Package ds exposes the qri dataset document model into skylark
package ds

import (
	"encoding/json"
	"fmt"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
	"github.com/qri-io/starlib/util"
)

// Dataset is a qri dataset skylark type
type Dataset struct {
	ds     *dataset.Dataset
	infile cafs.File
	body   skylark.Iterable
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

// Methods exposes dataset methods as skylark values
func (d *Dataset) Methods() *skylarkstruct.Struct {
	return skylarkstruct.FromStringDict(skylarkstruct.Default, skylark.StringDict{
		"set_meta":   skylark.NewBuiltin("set_meta", d.SetMeta),
		"set_schema": skylark.NewBuiltin("set_schema", d.SetSchema),
		"set_body":   skylark.NewBuiltin("set_body", d.SetBody),
	})
}

// SetMeta sets a dataset meta field
func (d *Dataset) SetMeta(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var keyx, valx skylark.Value
	if err := skylark.UnpackPositionalArgs("set_meta", args, kwargs, 2, &keyx, &valx); err != nil {
		return nil, err
	}

	if keyx.Type() != "string" {
		return nil, fmt.Errorf("expected key to be a string")
	}

	key, err := util.AsString(keyx)
	if err != nil {
		return nil, fmt.Errorf("parsing string key: %s", err.Error())
	}

	val, err := util.Unmarshal(valx)
	if err != nil {
		return nil, err
	}

	return skylark.None, d.ds.Meta.Set(key, val)
}

// SetSchema sets the dataset schema field
func (d *Dataset) SetSchema(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var valx skylark.Value
	if err := skylark.UnpackPositionalArgs("set_schema", args, kwargs, 1, &valx); err != nil {
		return nil, err
	}

	rs := &jsonschema.RootSchema{}
	if err := json.Unmarshal([]byte(valx.String()), rs); err != nil {
		return skylark.None, err
	}

	if d.ds.Structure == nil {
		d.ds.Structure = &dataset.Structure{}
	}
	d.ds.Structure.Schema = rs
	return skylark.None, nil
}

// GetBody returns the body of the dataset we're transforming
func (d *Dataset) GetBody(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if d.body != nil {
		return d.body, nil
	}

	if d.infile == nil {
		return skylark.None, fmt.Errorf("qri.get_body failed: no DataFile")
	}
	if d.ds == nil || d.ds.Structure == nil {
		return skylark.None, fmt.Errorf("error: no structure for previous dataset")
	}
	rr, err := dsio.NewEntryReader(d.ds.Structure, d.infile)
	if err != nil {
		return skylark.None, fmt.Errorf("error allocating data reader: %s", err)
	}
	w, err := NewSkylarkEntryWriter(d.ds.Structure)
	if err != nil {
		return skylark.None, fmt.Errorf("error allocating skylark entry writer: %s", err)
	}

	err = dsio.Copy(rr, w)
	if err != nil {
		return skylark.None, err
	}
	if iter, ok := w.Value().(skylark.Iterable); ok {
		d.body = iter
		return d.body, nil
	}
	return skylark.None, fmt.Errorf("value is not iterable")
}

// SetBody assigns the dataset body
func (d *Dataset) SetBody(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var data skylark.Iterable
	if err := skylark.UnpackPositionalArgs("set_body", args, kwargs, 1, &data); err != nil {
		return skylark.None, err
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
		return nil, err
	}

	r := NewEntryReader(st, data)
	dsio.Copy(r, w)
	if err := w.Close(); err != nil {
		return skylark.None, err
	}
	d.infile = cafs.NewMemfileBytes("data.json", w.Bytes())

	return skylark.None, nil
}

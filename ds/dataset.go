// Package ds exposes the qri dataset document model into starlark
package ds

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/fs"
	"github.com/qri-io/starlib/util"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// MutateFieldCheck is a function to check if a dataset field can be mutated
// before mutating a field, dataset will call MutateFieldCheck with as specific
// a path as possible and bail if an error is returned
type MutateFieldCheck func(path ...string) error

// Dataset is a qri dataset starlark type
type Dataset struct {
	ds    *dataset.Dataset
	body  starlark.Iterable
	check MutateFieldCheck
}

// NewDataset creates a dataset object
func NewDataset(ds *dataset.Dataset, check MutateFieldCheck) *Dataset {
	return &Dataset{ds: ds, check: check}
}

// Dataset returns the underlying dataset
func (d *Dataset) Dataset() *dataset.Dataset {
	return d.ds
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

// checkField runs the check function if one is defined
func (d *Dataset) checkField(path ...string) error {
	if d.check != nil {
		return d.check(path...)
	}
	return nil
}

// SetMeta sets a dataset meta field
func (d *Dataset) SetMeta(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		keyx starlark.String
		valx starlark.Value
	)
	if err := starlark.UnpackPositionalArgs("set_meta", args, kwargs, 2, &keyx, &valx); err != nil {
		return nil, err
	}

	key := keyx.String()

	if err := d.checkField("meta", "key"); err != nil {
		return starlark.None, err
	}

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

	if err := d.checkField("structure", "schema"); err != nil {
		return starlark.None, err
	}

	rs := map[string]interface{}{}
	if err := json.Unmarshal([]byte(valx.String()), &rs); err != nil {
		return starlark.None, err
	}

	if d.ds.Structure == nil {
		d.ds.Structure = &dataset.Structure{
			Format: "json",
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

	if d.ds.BodyFile() == nil {
		return starlark.None, fmt.Errorf("this dataset has no body")
	}
	if d.ds.Structure == nil {
		return starlark.None, fmt.Errorf("error: no structure for previous dataset")
	}

	// TODO - this is bad. make not bad.
	data, err := ioutil.ReadAll(d.ds.BodyFile())
	if err != nil {
		return starlark.None, err
	}
	d.ds.SetBodyFile(fs.NewMemfileBytes("data.json", data))

	rr, err := dsio.NewEntryReader(d.ds.Structure, fs.NewMemfileBytes("data.json", data))
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

	if err := d.checkField("body"); err != nil {
		return starlark.None, err
	}

	if raw {
		if str, ok := data.(starlark.String); ok {
			d.ds.SetBodyFile(fs.NewMemfileBytes("data", []byte(string(str))))
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
		Format: "json",
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

	d.ds.SetBodyFile(fs.NewMemfileBytes("data.json", w.Bytes()))

	return starlark.None, nil
}

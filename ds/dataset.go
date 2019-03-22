// Package ds exposes the qri dataset document model into starlark
package ds

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qfs"
	"github.com/qri-io/starlib/util"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// ModuleName defines the expected name for this Module when used
// in starlark's load() function, eg: load('dataset.star', 'dataset')
const ModuleName = "dataset.star"

var (
	once          sync.Once
	datasetModule starlark.StringDict
)

// LoadModule loads the base64 module.
// It is concurrency-safe and idempotent.
func LoadModule() (starlark.StringDict, error) {
	once.Do(func() {
		datasetModule = starlark.StringDict{
			"dataset": starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
				"new": starlark.NewBuiltin("new", New),
			}),
		}
	})
	return datasetModule, nil
}

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

// NewDataset creates a dataset object, intended to be called from go-land to prepare datasets
// for handing to other functions
func NewDataset(ds *dataset.Dataset, check MutateFieldCheck) *Dataset {
	return &Dataset{ds: ds, check: check}
}

// New creates a new dataset from starlark land
func New(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	d := &Dataset{ds: &dataset.Dataset{}}
	return d.Methods(), nil
}

// Dataset returns the underlying dataset
func (d *Dataset) Dataset() *dataset.Dataset {
	return d.ds
}

// Methods exposes dataset methods as starlark values
func (d *Dataset) Methods() *starlarkstruct.Struct {
	return starlarkstruct.FromStringDict(starlarkstruct.Default, starlark.StringDict{
		"set_meta":      starlark.NewBuiltin("set_meta", d.SetMeta),
		"get_structure": starlark.NewBuiltin("get_structure", d.GetStructure),
		"set_structure": starlark.NewBuiltin("set_structure", d.SetStructure),
		"get_body":      starlark.NewBuiltin("get_body", d.GetBody),
		"set_body":      starlark.NewBuiltin("set_body", d.SetBody),
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

	key := keyx.GoString()

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

// GetStructure gets a dataset structure component
func (d *Dataset) GetStructure(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if d.ds.Structure == nil {
		return starlark.None, nil
	}

	data, err := json.Marshal(d.ds.Structure)
	if err != nil {
		return starlark.None, err
	}

	jsonData := map[string]interface{}{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return starlark.None, err
	}

	return util.Marshal(jsonData)
}

// SetStructure sets the dataset structure component
func (d *Dataset) SetStructure(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var valx starlark.Value
	if err := starlark.UnpackPositionalArgs("set_structure", args, kwargs, 1, &valx); err != nil {
		return nil, err
	}

	if err := d.checkField("structure"); err != nil {
		return starlark.None, err
	}

	d.ds.Structure = &dataset.Structure{}

	val, err := util.Unmarshal(valx)
	if err != nil {
		return starlark.None, err
	}

	data, err := json.Marshal(val)
	if err != nil {
		return starlark.None, err
	}

	err = json.Unmarshal(data, d.ds.Structure)
	return starlark.None, err
}

// GetBody returns the body of the dataset we're transforming
func (d *Dataset) GetBody(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if d.body != nil {
		return d.body, nil
	}

	var valx starlark.Value
	if err := starlark.UnpackArgs("get_body", args, kwargs, "default?", &valx); err != nil {
		return starlark.None, err
	}

	if d.ds.BodyFile() == nil {
		return valx, nil
	}
	if d.ds.Structure == nil {
		return starlark.None, fmt.Errorf("error: no structure for previous dataset")
	}

	// TODO - this is bad. make not bad.
	data, err := ioutil.ReadAll(d.ds.BodyFile())
	if err != nil {
		return starlark.None, err
	}
	d.ds.SetBodyFile(qfs.NewMemfileBytes("data.json", data))

	rr, err := dsio.NewEntryReader(d.ds.Structure, qfs.NewMemfileBytes("data.json", data))
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
		data       starlark.Value
		raw        starlark.Bool
		dataFormat starlark.String
	)

	if err := starlark.UnpackArgs("set_body", args, kwargs, "data", &data, "raw?", &raw, "data_format", &dataFormat); err != nil {
		return starlark.None, err
	}

	if err := d.checkField("body"); err != nil {
		return starlark.None, err
	}

	df := dataFormat.GoString()
	if df == "" {
		// default to json
		df = "json"
	}

	if _, err := dataset.ParseDataFormatString(df); err != nil {
		return starlark.None, fmt.Errorf("invalid data_format: '%s'", df)
	}

	if raw {
		if str, ok := data.(starlark.String); ok {
			d.ds.SetBodyFile(qfs.NewMemfileBytes(fmt.Sprintf("data.%s", df), []byte(string(str))))
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
		Format: df,
		Schema: sch,
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

	d.ds.SetBodyFile(qfs.NewMemfileBytes(fmt.Sprintf("data.%s", df), w.Bytes()))

	return starlark.None, nil
}

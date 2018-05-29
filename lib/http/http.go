package http

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
	"github.com/qri-io/dataset"
	"github.com/qri-io/skytf/lib"
)

// ModuleName defines the expected name for this module when used
// in skylark's load() function, eg: load('http.sky', 'http')
const ModuleName = "http.sky"

// NewModule creates an http module
func NewModule(ds *dataset.Dataset) (skylark.StringDict, error) {
	m := &module{ds: ds, cli: http.DefaultClient}
	st := skylarkstruct.FromStringDict(skylarkstruct.Default, skylark.StringDict{
		"get":      skylark.NewBuiltin("get", m.Get),
		"get_json": skylark.NewBuiltin("get_json", m.GetJSON),
	})

	return skylark.StringDict{"http": st}, nil
}

// module joins http tools to a dataset, allowing dataset
// to follow along with http requests
type module struct {
	cli *http.Client
	ds  *dataset.Dataset
}

// Get a URL
func (m *module) Get(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var url skylark.Value
	if err := skylark.UnpackPositionalArgs("get", args, kwargs, 1, &url); err != nil {
		return nil, err
	}

	urlstr, err := lib.AsString(url)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", urlstr, nil)
	if err != nil {
		return nil, err
	}

	res, err := m.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return skylark.String(string(data)), nil
}

// GetJSON fetches a url & parses it as json, passing back a skylark value
func (m *module) GetJSON(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var url skylark.Value
	if err := skylark.UnpackPositionalArgs("get_json", args, kwargs, 1, &url); err != nil {
		return nil, err
	}

	urlstr, err := lib.AsString(url)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", urlstr, nil)
	if err != nil {
		return nil, err
	}

	res, err := m.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var data interface{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, err
	}

	if m.ds.Meta == nil {
		m.ds.Meta = &dataset.Meta{}
	}
	cite := &dataset.Citation{
		URL: urlstr,
	}
	m.ds.Meta.Citations = append(m.ds.Meta.Citations, cite)

	return lib.Marshal(data)
}

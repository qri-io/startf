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

// ModuleName defines the expected name for this Module when used
// in skylark's load() function, eg: load('http.sky', 'http')
const ModuleName = "http.sky"

// NewModule creates an http Module
func NewModule(ds *dataset.Dataset) *Module {
	return &Module{ds: ds, cli: http.DefaultClient}
}

// Module joins http tools to a dataset, allowing dataset
// to follow along with http requests
type Module struct {
	cli *http.Client
	ds  *dataset.Dataset
}

// Struct returns this module's methods as a skylark Struct
func (m *Module) Struct() *skylarkstruct.Struct {
	return skylarkstruct.FromStringDict(skylarkstruct.Default, m.StringDict())
}

// StringDict returns all module methdos in a skylark.StringDict
func (m *Module) StringDict() skylark.StringDict {
	return skylark.StringDict{
		"get":      skylark.NewBuiltin("get", m.Get),
		"get_json": skylark.NewBuiltin("get_json", m.GetJSON),
	}
}

// Get a URL
func (m *Module) Get(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
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
func (m *Module) GetJSON(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
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

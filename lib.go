package skytf

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/skylark"
	"github.com/qri-io/dataset"
)

// commit gets called once to set the data to be created
type commit struct {
	called bool
	data   skylark.Iterable
}

// Do executes a commit. must be called exactly once per transformation
func (c *commit) Do(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if c.called {
		return skylark.False, fmt.Errorf("commit can only be called once per transformation")
	}

	if err := skylark.UnpackPositionalArgs("commit", args, kwargs, 1, &c.data); err != nil {
		return nil, err
	}

	if !(c.data.Type() == "dict" || c.data.Type() == "list") {
		return nil, fmt.Errorf("invalid type: %s, commit must be called with either a list or a dict", c.data.Type())
	}

	c.called = true
	return skylark.True, nil
}

// config fetches configuration details from a qri transformation
type config struct {
	ds *dataset.Dataset
}

func newConfig(ds *dataset.Dataset) *config {
	return &config{
		ds: ds,
	}
}

// GetConfig returns transformation configuration details
func (c *config) GetConfig(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if c.ds.Transform.Config == nil {
		return skylark.None, nil
	}

	return parseJSONSkylark(c.ds.Transform.Config)
}

// httpRequests joins http tools to a dataset, allowing dataset
// to follow along with http requests
type httpRequests struct {
	cli *http.Client
	ds  *dataset.Dataset
}

func newHTTPRequests(ds *dataset.Dataset) *httpRequests {
	return &httpRequests{
		cli: http.DefaultClient,
		ds:  ds,
	}
}

// FetchJSONUrl fetches a url & parses it as json, passing back a skylark value
func (hr *httpRequests) FetchJSONUrl(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var url skylark.Value
	if err := skylark.UnpackPositionalArgs("fetch_json_url", args, kwargs, 1, &url); err != nil {
		return nil, err
	}

	urlstr, err := asString(url)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("GET", urlstr, nil)
	if err != nil {
		return nil, err
	}

	res, err := hr.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var data interface{}
	if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
		return nil, err
	}

	if hr.ds.Meta == nil {
		hr.ds.Meta = &dataset.Meta{}
	}
	cite := &dataset.Citation{
		URL: urlstr,
	}
	hr.ds.Meta.Citations = append(hr.ds.Meta.Citations, cite)

	return parseJSONSkylark(data)
}

func asString(x skylark.Value) (string, error) {
	return strconv.Unquote(x.String())
}

func parseJSONSkylark(data interface{}) (v skylark.Value, err error) {
	switch x := data.(type) {
	case nil:
		v = skylark.None
	case bool:
		v = skylark.Bool(x)
	case string:
		v = skylark.String(x)
	case float64:
		// TODO - ints?
		v = skylark.Float(x)
	case []interface{}:
		var elems = make([]skylark.Value, len(x))
		for i, val := range x {
			elems[i], err = parseJSONSkylark(val)
			if err != nil {
				return
			}
		}
		v = skylark.NewList(elems)
	case map[string]interface{}:
		dict := &skylark.Dict{}
		var elem skylark.Value
		for key, val := range x {
			elem, err = parseJSONSkylark(val)
			if err != nil {
				return
			}
			if err = dict.Set(skylark.String(key), elem); err != nil {
				return
			}
		}
		v = dict
	}
	return
}

package qri

import (
	"encoding/json"
	"fmt"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/jsonschema"
	"github.com/qri-io/skytf/lib"
)

// ModuleName defines the expected name for this module when used
// in skylark's load() function, eg: load('qri.sky', 'qri')
const ModuleName = "qri.sky"

// NewModule creates a new qri module instance
func NewModule(ds *dataset.Dataset, secrets map[string]interface{}, infile cafs.File) *Module {
	return &Module{ds: ds, secrets: secrets, infile: infile}
}

// Module encapsulates state for a qri skylark module
type Module struct {
	ds      *dataset.Dataset
	secrets map[string]interface{}
	infile  cafs.File
}

// Struct returns this module's methods as a skylark Struct
func (m *Module) Struct() *skylarkstruct.Struct {
	return skylarkstruct.FromStringDict(skylarkstruct.Default, m.AddAllMethods(skylark.StringDict{}))
}

// AddAllMethods augments a skylark.StringDict with all qri builtins. Should really only be used during "transform" step
func (m *Module) AddAllMethods(sd skylark.StringDict) skylark.StringDict {
	sd["set_meta"] = skylark.NewBuiltin("set_meta", m.SetMeta)
	sd["set_schema"] = skylark.NewBuiltin("set_schema", m.SetSchema)
	sd["get_config"] = skylark.NewBuiltin("get_config", m.GetConfig)
	sd["get_secret"] = skylark.NewBuiltin("get_secret", m.GetSecret)
	sd["get_body"] = skylark.NewBuiltin("get_body", m.GetBody)
	return sd
}

// GetConfig returns transformation configuration details
// TODO - supplying a string argument to qri.get_config('foo') should return the single config value instead of the whole map
func (m *Module) GetConfig(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.ds.Transform.Config == nil {
		return skylark.None, nil
	}

	var keyx skylark.Value
	if err := skylark.UnpackPositionalArgs("get_config", args, kwargs, 1, &keyx); err != nil {
		return nil, err
	}

	if keyx.Type() != "string" {
		return nil, fmt.Errorf("expected key to be a string")
	}

	key, err := lib.AsString(keyx)
	if err != nil {
		return nil, fmt.Errorf("parsing string key: %s", err.Error())
	}

	return lib.Marshal(m.ds.Transform.Config[key])
}

// GetBody returns the body of the dataset we're transforming
func (m *Module) GetBody(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.infile == nil {
		return skylark.None, fmt.Errorf("qri.get_body failed: no DataFile")
	}
	rr, err := dsio.NewEntryReader(m.ds.Structure, m.infile)
	if err != nil {
		return skylark.None, fmt.Errorf("error allocating data reader: %s", err)
	}
	w, err := NewSkylarkEntryWriter(m.ds.Structure)
	if err != nil {
		return skylark.None, fmt.Errorf("error allocating skylark entry writer: %s", err)
	}

	err = dsio.Copy(rr, w)
	if err != nil {
		return skylark.None, err
	}
	return w.Value(), nil
}

// SetMeta sets a dataset meta field
func (m *Module) SetMeta(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var keyx, valx skylark.Value
	if err := skylark.UnpackPositionalArgs("set_meta", args, kwargs, 2, &keyx, &valx); err != nil {
		return nil, err
	}

	if keyx.Type() != "string" {
		return nil, fmt.Errorf("expected key to be a string")
	}

	key, err := lib.AsString(keyx)
	if err != nil {
		return nil, fmt.Errorf("parsing string key: %s", err.Error())
	}

	val, err := lib.Unmarshal(valx)
	if err != nil {
		return nil, err
	}

	return skylark.None, m.ds.Meta.Set(key, val)
}

// SetSchema sets the dataset schema field
func (m *Module) SetSchema(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var valx skylark.Value
	if err := skylark.UnpackPositionalArgs("set_schema", args, kwargs, 1, &valx); err != nil {
		return nil, err
	}

	rs := &jsonschema.RootSchema{}
	if err := json.Unmarshal([]byte(valx.String()), rs); err != nil {
		return skylark.None, err
	}

	if m.ds.Structure == nil {
		m.ds.Structure = &dataset.Structure{}
	}
	m.ds.Structure.Schema = rs
	return skylark.None, nil
}

// GetSecret fetches a secret for a given string
func (m *Module) GetSecret(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.secrets == nil {
		return skylark.None, nil
	}

	var keyx skylark.Value
	if err := skylark.UnpackPositionalArgs("get_secret", args, kwargs, 1, &keyx); err != nil {
		return nil, err
	}

	if keyx.Type() != "string" {
		return nil, fmt.Errorf("expected key to be a string")
	}

	key, err := lib.AsString(keyx)
	if err != nil {
		return nil, fmt.Errorf("parsing string key: %s", err.Error())
	}

	return lib.Marshal(m.secrets[key])
}

package qri

import (
	"fmt"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
	"github.com/qri-io/dataset"
	"github.com/qri-io/skytf/lib"
)

// ModuleName defines the expected name for this module when used
// in skylark's load() function, eg: load('qri.sky', 'qri')
const ModuleName = "qri.sky"

// NewModule creates a new qri module instance
func NewModule(ds *dataset.Dataset, secrets map[string]interface{}) *Module {
	return &Module{ds: ds, secrets: secrets}
}

// Module encapsulates state for a qri skylark module
type Module struct {
	ds      *dataset.Dataset
	secrets map[string]interface{}
	data    skylark.Iterable
}

// Load creates a skylark module from a module instance
func (m *Module) Load() (skylark.StringDict, error) {
	st := skylarkstruct.FromStringDict(skylarkstruct.Default, skylark.StringDict{
		"commit":     skylark.NewBuiltin("commit", m.Commit),
		"set_meta":   skylark.NewBuiltin("set_meta", m.SetMeta),
		"get_config": skylark.NewBuiltin("get_config", m.GetConfig),
		"get_secret": skylark.NewBuiltin("get_secret", m.GetSecret),
	})

	return skylark.StringDict{"qri": st}, nil
}

// Commit sets the data that is the result of executing this transform. must be called exactly once per transformation
func (m *Module) Commit(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.data != nil {
		return skylark.False, fmt.Errorf("commit can only be called once per transformation")
	}

	if err := skylark.UnpackPositionalArgs("commit", args, kwargs, 1, &m.data); err != nil {
		return nil, err
	}

	if !(m.data.Type() == "dict" || m.data.Type() == "list") {
		return nil, fmt.Errorf("invalid type: %s, commit must be called with either a list or a dict", m.data.Type())
	}

	return skylark.True, nil
}

// Data gives the commit result of this transform
func (m *Module) Data() (skylark.Iterable, error) {
	if m.data == nil {
		return nil, fmt.Errorf("commit wasn't called in skylark transformation")
	}
	return m.data, nil
}

// GetConfig returns transformation configuration details
// TODO - supplying a string argument to qri.get_config('foo') should return the single config value instead of the whole map
func (m *Module) GetConfig(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.ds.Transform.Config == nil {
		return skylark.None, nil
	}
	return lib.Marshal(m.ds.Transform.Config)
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

// GetSecret fetches a dict of secrets
// TODO - supplying a string argument to qri.get_secret('foo') should return the single secret value instead of the whole map
func (m *Module) GetSecret(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.secrets == nil {
		return skylark.None, nil
	}

	return lib.Marshal(m.secrets)
}

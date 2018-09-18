package qri

import (
	"encoding/json"
	"fmt"

	"github.com/google/skylark"
	"github.com/google/skylark/skylarkstruct"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qri/p2p"
	"github.com/qri-io/qri/repo"
	"github.com/qri-io/starlib/util"
)

// ModuleName defines the expected name for this module when used
// in skylark's load() function, eg: load('qri.sky', 'qri')
const ModuleName = "qri.sky"

// NewModule creates a new qri module instance
func NewModule(node *p2p.QriNode, ds *dataset.Dataset, secrets map[string]interface{}, infile cafs.File) *Module {
	return &Module{node: node, ds: ds, secrets: secrets, infile: infile}
}

// Module encapsulates state for a qri skylark module
type Module struct {
	node    *p2p.QriNode
	ds      *dataset.Dataset
	secrets map[string]interface{}
	infile  cafs.File
}

// Namespace produces this module's exported namespace
func (m *Module) Namespace() skylark.StringDict {
	return skylark.StringDict{
		"qri": m.Struct(),
	}
}

// Struct returns this module's methods as a skylark Struct
func (m *Module) Struct() *skylarkstruct.Struct {
	return skylarkstruct.FromStringDict(skylarkstruct.Default, m.AddAllMethods(skylark.StringDict{}))
}

// AddAllMethods augments a skylark.StringDict with all qri builtins. Should really only be used during "transform" step
func (m *Module) AddAllMethods(sd skylark.StringDict) skylark.StringDict {
	sd["get_config"] = skylark.NewBuiltin("get_config", m.GetConfig)
	sd["get_secret"] = skylark.NewBuiltin("get_secret", m.GetSecret)
	sd["list_datasets"] = skylark.NewBuiltin("list_dataset", m.ListDatasets)
	sd["load_dataset_body"] = skylark.NewBuiltin("load_dataset_body", m.LoadDatasetBody)
	sd["load_dataset_head"] = skylark.NewBuiltin("load_dataset_head", m.LoadDatasetHead)
	return sd
}

// ListDatasets shows current local datasets
func (m *Module) ListDatasets(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	if m.node == nil {
		return skylark.None, fmt.Errorf("no qri node available to list datasets")
	}

	refs, err := m.node.Repo.References(1000, 0)
	if err != nil {
		return skylark.None, fmt.Errorf("error getting dataset list: %s", err.Error())
	}

	l := &skylark.List{}
	for _, ref := range refs {
		l.Append(skylark.String(ref.String()))
	}
	return l, nil
}

// LoadDatasetHead grabs everything except the dataset head
func (m *Module) LoadDatasetHead(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var refstr skylark.String
	if err := skylark.UnpackArgs("load_dataset_head", args, kwargs, "ref", &refstr); err != nil {
		return skylark.None, err
	}

	ds, err := m.loadDsHead(string(refstr))
	if err != nil {
		return skylark.None, err
	}

	data, err := json.Marshal(ds.Encode())
	if err != nil {
		return skylark.None, err
	}
	dse := map[string]interface{}{}
	if err := json.Unmarshal(data, &dse); err != nil {
		return skylark.None, err
	}

	return util.Marshal(dse)
}

// LoadDatasetBody loads a datasets body data
func (m *Module) LoadDatasetBody(thread *skylark.Thread, _ *skylark.Builtin, args skylark.Tuple, kwargs []skylark.Tuple) (skylark.Value, error) {
	var refstr skylark.String
	if err := skylark.UnpackArgs("load_dataset_body", args, kwargs, "ref", &refstr); err != nil {
		return skylark.None, err
	}

	if m.node == nil {
		return skylark.None, fmt.Errorf("no qri node available to load dataset: %s", string(refstr))
	}

	ds, err := m.loadDsHead(string(refstr))
	if err != nil {
		return skylark.None, err
	}

	f, err := m.node.Repo.Store().Get(datastore.NewKey(ds.BodyPath))
	if err != nil {
		return skylark.None, err
	}

	rr, err := dsio.NewEntryReader(ds.Structure, f)
	if err != nil {
		return skylark.None, fmt.Errorf("error allocating data reader: %s", err)
	}
	w, err := NewSkylarkEntryWriter(ds.Structure)
	if err != nil {
		return skylark.None, fmt.Errorf("error allocating skylark entry writer: %s", err)
	}

	err = dsio.Copy(rr, w)
	if err != nil {
		return skylark.None, err
	}

	return w.Value(), err
}

func (m *Module) loadDsHead(refstr string) (*dataset.Dataset, error) {
	if m.node == nil {
		return nil, fmt.Errorf("no qri node available to load dataset: %s", refstr)
	}

	ref, err := repo.ParseDatasetRef(refstr)
	if err != nil {
		return nil, err
	}
	if err := repo.CanonicalizeDatasetRef(m.node.Repo, &ref); err != nil {
		return nil, err
	}
	m.node.LocalStreams.Out.Write([]byte(fmt.Sprintf("loading dataset: %s", ref.String())))

	ds, err := dsfs.LoadDataset(m.node.Repo.Store(), datastore.NewKey(ref.Path))
	if err != nil {
		return nil, err
	}

	if m.ds.Transform.Resources == nil {
		m.ds.Transform.Resources = map[string]*dataset.TransformResource{}
	}
	m.ds.Transform.Resources[ref.Path] = &dataset.TransformResource{Path: ref.String()}

	return ds, nil
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

	key, err := util.AsString(keyx)
	if err != nil {
		return nil, fmt.Errorf("parsing string key: %s", err.Error())
	}

	return util.Marshal(m.secrets[key])
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

	key, err := util.AsString(keyx)
	if err != nil {
		return nil, fmt.Errorf("parsing string key: %s", err.Error())
	}

	return util.Marshal(m.ds.Transform.Config[key])
}

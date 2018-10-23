package qri

import (
	"encoding/json"
	"fmt"

	starlark "github.com/google/skylark"
	starlarkstruct "github.com/google/skylark/skylarkstruct"
	"github.com/ipfs/go-datastore"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsfs"
	"github.com/qri-io/dataset/dsio"
	"github.com/qri-io/qri/p2p"
	"github.com/qri-io/qri/repo"
	"github.com/qri-io/starlib/util"
)

// ModuleName defines the expected name for this module when used
// in starlark's load() function, eg: load('qri.star', 'qri')
const ModuleName = "qri.star"

// NewModule creates a new qri module instance
func NewModule(node *p2p.QriNode, ds *dataset.Dataset) *Module {
	return &Module{node: node, ds: ds}
}

// Module encapsulates state for a qri starlark module
type Module struct {
	node *p2p.QriNode
	ds   *dataset.Dataset
}

// Namespace produces this module's exported namespace
func (m *Module) Namespace() starlark.StringDict {
	return starlark.StringDict{
		"qri": m.Struct(),
	}
}

// Struct returns this module's methods as a starlark Struct
func (m *Module) Struct() *starlarkstruct.Struct {
	return starlarkstruct.FromStringDict(starlarkstruct.Default, m.AddAllMethods(starlark.StringDict{}))
}

// AddAllMethods augments a starlark.StringDict with all qri builtins. Should really only be used during "transform" step
func (m *Module) AddAllMethods(sd starlark.StringDict) starlark.StringDict {
	sd["list_datasets"] = starlark.NewBuiltin("list_dataset", m.ListDatasets)
	sd["load_dataset_body"] = starlark.NewBuiltin("load_dataset_body", m.LoadDatasetBody)
	sd["load_dataset_head"] = starlark.NewBuiltin("load_dataset_head", m.LoadDatasetHead)
	return sd
}

// ListDatasets shows current local datasets
func (m *Module) ListDatasets(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	if m.node == nil {
		return starlark.None, fmt.Errorf("no qri node available to list datasets")
	}

	refs, err := m.node.Repo.References(1000, 0)
	if err != nil {
		return starlark.None, fmt.Errorf("error getting dataset list: %s", err.Error())
	}

	l := &starlark.List{}
	for _, ref := range refs {
		l.Append(starlark.String(ref.String()))
	}
	return l, nil
}

// LoadDatasetHead grabs everything except the dataset head
func (m *Module) LoadDatasetHead(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var refstr starlark.String
	if err := starlark.UnpackArgs("load_dataset_head", args, kwargs, "ref", &refstr); err != nil {
		return starlark.None, err
	}

	ds, err := m.loadDsHead(string(refstr))
	if err != nil {
		return starlark.None, err
	}

	data, err := json.Marshal(ds.Encode())
	if err != nil {
		return starlark.None, err
	}
	dse := map[string]interface{}{}
	if err := json.Unmarshal(data, &dse); err != nil {
		return starlark.None, err
	}

	return util.Marshal(dse)
}

// LoadDatasetBody loads a datasets body data
func (m *Module) LoadDatasetBody(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var refstr starlark.String
	if err := starlark.UnpackArgs("load_dataset_body", args, kwargs, "ref", &refstr); err != nil {
		return starlark.None, err
	}

	if m.node == nil {
		return starlark.None, fmt.Errorf("no qri node available to load dataset: %s", string(refstr))
	}

	ds, err := m.loadDsHead(string(refstr))
	if err != nil {
		return starlark.None, err
	}

	f, err := m.node.Repo.Store().Get(datastore.NewKey(ds.BodyPath))
	if err != nil {
		return starlark.None, err
	}

	rr, err := dsio.NewEntryReader(ds.Structure, f)
	if err != nil {
		return starlark.None, fmt.Errorf("error allocating data reader: %s", err)
	}
	w, err := NewSkylarkEntryWriter(ds.Structure)
	if err != nil {
		return starlark.None, fmt.Errorf("error allocating starlark entry writer: %s", err)
	}

	err = dsio.Copy(rr, w)
	if err != nil {
		return starlark.None, err
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

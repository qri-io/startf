package startf

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	starlark "github.com/google/skylark"
	"github.com/qri-io/cafs"
	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

func scriptFile(t *testing.T, path string) *cafs.Memfile {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return cafs.NewMemfileBytes(path, data)
}

func TestExecScript(t *testing.T) {
	ds := &dataset.Dataset{}
	script := scriptFile(t, "testdata/tf.star")

	body, err := ExecScript(ds, script, nil)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if ds.Transform == nil {
		t.Error("expected transform")
	}

	entryReader, err := dsio.NewEntryReader(ds.Structure, body)
	if err != nil {
		t.Errorf("couldn't create entry reader from returned dataset & body file: %s", err.Error())
		return
	}

	i := 0
	dsio.EachEntry(entryReader, func(_ int, x dsio.Entry, e error) error {
		if e != nil {
			t.Errorf("entry %d iteration error: %s", i, e.Error())
		}
		i++
		return nil
	})

	if i != 8 {
		t.Errorf("expected 8 entries, got: %d", i)
	}
}

func TestExecScript2(t *testing.T) {

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"foo":["bar","baz","bat"]}`))
	}))

	ds := &dataset.Dataset{}
	script := scriptFile(t, "testdata/fetch.star")
	_, err := ExecScript(ds, script, nil, func(o *ExecOpts) {
		o.Globals["test_server_url"] = starlark.String(s.URL)
	})

	if err != nil {
		t.Error(err.Error())
		return
	}
	if ds.Transform == nil {
		t.Error("expected transform")
	}
}

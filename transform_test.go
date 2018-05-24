package skytf

import (
	"testing"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/dsio"
)

func TestExecFile(t *testing.T) {
	ds := &dataset.Dataset{}
	er, err := ExecFile(ds, "testdata/tf.sky")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if ds.Transform == nil {
		t.Error("expected transform")
	}

	i := 0
	dsio.EachEntry(er, func(_ int, x dsio.Entry, e error) error {
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

func TestExecFile2(t *testing.T) {
	ds := &dataset.Dataset{}
	_, err := ExecFile(ds, "testdata/fetch.sky")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if ds.Transform == nil {
		t.Error("expected transform")
	}
}

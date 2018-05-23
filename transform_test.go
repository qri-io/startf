package sltf

import (
	"testing"

	"github.com/qri-io/dataset/dsio"
)

func TestExecFile(t *testing.T) {
	ds, er, err := ExecFile("testdata/tf.sky")
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

	if i != 3 {
		t.Errorf("expected 3 entries, got: %d", i)
	}
}

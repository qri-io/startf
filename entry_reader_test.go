package skytf

import (
	"testing"

	"github.com/qri-io/dataset/dsio"
)

// assert *EntryReader conforms to dsio.EntryReader interface
var _ dsio.EntryReader = (*EntryReader)(nil)

func TestEntryReader(t *testing.T) {
	// TODO
}

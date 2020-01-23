package arenaskl

import (
	"bytes"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInvalidInternalKeyDecoding(t *testing.T) {
	a := NewArena(arenaSize, 0)

	// mock key that is invalid since it doesn't have an 8 byte trailer
	nd, err := newRawNode(a, 1, 1, 1)
	require.Nil(t, err)

	l := NewSkiplist(a, bytes.Compare)
	it := Iterator{
		list: l,
		nd:   nd,
	}
	it.decodeKey()
	require.Nil(t, it.key.UserKey)
	require.Equal(t, uint64(base.InternalKeyKindInvalid), it.key.Trailer)
}

// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/blockiter"
	"github.com/stretchr/testify/require"
)

// TestIndexIterIgnoresSuffixMask verifies that an IndexIter does not apply
// SuffixMasks to its separator keys. The mask is intended to filter point
// keys at the data-block level; if it were applied to index separator keys,
// the iterator would skip the entire data block whose separator's suffix
// falls in the masked range — including data-block keys whose own suffixes
// are outside the mask range.
func TestIndexIterIgnoresSuffixMask(t *testing.T) {
	// Build an index block whose separator keys include suffixes inside and
	// outside the mask range. Under testkeys.Compare, "@N" sorts newest-first
	// (smaller N = larger key), so the separators below are listed in
	// ascending sort order.
	separators := []string{
		"kprjuqa@37",
		"kprjuqa@30",
		"kprjuqa@23",
		"kprjuqa@10", // suffix falls inside the mask [@14,@8).
	}

	w := &Writer{RestartInterval: 1}
	for i, sep := range separators {
		// The value is an encoded block handle; the contents don't matter for
		// this test, but the handle must be decodable.
		bh := block.HandleWithProperties{
			Handle: block.Handle{Offset: uint64(i * 100), Length: 50},
		}
		buf := make([]byte, 32)
		val := buf[:bh.Handle.EncodeVarints(buf)]
		require.NoError(t, w.Add(base.MakeInternalKey([]byte(sep), 1, base.InternalKeyKindSeparator), val))
	}
	blockData := w.Finish()

	mask := blockiter.SuffixMask{Lower: []byte("@14"), Upper: []byte("@8")}
	transforms := blockiter.Transforms{
		SuffixMasks: []blockiter.SuffixMask{mask},
	}

	var iter IndexIter
	require.NoError(t, iter.Init(testkeys.Comparer, blockData, transforms))
	defer func() { _ = iter.Close() }()

	// Iterate forward; every separator must appear, even kprjuqa@10 whose
	// suffix is inside the mask range.
	var got []string
	for ok := iter.First(); ok; ok = iter.Next() {
		got = append(got, string(iter.Separator()))
	}
	require.Equal(t, separators, got)

	// Iterate backward; same expectation in reverse.
	got = got[:0]
	for ok := iter.Last(); ok; ok = iter.Prev() {
		got = append(got, string(iter.Separator()))
	}
	expectedRev := make([]string, len(separators))
	for i, s := range separators {
		expectedRev[len(separators)-1-i] = s
	}
	require.Equal(t, expectedRev, got)
}

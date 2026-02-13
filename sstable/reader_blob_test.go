// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/blobtest"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/stretchr/testify/require"
)

// testBlobReferences implements BlobReferences.
type testBlobReferences struct {
	fileIDs []base.BlobFileID
}

func (t *testBlobReferences) BlobFileIDByID(i base.BlobReferenceID) base.BlobFileID {
	return t.fileIDs[i]
}
func (t *testBlobReferences) IDByBlobFileID(fileID base.BlobFileID) (base.BlobReferenceID, bool) {
	for i, id := range t.fileIDs {
		if id == fileID {
			return base.BlobReferenceID(i), true
		}
	}
	return 0, false
}

// TestDualTierBlobHandlePrimaryAndSecondaryFetchMatch builds an sstable with
// dual-tier blob values, then retrieves each value using both the primary
// (hot) and secondary (cold) blob handles and validates that both fetches
// return the same value.
func TestDualTierBlobHandlePrimaryAndSecondaryFetchMatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	input := `e@100#1,SET:hot-blob{fileNum=1 valueID=0 valueLen=3 value=foo}cold-blob{fileNum=2 valueID=0 valueLen=3 value=foo}attr=10`

	bv := &blobtest.Values{}
	_, err := ParseTestKVsAndSpans(input, bv)
	require.NoError(t, err)

	keySchema := colblk.DefaultKeySchema(testkeys.Comparer, 16)
	tiers := []base.StorageTier{base.HotTier, base.ColdTier}
	opts := WriterOptions{
		Comparer:                  testkeys.Comparer,
		KeySchema:                 &keySchema,
		TableFormat:               TableFormatMax,
		TieringSpanIDGetter:       func(key []byte) base.TieringSpanID { return 0 },
		TieringAttributeExtractor: func([]byte, int, []byte) (base.TieringAttribute, error) { return 0, nil },
		WriteTieringHistograms:    true,
		DisableValueBlocks:        true,
	}
	opts.SetInternal(sstableinternal.WriterOptions{BlobReferenceTiers: tiers})

	obj := &objstorage.MemObj{}
	w := NewRawWriter(obj, opts)
	require.NoError(t, ParseTestSST(w, input, bv))
	require.NoError(t, w.Close())

	blobRefs := &testBlobReferences{fileIDs: []base.BlobFileID{1, 2}}

	readerOpts := ReaderOptions{
		Comparer:   opts.Comparer,
		KeySchemas: KeySchemas{keySchema.Name: &keySchema},
	}
	r, err := NewMemReader(obj.Data(), readerOpts)
	require.NoError(t, err)
	defer r.Close()

	blobCtx := TableBlobContext{ValueFetcher: bv, References: blobRefs}
	iter, err := r.NewPointIter(context.Background(), IterOptions{
		Transforms:  NoTransforms,
		BlobContext: blobCtx,
	})
	require.NoError(t, err)
	defer iter.Close()

	metaIter, ok := iter.(base.InternalIteratorWithKVMeta)
	require.True(t, ok, "sstable iterator must implement InternalIteratorWithKVMeta")

	ctx := context.Background()
	for kv, meta := metaIter.FirstWithMeta(); kv != nil; kv, meta = metaIter.NextWithMeta() {
		primaryVal, _, err := kv.Value(nil /* buf */)
		require.NoError(t, err)

		if len(meta.SecondaryBlobHandle) == 0 {
			continue
		}

		secondaryVal, _, err := blobCtx.FetchValueFromSecondaryHandle(ctx, meta.SecondaryBlobHandle, nil)
		require.NoError(t, err)
		require.Equal(t, primaryVal, secondaryVal, "key %s: value from primary handle != value from secondary handle", kv.K.UserKey)
	}
}

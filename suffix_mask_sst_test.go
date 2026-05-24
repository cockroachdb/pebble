// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Suffix-mask tests: SST-direct iteration. These tests build SSTs with
// sstable.Writer and exercise suffix masking through the sstable.Reader
// iterator API (no DB involved), covering point keys, range keys, forward
// and backward iteration, the columnar vs row-block parity, the fallback
// (non-SuffixMaskChecker) path, and the no-keys-masked optimization.

package pebble

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
)

// TestSuffixMaskRangeKeys exercises suffix masking on range key entries in
// both columnar (pebblev5) and row-oriented (pebblev4) table formats.
func TestSuffixMaskRangeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, format := range []struct {
		name   string
		format sstable.TableFormat
	}{
		{"columnar", sstable.TableFormatPebblev5},
		{"rowblk", sstable.TableFormatPebblev4},
	} {
		t.Run(format.name, func(t *testing.T) {
			testSuffixMaskRangeKeys(t, format.format)
		})
	}
}

func testSuffixMaskRangeKeys(t *testing.T, tableFormat sstable.TableFormat) {
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()

	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := sstable.WriterOptions{
		Comparer:    comparer,
		TableFormat: tableFormat,
	}
	if tableFormat.BlockColumnar() {
		writerOpts.KeySchema = &cockroachkvs.KeySchema
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)

	start := testMakeEngineKey([]byte("a"), 0, 0)
	end := testMakeEngineKey([]byte("z"), 0, 0)
	for _, wall := range []uint64{200, 150, 100, 80, 50} {
		require.NoError(t, w.RangeKeySet(start, end, testMakeSuffix(wall, 0), []byte(fmt.Sprintf("val@%d", wall))))
	}
	require.NoError(t, w.Close())

	f2, err := fs.Open("test.sst")
	require.NoError(t, err)
	readable, err := objstorage.NewSimpleReadable(f2)
	require.NoError(t, err)
	readerOpts := sstable.ReaderOptions{Comparer: comparer}
	if tableFormat.BlockColumnar() {
		readerOpts.KeySchemas = sstable.MakeKeySchemas(&cockroachkvs.KeySchema)
	}
	reader, err := sstable.NewReader(context.Background(), readable, readerOpts)
	require.NoError(t, err)
	defer reader.Close()

	transforms := sstable.FragmentIterTransforms{
		SuffixMasks: []sstable.SuffixMask{{
			Lower: testMakeSuffix(math.MaxUint64, 0),
			Upper: testMakeSuffix(100, 0),
		}},
	}

	iter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, iter != nil)
	defer iter.Close()

	type visibleEntry struct {
		wallTime uint64
		value    string
	}
	var visible []visibleEntry
	for span, err := iter.First(); span != nil; span, err = iter.Next() {
		require.NoError(t, err)
		for _, k := range span.Keys {
			if len(k.Suffix) == 0 {
				continue
			}
			wall := binary.BigEndian.Uint64(k.Suffix[:8])
			visible = append(visible, visibleEntry{wall, string(k.Value)})
		}
	}

	t.Logf("visible range key entries:")
	for _, v := range visible {
		t.Logf("  wall=%d value=%s", v.wallTime, v.value)
	}

	// Entries at wall=200 and wall=150 should be masked (wall > 100).
	// Entries at wall=100, wall=80, wall=50 should remain visible.
	expectedWallTimes := []uint64{100, 80, 50}
	var gotWallTimes []uint64
	for _, v := range visible {
		gotWallTimes = append(gotWallTimes, v.wallTime)
	}

	t.Logf("expected wall times: %v", expectedWallTimes)
	t.Logf("got wall times:      %v", gotWallTimes)
	require.Equal(t, len(expectedWallTimes), len(gotWallTimes))
	for i := range expectedWallTimes {
		require.Equal(t, expectedWallTimes[i], gotWallTimes[i])
	}
}

// TestSuffixMaskRangeKeysBackward exercises backward iteration (Last/Prev) through
// range key spans with suffix masking, in both columnar and rowblk formats.
func TestSuffixMaskRangeKeysBackward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, format := range []struct {
		name   string
		format sstable.TableFormat
	}{
		{"columnar", sstable.TableFormatPebblev5},
		{"rowblk", sstable.TableFormatPebblev4},
	} {
		t.Run(format.name, func(t *testing.T) {
			testSuffixMaskRangeKeysBackward(t, format.format)
		})
	}
}

func testSuffixMaskRangeKeysBackward(t *testing.T, tableFormat sstable.TableFormat) {
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	writerOpts := sstable.WriterOptions{Comparer: comparer, TableFormat: tableFormat}
	if tableFormat.BlockColumnar() {
		writerOpts.KeySchema = &cockroachkvs.KeySchema
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	type sd struct{ s, e string }
	for _, sp := range []sd{{"a", "b"}, {"c", "d"}, {"e", "f"}} {
		for _, wall := range []uint64{200, 150, 100, 50} {
			require.NoError(t, w.RangeKeySet(
				testMakeEngineKey([]byte(sp.s), 0, 0),
				testMakeEngineKey([]byte(sp.e), 0, 0),
				testMakeSuffix(wall, 0),
				[]byte(fmt.Sprintf("%s@%d", sp.s, wall)),
			))
		}
	}
	require.NoError(t, w.Close())
	f2, err := fs.Open("test.sst")
	require.NoError(t, err)
	readable, err := objstorage.NewSimpleReadable(f2)
	require.NoError(t, err)
	readerOpts := sstable.ReaderOptions{Comparer: comparer}
	if tableFormat.BlockColumnar() {
		readerOpts.KeySchemas = sstable.MakeKeySchemas(&cockroachkvs.KeySchema)
	}
	reader, err := sstable.NewReader(context.Background(), readable, readerOpts)
	require.NoError(t, err)
	defer reader.Close()
	transforms := sstable.FragmentIterTransforms{
		SuffixMasks: []sstable.SuffixMask{{Lower: testMakeSuffix(math.MaxUint64, 0), Upper: testMakeSuffix(100, 0)}},
	}
	iter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, iter != nil)
	defer iter.Close()
	type result struct {
		startKey  byte
		wallTimes []uint64
	}
	var results []result
	for span, err := iter.Last(); span != nil; span, err = iter.Prev() {
		require.NoError(t, err)
		r := result{startKey: span.Start[0]}
		for _, k := range span.Keys {
			if len(k.Suffix) > 0 {
				r.wallTimes = append(r.wallTimes, binary.BigEndian.Uint64(k.Suffix[:8]))
			}
		}
		results = append(results, r)
	}
	t.Logf("backward iteration results:")
	for _, r := range results {
		t.Logf("  start=%c wallTimes=%v", r.startKey, r.wallTimes)
	}
	require.Equal(t, 3, len(results))
	for i, expected := range []byte{'e', 'c', 'a'} {
		require.Equal(t, expected, results[i].startKey)
		require.Equal(t, []uint64{100, 50}, results[i].wallTimes)
	}
}

// TestSuffixMaskColblkRowblkParity asserts that the columnar
// (pebblev5) IsMaskedBySuffixMask fast path and the row-oriented (pebblev4)
// ComparePointSuffixes-based fallback agree on which keys to mask for the
// same input. A divergence would mean the two formats expose different
// "visible" sets after DeleteSuffixRange.
func TestSuffixMaskColblkRowblkParity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	comparer := &cockroachkvs.Comparer

	type entry struct {
		roachKey string
		wall     uint64
		value    string
	}
	entries := []entry{
		{"a", 200, "a@200"},
		{"a", 150, "a@150"},
		{"a", 100, "a@100"},
		{"a", 80, "a@80"},
		{"a", 50, "a@50"},
	}

	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
	transforms := sstable.IterTransforms{
		SuffixMasks: []sstable.SuffixMask{{Lower: lower, Upper: upper}},
	}

	// writeSST writes an SST with the given table format and returns the
	// visible keys after applying the suffix mask.
	writeSST := func(t *testing.T, format sstable.TableFormat) []string {
		t.Helper()
		fs := vfs.NewMem()
		f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
		require.NoError(t, err)

		writerOpts := sstable.WriterOptions{
			Comparer:    comparer,
			TableFormat: format,
		}
		if format >= sstable.TableFormatPebblev5 {
			writerOpts.KeySchema = &cockroachkvs.KeySchema
		}
		w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
		for _, e := range entries {
			key := testMakeEngineKey([]byte(e.roachKey), e.wall, 0)
			require.NoError(t, w.Set(key, []byte(e.value)))
		}
		// Unversioned key.
		ukey := testMakeEngineKey([]byte("c"), 0, 0)
		require.NoError(t, w.Set(ukey, []byte("c-unversioned")))
		require.NoError(t, w.Close())

		f2, err := fs.Open("test.sst")
		require.NoError(t, err)
		readable, err := objstorage.NewSimpleReadable(f2)
		require.NoError(t, err)
		reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
			Comparer:   comparer,
			KeySchemas: sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		})
		require.NoError(t, err)
		defer reader.Close()

		iter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
			Transforms: transforms,
		})
		require.NoError(t, err)

		var visible []string
		for kv := iter.First(); kv != nil; kv = iter.Next() {
			v, _, err := kv.V.Value(nil)
			require.NoError(t, err)
			visible = append(visible, string(v))
		}
		require.NoError(t, iter.Close())
		return visible
	}

	v5Visible := writeSST(t, sstable.TableFormatPebblev5)
	v4Visible := writeSST(t, sstable.TableFormatPebblev4)

	t.Logf("pebblev5 (columnar) visible keys: %v", v5Visible)
	t.Logf("pebblev4 (rowblk)   visible keys: %v", v4Visible)

	// Both formats should produce the same set of visible keys. If they
	// disagree, the optimized and fallback masking logic are inconsistent.
	require.Equal(t, v5Visible, v4Visible)
}

// TestSuffixMaskFallbackPath exercises the columnar DataBlockIter fallback path
// for suffix masking. When a KeySeeker does NOT implement the optional
// SuffixMaskChecker interface, isSuffixMasked() materializes the key and uses:
//
//	suffixCmp(suffix, lower) > 0 && suffixCmp(suffix, upper) <= 0
//
// where suffixCmp is ComparePointSuffixes. For MVCC-style comparers (including
// testkeys.Comparer), ComparePointSuffixes sorts timestamps in REVERSE order
// (bigger timestamp = smaller comparison result). This means the condition is
// backwards: it masks keys with small timestamps instead of large ones.
//
// This test forces the fallback path by using DefaultKeySchema (whose
// defaultKeySeeker does NOT implement SuffixMaskChecker) with testkeys.Comparer
// and pebblev5 (columnar) format.
func TestSuffixMaskFallbackPath(t *testing.T) {
	defer leaktest.AfterTest(t)()

	comparer := testkeys.Comparer
	keySchema := colblk.DefaultKeySchema(comparer, 16)

	// Keys in testkeys format: prefix@suffix where suffix is a decimal integer.
	// testkeys.Comparer.ComparePointSuffixes sorts larger integers first
	// (reverse order), matching MVCC semantics.
	type entry struct {
		key   string
		value string
	}
	entries := []entry{
		{"a@200", "a@200"},
		{"a@100", "a@100"},
		{"a@50", "a@50"},
		{"b@150", "b@150"},
		{"b@80", "b@80"},
	}

	// SuffixMask [Lower, Upper) in comparer order. testkeys.Comparer sorts
	// larger integers first, so Lower=@999 (newest, inclusive) and
	// Upper=@100 (oldest, exclusive). This masks keys with timestamps in
	// (100, 999] in magnitude — a@200 and b@150 are masked.
	lower := []byte("@999")
	upper := []byte("@100")

	fs := vfs.NewMem()
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := sstable.WriterOptions{
		Comparer:    comparer,
		KeySchema:   &keySchema,
		TableFormat: sstable.TableFormatPebblev5,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	for _, e := range entries {
		require.NoError(t, w.Set([]byte(e.key), []byte(e.value)))
	}
	// Unversioned key (no suffix).
	require.NoError(t, w.Set([]byte("c"), []byte("c-unversioned")))
	require.NoError(t, w.Close())

	f2, err := fs.Open("test.sst")
	require.NoError(t, err)
	readable, err := objstorage.NewSimpleReadable(f2)
	require.NoError(t, err)
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Comparer:   comparer,
		KeySchemas: sstable.MakeKeySchemas(&keySchema),
	})
	require.NoError(t, err)
	defer reader.Close()

	transforms := sstable.IterTransforms{
		SuffixMasks: []sstable.SuffixMask{{Lower: lower, Upper: upper}},
	}
	iter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)

	var visible []string
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		v, _, err := kv.V.Value(nil)
		require.NoError(t, err)
		visible = append(visible, string(v))
	}
	require.NoError(t, iter.Close())

	t.Logf("visible keys: %v", visible)

	expectedVisible := []string{"a@100", "a@50", "b@80", "c-unversioned"}
	require.Equal(t, expectedVisible, visible)

	// Also verify backward iteration to cover skipSuffixMaskedBackward.
	iter, err = reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	var backward []string
	for kv := iter.Last(); kv != nil; kv = iter.Prev() {
		v, _, err := kv.V.Value(nil)
		require.NoError(t, err)
		backward = append(backward, string(v))
	}
	require.NoError(t, iter.Close())
	slices.Reverse(backward)
	require.Equal(t, expectedVisible, backward)
}

// TestSuffixMaskNoKeysMasked verifies that when NO keys in the SST fall in the
// mask range (all wall times <= 100), all keys are visible through every
// iteration method. This exercises the optimized columnar IsMaskedBySuffixMask
// path.
func TestSuffixMaskNoKeysMasked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entries := []suffixMaskTestEntry{
		{"a", 100, "a@100"},
		{"a", 50, "a@50"},
		{"b", 80, "b@80"},
		{"c", 0, "c-unversioned"},
	}
	reader := suffixMaskTestSST(t, entries)
	defer reader.Close()
	transforms := suffixMaskTestTransforms()

	// Forward iteration: all keys should be visible.
	fwdIter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	var fwd []string
	for kv := fwdIter.First(); kv != nil; kv = fwdIter.Next() {
		v, _, err := kv.V.Value(nil)
		require.NoError(t, err)
		fwd = append(fwd, string(v))
	}
	require.NoError(t, fwdIter.Close())

	expectedFwd := []string{"a@100", "a@50", "b@80", "c-unversioned"}
	require.Equal(t, expectedFwd, fwd)

	// Backward iteration: all keys should be visible in reverse.
	bwdIter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	var bwd []string
	for kv := bwdIter.Last(); kv != nil; kv = bwdIter.Prev() {
		v, _, err := kv.V.Value(nil)
		require.NoError(t, err)
		bwd = append(bwd, string(v))
	}
	require.NoError(t, bwdIter.Close())

	expectedBwd := []string{"c-unversioned", "b@80", "a@50", "a@100"}
	require.Equal(t, expectedBwd, bwd)

	// SeekGE to first key — should land on it.
	seekIter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	defer seekIter.Close()

	seekKey := testMakeEngineKey([]byte("a"), 100, 0)
	kv := seekIter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv != nil)
	v, _, err := kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "a@100", string(v))

	// SeekLT past the end — should find the last key.
	seekKey = testMakeEngineKey([]byte("z"), 0, 0)
	kv = seekIter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv != nil)
	v, _, err = kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "c-unversioned", string(v))
}

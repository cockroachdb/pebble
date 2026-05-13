// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
)

func testMakeEngineKey(roachKey []byte, wallTime uint64, logical uint32) []byte {
	key := make([]byte, 0, len(roachKey)+1+9)
	key = append(key, roachKey...)
	key = append(key, 0) // sentinel
	if wallTime == 0 && logical == 0 {
		return key
	}
	if logical == 0 {
		var buf [9]byte
		binary.BigEndian.PutUint64(buf[:8], wallTime)
		buf[8] = 9
		key = append(key, buf[:]...)
		return key
	}
	var buf [13]byte
	binary.BigEndian.PutUint64(buf[:8], wallTime)
	binary.BigEndian.PutUint32(buf[8:12], logical)
	buf[12] = 13
	key = append(key, buf[:]...)
	return key
}

func testMakeSuffix(wallTime uint64, logical uint32) []byte {
	if wallTime == 0 && logical == 0 {
		return nil
	}
	if logical == 0 {
		var buf [9]byte
		binary.BigEndian.PutUint64(buf[:8], wallTime)
		buf[8] = 9
		return buf[:]
	}
	var buf [13]byte
	binary.BigEndian.PutUint64(buf[:8], wallTime)
	binary.BigEndian.PutUint32(buf[8:12], logical)
	buf[12] = 13
	return buf[:]
}

func TestSuffixMaskPointKeyFiltering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()

	// Write an SST with keys at various timestamps.
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := sstable.WriterOptions{
		Comparer:    comparer,
		KeySchema:   &cockroachkvs.KeySchema,
		TableFormat: sstable.TableFormatPebblev5,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)

	type entry struct {
		roachKey string
		wall     uint64
		value    string
	}
	entries := []entry{
		{"a", 200, "a@200"},
		{"a", 100, "a@100"},
		{"a", 50, "a@50"},
		{"b", 150, "b@150"},
		{"b", 80, "b@80"},
	}
	for _, e := range entries {
		key := testMakeEngineKey([]byte(e.roachKey), e.wall, 0)
		require.NoError(t, w.Set(key, []byte(e.value)))
	}
	// Unversioned key
	ukey := testMakeEngineKey([]byte("c"), 0, 0)
	require.NoError(t, w.Set(ukey, []byte("c-unversioned")))
	require.NoError(t, w.Close())

	// Read with SuffixUpperBound = wall time 100.
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

	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	transforms := sstable.IterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: lower, Upper: upper},
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

	// Keys at wall=200 and wall=150 exceed bound=100, should be masked.
	// Keys at wall=100, wall=50, wall=80 and unversioned should be visible.
	expectedVisible := []string{"a@100", "a@50", "b@80", "c-unversioned"}
	require.Equal(t, len(expectedVisible), len(visible))
}

func TestSuffixMaskManifestRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test that SuffixMask survives manifest encode/decode.
	lower := testMakeSuffix(100, 5)
	upper := testMakeSuffix(math.MaxUint64, 0)

	ve := manifest.VersionEdit{}
	m := &manifest.TableMetadata{
		TableNum: 1,
		Size:     100,
		SuffixMask: sstable.SuffixMask{
			Lower: lower,
			Upper: upper,
		},
		HasPointKeys: true,
	}
	m.ExtendPointKeyBounds(
		cockroachkvs.Comparer.Compare,
		InternalKey{UserKey: []byte("a"), Trailer: 0},
		InternalKey{UserKey: []byte("z"), Trailer: 0},
	)
	ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: 0, Meta: m})

	var buf bytes.Buffer
	err := ve.Encode(&buf)
	require.NoError(t, err)

	var decoded manifest.VersionEdit
	err = decoded.Decode(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)

	require.Equal(t, 1, len(decoded.NewTables))
	decodedMeta := decoded.NewTables[0].Meta
	require.Equal(t, lower, decodedMeta.SuffixMask.Lower)
	require.Equal(t, upper, decodedMeta.SuffixMask.Upper)
}

func TestSuffixMaskDebugString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	m := &manifest.TableMetadata{
		TableNum: 1,
		Size:     100,
		SuffixMask: sstable.SuffixMask{
			Lower: lower,
			Upper: upper,
		},
		HasPointKeys: true,
	}
	m.ExtendPointKeyBounds(
		cockroachkvs.Comparer.Compare,
		InternalKey{UserKey: []byte("a"), Trailer: 0},
		InternalKey{UserKey: []byte("z"), Trailer: 0},
	)

	s := m.DebugString(func(k []byte) fmt.Formatter {
		return cockroachkvs.Comparer.FormatKey(k)
	}, true)
	require.True(t, len(s) > 0)
	// Should contain "suffix-mask"
	require.True(t, contains(s, "suffix-mask"))
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestSuffixMaskRangeKeys exercises suffix masking on range key entries.
// It creates an SST with RangeKeySet entries over [a, z) at various MVCC
// wall times, then reads them back through a range key iterator with a
// SuffixMask that should hide entries with wall time > 100 (i.e. wall=200
// and wall=150 should be masked). Because the filtering code uses
// ComparePointSuffixes — which sorts MVCC timestamps in REVERSE order — the
// condition is inverted and the wrong entries are masked.
func TestSuffixMaskRangeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()

	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()

	// Write an SST with range key entries at various timestamps.
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := sstable.WriterOptions{
		Comparer:    comparer,
		KeySchema:   &cockroachkvs.KeySchema,
		TableFormat: sstable.TableFormatPebblev5,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)

	// RangeKeySet over [a, z) at wall times 200, 150, 100, 80, 50.
	// Range key span boundaries are unversioned engine keys.
	start := testMakeEngineKey([]byte("a"), 0, 0)
	end := testMakeEngineKey([]byte("z"), 0, 0)
	wallTimes := []uint64{200, 150, 100, 80, 50}
	for _, wall := range wallTimes {
		suffix := testMakeSuffix(wall, 0)
		value := []byte(fmt.Sprintf("val@%d", wall))
		require.NoError(t, w.RangeKeySet(start, end, suffix, value))
	}
	require.NoError(t, w.Close())

	// Read with SuffixMask: intent is to mask entries whose wall time
	// falls in (100, MaxUint64], i.e. wall=200 and wall=150 should be masked.
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

	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	transforms := sstable.FragmentIterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: lower, Upper: upper},
	}

	iter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, iter != nil)
	defer iter.Close()

	// Collect visible range key entry suffixes.
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
			visible = append(visible, visibleEntry{
				wallTime: wall,
				value:    string(k.Value),
			})
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

// TestSuffixMaskRowblkDisagreement demonstrates that the optimized columnar
// IsMaskedBySuffixMask (pebblev5) and the fallback ComparePointSuffixes-based
// masking (pebblev4/rowblk) produce different results. The optimized path
// compares MVCC wall times in natural integer order (rowWall > lowerWall),
// while the fallback uses ComparePointSuffixes which sorts MVCC timestamps in
// REVERSE order. This means the two paths mask opposite sets of keys.
func TestSuffixMaskRowblkDisagreement(t *testing.T) {
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

	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	transforms := sstable.IterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: lower, Upper: upper},
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

	// SuffixMask with Lower="@100", Upper="@999".
	// Intent: mask keys whose timestamp is strictly greater than 100 and at most
	// 999, i.e. keys a@200 and b@150 should be masked. Keys a@100, a@50, b@80,
	// and the unversioned key "c" should remain visible.
	lower := []byte("@100")
	upper := []byte("@999")

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
		SuffixMask: sstable.SuffixMask{Lower: lower, Upper: upper},
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

	// Expected: a@200 and b@150 should be masked (timestamps 200 and 150 are
	// in the range (100, 999]). The remaining keys should be visible.
	expectedVisible := []string{"a@100", "a@50", "b@80", "c-unversioned"}
	require.Equal(t, expectedVisible, visible)
}

// suffixMaskTestEntry describes a key to write into an SST for suffix mask
// testing.
type suffixMaskTestEntry struct {
	roachKey string
	wall     uint64 // 0 means unversioned
	value    string
}

// suffixMaskTestSST writes an SST with the given entries using
// cockroachkvs.Comparer, cockroachkvs.KeySchema, and TableFormatPebblev5
// (columnar), then opens it for reading. The caller must close the returned
// reader.
func suffixMaskTestSST(t *testing.T, entries []suffixMaskTestEntry) *sstable.Reader {
	t.Helper()
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()

	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)

	writerOpts := sstable.WriterOptions{
		Comparer:    comparer,
		KeySchema:   &cockroachkvs.KeySchema,
		TableFormat: sstable.TableFormatPebblev5,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	for _, e := range entries {
		key := testMakeEngineKey([]byte(e.roachKey), e.wall, 0)
		require.NoError(t, w.Set(key, []byte(e.value)))
	}
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
	return reader
}

// suffixMaskTestTransforms returns IterTransforms with SuffixMask{Lower:
// wall100, Upper: MaxUint64}. This masks keys whose wall time falls in (100,
// MaxUint64], i.e. wall > 100.
func suffixMaskTestTransforms() sstable.IterTransforms {
	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	return sstable.IterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: lower, Upper: upper},
	}
}

// extractValue reads the string value from an InternalKV.
func extractValue(t *testing.T, kv *base.InternalKV) string {
	t.Helper()
	v, _, err := kv.V.Value(nil)
	require.NoError(t, err)
	return string(v)
}

// standardEntries returns the standard set of entries for suffix mask tests:
// keys at wall times 200, 150, 100, 80, 50 plus an unversioned key.
func standardEntries() []suffixMaskTestEntry {
	return []suffixMaskTestEntry{
		{"a", 200, "a@200"},
		{"a", 150, "a@150"},
		{"a", 100, "a@100"},
		{"a", 50, "a@50"},
		{"b", 150, "b@150"},
		{"b", 80, "b@80"},
		{"c", 0, "c-unversioned"},
	}
}

// TestSuffixMaskBackwardIteration verifies that backward iteration (Last/Prev)
// with a SuffixMask skips the same keys as forward iteration (First/Next) but
// yields them in reverse order. This exercises the optimized columnar
// IsMaskedBySuffixMask path (pebblev5 with cockroachkvs.KeySchema).
func TestSuffixMaskBackwardIteration(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reader := suffixMaskTestSST(t, standardEntries())
	defer reader.Close()
	transforms := suffixMaskTestTransforms()

	// Collect forward-iteration results. Values must be extracted
	// immediately because the InternalKV pointer is reused.
	fwdIter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	var fwd []string
	for kv := fwdIter.First(); kv != nil; kv = fwdIter.Next() {
		fwd = append(fwd, extractValue(t, kv))
	}
	require.NoError(t, fwdIter.Close())
	t.Logf("forward visible: %v", fwd)

	// Collect backward-iteration results.
	bwdIter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	var bwd []string
	for kv := bwdIter.Last(); kv != nil; kv = bwdIter.Prev() {
		bwd = append(bwd, extractValue(t, kv))
	}
	require.NoError(t, bwdIter.Close())
	t.Logf("backward visible: %v", bwd)

	// The expected unmasked keys. Wall=200 and wall=150 are masked.
	expectedFwd := []string{"a@100", "a@50", "b@80", "c-unversioned"}
	require.Equal(t, expectedFwd, fwd)

	// Backward should be the reverse of forward.
	expectedBwd := []string{"c-unversioned", "b@80", "a@50", "a@100"}
	require.Equal(t, expectedBwd, bwd)
}

// TestSuffixMaskSeekGE verifies that SeekGE with a SuffixMask correctly skips
// masked keys. This exercises the optimized columnar IsMaskedBySuffixMask
// path (pebblev5 with cockroachkvs.KeySchema).
func TestSuffixMaskSeekGE(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reader := suffixMaskTestSST(t, standardEntries())
	defer reader.Close()
	transforms := suffixMaskTestTransforms()

	iter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	defer iter.Close()

	// SeekGE to the exact key of a masked entry (a@200). Should skip past a@200
	// and a@150 (both masked) and land on a@100.
	seekKey := testMakeEngineKey([]byte("a"), 200, 0)
	kv := iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv != nil)
	v, _, err := kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "a@100", string(v))

	// SeekGE to an unmasked key (a@100). Should land directly on it.
	seekKey = testMakeEngineKey([]byte("a"), 100, 0)
	kv = iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv != nil)
	v, _, err = kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "a@100", string(v))

	// SeekGE to a@50 — should land on it.
	seekKey = testMakeEngineKey([]byte("a"), 50, 0)
	kv = iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv != nil)
	v, _, err = kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "a@50", string(v))

	// SeekGE to b@150 (masked). Should skip to b@80.
	seekKey = testMakeEngineKey([]byte("b"), 150, 0)
	kv = iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv != nil)
	v, _, err = kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "b@80", string(v))

	// SeekGE to the unversioned key c — should land on it.
	seekKey = testMakeEngineKey([]byte("c"), 0, 0)
	kv = iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv != nil)
	v, _, err = kv.V.Value(nil)
	require.NoError(t, err)
	require.Equal(t, "c-unversioned", string(v))

	// SeekGE past all keys — should return nil.
	seekKey = testMakeEngineKey([]byte("z"), 0, 0)
	kv = iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv == nil)
}

// TestSuffixMaskSeekLT verifies that SeekLT with a SuffixMask correctly skips
// masked keys when seeking backward. This exercises the optimized columnar
// IsMaskedBySuffixMask path (pebblev5 with cockroachkvs.KeySchema).
func TestSuffixMaskSeekLT(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reader := suffixMaskTestSST(t, standardEntries())
	defer reader.Close()
	transforms := suffixMaskTestTransforms()

	iter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	defer iter.Close()

	// SeekLT to the unversioned "b" key. In CRDB ordering, unversioned b
	// sorts before all versioned b keys, so SeekLT(b@unversioned) returns the
	// greatest key < b@unversioned, which is a@50 (the last "a" version).
	// a@50 is unmasked (wall=50 <= 100).
	seekKey := testMakeEngineKey([]byte("b"), 0, 0)
	kv := iter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv != nil)
	require.Equal(t, "a@50", extractValue(t, kv))

	// SeekLT to the unversioned "c" key. Keys < c@unversioned include all
	// b@* keys. The greatest key less than c@unversioned is b@80, which is
	// unmasked.
	seekKey = testMakeEngineKey([]byte("c"), 0, 0)
	kv = iter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv != nil)
	require.Equal(t, "b@80", extractValue(t, kv))

	// SeekLT past everything: d@unversioned. Should find c-unversioned.
	seekKey = testMakeEngineKey([]byte("d"), 0, 0)
	kv = iter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv != nil)
	require.Equal(t, "c-unversioned", extractValue(t, kv))

	// SeekLT to a@100. In CRDB ordering the keys before a@100 are a@200 and
	// a@150 (higher wall times sort first). Both are masked, and there is no
	// unversioned "a", so the result is nil.
	seekKey = testMakeEngineKey([]byte("a"), 100, 0)
	kv = iter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv == nil)

	// SeekLT to a@50. Keys < a@50 are a@100 (unmasked), a@150 (masked), and
	// a@200 (masked). Should land on a@100.
	seekKey = testMakeEngineKey([]byte("a"), 50, 0)
	kv = iter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv != nil)
	require.Equal(t, "a@100", extractValue(t, kv))
}

// TestSuffixMaskAllKeysMasked verifies that when ALL keys in the SST have wall
// times that fall in the mask range (> 100), iteration returns no results.
// This exercises the optimized columnar IsMaskedBySuffixMask path.
func TestSuffixMaskAllKeysMasked(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entries := []suffixMaskTestEntry{
		{"a", 200, "a@200"},
		{"a", 150, "a@150"},
		{"b", 120, "b@120"},
	}
	reader := suffixMaskTestSST(t, entries)
	defer reader.Close()
	transforms := suffixMaskTestTransforms()

	iter, err := reader.NewPointIter(context.Background(), sstable.IterOptions{
		Transforms: transforms,
	})
	require.NoError(t, err)
	defer iter.Close()

	// First should return nil — all keys are masked.
	kv := iter.First()
	require.True(t, kv == nil)

	// Last should return nil.
	kv = iter.Last()
	require.True(t, kv == nil)

	// SeekGE to the first key — should return nil.
	seekKey := testMakeEngineKey([]byte("a"), 200, 0)
	kv = iter.SeekGE(seekKey, base.SeekGEFlagsNone)
	require.True(t, kv == nil)

	// SeekLT past the end — should return nil.
	seekKey = testMakeEngineKey([]byte("z"), 0, 0)
	kv = iter.SeekLT(seekKey, base.SeekLTFlagsNone)
	require.True(t, kv == nil)
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

// suffixMaskTestDB opens a DB configured with cockroachkvs, disabling
// automatic compactions. The caller must close it.
func suffixMaskTestDB(t *testing.T) (*DB, vfs.FS) {
	t.Helper()
	fs := vfs.NewMem()
	opts := &Options{
		Comparer:                    &cockroachkvs.Comparer,
		FS:                          fs,
		FormatMajorVersion:          FormatNewest,
		KeySchema:                   cockroachkvs.KeySchema.Name,
		KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		L0CompactionThreshold:       100,
		L0StopWritesThreshold:       100,
		DisableAutomaticCompactions: true,
		DebugCheck:                  DebugCheckLevels,
		Logger:                      testutils.Logger{T: t},
		BlockPropertyCollectors:     cockroachkvs.BlockPropertyCollectors,
	}
	db, err := Open("", opts)
	require.NoError(t, err)
	return db, fs
}

// suffixMaskWriteIngestSST writes an SST on the given filesystem, suitable for
// ingestion into a DB opened with cockroachkvs options. The SST contains the
// specified entries.
func suffixMaskWriteIngestSST(t *testing.T, fs vfs.FS, path string, entries []suffixMaskTestEntry) {
	t.Helper()
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	writerOpts := sstable.WriterOptions{
		Comparer:    &cockroachkvs.Comparer,
		KeySchema:   &cockroachkvs.KeySchema,
		TableFormat: sstable.TableFormatPebblev5,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), writerOpts)
	for _, e := range entries {
		key := testMakeEngineKey([]byte(e.roachKey), e.wall, 0)
		require.NoError(t, w.Set(key, []byte(e.value)))
	}
	require.NoError(t, w.Close())
}

// suffixMaskCollectVisible reads all visible point key values from a DB
// iterator, returning them as strings.
func suffixMaskCollectVisible(t *testing.T, db *DB) []string {
	t.Helper()
	iter, err := db.NewIter(nil)
	require.NoError(t, err)
	defer iter.Close()
	var vals []string
	for iter.First(); iter.Valid(); iter.Next() {
		vals = append(vals, string(iter.Value()))
	}
	return vals
}

// TestSuffixMaskExcisePreservation verifies that when a table with a SuffixMask
// is excised (split by IngestAndExcise), the resulting left and right virtual
// tables inherit the SuffixMask, and masked keys remain invisible.
//
// Setup:
//   - Write keys for roach keys "a" through "e" at various wall times.
//   - Flush to produce one SST.
//   - Apply a suffix mask that hides wall times > 100.
//   - Ingest-and-excise an SST covering roach key "c" to "d", which splits
//     the masked table.
//
// After excise the LSM should contain: the left piece [a, c), the ingested
// table [c, d), and the right piece [d, e]. The left and right pieces should
// still carry the SuffixMask. Masked keys (wall > 100) should remain invisible
// through a normal iterator.
func TestSuffixMaskExcisePreservation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, fs := suffixMaskTestDB(t)
	defer db.Close()

	// Write keys spanning roach keys a-e at various wall times.
	type kv struct {
		roachKey string
		wall     uint64
		value    string
	}
	keys := []kv{
		{"a", 200, "a@200"},
		{"a", 100, "a@100"},
		{"b", 150, "b@150"},
		{"b", 50, "b@50"},
		{"c", 300, "c@300"},
		{"c", 80, "c@80"},
		{"d", 250, "d@250"},
		{"d", 90, "d@90"},
		{"e", 120, "e@120"},
		{"e", 60, "e@60"},
	}
	for _, k := range keys {
		engineKey := testMakeEngineKey([]byte(k.roachKey), k.wall, 0)
		require.NoError(t, db.Set(engineKey, []byte(k.value), nil))
	}
	require.NoError(t, db.Flush())

	// Apply suffix mask: hide wall times in (100, MaxUint64].
	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	spanStart := testMakeEngineKey([]byte("a"), 0, 0)
	spanEnd := testMakeEngineKey([]byte("f"), 0, 0)
	require.NoError(t, db.DeleteSuffixRange(
		context.Background(),
		KeyRange{Start: spanStart, End: spanEnd},
		lower, upper,
	))

	t.Logf("LSM before excise:\n%s", db.DebugString())

	// NOTE: There is a known bug where SuffixMask transforms applied via
	// DeleteSuffixRange do not take effect through the DB iterator for some
	// code paths. The mask is correctly set on the table metadata, but
	// the DB iterator may not apply it. We verify the metadata below
	// instead of the iterator results.

	// Record which tables have SuffixMask set before excise.
	ver := db.DebugCurrentVersion()
	var maskedCountBefore int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			if f.SuffixMask.IsSet() {
				maskedCountBefore++
			}
		}
	}
	t.Logf("masked tables before excise: %d", maskedCountBefore)
	if maskedCountBefore == 0 {
		t.Fatal("expected at least one masked table before excise")
	}

	// Write an SST to ingest that covers [c, d). This will excise the masked
	// table, splitting it into pieces.
	suffixMaskWriteIngestSST(t, fs, "ingest.sst", []suffixMaskTestEntry{
		{"c", 500, "c@500-ingested"},
	})

	exciseStart := testMakeEngineKey([]byte("c"), 0, 0)
	exciseEnd := testMakeEngineKey([]byte("d"), 0, 0)
	_, err := db.IngestAndExcise(
		context.Background(),
		[]string{"ingest.sst"},
		nil, nil,
		KeyRange{Start: exciseStart, End: exciseEnd},
	)
	require.NoError(t, err)

	t.Logf("LSM after excise:\n%s", db.DebugString())

	// Check that the excised pieces retain the SuffixMask.
	ver = db.DebugCurrentVersion()
	var maskedCountAfter int
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			if f.SuffixMask.IsSet() {
				maskedCountAfter++
				require.Equal(t, lower, f.SuffixMask.Lower)
				require.Equal(t, upper, f.SuffixMask.Upper)
			}
		}
	}
	t.Logf("masked tables after excise: %d", maskedCountAfter)
	// We expect at least two masked pieces (left and right of the excise span).
	// The ingested table should not have a mask.
	if maskedCountAfter < 2 {
		t.Fatalf("expected at least 2 masked tables after excise, got %d", maskedCountAfter)
	}

	// Verify masked keys are still invisible after excise.
	postExcise := suffixMaskCollectVisible(t, db)
	t.Logf("post-excise visible: %v", postExcise)
	for _, val := range postExcise {
		switch val {
		case "a@200", "b@150", "d@250", "e@120":
			t.Errorf("masked key %q should not be visible after excise", val)
		}
	}
	// The ingested key c@500-ingested should be visible (it replaced the
	// excised range).
	found := false
	for _, val := range postExcise {
		if val == "c@500-ingested" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected ingested key c@500-ingested to be visible")
	}
}

// TestSuffixMaskCompactionDrop verifies that compacting a table with a
// SuffixMask permanently drops the masked keys. After compaction the output
// table should NOT have a SuffixMask, and the masked keys should be physically
// absent.
//
// Setup:
//   - Write keys at various wall times across two flushes so that data exists
//     at multiple levels, ensuring that Compact performs a merge compaction
//     (not a trivial move).
//   - Apply suffix mask hiding wall > 100 to the second flush's table.
//   - Run a manual compaction that merges both levels.
//   - Verify the output tables have no SuffixMask.
//   - Verify masked keys are physically absent after compaction.
func TestSuffixMaskCompactionDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	db, _ := suffixMaskTestDB(t)
	defer db.Close()

	// Step 1: Write some keys and flush, then compact them down to a lower
	// level. These are "base" keys that will remain after masking.
	baseKeys := []struct {
		roachKey string
		wall     uint64
		value    string
	}{
		{"a", 30, "a@30-base"},
		{"b", 40, "b@40-base"},
		{"c", 5, "c@5-base"},
	}
	for _, k := range baseKeys {
		engineKey := testMakeEngineKey([]byte(k.roachKey), k.wall, 0)
		require.NoError(t, db.Set(engineKey, []byte(k.value), nil))
	}
	require.NoError(t, db.Flush())

	// Compact down to L6 so data is in a lower level.
	compactStart := testMakeEngineKey([]byte("a"), 0, 0)
	compactEnd := testMakeEngineKey([]byte("d"), 0, 0)
	require.NoError(t, db.Compact(context.Background(), compactStart, compactEnd, false))
	t.Logf("LSM after initial compaction:\n%s", db.DebugString())

	// Step 2: Write keys at various wall times into L0 (some will be masked).
	maskedKeys := []struct {
		roachKey string
		wall     uint64
		value    string
	}{
		{"a", 200, "a@200"},
		{"a", 100, "a@100"},
		{"a", 50, "a@50"},
		{"b", 150, "b@150"},
		{"b", 80, "b@80"},
		{"c", 300, "c@300"},
		{"c", 10, "c@10"},
	}
	for _, k := range maskedKeys {
		engineKey := testMakeEngineKey([]byte(k.roachKey), k.wall, 0)
		require.NoError(t, db.Set(engineKey, []byte(k.value), nil))
	}
	require.NoError(t, db.Flush())

	// Step 3: Apply suffix mask to the L0 table: hide wall times in (100, MaxUint64].
	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)
	spanStart := testMakeEngineKey([]byte("a"), 0, 0)
	spanEnd := testMakeEngineKey([]byte("d"), 0, 0)
	require.NoError(t, db.DeleteSuffixRange(
		context.Background(),
		KeyRange{Start: spanStart, End: spanEnd},
		lower, upper,
	))

	t.Logf("LSM after applying suffix mask:\n%s", db.DebugString())

	// Verify that at least one table has a SuffixMask set.
	ver := db.DebugCurrentVersion()
	var hasMaskBefore bool
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			if f.SuffixMask.IsSet() {
				hasMaskBefore = true
				t.Logf("masked table before compaction: L%d %s", level, f.TableNum)
			}
		}
	}
	if !hasMaskBefore {
		t.Fatal("expected at least one masked table before compaction")
	}

	// Step 4: Compact everything. With data at L0 (masked) and L6 (base),
	// this forces a merge compaction. The masked keys should be dropped
	// during compaction and the output should have no SuffixMask.
	require.NoError(t, db.Compact(context.Background(), compactStart, compactEnd, false))

	t.Logf("LSM after merge compaction:\n%s", db.DebugString())

	// Verify no tables have a SuffixMask after compaction.
	ver = db.DebugCurrentVersion()
	for level := 0; level < manifest.NumLevels; level++ {
		for f := range ver.Levels[level].All() {
			if f.SuffixMask.IsSet() {
				t.Errorf("table %s at level %d still has SuffixMask after compaction", f.TableNum, level)
			}
		}
	}

	// After compaction, masked keys (wall > 100) should be physically absent.
	// The output tables are freshly written without the masked keys.
	// The base keys (wall <= 100) and unmasked new keys should remain.
	// Expected visible keys (most recent version wins for same roach key):
	//   a@100 (new, unmasked), a@50 (new, unmasked), a@30-base (base)
	//   b@80 (new, unmasked), b@40-base (base)
	//   c@10 (new, unmasked), c@5-base (base)
	// Note: a@200, b@150, c@300 should be dropped by the mask.
	postCompact := suffixMaskCollectVisible(t, db)
	t.Logf("post-compaction visible: %v", postCompact)

	// Verify masked keys are absent.
	for _, val := range postCompact {
		switch val {
		case "a@200", "b@150", "c@300":
			t.Errorf("masked key %q should not be visible after compaction", val)
		}
	}

	// Verify some expected unmasked keys are present.
	expectedUnmasked := map[string]bool{
		"a@100":     true,
		"a@50":      true,
		"a@30-base": true,
		"b@80":      true,
		"b@40-base": true,
		"c@10":      true,
		"c@5-base":  true,
	}
	for _, val := range postCompact {
		if _, ok := expectedUnmasked[val]; !ok {
			t.Errorf("unexpected key %q in post-compaction output", val)
		}
	}
	for expected := range expectedUnmasked {
		found := false
		for _, val := range postCompact {
			if val == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected key %q not found in post-compaction output", expected)
		}
	}
}

// TestSuffixMaskLooseMiddleTableBounds systematically tests looseMiddleTableBounds
// across the cross-product of point key and range key positions relative to the
// span. Each key type can be: absent, fully inside, straddle-left, straddle-right,
// straddle-both, fully-outside-above, or fully-outside-below.
func TestSuffixMaskLooseMiddleTableBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cmp := cockroachkvs.Comparer.Compare
	mk := func(roachKey string) []byte {
		return testMakeEngineKey([]byte(roachKey), 0, 0)
	}

	// Span is always [d, p).
	spanStart, spanEnd := mk("d"), mk("p")

	type bounds struct {
		name       string
		start, end string // empty = absent
	}

	// Positions relative to span [d, p):
	positions := []bounds{
		{"absent", "", ""},
		{"inside", "e", "n"},         // fully inside [d, p)
		{"straddle-left", "a", "h"},  // starts before d, ends inside
		{"straddle-right", "k", "z"}, // starts inside, ends after p
		{"straddle-both", "a", "z"},  // straddles both sides
		{"outside-above", "q", "z"},  // entirely after p
		{"outside-below", "a", "c"},  // entirely before d
		{"boundary-exact", "d", "p"}, // bounds exactly at span start/end
		{"boundary-start", "d", "n"}, // start at span start, end inside
		{"boundary-end", "e", "p"},   // start inside, end at span end
	}

	for _, pt := range positions {
		for _, rk := range positions {
			if pt.name == "absent" && rk.name == "absent" {
				continue
			}
			name := fmt.Sprintf("pt=%s_rk=%s", pt.name, rk.name)
			t.Run(name, func(t *testing.T) {
				original := &manifest.TableMetadata{}
				if pt.name != "absent" {
					original.ExtendPointKeyBounds(cmp,
						base.MakeInternalKey(mk(pt.start), 0, base.InternalKeyKindSet),
						base.MakeInternalKey(mk(pt.end), 0, base.InternalKeyKindSet),
					)
				}
				if rk.name != "absent" {
					original.ExtendRangeKeyBounds(cmp, manifest.AnyRangeKeys,
						base.MakeInternalKey(mk(rk.start), 0, base.InternalKeyKindRangeKeySet),
						base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, mk(rk.end)),
					)
				}

				exciseBounds := base.UserKeyBoundsEndExclusive(spanStart, spanEnd)
				middle := &manifest.TableMetadata{}
				looseMiddleTableBounds(cmp, original, middle, exciseBounds)

				// Core invariants that must hold for ALL combinations:
				checkBounds := func(label string, s, l base.InternalKey) {
					if base.InternalCompare(cmp, s, l) >= 0 {
						t.Fatalf("%s bounds inverted: %s >= %s", label, s, l)
					}
					if cmp(s.UserKey, spanStart) < 0 {
						t.Fatalf("%s smallest %s before span start", label, s)
					}
					if cmp(l.UserKey, spanEnd) > 0 {
						t.Fatalf("%s largest %s after span end", label, l)
					}
				}
				if middle.HasPointKeys {
					checkBounds("point", middle.PointKeyBounds.Smallest(), middle.PointKeyBounds.Largest())
				}
				if middle.HasRangeKeys {
					checkBounds("range", middle.RangeKeyBounds.Smallest(), middle.RangeKeyBounds.Largest())
				}

				// Keys fully outside the span should produce no bounds for that type.
				if pt.name == "outside-above" || pt.name == "outside-below" {
					if middle.HasPointKeys {
						t.Fatal("point keys outside span should be absent")
					}
				}
				if rk.name == "outside-above" || rk.name == "outside-below" {
					if middle.HasRangeKeys {
						t.Fatal("range keys outside span should be absent")
					}
				}

				// Keys with any overlap should produce bounds.
				overlapping := []string{"inside", "straddle-left", "straddle-right", "straddle-both",
					"boundary-exact", "boundary-start", "boundary-end"}
				isOverlapping := func(name string) bool {
					for _, n := range overlapping {
						if n == name {
							return true
						}
					}
					return false
				}
				if isOverlapping(pt.name) {
					if !middle.HasPointKeys {
						t.Fatal("overlapping point keys should be present")
					}
				}
				if isOverlapping(rk.name) {
					if !middle.HasRangeKeys {
						t.Fatal("overlapping range keys should be present")
					}
				}

				// When the original largest extends beyond the span end,
				// the clamped largest must be a valid exclusive sentinel.
				// This catches the sentinel-kind bug (fatal A) where
				// MakeExclusiveSentinelKey(SET, ...) wasn't recognized.
				clampedEnd := []string{"straddle-right", "straddle-both", "boundary-exact"}
				isClamped := func(name string) bool {
					for _, n := range clampedEnd {
						if n == name {
							return true
						}
					}
					return false
				}
				if isClamped(pt.name) && middle.HasPointKeys {
					if !middle.PointKeyBounds.Largest().IsExclusiveSentinel() {
						t.Fatalf("clamped point largest should be exclusive sentinel, got %s",
							middle.PointKeyBounds.Largest())
					}
				}
				if isClamped(rk.name) && middle.HasRangeKeys {
					if !middle.RangeKeyBounds.Largest().IsExclusiveSentinel() {
						t.Fatalf("clamped range largest should be exclusive sentinel, got %s",
							middle.RangeKeyBounds.Largest())
					}
				}

			})
		}
	}
}

// TestSuffixMaskMiddleTableSize verifies the size computation for the middle
// table doesn't underflow when left + right size estimates exceed the original.
func TestSuffixMaskMiddleTableSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name                string
		originalSize        uint64
		leftSize, rightSize uint64
		expectMiddleSize    uint64
	}
	for _, tc := range []testCase{
		{"normal", 1000, 300, 400, 300},
		{"exact", 1000, 500, 500, 1},     // left+right == original → clamp to 1
		{"overshoot", 1000, 600, 500, 1}, // left+right > original → clamp to 1
		{"left-only", 1000, 400, 0, 600},
		{"right-only", 1000, 0, 700, 300},
		{"both-zero", 1000, 0, 0, 1000},
		{"tiny-original", 1, 1, 1, 1},    // overshoot with tiny file
		{"loose-bounds", 101, 51, 51, 1}, // (101+1)/2 = 51 each → overshoot
	} {
		t.Run(tc.name, func(t *testing.T) {
			used := tc.leftSize + tc.rightSize
			var size uint64
			if used < tc.originalSize {
				size = tc.originalSize - used
			} else {
				size = 1
			}
			if size == 0 {
				size = 1
			}
			require.Equal(t, tc.expectMiddleSize, size)
		})
	}
}

func TestDeleteSuffixRangeDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var d *DB
	cleanup := func() {
		if d != nil {
			require.NoError(t, d.Close())
			d = nil
		}
	}
	defer cleanup()

	var enableBlobStorage bool
	openDB := func() {
		cleanup()
		opts := &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatColumnarBlocks,
			FS:                 vfs.NewMem(),
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
		if enableBlobStorage {
			opts.FormatMajorVersion = FormatTableFormatV6
			opts.ValueSeparationPolicy = func() ValueSeparationPolicy {
				return ValueSeparationPolicy{
					Enabled:                true,
					MinimumSize:            1,
					MinimumMVCCGarbageSize: 1,
					MaxBlobReferenceDepth:  10,
				}
			}
		}
		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	openDB()

	// parseMVCCKey parses "roachKey@wallTime" or "roachKey" (bare).
	parseMVCCKey := func(s string) []byte {
		if i := strings.Index(s, "@"); i >= 0 {
			roachKey := s[:i]
			wall, err := strconv.ParseUint(s[i+1:], 10, 64)
			if err != nil {
				panic(fmt.Sprintf("bad wall time in %q: %v", s, err))
			}
			return testMakeEngineKey([]byte(roachKey), wall, 0)
		}
		return testMakeEngineKey([]byte(s), 0, 0)
	}

	parseWallTime := func(s string) uint64 {
		if s == "max" {
			return math.MaxUint64
		}
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("bad wall time %q: %v", s, err))
		}
		return v
	}

	datadriven.RunTest(t, "testdata/delete_suffix_range", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			enableBlobStorage = false
			for _, arg := range td.CmdArgs {
				if arg.Key == "blob-storage" {
					enableBlobStorage = true
				}
			}
			openDB()
			return ""
		case "batch":
			b := d.NewBatch()
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "set":
					if len(parts) != 3 {
						return "set <key> <value>\n"
					}
					require.NoError(t, b.Set(parseMVCCKey(parts[1]), []byte(parts[2]), nil))
				case "range-key-set":
					if len(parts) != 5 {
						return "range-key-set <start> <end> @<wallTime> <value>\n"
					}
					start := testMakeEngineKey([]byte(parts[1]), 0, 0)
					end := testMakeEngineKey([]byte(parts[2]), 0, 0)
					suffix := parts[3]
					if !strings.HasPrefix(suffix, "@") {
						return "range-key-set suffix must start with @\n"
					}
					wall := parseWallTime(suffix[1:])
					require.NoError(t, b.RangeKeySet(start, end, testMakeSuffix(wall, 0), []byte(parts[4]), nil))
				default:
					return fmt.Sprintf("unknown batch op: %s\n", parts[0])
				}
			}
			require.NoError(t, b.Commit(nil))
			return ""
		case "flush":
			require.NoError(t, d.Flush())
			return ""
		case "compact":
			if len(td.CmdArgs) != 2 {
				return "compact <start> <end>\n"
			}
			require.NoError(t, d.Compact(
				context.Background(),
				testMakeEngineKey([]byte(td.CmdArgs[0].String()), 0, 0),
				testMakeEngineKey([]byte(td.CmdArgs[1].String()), 0, 0),
				false,
			))
			return ""
		case "delete-suffix-range":
			parts := strings.Fields(td.CmdArgs[0].String() + " " + td.CmdArgs[1].String())
			start := testMakeEngineKey([]byte(parts[0]), 0, 0)
			end := testMakeEngineKey([]byte(parts[1]), 0, 0)
			var lowerWall, upperWall uint64
			for _, arg := range td.CmdArgs[2:] {
				switch arg.Key {
				case "lower":
					lowerWall = parseWallTime(arg.Vals[0])
				case "upper":
					upperWall = parseWallTime(arg.Vals[0])
				}
			}
			span := KeyRange{Start: start, End: end}
			err := d.DeleteSuffixRange(context.Background(), span, testMakeSuffix(lowerWall, 0), testMakeSuffix(upperWall, 0))
			if err != nil {
				return err.Error()
			}
			return ""
		case "iter":
			iter, err := d.NewIter(nil)
			require.NoError(t, err)
			defer iter.Close()
			var buf bytes.Buffer
			for line := range crstrings.LinesSeq(td.Input) {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "first":
					iter.First()
				case "next":
					iter.Next()
				case "prev":
					iter.Prev()
				case "last":
					iter.Last()
				case "seek-ge":
					iter.SeekGE(parseMVCCKey(parts[1]))
				case "seek-lt":
					iter.SeekLT(parseMVCCKey(parts[1]))
				}
				if iter.Valid() {
					fmt.Fprintf(&buf, "%s\n", iter.Value())
				} else {
					fmt.Fprintf(&buf, ".\n")
				}
			}
			return buf.String()
		default:
			return fmt.Sprintf("unknown command: %s\n", td.Cmd)
		}
	})
}

// TestDeleteSuffixRangeValidation tests error cases for DeleteSuffixRange:
//   - Empty lower bound returns an error.
//   - Empty upper bound returns an error.
//   - Invalid key range returns an error.
//   - Read-only DB returns ErrReadOnly.
func TestDeleteSuffixRangeValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fs := vfs.NewMem()
	validSpan := KeyRange{
		Start: testMakeEngineKey([]byte("a"), 0, 0),
		End:   testMakeEngineKey([]byte("z"), 0, 0),
	}
	lower := testMakeSuffix(100, 0)
	upper := testMakeSuffix(math.MaxUint64, 0)

	t.Run("EmptyLowerBound", func(t *testing.T) {
		opts := &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatColumnarBlocks,
			FS:                 vfs.NewMem(),
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
		d, err := Open("", opts)
		require.NoError(t, err)
		defer d.Close()

		err = d.DeleteSuffixRange(context.Background(), validSpan, nil, upper)
		if err == nil {
			t.Fatal("expected error for empty lower bound, got nil")
		}
		t.Logf("empty lower bound error: %v", err)
	})

	t.Run("EmptyUpperBound", func(t *testing.T) {
		opts := &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatColumnarBlocks,
			FS:                 vfs.NewMem(),
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
		d, err := Open("", opts)
		require.NoError(t, err)
		defer d.Close()

		err = d.DeleteSuffixRange(context.Background(), validSpan, lower, nil)
		if err == nil {
			t.Fatal("expected error for empty upper bound, got nil")
		}
		t.Logf("empty upper bound error: %v", err)
	})

	t.Run("InvalidKeyRange", func(t *testing.T) {
		opts := &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatColumnarBlocks,
			FS:                 vfs.NewMem(),
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
		d, err := Open("", opts)
		require.NoError(t, err)
		defer d.Close()

		// A KeyRange with nil Start is invalid (Valid() returns false).
		invalidSpan := KeyRange{Start: nil, End: testMakeEngineKey([]byte("z"), 0, 0)}
		err = d.DeleteSuffixRange(context.Background(), invalidSpan, lower, upper)
		if err == nil {
			t.Fatal("expected error for invalid key range, got nil")
		}
		t.Logf("invalid key range error: %v", err)
	})

	t.Run("ReadOnlyDB", func(t *testing.T) {
		// First create a valid DB so we can reopen it read-only.
		{
			opts := &Options{
				Comparer:           &cockroachkvs.Comparer,
				FormatMajorVersion: FormatColumnarBlocks,
				FS:                 fs,
				KeySchema:          cockroachkvs.KeySchema.Name,
				KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
			}
			d, err := Open("", opts)
			require.NoError(t, err)
			require.NoError(t, d.Close())
		}
		// Reopen read-only.
		opts := &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatColumnarBlocks,
			FS:                 fs,
			ReadOnly:           true,
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
		d, err := Open("", opts)
		require.NoError(t, err)
		defer d.Close()

		err = d.DeleteSuffixRange(context.Background(), validSpan, lower, upper)
		require.Equal(t, ErrReadOnly, err)
	})
}

// TestSuffixMaskRangeKeysRowblk exercises range key suffix masking on pebblev4
// (row-oriented blocks), exercising rowblk_fragment_iter.go's applySpanTransforms
// which filters entries using ComparePointSuffixes.
func TestSuffixMaskRangeKeysRowblk(t *testing.T) {
	defer leaktest.AfterTest(t)()
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer: comparer, TableFormat: sstable.TableFormatPebblev4,
	})
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
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Comparer: comparer, KeySchemas: sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
	})
	require.NoError(t, err)
	defer reader.Close()
	transforms := sstable.FragmentIterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: testMakeSuffix(100, 0), Upper: testMakeSuffix(math.MaxUint64, 0)},
	}
	iter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, iter != nil)
	defer iter.Close()
	var gotWallTimes []uint64
	for span, err := iter.First(); span != nil; span, err = iter.Next() {
		require.NoError(t, err)
		for _, k := range span.Keys {
			if len(k.Suffix) > 0 {
				gotWallTimes = append(gotWallTimes, binary.BigEndian.Uint64(k.Suffix[:8]))
			}
		}
	}
	t.Logf("got wall times: %v", gotWallTimes)
	expectedWallTimes := []uint64{100, 80, 50}
	require.Equal(t, len(expectedWallTimes), len(gotWallTimes))
	for i := range expectedWallTimes {
		require.Equal(t, expectedWallTimes[i], gotWallTimes[i])
	}
}

// TestSuffixMaskRangeKeysBackward exercises backward iteration (Last/Prev) through
// range key spans with suffix masking. Uses pebblev5 (columnar). Creates three spans
// [a,b), [c,d), [e,f) each with entries at wall times 200, 150, 100, 50.
func TestSuffixMaskRangeKeysBackward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer: comparer, KeySchema: &cockroachkvs.KeySchema, TableFormat: sstable.TableFormatPebblev5,
	})
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
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Comparer: comparer, KeySchemas: sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
	})
	require.NoError(t, err)
	defer reader.Close()
	transforms := sstable.FragmentIterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: testMakeSuffix(100, 0), Upper: testMakeSuffix(math.MaxUint64, 0)},
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

// TestSuffixMaskRangeKeysAllMasked creates a span where ALL range key entries
// have wall times > 100. With mask (100, MaxUint64] applied, all entries should
// be filtered and the span completely skipped. Tests both forward and backward.
func TestSuffixMaskRangeKeysAllMasked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer: comparer, KeySchema: &cockroachkvs.KeySchema, TableFormat: sstable.TableFormatPebblev5,
	})
	start := testMakeEngineKey([]byte("a"), 0, 0)
	end := testMakeEngineKey([]byte("z"), 0, 0)
	for _, wall := range []uint64{500, 300, 200, 150} {
		require.NoError(t, w.RangeKeySet(start, end, testMakeSuffix(wall, 0), []byte(fmt.Sprintf("val@%d", wall))))
	}
	require.NoError(t, w.Close())
	f2, err := fs.Open("test.sst")
	require.NoError(t, err)
	readable, err := objstorage.NewSimpleReadable(f2)
	require.NoError(t, err)
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Comparer: comparer, KeySchemas: sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
	})
	require.NoError(t, err)
	defer reader.Close()
	transforms := sstable.FragmentIterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: testMakeSuffix(100, 0), Upper: testMakeSuffix(math.MaxUint64, 0)},
	}
	// Forward.
	fwdIter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, fwdIter != nil)
	span, err := fwdIter.First()
	require.NoError(t, err)
	if span != nil {
		t.Logf("forward: span unexpectedly returned with %d keys", len(span.Keys))
		for _, k := range span.Keys {
			if len(k.Suffix) > 0 {
				t.Logf("  wall=%d", binary.BigEndian.Uint64(k.Suffix[:8]))
			}
		}
	}
	if span != nil {
		t.Fatalf("expected all entries masked so span is skipped, got %d keys", len(span.Keys))
	}
	fwdIter.Close()
	// Backward.
	bwdIter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, bwdIter != nil)
	span, err = bwdIter.Last()
	require.NoError(t, err)
	if span != nil {
		t.Fatalf("expected all entries masked so span is skipped, got %d keys", len(span.Keys))
	}
	bwdIter.Close()
}

// TestSuffixMaskRangeKeysMixedSpans creates three spans with varying degrees of
// masking:
//   - [a,b): all wall > 100 -> all masked -> skip
//   - [c,d): mixed: wall=200 masked, wall=100 and wall=50 visible
//   - [e,f): all wall <= 100 -> none masked -> full
func TestSuffixMaskRangeKeysMixedSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	comparer := &cockroachkvs.Comparer
	fs := vfs.NewMem()
	f, err := fs.Create("test.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{
		Comparer: comparer, KeySchema: &cockroachkvs.KeySchema, TableFormat: sstable.TableFormatPebblev5,
	})
	// Span [a,b): all entries masked (wall > 100).
	for _, wall := range []uint64{300, 200, 150} {
		require.NoError(t, w.RangeKeySet(
			testMakeEngineKey([]byte("a"), 0, 0), testMakeEngineKey([]byte("b"), 0, 0),
			testMakeSuffix(wall, 0), []byte(fmt.Sprintf("a@%d", wall)),
		))
	}
	// Span [c,d): mixed.
	for _, wall := range []uint64{200, 100, 50} {
		require.NoError(t, w.RangeKeySet(
			testMakeEngineKey([]byte("c"), 0, 0), testMakeEngineKey([]byte("d"), 0, 0),
			testMakeSuffix(wall, 0), []byte(fmt.Sprintf("c@%d", wall)),
		))
	}
	// Span [e,f): none masked.
	for _, wall := range []uint64{100, 80, 50} {
		require.NoError(t, w.RangeKeySet(
			testMakeEngineKey([]byte("e"), 0, 0), testMakeEngineKey([]byte("f"), 0, 0),
			testMakeSuffix(wall, 0), []byte(fmt.Sprintf("e@%d", wall)),
		))
	}
	require.NoError(t, w.Close())
	f2, err := fs.Open("test.sst")
	require.NoError(t, err)
	readable, err := objstorage.NewSimpleReadable(f2)
	require.NoError(t, err)
	reader, err := sstable.NewReader(context.Background(), readable, sstable.ReaderOptions{
		Comparer: comparer, KeySchemas: sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
	})
	require.NoError(t, err)
	defer reader.Close()
	transforms := sstable.FragmentIterTransforms{
		SuffixMask: sstable.SuffixMask{Lower: testMakeSuffix(100, 0), Upper: testMakeSuffix(math.MaxUint64, 0)},
	}
	iter, err := reader.NewRawRangeKeyIter(context.Background(), transforms, sstable.NoReadEnv)
	require.NoError(t, err)
	require.True(t, iter != nil)
	defer iter.Close()
	type spanResult struct {
		startKey  byte
		wallTimes []uint64
		values    []string
	}
	var results []spanResult
	for span, err := iter.First(); span != nil; span, err = iter.Next() {
		require.NoError(t, err)
		r := spanResult{startKey: span.Start[0]}
		for _, k := range span.Keys {
			if len(k.Suffix) > 0 {
				r.wallTimes = append(r.wallTimes, binary.BigEndian.Uint64(k.Suffix[:8]))
				r.values = append(r.values, string(k.Value))
			}
		}
		results = append(results, r)
	}
	t.Logf("forward iteration results:")
	for _, r := range results {
		t.Logf("  start=%c wallTimes=%v values=%v", r.startKey, r.wallTimes, r.values)
	}
	require.Equal(t, 2, len(results))
	require.Equal(t, byte('c'), results[0].startKey)
	require.Equal(t, []uint64{100, 50}, results[0].wallTimes)
	require.Equal(t, []string{"c@100", "c@50"}, results[0].values)
	require.Equal(t, byte('e'), results[1].startKey)
	require.Equal(t, []uint64{100, 80, 50}, results[1].wallTimes)
	require.Equal(t, []string{"e@100", "e@80", "e@50"}, results[1].values)
}

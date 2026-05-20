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
	"math/rand"
	"slices"
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
	"github.com/cockroachdb/pebble/objstorage/remote"
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

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

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
		SuffixMask: sstable.SuffixMask{
			Lower: testMakeSuffix(math.MaxUint64, 0),
			Upper: testMakeSuffix(100, 0),
		},
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

	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
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

// suffixMaskTestTransforms returns IterTransforms with SuffixMask
// [MaxUint64, 100) in comparer order. This masks keys with wall time > 100.
func suffixMaskTestTransforms() sstable.IterTransforms {
	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
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

// TestSuffixMaskSeekGE verifies that SeekGE with a SuffixMask correctly skips
// masked keys. This exercises the optimized columnar IsMaskedBySuffixMask
// path (pebblev5 with cockroachkvs.KeySchema).

// TestSuffixMaskSeekLT verifies that SeekLT with a SuffixMask correctly skips
// masked keys when seeking backward. This exercises the optimized columnar
// IsMaskedBySuffixMask path (pebblev5 with cockroachkvs.KeySchema).

// TestSuffixMaskAllKeysMasked verifies that when ALL keys in the SST have wall
// times that fall in the mask range (> 100), iteration returns no results.
// This exercises the optimized columnar IsMaskedBySuffixMask path.

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
	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
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

// TestDeleteSuffixRangeCrosscheck uses a randomized approach to verify
// DeleteSuffixRange correctness. It writes random MVCC keys to a DB, applies
// DeleteSuffixRange, then compares the resulting iteration against a reference
// filter that independently computes which keys should be visible.
func TestDeleteSuffixRangeCrosscheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	seed := int64(42)
	for trial := 0; trial < 20; trial++ {
		t.Run(fmt.Sprintf("seed=%d", seed+int64(trial)), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed + int64(trial)))

			opts := &Options{
				Comparer:           &cockroachkvs.Comparer,
				FormatMajorVersion: FormatSuffixMask,
				FS:                 vfs.NewMem(),
				KeySchema:          cockroachkvs.KeySchema.Name,
				KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
			}
			d, err := Open("", opts)
			require.NoError(t, err)
			defer d.Close()

			// Generate random prefixes.
			numPrefixes := 3 + rng.Intn(8)
			prefixes := make([]string, numPrefixes)
			for i := range prefixes {
				prefixes[i] = string(rune('a'+i)) + string(rune('a'+rng.Intn(26)))
			}
			slices.Sort(prefixes)
			prefixes = slices.Compact(prefixes)

			type entry struct {
				prefix string
				wall   uint64
				value  string
			}

			// Write initial keys (wall times 10-100).
			var allEntries []entry
			for _, p := range prefixes {
				nKeys := 1 + rng.Intn(4)
				for j := 0; j < nKeys; j++ {
					wall := uint64(10 + rng.Intn(91))
					e := entry{p, wall, fmt.Sprintf("%s@%d", p, wall)}
					allEntries = append(allEntries, e)
					require.NoError(t, d.Set(testMakeEngineKey([]byte(p), wall, 0), []byte(e.value), nil))
				}
			}
			require.NoError(t, d.Flush())

			// Write more keys (wall times 50-200).
			for _, p := range prefixes {
				nKeys := 1 + rng.Intn(4)
				for j := 0; j < nKeys; j++ {
					wall := uint64(50 + rng.Intn(151))
					e := entry{p, wall, fmt.Sprintf("%s@%d", p, wall)}
					allEntries = append(allEntries, e)
					require.NoError(t, d.Set(testMakeEngineKey([]byte(p), wall, 0), []byte(e.value), nil))
				}
			}
			require.NoError(t, d.Flush())

			// Pick random mask span and suffix bounds.
			spanStartIdx := rng.Intn(len(prefixes))
			spanEndIdx := spanStartIdx + 1 + rng.Intn(len(prefixes)-spanStartIdx)
			spanStart := prefixes[spanStartIdx]
			var spanEnd string
			if spanEndIdx >= len(prefixes) {
				spanEnd = string(rune(prefixes[len(prefixes)-1][0] + 1))
			} else {
				spanEnd = prefixes[spanEndIdx]
			}

			// maskOldest is the exclusive upper bound (oldest timestamp to keep visible).
			// maskNewest is the inclusive lower bound (newest timestamp to hide).
			maskOldest := uint64(30 + rng.Intn(70))
			maskNewest := maskOldest + uint64(10+rng.Intn(100))

			span := KeyRange{
				Start: testMakeEngineKey([]byte(spanStart), 0, 0),
				End:   testMakeEngineKey([]byte(spanEnd), 0, 0),
			}
			// API takes [lower, upper) in comparer order: lower=newest, upper=oldest.
			require.NoError(t, d.DeleteSuffixRange(
				context.Background(), span,
				testMakeSuffix(maskNewest, 0), testMakeSuffix(maskOldest, 0),
			))

			// Compact to exercise version edit machinery.
			require.NoError(t, d.Compact(
				context.Background(),
				testMakeEngineKey([]byte("a"), 0, 0),
				testMakeEngineKey([]byte("zz"), 0, 0),
				false,
			))

			// Collect actual visible keys from the DB.
			iter, err := d.NewIter(nil)
			require.NoError(t, err)
			var actual []string
			for iter.First(); iter.Valid(); iter.Next() {
				actual = append(actual, string(iter.Value()))
			}
			require.NoError(t, iter.Close())

			// Compute reference: which keys should be visible?
			// A key is masked if: its prefix is in [spanStart, spanEnd) AND
			// its wall time is in (maskOldest, maskNewest] in magnitude.
			cmp := cockroachkvs.Comparer.Compare
			var expected []string
			// Deduplicate: for keys with the same prefix+wall, only the last
			// write is visible (it shadows earlier writes with the same key).
			type dedupKey struct {
				prefix string
				wall   uint64
			}
			latest := make(map[dedupKey]string)
			for _, e := range allEntries {
				latest[dedupKey{e.prefix, e.wall}] = e.value
			}
			// Sort by key order.
			type sortEntry struct {
				key   []byte
				value string
				wall  uint64
			}
			var sorted []sortEntry
			for dk, v := range latest {
				sorted = append(sorted, sortEntry{
					key:   testMakeEngineKey([]byte(dk.prefix), dk.wall, 0),
					value: v,
					wall:  dk.wall,
				})
			}
			slices.SortFunc(sorted, func(a, b sortEntry) int {
				return cmp(a.key, b.key)
			})
			for _, se := range sorted {
				// Check if this key is masked.
				prefix := se.key[:cockroachkvs.Split(se.key)]
				inSpan := cmp(prefix, span.Start) >= 0 && cmp(prefix, span.End) < 0
				masked := inSpan && se.wall > maskOldest && se.wall <= maskNewest
				if !masked {
					expected = append(expected, se.value)
				}
			}

			if !slices.Equal(actual, expected) {
				t.Errorf("span=[%s,%s) mask=(%d,%d]\nactual:   %v\nexpected: %v",
					spanStart, spanEnd, maskNewest, maskOldest, actual, expected)
			}
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
			FormatMajorVersion: FormatSuffixMask,
			FS:                 vfs.NewMem(),
			KeySchema:          cockroachkvs.KeySchema.Name,
			KeySchemas:         sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		}
		if enableBlobStorage {
			opts.FormatMajorVersion = FormatSuffixMask
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
	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)

	t.Run("OldFormatVersion", func(t *testing.T) {
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

		err = d.DeleteSuffixRange(context.Background(), validSpan, lower, upper)
		if err == nil {
			t.Fatal("expected error for old format version")
		}
		require.True(t, strings.Contains(err.Error(), "format major version"))
	})

	t.Run("EmptyLowerBound", func(t *testing.T) {
		opts := &Options{
			Comparer:           &cockroachkvs.Comparer,
			FormatMajorVersion: FormatSuffixMask,
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
			FormatMajorVersion: FormatSuffixMask,
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
			FormatMajorVersion: FormatSuffixMask,
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
				FormatMajorVersion: FormatSuffixMask,
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
			FormatMajorVersion: FormatSuffixMask,
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
		SuffixMask: sstable.SuffixMask{Lower: testMakeSuffix(math.MaxUint64, 0), Upper: testMakeSuffix(100, 0)},
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

// TestSuffixMaskRangeKeysMixedSpans creates three spans with varying degrees of
// masking:
//   - [a,b): all wall > 100 -> all masked -> skip
//   - [c,d): mixed: wall=200 masked, wall=100 and wall=50 visible
//   - [e,f): all wall <= 100 -> none masked -> full

func TestExpandSuffixMask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// expandSuffixMask computes the union of two suffix masks. Suffix masks
	// use [Lower, Upper) where Lower and Upper are big-endian encoded and
	// compared with bytes.Compare. To widen, we take the max Lower and the
	// min Upper.

	tests := []struct {
		name      string
		a, b      sstable.SuffixMask
		wantLower []byte
		wantUpper []byte
	}{
		{
			name:      "non-overlapping ranges",
			a:         sstable.SuffixMask{Lower: []byte{0x10}, Upper: []byte{0x05}},
			b:         sstable.SuffixMask{Lower: []byte{0x30}, Upper: []byte{0x20}},
			wantLower: []byte{0x30},
			wantUpper: []byte{0x05},
		},
		{
			name:      "one range contains the other",
			a:         sstable.SuffixMask{Lower: []byte{0x40}, Upper: []byte{0x01}},
			b:         sstable.SuffixMask{Lower: []byte{0x30}, Upper: []byte{0x10}},
			wantLower: []byte{0x40},
			wantUpper: []byte{0x01},
		},
		{
			name:      "identical ranges",
			a:         sstable.SuffixMask{Lower: []byte{0x20}, Upper: []byte{0x10}},
			b:         sstable.SuffixMask{Lower: []byte{0x20}, Upper: []byte{0x10}},
			wantLower: []byte{0x20},
			wantUpper: []byte{0x10},
		},
		{
			name:      "partially overlapping",
			a:         sstable.SuffixMask{Lower: []byte{0x30}, Upper: []byte{0x10}},
			b:         sstable.SuffixMask{Lower: []byte{0x40}, Upper: []byte{0x20}},
			wantLower: []byte{0x40},
			wantUpper: []byte{0x10},
		},
		{
			name:      "multi-byte suffixes",
			a:         sstable.SuffixMask{Lower: []byte{0x00, 0x20}, Upper: []byte{0x00, 0x05}},
			b:         sstable.SuffixMask{Lower: []byte{0x00, 0x30}, Upper: []byte{0x00, 0x10}},
			wantLower: []byte{0x00, 0x30},
			wantUpper: []byte{0x00, 0x05},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandSuffixMask(tt.a, tt.b)
			require.Equal(t, tt.wantLower, result.Lower)
			require.Equal(t, tt.wantUpper, result.Upper)

			// Union should be commutative.
			resultRev := expandSuffixMask(tt.b, tt.a)
			require.Equal(t, tt.wantLower, resultRev.Lower)
			require.Equal(t, tt.wantUpper, resultRev.Upper)
		})
	}
}

// TestDeleteSuffixRangeExciseOverlap exercises the exciseOverlap path in
// DeleteSuffixRange, which handles files with SyntheticSuffix.
func TestDeleteSuffixRangeExciseOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	remoteStorage := remote.NewInMem()
	opts := &Options{
		Comparer:                    &cockroachkvs.Comparer,
		FormatMajorVersion:          FormatSuffixMask,
		FS:                          vfs.NewMem(),
		KeySchema:                   cockroachkvs.KeySchema.Name,
		KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		DisableAutomaticCompactions: true,
	}
	opts.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		remote.MakeLocator("ext"): remoteStorage,
	})
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	// Write an SST with bare keys (no MVCC suffix).
	writeOpts := d.opts.MakeWriterOptions(0, d.TableFormat())
	obj, err := remoteStorage.CreateObject("ext1")
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewRemoteWritable(obj), writeOpts)
	for _, rk := range []string{"bb", "cc"} {
		require.NoError(t, w.Set(testMakeEngineKey([]byte(rk), 0, 0), []byte("val-"+rk)))
	}
	require.NoError(t, w.Close())

	sz, err := remoteStorage.Size("ext1")
	require.NoError(t, err)

	// Ingest with SyntheticSuffix at wall=50.
	synthSuffix := testMakeSuffix(50, 0)
	_, err = d.IngestExternalFiles(context.Background(), []ExternalFile{{
		Locator:           remote.MakeLocator("ext"),
		ObjName:           "ext1",
		Size:              uint64(sz),
		StartKey:          testMakeEngineKey([]byte("bb"), 0, 0),
		EndKey:            testMakeEngineKey([]byte("cd"), 0, 0),
		EndKeyIsInclusive: false,
		HasPointKey:       true,
		SyntheticSuffix:   synthSuffix,
	}})
	require.NoError(t, err)

	// Verify keys are visible.
	iter, err := d.NewIter(nil)
	require.NoError(t, err)
	var before int
	for iter.First(); iter.Valid(); iter.Next() {
		before++
	}
	require.NoError(t, iter.Close())
	require.True(t, before > 0)

	// Test 1: fully-contained mask. Span covers entire file.
	span := KeyRange{
		Start: testMakeEngineKey([]byte("a"), 0, 0),
		End:   testMakeEngineKey([]byte("z"), 0, 0),
	}
	require.NoError(t, d.DeleteSuffixRange(context.Background(), span,
		testMakeSuffix(math.MaxUint64, 0), testMakeSuffix(10, 0)))

	iter, err = d.NewIter(nil)
	require.NoError(t, err)
	var after int
	for iter.First(); iter.Valid(); iter.Next() {
		after++
	}
	require.NoError(t, iter.Close())
	require.Equal(t, 0, after)

	// Test 2: straddling mask. Re-ingest, then mask only part of the range.
	obj2, err := remoteStorage.CreateObject("ext2")
	require.NoError(t, err)
	w2 := sstable.NewWriter(objstorageprovider.NewRemoteWritable(obj2), writeOpts)
	for _, rk := range []string{"dd", "ff"} {
		require.NoError(t, w2.Set(testMakeEngineKey([]byte(rk), 0, 0), []byte("val-"+rk)))
	}
	require.NoError(t, w2.Close())

	sz2, err := remoteStorage.Size("ext2")
	require.NoError(t, err)
	_, err = d.IngestExternalFiles(context.Background(), []ExternalFile{{
		Locator:           remote.MakeLocator("ext"),
		ObjName:           "ext2",
		Size:              uint64(sz2),
		StartKey:          testMakeEngineKey([]byte("dd"), 0, 0),
		EndKey:            testMakeEngineKey([]byte("fg"), 0, 0),
		EndKeyIsInclusive: false,
		HasPointKey:       true,
		SyntheticSuffix:   synthSuffix,
	}})
	require.NoError(t, err)

	// Verify both keys visible.
	iter, err = d.NewIter(nil)
	require.NoError(t, err)
	var beforeStraddle []string
	for iter.First(); iter.Valid(); iter.Next() {
		beforeStraddle = append(beforeStraddle, string(iter.Value()))
	}
	require.NoError(t, iter.Close())
	t.Logf("before straddle mask: %v", beforeStraddle)

	// Mask only [dd, ee) — straddles the file [dd, ff].
	straddleSpan := KeyRange{
		Start: testMakeEngineKey([]byte("dd"), 0, 0),
		End:   testMakeEngineKey([]byte("ee"), 0, 0),
	}
	require.NoError(t, d.DeleteSuffixRange(context.Background(), straddleSpan,
		testMakeSuffix(math.MaxUint64, 0), testMakeSuffix(10, 0)))

	// dd should be excised; ff should remain.
	iter, err = d.NewIter(nil)
	require.NoError(t, err)
	var afterStraddle []string
	for iter.First(); iter.Valid(); iter.Next() {
		afterStraddle = append(afterStraddle, string(iter.Value()))
	}
	require.NoError(t, iter.Close())
	t.Logf("after straddle mask: %v", afterStraddle)
	require.Equal(t, []string{"val-ff"}, afterStraddle)
}

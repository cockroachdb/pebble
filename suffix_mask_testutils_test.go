// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Suffix-mask tests: shared helpers and conventions.
//
// All test files named suffix_mask*_test.go share a small set of helpers
// defined here. They embed two conventions used by every test:
//
//  1. MVCC-style engine keys are constructed by appending a CockroachDB-style
//     sentinel byte (0x00) to a raw key, then optionally appending a 9- or
//     13-byte MVCC suffix encoding a big-endian wall time and optional logical
//     tick. testMakeEngineKey and testMakeSuffix produce those encodings;
//     parseEngineKeyWallLogical inverts them.
//
//  2. SuffixMask{Lower, Upper} is a half-open interval in comparer order. For
//     cockroachkvs (and any MVCC encoding) larger wall times sort first, so
//     Lower carries the larger ("newest") wall time and Upper carries the
//     smaller ("oldest") wall time. suffixMaskTestTransforms returns the
//     standard mask used by several tests: hide every key with wall > 100.
//
// Helpers grouped by purpose:
//
//   - testMakeEngineKey, testMakeSuffix, parseEngineKeyWallLogical: MVCC key
//     encoding and decoding.
//   - suffixMaskTestEntry: a (roachKey, wall, value) triple consumed by the
//     SST/DB builders.
//   - suffixMaskTestSST: build a single cockroachkvs+pebblev5 SST and open
//     it for reading.
//   - suffixMaskTestTransforms: the canonical "hide wall > 100" mask.
//   - suffixMaskTestDB: open an in-memory DB with cockroachkvs options and
//     automatic compactions disabled.
//   - suffixMaskWriteIngestSST: write an SST suitable for ingestion into a
//     suffixMaskTestDB.
//   - suffixMaskCollectVisible: scan a DB and return the visible point-key
//     values in order.

package pebble

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/cockroachdb/crlib/testutils/require"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
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

// parseEngineKeyWallLogical decodes the MVCC suffix appended by
// testMakeEngineKey. Returns 0,0 for suffixless keys.
func parseEngineKeyWallLogical(k []byte) (wall uint64, logical uint32) {
	if len(k) == 0 {
		return 0, 0
	}
	suffixLen := int(k[len(k)-1])
	if suffixLen == 0 {
		return 0, 0
	}
	suffix := k[len(k)-suffixLen : len(k)-1]
	if len(suffix) >= 8 {
		wall = binary.BigEndian.Uint64(suffix[:8])
	}
	if len(suffix) >= 12 {
		logical = binary.BigEndian.Uint32(suffix[8:12])
	}
	return wall, logical
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

// suffixMaskTestTransforms returns IterTransforms with a single SuffixMask
// [MaxUint64, 100) in comparer order. This masks keys with wall time > 100.
func suffixMaskTestTransforms() sstable.IterTransforms {
	lower := testMakeSuffix(math.MaxUint64, 0)
	upper := testMakeSuffix(100, 0)
	return sstable.IterTransforms{
		SuffixMasks: []sstable.SuffixMask{{Lower: lower, Upper: upper}},
	}
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

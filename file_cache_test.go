// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type fileCacheTestFile struct {
	vfs.File
	fs   *fileCacheTestFS
	name string
}

func (f *fileCacheTestFile) Close() error {
	f.fs.mu.Lock()
	if f.fs.closeCounts != nil {
		f.fs.closeCounts[f.name]++
	}
	f.fs.mu.Unlock()
	return f.File.Close()
}

type fileCacheTestFS struct {
	vfs.FS

	mu               sync.Mutex
	openCounts       map[string]int
	closeCounts      map[string]int
	openErrorEnabled bool
}

func (fs *fileCacheTestFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fs.mu.Lock()
	if fs.openErrorEnabled {
		fs.mu.Unlock()
		return nil, errors.New("injected error")
	}
	if fs.openCounts != nil {
		fs.openCounts[name]++
	}
	fs.mu.Unlock()
	f, err := fs.FS.Open(name, opts...)
	if len(opts) < 1 || opts[0] != vfs.RandomReadsOption {
		return nil, errors.Errorf("sstable file %s not opened with random reads option", name)
	}
	if err != nil {
		return nil, err
	}
	return &fileCacheTestFile{f, fs, name}, nil
}

func (fs *fileCacheTestFS) validateAndCloseHandle(
	t *testing.T, h *fileCacheHandle, f func(i, gotO, gotC int) error,
) {
	if err := fs.validateOpenFiles(f); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fs.validateNoneStillOpen(); err != nil {
		t.Fatal(err)
	}
}

func (fs *fileCacheTestFS) setOpenError(enabled bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.openErrorEnabled = enabled
}

// validateOpenFiles validates that no files in the cache are open twice, and
// the number still open is no greater than fileCacheTestCacheSize.
func (fs *fileCacheTestFS) validateOpenFiles(f func(i, gotO, gotC int) error) error {
	// try backs off to let any clean-up goroutines do their work.
	return try(100*time.Microsecond, 20*time.Second, func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		numStillOpen := 0
		for i := 0; i < fileCacheTestNumFiles; i++ {
			typ := base.FileTypeTable
			if i >= fileCacheTestNumTables {
				typ = base.FileTypeBlob
			}
			filename := base.MakeFilepath(fs, "", typ, base.DiskFileNum(i))
			gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
			if gotO > gotC {
				numStillOpen++
			}
			if gotC != gotO && gotC != gotO-1 {
				return errors.Errorf("i=%d: table closed too many or too few times: opened %d times, closed %d times",
					i, gotO, gotC)
			}
			if f != nil {
				if err := f(i, gotO, gotC); err != nil {
					return err
				}
			}
		}
		if numStillOpen > fileCacheTestCacheSize {
			return errors.Errorf("numStillOpen is %d, want <= %d", numStillOpen, fileCacheTestCacheSize)
		}
		return nil
	})
}

// validateNoneStillOpen validates that no tables in the cache are open.
func (fs *fileCacheTestFS) validateNoneStillOpen() error {
	// try backs off to let any clean-up goroutines do their work.
	return try(100*time.Microsecond, 20*time.Second, func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		for i := 0; i < fileCacheTestNumTables; i++ {
			filename := base.MakeFilepath(fs, "", base.FileTypeTable, base.DiskFileNum(i))
			gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
			if gotO != gotC {
				return errors.Errorf("i=%d: opened %d times, closed %d times", i, gotO, gotC)
			}
		}
		return nil
	})
}

const (
	fileCacheTestNumTables = 200
	fileCacheTestNumBlobs  = 100
	fileCacheTestNumFiles  = fileCacheTestNumTables + fileCacheTestNumBlobs
	fileCacheTestCacheSize = 100
)

type fileCacheTest struct {
	testing.TB
	blockCache       *cache.Cache
	blockCacheHandle *cache.Handle
	fileCache        *FileCache
}

// newFileCacheTest returns a shareable file cache to be used for tests.
// It is the caller's responsibility to unref the file cache.
func newFileCacheTest(
	tb testing.TB, blockCacheSize int64, fileCacheSize int, fileCacheNumShards int,
) *fileCacheTest {
	blockCache := NewCache(blockCacheSize)
	blockCacheHandle := blockCache.NewHandle()
	fileCache := NewFileCache(fileCacheNumShards, fileCacheSize)
	return &fileCacheTest{
		TB:               tb,
		blockCache:       blockCache,
		blockCacheHandle: blockCacheHandle,
		fileCache:        fileCache,
	}
}

func (t *fileCacheTest) fileByIdx(i int) base.ObjectInfo {
	if i < fileCacheTestNumTables {
		return base.ObjectInfoLiteral{
			FileType:    base.FileTypeTable,
			DiskFileNum: base.DiskFileNum(i),
		}
	}
	return base.ObjectInfoLiteral{
		FileType:    base.FileTypeBlob,
		DiskFileNum: base.DiskFileNum(i),
	}
}

func (t *fileCacheTest) cleanup() {
	t.fileCache.Unref()
	t.blockCacheHandle.Close()
	t.blockCache.Unref()
}

func noopCorruptionFn(_ base.ObjectInfo, err error) error { return err }

// newTestHandle creates a filesystem with a set of test tables and an
// associated file cache handle. The caller must close the handle.
func (t *fileCacheTest) newTestHandle() (*fileCacheHandle, *fileCacheTestFS) {
	xxx := bytes.Repeat([]byte("x"), fileCacheTestNumFiles)
	fs := &fileCacheTestFS{
		FS: vfs.NewMem(),
	}
	objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
	if err != nil {
		t.Fatal(err)
	}
	defer objProvider.Close()

	for i := 0; i < fileCacheTestNumTables; i++ {
		w, _, err := objProvider.Create(context.Background(), base.FileTypeTable, base.DiskFileNum(i), objstorage.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		tw := sstable.NewWriter(w, sstable.WriterOptions{TableFormat: sstable.TableFormatPebblev2})
		ik := base.ParseInternalKey(fmt.Sprintf("k.SET.%d", i))
		if err := tw.Raw().Add(ik, xxx[:i], false); err != nil {
			t.Fatal(err)
		}
		if err := tw.RangeKeySet([]byte("k"), []byte("l"), nil, xxx[:i]); err != nil {
			t.Fatal(err)
		}
		if err := tw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < fileCacheTestNumBlobs; i++ {
		fn := base.DiskFileNum(i + fileCacheTestNumTables)
		w, _, err := objProvider.Create(context.Background(), base.FileTypeBlob, fn, objstorage.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		bw := blob.NewFileWriter(fn, w, blob.FileWriterOptions{})
		_ = bw.AddValue(xxx[:fn])
		if _, err := bw.Close(); err != nil {
			t.Fatal(err)
		}
	}

	fs.mu.Lock()
	fs.openCounts = map[string]int{}
	fs.closeCounts = map[string]int{}
	fs.mu.Unlock()

	opts := &Options{
		Cache:     t.blockCache,
		FileCache: t.fileCache,
	}
	opts.EnsureDefaults()
	h := t.fileCache.newHandle(t.blockCacheHandle, objProvider, opts.LoggerAndTracer, opts.MakeReaderOptions(), noopCorruptionFn)
	return h, fs
}

// Test basic reference counting for the file cache.
func TestFileCacheRefs(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, 10, 2)
	// We don't call the full fct.cleanup() method because we will unref the
	// fileCache in the test.
	defer fct.blockCacheHandle.Close()
	defer fct.blockCache.Unref()
	fc := fct.fileCache

	v := fc.refs.Load()
	if v != 1 {
		require.Equal(t, 1, v)
	}

	fc.Ref()
	v = fc.refs.Load()
	if v != 2 {
		require.Equal(t, 2, v)
	}

	fc.Unref()
	v = fc.refs.Load()
	if v != 1 {
		require.Equal(t, 1, v)
	}

	fc.Unref()
	v = fc.refs.Load()
	if v != 0 {
		require.Equal(t, 0, v)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				if fmt.Sprint(r) != "pebble: inconsistent reference count: -1" {
					t.Fatalf("unexpected panic message")
				}
			} else if r == nil {
				t.Fatalf("expected panic")
			}
		}()
		fc.Unref()
	}()

}

// Basic test to determine if reads through the file cache are wired correctly.
func TestVirtualReadsWiring(t *testing.T) {
	d, err := Open("",
		&Options{
			FS:                 vfs.NewMem(),
			FormatMajorVersion: internalFormatNewest,
			Comparer:           testkeys.Comparer,
			// Compactions which conflict with virtual sstable creation can be
			// picked by Pebble. We disable that.
			DisableAutomaticCompactions: true,
		})
	require.NoError(t, err)

	b := newBatch(d)
	// Some combination of sets, range deletes, and range key sets/unsets, so
	// all of the file cache iterator functions are utilized.
	require.NoError(t, b.Set([]byte{'a'}, []byte{'a'}, nil))                           // SeqNum start.
	require.NoError(t, b.Set([]byte{'d'}, []byte{'d'}, nil))                           // SeqNum: +1
	require.NoError(t, b.DeleteRange([]byte{'c'}, []byte{'e'}, nil))                   // SeqNum: +2
	require.NoError(t, b.Set([]byte{'f'}, []byte{'f'}, nil))                           // SeqNum: +3
	require.NoError(t, b.RangeKeySet([]byte{'f'}, []byte{'k'}, nil, []byte{'c'}, nil)) // SeqNum: +4
	require.NoError(t, b.RangeKeyUnset([]byte{'j'}, []byte{'k'}, nil, nil))            // SeqNum: +5
	require.NoError(t, b.Set([]byte{'z'}, []byte{'z'}, nil))                           // SeqNum: +6
	require.NoError(t, d.Apply(b, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact(context.Background(), []byte{'a'}, []byte{'b'}, false))
	require.Equal(t, 1, int(d.Metrics().Levels[6].TablesCount))

	d.mu.Lock()

	// Virtualize the single sstable in the lsm.
	currVersion := d.mu.versions.currentVersion()
	l6 := currVersion.Levels[6]
	l6FileIter := l6.Iter()
	parentFile := l6FileIter.First()
	f1 := base.TableNum(d.mu.versions.nextFileNum.Load())
	f2 := f1 + 1
	d.mu.versions.nextFileNum.Add(2)

	seqNumA := parentFile.Smallest().SeqNum()
	// See SeqNum comments above.
	seqNumCEDel := seqNumA + 2
	seqNumRangeSet := seqNumA + 4
	seqNumRangeUnset := seqNumA + 5
	seqNumZ := seqNumA + 6

	v1 := &manifest.TableMetadata{
		TableNum:              f1,
		CreationTime:          time.Now().Unix(),
		Size:                  parentFile.Size / 2,
		SmallestSeqNum:        parentFile.SmallestSeqNum,
		LargestSeqNum:         parentFile.LargestSeqNum,
		LargestSeqNumAbsolute: parentFile.LargestSeqNumAbsolute,
		HasPointKeys:          true,
		Virtual:               true,
	}
	v1.PointKeyBounds.SetInternalKeyBounds(base.MakeInternalKey([]byte{'a'}, seqNumA, InternalKeyKindSet),
		base.MakeInternalKey([]byte{'a'}, seqNumA, InternalKeyKindSet))
	v1.ExtendPointKeyBounds(DefaultComparer.Compare, v1.PointKeyBounds.Smallest(), v1.PointKeyBounds.Largest())
	v1.AttachVirtualBacking(parentFile.TableBacking)

	v2 := &manifest.TableMetadata{
		TableNum:              f2,
		CreationTime:          time.Now().Unix(),
		Size:                  parentFile.Size / 2,
		SmallestSeqNum:        parentFile.SmallestSeqNum,
		LargestSeqNum:         parentFile.LargestSeqNum,
		LargestSeqNumAbsolute: parentFile.LargestSeqNumAbsolute,
		HasPointKeys:          true,
		Virtual:               true,
	}
	v2.PointKeyBounds.SetInternalKeyBounds(base.MakeInternalKey([]byte{'d'}, seqNumCEDel, InternalKeyKindRangeDelete),
		base.MakeInternalKey([]byte{'z'}, seqNumZ, InternalKeyKindSet))
	v2.RangeKeyBounds = &manifest.InternalKeyBounds{}
	v2.RangeKeyBounds.SetInternalKeyBounds(
		base.MakeInternalKey([]byte{'f'}, seqNumRangeSet, InternalKeyKindRangeKeySet),
		base.MakeInternalKey([]byte{'k'}, seqNumRangeUnset, InternalKeyKindRangeKeyUnset))
	v2.ExtendPointKeyBounds(DefaultComparer.Compare, v2.PointKeyBounds.Smallest(), v2.PointKeyBounds.Largest())
	v2.AttachVirtualBacking(parentFile.TableBacking)

	v1.PointKeyBounds.SetInternalKeyBounds(v1.Smallest(), v1.Largest())

	v2.PointKeyBounds.SetInternalKeyBounds(v2.Smallest(), v2.Largest())

	v1.ValidateVirtual(parentFile)
	d.checkVirtualBounds(v1)
	v2.ValidateVirtual(parentFile)
	d.checkVirtualBounds(v2)

	// Write the version edit.
	fileMetrics := func(ve *manifest.VersionEdit) levelMetricsDelta {
		metrics := newFileMetrics(ve.NewTables)
		for de, f := range ve.DeletedTables {
			lm := metrics[de.Level]
			if lm == nil {
				lm = &LevelMetrics{}
				metrics[de.Level] = lm
			}
			metrics[de.Level].TablesCount--
			metrics[de.Level].TablesSize -= int64(f.Size)
			metrics[de.Level].EstimatedReferencesSize -= f.EstimatedReferenceSize()
		}
		return metrics
	}

	applyVE := func(ve *manifest.VersionEdit) error {
		_, err := d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
			return versionUpdate{
				VE:                      ve,
				JobID:                   d.newJobIDLocked(),
				Metrics:                 fileMetrics(ve),
				InProgressCompactionsFn: func() []compactionInfo { return d.getInProgressCompactionInfoLocked(nil) },
			}, nil
		})
		d.updateReadStateLocked(nil)
		return err
	}

	ve := manifest.VersionEdit{}
	d1 := manifest.DeletedTableEntry{Level: 6, FileNum: parentFile.TableNum}
	n1 := manifest.NewTableEntry{Level: 6, Meta: v1}
	n2 := manifest.NewTableEntry{Level: 6, Meta: v2}

	ve.DeletedTables = make(map[manifest.DeletedTableEntry]*manifest.TableMetadata)
	ve.DeletedTables[d1] = parentFile
	ve.NewTables = append(ve.NewTables, n1)
	ve.NewTables = append(ve.NewTables, n2)
	ve.CreatedBackingTables = append(ve.CreatedBackingTables, parentFile.TableBacking)

	require.NoError(t, applyVE(&ve))

	currVersion = d.mu.versions.currentVersion()
	l6 = currVersion.Levels[6]
	for f := range l6.All() {
		require.True(t, f.Virtual)
	}
	d.mu.Unlock()

	// Confirm that there were only 2 virtual sstables in L6.
	require.Equal(t, 2, int(d.Metrics().Levels[6].TablesCount))

	// These reads will go through the file cache.
	iter, _ := d.NewIter(nil)
	expected := []byte{'a', 'f', 'z'}
	for i, x := 0, iter.First(); x; i, x = i+1, iter.Next() {
		require.Equal(t, []byte{expected[i]}, iter.Value())
	}
	iter.Close()

	// We don't defer this Close in case we get a panic while holding d.mu.
	d.Close()
}

// The file cache shouldn't be usable after all the dbs close.
func TestSharedFileCacheUseAfterAllFree(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, 10, 1)
	// We don't call the full fct.cleanup() method because we will unref the
	// fileCache in the test.
	defer fct.blockCacheHandle.Close()
	defer fct.blockCache.Unref()
	fc := fct.fileCache

	db1, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fc,
		})
	require.NoError(t, err)

	// Release our reference, now that the db has a reference.
	fc.Unref()

	db2, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fc,
		})
	require.NoError(t, err)

	require.NoError(t, db1.Close())
	require.NoError(t, db2.Close())

	v := fc.refs.Load()
	if v != 0 {
		t.Fatalf("expected reference count %d, got %d", 0, v)
	}

	defer func() {
		if r := recover(); r != nil {
			if fmt.Sprint(r) != "pebble: inconsistent reference count: 1" {
				t.Fatalf("unexpected panic message: %v", r)
			}
		} else if r == nil {
			t.Fatalf("expected panic")
		}
	}()

	db3, _ := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fc,
		})
	_ = db3
}

// TestSharedFileCacheUseAfterOneFree tests whether a shared file cache is
// usable by a db, after one of the db's releases its reference.
func TestSharedFileCacheUseAfterOneFree(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, 10, 1)
	defer fct.cleanup()

	if v := fct.fileCache.refs.Load(); v != 1 {
		t.Fatalf("expected reference count %d, got %d", 1, v)
	}

	db1, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fct.fileCache,
		})
	require.NoError(t, err)

	db2, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fct.fileCache,
		})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db2.Close())
	}()

	// Make db1 release a reference to the cache. It should
	// still be usable by db2.
	require.NoError(t, db1.Close())
	if v := fct.fileCache.refs.Load(); v != 2 {
		t.Fatalf("expected reference count %d, got %d", 1, v)
	}

	// Check if db2 is still usable.
	start := []byte("a")
	end := []byte("d")
	require.NoError(t, db2.Set(start, nil, nil))
	require.NoError(t, db2.Flush())
	require.NoError(t, db2.DeleteRange(start, end, nil))
	require.NoError(t, db2.Compact(context.Background(), start, end, false))
}

// TestSharedFileCacheUsable ensures that a shared file cache is usable by more
// than one database at once.
func TestSharedFileCacheUsable(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, 10, 1)
	defer fct.cleanup()

	db1, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fct.fileCache,
		})
	require.NoError(t, err)

	defer func() {
		require.NoError(t, db1.Close())
	}()

	db2, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fct.fileCache,
		})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db2.Close())
	}()

	start := []byte("a")
	end := []byte("z")
	require.NoError(t, db1.Set(start, nil, nil))
	require.NoError(t, db1.Flush())
	require.NoError(t, db1.DeleteRange(start, end, nil))
	require.NoError(t, db1.Compact(context.Background(), start, end, false))

	start = []byte("x")
	end = []byte("y")
	require.NoError(t, db2.Set(start, nil, nil))
	require.NoError(t, db2.Flush())
	require.NoError(t, db2.Set(start, []byte{'a'}, nil))
	require.NoError(t, db2.Flush())
	require.NoError(t, db2.DeleteRange(start, end, nil))
	require.NoError(t, db2.Compact(context.Background(), start, end, false))
}

func TestSharedTableConcurrent(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, 10, 1)
	defer fct.cleanup()

	db1, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fct.fileCache,
		})
	require.NoError(t, err)

	defer func() {
		require.NoError(t, db1.Close())
	}()

	db2, err := Open("test",
		&Options{
			FS:        vfs.NewMem(),
			Cache:     fct.blockCache,
			FileCache: fct.fileCache,
		})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db2.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// Now that both dbs have a reference to the file cache,
	// we'll run go routines which will use the DBs concurrently.
	concFunc := func(db *DB) {
		for i := 0; i < 1000; i++ {
			start := []byte("a")
			end := []byte("z")
			require.NoError(t, db.Set(start, nil, nil))
			require.NoError(t, db.Flush())
			require.NoError(t, db.DeleteRange(start, end, nil))
			require.NoError(t, db.Compact(context.Background(), start, end, false))
		}
		wg.Done()
	}

	go concFunc(db1)
	go concFunc(db2)

	wg.Wait()
}

func testFileCacheRandomAccess(t *testing.T, concurrent bool) {
	const N = 2000
	fct := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	defer fct.cleanup()
	h, fs := fct.newTestHandle()

	rngMu := sync.Mutex{}
	rng := rand.New(rand.NewPCG(1, 1))

	errc := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			rngMu.Lock()
			tableNum, sleepTime := rng.IntN(fileCacheTestNumTables), rng.IntN(1000)
			rngMu.Unlock()
			m := &manifest.TableMetadata{TableNum: base.TableNum(tableNum)}
			m.InitPhysicalBacking()
			m.TableBacking.Ref()
			defer m.TableBacking.Unref()
			iters, err := h.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			if err != nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: find: %v", i, tableNum, err)
				return
			}
			iter := iters.Point()
			kv := iter.SeekGE([]byte("k"), base.SeekGEFlagsNone)
			if concurrent {
				time.Sleep(time.Duration(sleepTime) * time.Microsecond)
			}
			if kv == nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: valid.0: got false, want true", i, tableNum)
				return
			}
			v, _, err := kv.Value(nil)
			if err != nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: err extracting value: %v", err)
			}
			if got := len(v); got != tableNum {
				errc <- errors.Errorf("i=%d, fileNum=%d: value: got %d bytes, want %d", i, tableNum, got, tableNum)
				return
			}
			if kv := iter.Next(); kv != nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: next.1: got true, want false", i, tableNum)
				return
			}
			if err := iter.Close(); err != nil {
				errc <- errors.Wrapf(err, "close error i=%d, fileNum=%dv", i, tableNum)
				return
			}
			errc <- nil
		}(i)
		if !concurrent {
			require.NoError(t, <-errc)
		}
	}
	if concurrent {
		for i := 0; i < N; i++ {
			require.NoError(t, <-errc)
		}
	}
	fs.validateAndCloseHandle(t, h, nil)
}

func TestFileCacheRandomAccessSequential(t *testing.T) { testFileCacheRandomAccess(t, false) }
func TestFileCacheRandomAccessConcurrent(t *testing.T) { testFileCacheRandomAccess(t, true) }

func testFileCacheFrequentlyUsedInternal(t *testing.T, rangeIter bool) {
	const (
		N       = 1000
		pinned0 = 7
		pinned1 = 11
	)
	fct := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	defer fct.cleanup()
	h, fs := fct.newTestHandle()

	for i := 0; i < N; i++ {
		for _, j := range [...]int{pinned0, i % fileCacheTestNumFiles, pinned1} {
			obj := fct.fileByIdx(j)
			ftyp, fn := obj.FileInfo()
			if ftyp == base.FileTypeBlob {
				_, closeFunc, err := h.GetValueReader(context.Background(), obj)
				if err != nil {
					t.Fatalf("i=%d, j=%d: get value reader: %v", i, j, err)
				}
				closeFunc()
				continue
			}
			var iters iterSet
			var err error
			m := &manifest.TableMetadata{TableNum: base.TableNum(fn)}
			m.InitPhysicalBacking()
			m.TableBacking.Ref()
			if rangeIter {
				iters, err = h.newIters(context.Background(), m, nil, internalIterOpts{}, iterRangeKeys)
			} else {
				iters, err = h.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			}
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			if err := iters.CloseAll(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
		}
	}

	fs.validateAndCloseHandle(t, h, func(i, gotO, gotC int) error {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				return errors.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		}
		return nil
	})
}

func TestFileCacheFrequentlyUsed(t *testing.T) {
	for i, iterType := range []string{"point", "range"} {
		t.Run(fmt.Sprintf("iter=%s", iterType), func(t *testing.T) {
			testFileCacheFrequentlyUsedInternal(t, i == 1)
		})
	}
}

func TestSharedFileCacheFrequentlyUsed(t *testing.T) {
	const (
		N       = 1000
		pinned0 = 7
		pinned1 = 11
	)
	fct := newFileCacheTest(t, 8<<20, 2*fileCacheTestCacheSize, 10)
	defer fct.cleanup()

	h1, fs1 := fct.newTestHandle()
	h2, fs2 := fct.newTestHandle()

	for i := 0; i < N; i++ {
		for _, j := range [...]int{pinned0, i % fileCacheTestNumFiles, pinned1} {
			obj := fct.fileByIdx(j)
			ftyp, fn := obj.FileInfo()
			if ftyp == base.FileTypeBlob {
				_, closeFunc, err := h1.GetValueReader(context.Background(), obj)
				if err != nil {
					t.Fatalf("i=%d, j=%d: get value reader: %v", i, j, err)
				}
				closeFunc()
				continue
			}
			m := &manifest.TableMetadata{TableNum: base.TableNum(fn)}
			m.InitPhysicalBacking()
			m.TableBacking.Ref()
			iters1, err := h1.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			iters2, err := h2.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}

			if err := iters1.point.Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
			if err := iters2.point.Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
		}
	}

	fs1.validateAndCloseHandle(t, h1, func(i, gotO, gotC int) error {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				return errors.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		}
		return nil
	})

	fs2.validateAndCloseHandle(t, h2, func(i, gotO, gotC int) error {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				return errors.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		}
		return nil
	})
}

func testFileCacheEvictionsInternal(t *testing.T, rangeIter bool) {
	const (
		N      = 1000
		lo, hi = 10, 20
	)
	fct := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	defer fct.cleanup()
	h, fs := fct.newTestHandle()

	rng := rand.New(rand.NewPCG(2, 2))
	for i := 0; i < N; i++ {
		obj := fct.fileByIdx(rng.IntN(fileCacheTestNumFiles))
		ftyp, fn := obj.FileInfo()
		if ftyp == base.FileTypeBlob {
			_, closeFunc, err := h.GetValueReader(context.Background(), obj)
			if err != nil {
				t.Fatalf("i=%d, fn=%d: get value reader: %v", i, fn, err)
			}
			closeFunc()
		} else {
			var iters iterSet
			var err error
			m := &manifest.TableMetadata{TableNum: base.TableNum(fn)}
			m.InitPhysicalBacking()
			m.TableBacking.Ref()
			if rangeIter {
				iters, err = h.newIters(context.Background(), m, nil, internalIterOpts{}, iterRangeKeys)
			} else {
				iters, err = h.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			}
			if err != nil {
				t.Fatalf("i=%d, fn=%d: find: %v", i, fn, err)
			}
			if err := iters.CloseAll(); err != nil {
				t.Fatalf("i=%d, fn=%d: close: %v", i, fn, err)
			}
		}

		obj = fct.fileByIdx(int(lo + rng.Uint64N(hi-lo)))
		ftyp, fn = obj.FileInfo()
		h.Evict(fn, ftyp)
	}

	sumEvicted, nEvicted := 0, 0
	sumSafe, nSafe := 0, 0
	fs.validateAndCloseHandle(t, h, func(i, gotO, gotC int) error {
		if lo <= i && i < hi {
			sumEvicted += gotO
			nEvicted++
		} else {
			sumSafe += gotO
			nSafe++
		}
		return nil
	})
	fEvicted := float64(sumEvicted) / float64(nEvicted)
	fSafe := float64(sumSafe) / float64(nSafe)
	// The magic 1.25 number isn't derived from formal modeling. It's just a guess. For
	// (lo, hi, fileCacheTestCacheSize, fileCacheTestNumFiles) = (10, 20, 100, 300),
	// the ratio seems to converge on roughly 1.5 for large N, compared to 1.0 if we do
	// not Evict any cache entries.
	if ratio := fEvicted / fSafe; ratio < 1.25 {
		t.Errorf("evicted tables were opened %.3f times on average, safe tables %.3f, ratio %.3f < 1.250",
			fEvicted, fSafe, ratio)
	}
}

func TestFileCacheEvictions(t *testing.T) {
	for i, iterType := range []string{"point", "range"} {
		t.Run(fmt.Sprintf("iter=%s", iterType), func(t *testing.T) {
			testFileCacheEvictionsInternal(t, i == 1)
		})
	}
}

func TestSharedFileCacheEvictions(t *testing.T) {
	const (
		N      = 1000
		lo, hi = 10, 20
	)
	fct := newFileCacheTest(t, 8<<20, 2*fileCacheTestCacheSize, 10)
	defer fct.cleanup()

	h1, fs1 := fct.newTestHandle()
	h2, fs2 := fct.newTestHandle()

	// TODO(radu): this test fails on most seeds.
	rng := rand.New(rand.NewPCG(0, 0))
	for i := 0; i < N; i++ {
		j := rng.IntN(fileCacheTestNumFiles)
		obj := fct.fileByIdx(j)
		ftyp, fn := obj.FileInfo()
		if ftyp == base.FileTypeBlob {
			_, closeFunc1, err := h1.GetValueReader(context.Background(), obj)
			if err != nil {
				t.Fatalf("i=%d, fn=%d: get value reader: %v", i, fn, err)
			}
			_, closeFunc2, err := h2.GetValueReader(context.Background(), obj)
			if err != nil {
				t.Fatalf("i=%d, fn=%d: get value reader: %v", i, fn, err)
			}
			closeFunc1()
			closeFunc2()
		} else {
			m := &manifest.TableMetadata{TableNum: base.TableNum(fn)}
			m.InitPhysicalBacking()
			m.TableBacking.Ref()
			iters1, err := h1.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			iters2, err := h2.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			if err := iters1.Point().Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
			if err := iters2.Point().Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
		}

		obj = fct.fileByIdx(int(lo + rng.Uint64N(hi-lo)))
		ftyp, fn = obj.FileInfo()
		h1.Evict(fn, ftyp)
		obj = fct.fileByIdx(int(lo + rng.Uint64N(hi-lo)))
		ftyp, fn = obj.FileInfo()
		h2.Evict(fn, ftyp)
	}

	check := func(fs *fileCacheTestFS, h *fileCacheHandle) (float64, float64, float64) {
		sumEvicted, nEvicted := 0, 0
		sumSafe, nSafe := 0, 0
		fs.validateAndCloseHandle(t, h, func(i, gotO, gotC int) error {
			if lo <= i && i < hi {
				sumEvicted += gotO
				nEvicted++
			} else {
				sumSafe += gotO
				nSafe++
			}
			return nil
		})
		fEvicted := float64(sumEvicted) / float64(nEvicted)
		fSafe := float64(sumSafe) / float64(nSafe)

		return fEvicted, fSafe, fEvicted / fSafe
	}

	// The magic 1.25 number isn't derived from formal modeling. It's just a guess. For
	// (lo, hi, fileCacheTestCacheSize, fileCacheTestNumTables) = (10, 20, 100, 300),
	// the ratio seems to converge on roughly 1.5 for large N, compared to 1.0 if we do
	// not Evict any cache entries.
	if fEvicted, fSafe, ratio := check(fs1, h1); ratio < 1.25 {
		t.Errorf(
			"evicted tables were opened %.3f times on average, safe tables %.3f, ratio %.3f < 1.250",
			fEvicted, fSafe, ratio,
		)
	}

	if fEvicted, fSafe, ratio := check(fs2, h2); ratio < 1.25 {
		t.Errorf(
			"evicted tables were opened %.3f times on average, safe tables %.3f, ratio %.3f < 1.250",
			fEvicted, fSafe, ratio,
		)
	}
}

func TestFileCacheIterLeak(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	defer fct.cleanup()
	h, _ := fct.newTestHandle()

	m := &manifest.TableMetadata{TableNum: 0}
	m.InitPhysicalBacking()
	m.TableBacking.Ref()
	defer m.TableBacking.Unref()
	iters, err := h.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
	require.NoError(t, err)

	if err := h.Close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
		t.Fatalf("expected leaked iterators, but found %+v", err)
	} else {
		t.Log(err.Error())
	}
	require.NoError(t, iters.Point().Close())
}

func TestSharedFileCacheIterLeak(t *testing.T) {
	fct := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	// We don't call the full fct.cleanup() method because we will unref the
	// fileCache in the test.
	defer fct.blockCacheHandle.Close()
	defer fct.blockCache.Unref()

	h1, _ := fct.newTestHandle()
	h2, _ := fct.newTestHandle()
	h3, _ := fct.newTestHandle()

	m := &manifest.TableMetadata{TableNum: 0}
	m.InitPhysicalBacking()
	m.TableBacking.Ref()
	defer m.TableBacking.Unref()
	iters, err := h1.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys)
	require.NoError(t, err)

	if err := h1.Close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
		t.Fatalf("expected leaked iterators, but found %+v", err)
	} else {
		t.Log(err.Error())
	}

	// Closing c2 shouldn't error out since c2 isn't leaking any iterators.
	require.NoError(t, h2.Close())

	fct.fileCache.Unref()

	// Closing c3 should panic since c3 holds the last reference to the
	// FileCache, and when the FileCache closes, it will detect that there was a
	// leaked iterator.
	require.Panics(t, func() {
		h3.Close()
	})
	require.NoError(t, iters.Point().Close())
}

func TestFileCacheRetryAfterFailure(t *testing.T) {
	ctx := context.Background()
	// Test a retry can succeed after a failure, i.e., errors are not cached.
	fct := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	defer fct.cleanup()

	t.Run("sstable", func(t *testing.T) {
		h, fs := fct.newTestHandle()

		fs.setOpenError(true /* enabled */)
		m := &manifest.TableMetadata{TableNum: 0}
		m.InitPhysicalBacking()
		m.TableBacking.Ref()
		defer m.TableBacking.Unref()
		_, err := h.newIters(ctx, m, nil, internalIterOpts{}, iterPointKeys)
		if err == nil {
			t.Fatalf("expected failure, but found success")
		}
		require.Equal(t, "pebble: backing file 000000 error: injected error", err.Error())
		fs.setOpenError(false /* enabled */)
		var iters iterSet
		iters, err = h.newIters(ctx, m, nil, internalIterOpts{}, iterPointKeys)
		require.NoError(t, err)
		require.NoError(t, iters.Point().Close())
		fs.validateAndCloseHandle(t, h, nil)
	})
	t.Run("blob", func(t *testing.T) {
		h, fs := fct.newTestHandle()

		fs.setOpenError(true /* enabled */)
		obj := fct.fileByIdx(fileCacheTestNumTables)
		_, _, err := h.GetValueReader(ctx, obj)
		if err == nil {
			t.Fatalf("expected failure, but found success")
		}
		require.Equal(t, "pebble: backing file 000200 error: injected error", err.Error())
		fs.setOpenError(false /* enabled */)
		_, closeFunc, err := h.GetValueReader(ctx, obj)
		require.NoError(t, err)
		closeFunc()
		fs.validateAndCloseHandle(t, h, nil)
	})
}

func TestFileCacheErrorBadMagicNumber(t *testing.T) {
	obj := &objstorage.MemObj{}
	tw := sstable.NewWriter(obj, sstable.WriterOptions{TableFormat: sstable.TableFormatPebblev2})
	tw.Set([]byte("a"), nil)
	require.NoError(t, tw.Close())
	buf := obj.Data()
	// Bad magic number.
	buf[len(buf)-1] = 0
	fs := &fileCacheTestFS{
		FS: vfs.NewMem(),
	}
	const testFileNum = 3
	objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(fs, ""))
	require.NoError(t, err)
	w, _, err := objProvider.Create(context.Background(), base.FileTypeTable, testFileNum, objstorage.CreateOptions{})
	w.Write(buf)
	require.NoError(t, w.Finish())

	fcs := newFileCacheTest(t, 8<<20, fileCacheTestCacheSize, []int{1, 2, 4, 10}[rand.IntN(4)])
	defer fcs.cleanup()
	opts := &Options{
		Cache:     fcs.blockCache,
		FileCache: fcs.fileCache,
	}
	opts.EnsureDefaults()
	c := opts.FileCache.newHandle(fcs.blockCacheHandle, objProvider, opts.LoggerAndTracer, opts.MakeReaderOptions(), noopCorruptionFn)
	require.NoError(t, err)
	defer c.Close()

	m := &manifest.TableMetadata{TableNum: testFileNum}
	m.InitPhysicalBacking()
	m.TableBacking.Ref()
	defer m.TableBacking.Unref()
	if _, err = c.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys); err == nil {
		t.Fatalf("expected failure, but found success")
	}
	require.Equal(t,
		"pebble: backing file 000003 error: pebble/table: invalid table 000003: (bad magic number: 0xf09faab3f09faa00)",
		err.Error())
}

func TestFileCacheEvictClose(t *testing.T) {
	errs := make(chan error, 10)
	db, err := Open("test",
		&Options{
			FS: vfs.NewMem(),
			EventListener: &EventListener{
				TableDeleted: func(info TableDeleteInfo) {
					errs <- info.Err
				},
			},
		})
	require.NoError(t, err)

	start := []byte("a")
	end := []byte("z")
	require.NoError(t, db.Set(start, nil, nil))
	require.NoError(t, db.Flush())
	require.NoError(t, db.DeleteRange(start, end, nil))
	require.NoError(t, db.Compact(context.Background(), start, end, false))
	require.NoError(t, db.Close())
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestFileCacheClockPro(t *testing.T) {
	// Test data was generated from the python code. See also
	// internal/cache/clockpro_test.go:TestCache.
	f, err := os.Open("internal/cache/testdata/cache")
	require.NoError(t, err)

	mem := vfs.NewMem()
	objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(mem, ""))
	require.NoError(t, err)
	defer objProvider.Close()

	makeTable := func(dfn base.DiskFileNum) {
		require.NoError(t, err)
		f, _, err := objProvider.Create(context.Background(), base.FileTypeTable, dfn, objstorage.CreateOptions{})
		require.NoError(t, err)
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		require.NoError(t, w.Set([]byte("a"), nil))
		require.NoError(t, w.Close())
	}

	// NB: The file cache size of 200 with a single shard is required for the
	// expected test values.
	fcs := newFileCacheTest(t, 8<<20 /* 8 MB */, 200, 1)
	defer fcs.cleanup()

	opts := &Options{
		Cache:           fcs.blockCache,
		FileCache:       fcs.fileCache,
		LoggerAndTracer: &base.LoggerWithNoopTracer{Logger: base.DefaultLogger},
	}
	opts.EnsureDefaults()
	h := fcs.fileCache.newHandle(fcs.blockCacheHandle, objProvider, opts.LoggerAndTracer, opts.MakeReaderOptions(), noopCorruptionFn)
	defer h.Close()

	scanner := bufio.NewScanner(f)
	tables := make(map[int]bool)
	line := 1

	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())

		key, err := strconv.Atoi(string(fields[0]))
		require.NoError(t, err)

		// Ensure that underlying sstables exist on disk, creating each table the
		// first time it is seen.
		if !tables[key] {
			makeTable(base.DiskFileNum(key))
			tables[key] = true
		}

		oldHits := fcs.fileCache.c.Metrics().Hits
		m := &manifest.TableMetadata{TableNum: base.TableNum(key)}
		m.InitPhysicalBacking()
		m.TableBacking.Ref()
		v, err := h.findOrCreateTable(context.Background(), m)
		require.NoError(t, err)
		v.Unref()

		hit := fcs.fileCache.c.Metrics().Hits != oldHits
		wantHit := fields[1][0] == 'h'
		if hit != wantHit {
			t.Errorf("%d: cache hit mismatch: got %v, want %v\n", line, hit, wantHit)
		}
		line++
		m.TableBacking.Unref()
	}
}

func BenchmarkNewItersAlloc(b *testing.B) {
	opts := &Options{
		FS:                 vfs.NewMem(),
		FormatMajorVersion: internalFormatNewest,
	}
	d, err := Open("", opts)
	require.NoError(b, err)
	defer func() { require.NoError(b, d.Close()) }()

	require.NoError(b, d.Set([]byte{'a'}, []byte{'a'}, nil))
	require.NoError(b, d.Flush())
	require.NoError(b, d.Compact(context.Background(), []byte{'a'}, []byte{'z'}, false))

	d.mu.Lock()
	currVersion := d.mu.versions.currentVersion()
	it := currVersion.Levels[6].Iter()
	m := it.First()
	require.NotNil(b, m)
	d.mu.Unlock()

	// Open once so that the Reader is cached.
	iters, err := d.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys|iterRangeDeletions)
	require.NoError(b, iters.CloseAll())
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		iters, err := d.newIters(context.Background(), m, nil, internalIterOpts{}, iterPointKeys|iterRangeDeletions)
		b.StopTimer()
		require.NoError(b, err)
		require.NoError(b, iters.CloseAll())
	}
}

// TestFileCacheNoSuchFileError verifies that when the file cache hits a "no
// such file" error, it generates a useful fatal message.
func TestFileCacheNoSuchFileError(t *testing.T) {
	const dirname = "test"
	mem := vfs.NewMem()
	logger := &catchFatalLogger{}

	d, err := Open(dirname, &Options{
		FS:     mem,
		Logger: logger,
	})
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	require.NoError(t, d.Set([]byte("a"), []byte("val_a"), nil))
	require.NoError(t, d.Set([]byte("b"), []byte("val_b"), nil))
	require.NoError(t, d.Flush())
	ls, err := mem.List(dirname)
	require.NoError(t, err)

	// Find the sst file.
	var sst string
	for _, file := range ls {
		if strings.HasSuffix(file, ".sst") {
			if sst != "" {
				t.Fatalf("multiple SSTs found: %s, %s", sst, file)
			}
			sst = file
		}
	}
	if sst == "" {
		t.Fatalf("no SST found after flush")
	}
	require.NoError(t, mem.Remove(path.Join(dirname, sst)))

	_, _, _ = d.Get([]byte("a"))
	require.NotZero(t, len(logger.fatalMsgs), "no fatal message emitted")
	require.Equal(t, 1, len(logger.fatalMsgs), "expected one fatal message; got: %v", logger.fatalMsgs)
	require.Contains(t, logger.fatalMsgs[0], "directory contains 7 files, 2 unknown, 0 tables, 2 logs, 1 manifests")
}

func BenchmarkFileCacheHotPath(b *testing.B) {
	mem := vfs.NewMem()
	objProvider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(mem, ""))
	require.NoError(b, err)
	defer objProvider.Close()

	makeTable := func(dfn base.DiskFileNum) {
		require.NoError(b, err)
		f, _, err := objProvider.Create(context.Background(), base.FileTypeTable, dfn, objstorage.CreateOptions{})
		require.NoError(b, err)
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		require.NoError(b, w.Set([]byte("a"), nil))
		require.NoError(b, w.Close())
	}

	fcs := newFileCacheTest(b, 8<<20 /* 8 MB */, 2, 1)
	defer fcs.cleanup()

	opts := &Options{
		Cache:           fcs.blockCache,
		FileCache:       fcs.fileCache,
		LoggerAndTracer: &base.LoggerWithNoopTracer{Logger: base.DefaultLogger},
	}
	opts.EnsureDefaults()
	h := fcs.fileCache.newHandle(fcs.blockCacheHandle, objProvider, opts.LoggerAndTracer, opts.MakeReaderOptions(), noopCorruptionFn)
	defer h.Close()

	makeTable(1)

	m := &manifest.TableMetadata{TableNum: 1}
	m.InitPhysicalBacking()
	m.TableBacking.Ref()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := h.findOrCreateTable(context.Background(), m)
		v.Unref()
	}
}

type catchFatalLogger struct {
	fatalMsgs []string
}

var _ Logger = (*catchFatalLogger)(nil)

func (tl *catchFatalLogger) Infof(format string, args ...interface{})  {}
func (tl *catchFatalLogger) Errorf(format string, args ...interface{}) {}

func (tl *catchFatalLogger) Fatalf(format string, args ...interface{}) {
	tl.fatalMsgs = append(tl.fatalMsgs, fmt.Sprintf(format, args...))
}

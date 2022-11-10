// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

type tableCacheTestFile struct {
	vfs.File
	fs   *tableCacheTestFS
	name string
}

func (f *tableCacheTestFile) Close() error {
	f.fs.mu.Lock()
	if f.fs.closeCounts != nil {
		f.fs.closeCounts[f.name]++
	}
	f.fs.mu.Unlock()
	return f.File.Close()
}

type tableCacheTestFS struct {
	vfs.FS

	mu               sync.Mutex
	openCounts       map[string]int
	closeCounts      map[string]int
	openErrorEnabled bool
}

func (fs *tableCacheTestFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
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
	return &tableCacheTestFile{f, fs, name}, nil
}

func (fs *tableCacheTestFS) validate(
	t *testing.T, c *tableCacheContainer, f func(i, gotO, gotC int) error,
) {
	if err := fs.validateOpenTables(f); err != nil {
		t.Error(err)
		return
	}
	c.close()
	if err := fs.validateNoneStillOpen(); err != nil {
		t.Error(err)
		return
	}
}

func (fs *tableCacheTestFS) setOpenError(enabled bool) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.openErrorEnabled = enabled
}

// validateOpenTables validates that no tables in the cache are open twice, and
// the number still open is no greater than tableCacheTestCacheSize.
func (fs *tableCacheTestFS) validateOpenTables(f func(i, gotO, gotC int) error) error {
	// try backs off to let any clean-up goroutines do their work.
	return try(100*time.Microsecond, 20*time.Second, func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		numStillOpen := 0
		for i := 0; i < tableCacheTestNumTables; i++ {
			filename := base.MakeFilepath(fs, "", fileTypeTable, FileNum(i))
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
		if numStillOpen > tableCacheTestCacheSize {
			return errors.Errorf("numStillOpen is %d, want <= %d", numStillOpen, tableCacheTestCacheSize)
		}
		return nil
	})
}

// validateNoneStillOpen validates that no tables in the cache are open.
func (fs *tableCacheTestFS) validateNoneStillOpen() error {
	// try backs off to let any clean-up goroutines do their work.
	return try(100*time.Microsecond, 20*time.Second, func() error {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		for i := 0; i < tableCacheTestNumTables; i++ {
			filename := base.MakeFilepath(fs, "", fileTypeTable, FileNum(i))
			gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
			if gotO != gotC {
				return errors.Errorf("i=%d: opened %d times, closed %d times", i, gotO, gotC)
			}
		}
		return nil
	})
}

const (
	tableCacheTestNumTables = 300
	tableCacheTestCacheSize = 100
)

// newTableCacheTest returns a shareable table cache to be used for tests.
// It is the caller's responsibility to unref the table cache.
func newTableCacheTest(size int64, tableCacheSize int, numShards int) *TableCache {
	cache := NewCache(size)
	defer cache.Unref()
	return NewTableCache(cache, numShards, tableCacheSize)
}

func newTableCacheContainerTest(
	tc *TableCache, dirname string,
) (*tableCacheContainer, *tableCacheTestFS, error) {
	xxx := bytes.Repeat([]byte("x"), tableCacheTestNumTables)
	fs := &tableCacheTestFS{
		FS: vfs.NewMem(),
	}
	for i := 0; i < tableCacheTestNumTables; i++ {
		f, err := fs.Create(base.MakeFilepath(fs, dirname, fileTypeTable, FileNum(i)))
		if err != nil {
			return nil, nil, errors.Wrap(err, "fs.Create")
		}
		tw := sstable.NewWriter(f, sstable.WriterOptions{TableFormat: sstable.TableFormatPebblev2})
		ik := base.ParseInternalKey(fmt.Sprintf("k.SET.%d", i))
		if err := tw.Add(ik, xxx[:i]); err != nil {
			return nil, nil, errors.Wrap(err, "tw.Set")
		}
		if err := tw.RangeKeySet([]byte("k"), []byte("l"), nil, xxx[:i]); err != nil {
			return nil, nil, errors.Wrap(err, "tw.Set")
		}
		if err := tw.Close(); err != nil {
			return nil, nil, errors.Wrap(err, "tw.Close")
		}
	}

	fs.mu.Lock()
	fs.openCounts = map[string]int{}
	fs.closeCounts = map[string]int{}
	fs.mu.Unlock()

	opts := &Options{}
	opts.EnsureDefaults()
	if tc == nil {
		opts.Cache = NewCache(8 << 20) // 8 MB
		defer opts.Cache.Unref()
	} else {
		opts.Cache = tc.cache
	}

	c := newTableCacheContainer(tc, opts.Cache.NewID(), dirname, fs, opts, tableCacheTestCacheSize)
	return c, fs, nil
}

// Test basic reference counting for the table cache.
func TestTableCacheRefs(t *testing.T) {
	tc := newTableCacheTest(8<<20, 10, 2)

	v := atomic.LoadInt64(&tc.atomic.refs)
	if v != 1 {
		require.Equal(t, 1, v)
	}

	tc.Ref()
	v = atomic.LoadInt64(&tc.atomic.refs)
	if v != 2 {
		require.Equal(t, 2, v)
	}

	tc.Unref()
	v = atomic.LoadInt64(&tc.atomic.refs)
	if v != 1 {
		require.Equal(t, 1, v)
	}

	tc.Unref()
	v = atomic.LoadInt64(&tc.atomic.refs)
	if v != 0 {
		require.Equal(t, 0, v)
	}

	defer func() {
		if r := recover(); r != nil {
			if fmt.Sprint(r) != "pebble: inconsistent reference count: -1" {
				t.Fatalf("unexpected panic message")
			}
		} else if r == nil {
			t.Fatalf("expected panic")
		}
	}()
	tc.Unref()
}

// The table cache shouldn't be usable after all the dbs close.
func TestSharedTableCacheUseAfterAllFree(t *testing.T) {
	tc := newTableCacheTest(8<<20, 10, 1)
	db1, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)

	// Release our reference, now that the db has a reference.
	tc.Unref()

	db2, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)

	require.NoError(t, db1.Close())
	require.NoError(t, db2.Close())

	v := atomic.LoadInt64(&tc.atomic.refs)
	if v != 0 {
		t.Fatalf("expected reference count %d, got %d", 0, v)
	}

	defer func() {
		// The cache ref gets incremented before the panic, so we should
		// decrement it to prevent the finalizer from detecting a leak.
		tc.cache.Unref()

		if r := recover(); r != nil {
			if fmt.Sprint(r) != "pebble: inconsistent reference count: 1" {
				t.Fatalf("unexpected panic message")
			}
		} else if r == nil {
			t.Fatalf("expected panic")
		}
	}()

	db3, _ := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	_ = db3
}

// Test whether a shared table cache is usable by a db, after
// one of the db's releases its reference.
func TestSharedTableCacheUseAfterOneFree(t *testing.T) {
	tc := newTableCacheTest(8<<20, 10, 1)
	db1, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)

	// Release our reference, now that the db has a reference.
	tc.Unref()

	db2, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db2.Close())
	}()

	// Make db1 release a reference to the cache. It should
	// still be usable by db2.
	require.NoError(t, db1.Close())
	v := atomic.LoadInt64(&tc.atomic.refs)
	if v != 1 {
		t.Fatalf("expected reference count %d, got %d", 1, v)
	}

	// Check if db2 is still usable.
	start := []byte("a")
	end := []byte("d")
	require.NoError(t, db2.Set(start, nil, nil))
	require.NoError(t, db2.Flush())
	require.NoError(t, db2.DeleteRange(start, end, nil))
	require.NoError(t, db2.Compact(start, end, false))
}

// A basic test which makes sure that a shared table cache is usable
// by more than one database at once.
func TestSharedTableCacheUsable(t *testing.T) {
	tc := newTableCacheTest(8<<20, 10, 1)
	db1, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)

	// Release our reference, now that the db has a reference.
	tc.Unref()

	defer func() {
		require.NoError(t, db1.Close())
	}()

	db2, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
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
	require.NoError(t, db1.Compact(start, end, false))

	start = []byte("x")
	end = []byte("y")
	require.NoError(t, db2.Set(start, nil, nil))
	require.NoError(t, db2.Flush())
	require.NoError(t, db2.Set(start, []byte{'a'}, nil))
	require.NoError(t, db2.Flush())
	require.NoError(t, db2.DeleteRange(start, end, nil))
	require.NoError(t, db2.Compact(start, end, false))
}

func TestSharedTableConcurrent(t *testing.T) {
	tc := newTableCacheTest(8<<20, 10, 1)
	db1, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)

	// Release our reference, now that the db has a reference.
	tc.Unref()

	defer func() {
		require.NoError(t, db1.Close())
	}()

	db2, err := Open("test",
		&Options{
			FS:         vfs.NewMem(),
			Cache:      tc.cache,
			TableCache: tc,
		})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db2.Close())
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	// Now that both dbs have a reference to the table cache,
	// we'll run go routines which will use the DBs concurrently.
	concFunc := func(db *DB) {
		for i := 0; i < 1000; i++ {
			start := []byte("a")
			end := []byte("z")
			require.NoError(t, db.Set(start, nil, nil))
			require.NoError(t, db.Flush())
			require.NoError(t, db.DeleteRange(start, end, nil))
			require.NoError(t, db.Compact(start, end, false))
		}
		wg.Done()
	}

	go concFunc(db1)
	go concFunc(db2)

	wg.Wait()
}

func testTableCacheRandomAccess(t *testing.T, concurrent bool) {
	const N = 2000
	c, fs, err := newTableCacheContainerTest(nil, "")
	require.NoError(t, err)

	rngMu := sync.Mutex{}
	rng := rand.New(rand.NewSource(1))

	errc := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			rngMu.Lock()
			fileNum, sleepTime := rng.Intn(tableCacheTestNumTables), rng.Intn(1000)
			rngMu.Unlock()
			iter, _, err := c.newIters(
				&fileMetadata{FileNum: FileNum(fileNum)},
				nil, /* iter options */
				internalIterOpts{})
			if err != nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: find: %v", i, fileNum, err)
				return
			}
			key, value := iter.SeekGE([]byte("k"), base.SeekGEFlagsNone)
			if concurrent {
				time.Sleep(time.Duration(sleepTime) * time.Microsecond)
			}
			if key == nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: valid.0: got false, want true", i, fileNum)
				return
			}
			v, _, err := value.Value(nil)
			if err != nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: err extracting value: %v", err)
			}
			if got := len(v); got != fileNum {
				errc <- errors.Errorf("i=%d, fileNum=%d: value: got %d bytes, want %d", i, fileNum, got, fileNum)
				return
			}
			if key, _ := iter.Next(); key != nil {
				errc <- errors.Errorf("i=%d, fileNum=%d: next.1: got true, want false", i, fileNum)
				return
			}
			if err := iter.Close(); err != nil {
				errc <- errors.Wrapf(err, "close error i=%d, fileNum=%dv", i, fileNum)
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
	fs.validate(t, c, nil)
}

func TestTableCacheRandomAccessSequential(t *testing.T) { testTableCacheRandomAccess(t, false) }
func TestTableCacheRandomAccessConcurrent(t *testing.T) { testTableCacheRandomAccess(t, true) }

func testTableCacheFrequentlyUsedInternal(t *testing.T, rangeIter bool) {
	const (
		N       = 1000
		pinned0 = 7
		pinned1 = 11
	)
	c, fs, err := newTableCacheContainerTest(nil, "")
	require.NoError(t, err)

	for i := 0; i < N; i++ {
		for _, j := range [...]int{pinned0, i % tableCacheTestNumTables, pinned1} {
			var iter io.Closer
			var err error
			if rangeIter {
				iter, err = c.newRangeKeyIter(
					&fileMetadata{FileNum: FileNum(j)},
					nil /* iter options */)
			} else {
				iter, _, err = c.newIters(
					&fileMetadata{FileNum: FileNum(j)},
					nil, /* iter options */
					internalIterOpts{})
			}
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			if err := iter.Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
		}
	}

	fs.validate(t, c, func(i, gotO, gotC int) error {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				return errors.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		}
		return nil
	})
}

func TestTableCacheFrequentlyUsed(t *testing.T) {
	for i, iterType := range []string{"point", "range"} {
		t.Run(fmt.Sprintf("iter=%s", iterType), func(t *testing.T) {
			testTableCacheFrequentlyUsedInternal(t, i == 1)
		})
	}
}

func TestSharedTableCacheFrequentlyUsed(t *testing.T) {
	const (
		N       = 1000
		pinned0 = 7
		pinned1 = 11
	)
	tc := newTableCacheTest(8<<20, 2*tableCacheTestCacheSize, 16)
	c1, fs1, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	c2, fs2, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	tc.Unref()

	for i := 0; i < N; i++ {
		for _, j := range [...]int{pinned0, i % tableCacheTestNumTables, pinned1} {
			iter1, _, err := c1.newIters(
				&fileMetadata{FileNum: FileNum(j)},
				nil, /* iter options */
				internalIterOpts{})
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			iter2, _, err := c2.newIters(
				&fileMetadata{FileNum: FileNum(j)},
				nil, /* iter options */
				internalIterOpts{})
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}

			if err := iter1.Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
			if err := iter2.Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
		}
	}

	fs1.validate(t, c1, func(i, gotO, gotC int) error {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				return errors.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		}
		return nil
	})

	fs2.validate(t, c2, func(i, gotO, gotC int) error {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				return errors.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		}
		return nil
	})
}

func testTableCacheEvictionsInternal(t *testing.T, rangeIter bool) {
	const (
		N      = 1000
		lo, hi = 10, 20
	)
	c, fs, err := newTableCacheContainerTest(nil, "")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(2))
	for i := 0; i < N; i++ {
		j := rng.Intn(tableCacheTestNumTables)
		var iter io.Closer
		var err error
		if rangeIter {
			iter, err = c.newRangeKeyIter(
				&fileMetadata{FileNum: FileNum(j)},
				nil /* iter options */)
		} else {
			iter, _, err = c.newIters(
				&fileMetadata{FileNum: FileNum(j)},
				nil, /* iter options */
				internalIterOpts{})
		}
		if err != nil {
			t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
		}

		c.evict(FileNum(lo + rng.Intn(hi-lo)))
	}

	sumEvicted, nEvicted := 0, 0
	sumSafe, nSafe := 0, 0
	fs.validate(t, c, func(i, gotO, gotC int) error {
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
	// (lo, hi, tableCacheTestCacheSize, tableCacheTestNumTables) = (10, 20, 100, 300),
	// the ratio seems to converge on roughly 1.5 for large N, compared to 1.0 if we do
	// not evict any cache entries.
	if ratio := fEvicted / fSafe; ratio < 1.25 {
		t.Errorf("evicted tables were opened %.3f times on average, safe tables %.3f, ratio %.3f < 1.250",
			fEvicted, fSafe, ratio)
	}
}

func TestTableCacheEvictions(t *testing.T) {
	for i, iterType := range []string{"point", "range"} {
		t.Run(fmt.Sprintf("iter=%s", iterType), func(t *testing.T) {
			testTableCacheEvictionsInternal(t, i == 1)
		})
	}
}

func TestSharedTableCacheEvictions(t *testing.T) {
	const (
		N      = 1000
		lo, hi = 10, 20
	)
	tc := newTableCacheTest(8<<20, 2*tableCacheTestCacheSize, 16)
	c1, fs1, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	c2, fs2, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	tc.Unref()

	rng := rand.New(rand.NewSource(2))
	for i := 0; i < N; i++ {
		j := rng.Intn(tableCacheTestNumTables)
		iter1, _, err := c1.newIters(
			&fileMetadata{FileNum: FileNum(j)},
			nil, /* iter options */
			internalIterOpts{})
		if err != nil {
			t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
		}

		iter2, _, err := c2.newIters(
			&fileMetadata{FileNum: FileNum(j)},
			nil, /* iter options */
			internalIterOpts{})
		if err != nil {
			t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
		}

		if err := iter1.Close(); err != nil {
			t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
		}

		if err := iter2.Close(); err != nil {
			t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
		}

		c1.evict(FileNum(lo + rng.Intn(hi-lo)))
		c2.evict(FileNum(lo + rng.Intn(hi-lo)))
	}

	check := func(fs *tableCacheTestFS, c *tableCacheContainer) (float64, float64, float64) {
		sumEvicted, nEvicted := 0, 0
		sumSafe, nSafe := 0, 0
		fs.validate(t, c, func(i, gotO, gotC int) error {
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
	// (lo, hi, tableCacheTestCacheSize, tableCacheTestNumTables) = (10, 20, 100, 300),
	// the ratio seems to converge on roughly 1.5 for large N, compared to 1.0 if we do
	// not evict any cache entries.
	if fEvicted, fSafe, ratio := check(fs1, c1); ratio < 1.25 {
		t.Errorf(
			"evicted tables were opened %.3f times on average, safe tables %.3f, ratio %.3f < 1.250",
			fEvicted, fSafe, ratio,
		)
	}

	if fEvicted, fSafe, ratio := check(fs2, c2); ratio < 1.25 {
		t.Errorf(
			"evicted tables were opened %.3f times on average, safe tables %.3f, ratio %.3f < 1.250",
			fEvicted, fSafe, ratio,
		)
	}
}

func TestTableCacheIterLeak(t *testing.T) {
	c, _, err := newTableCacheContainerTest(nil, "")
	require.NoError(t, err)

	iter, _, err := c.newIters(
		&fileMetadata{FileNum: 0},
		nil, /* iter options */
		internalIterOpts{})
	require.NoError(t, err)

	if err := c.close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
		t.Fatalf("expected leaked iterators, but found %+v", err)
	} else {
		t.Log(err.Error())
	}
	require.NoError(t, iter.Close())
}

func TestSharedTableCacheIterLeak(t *testing.T) {
	tc := newTableCacheTest(8<<20, 2*tableCacheTestCacheSize, 16)
	c1, _, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	c2, _, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	c3, _, err := newTableCacheContainerTest(tc, "")
	require.NoError(t, err)
	tc.Unref()

	iter, _, err := c1.newIters(
		&fileMetadata{FileNum: 0},
		nil, /* iter options */
		internalIterOpts{})
	require.NoError(t, err)

	if err := c1.close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
		t.Fatalf("expected leaked iterators, but found %+v", err)
	} else {
		t.Log(err.Error())
	}

	// Closing c2 shouldn't error out since c2 isn't leaking any iterators.
	require.NoError(t, c2.close())

	// Closing c3 should error out since c3 holds the last reference to
	// the TableCache, and when the TableCache closes, it will detect
	// that there was a leaked iterator.
	if err := c3.close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
		t.Fatalf("expected leaked iterators, but found %+v", err)
	} else {
		t.Log(err.Error())
	}

	require.NoError(t, iter.Close())
}

func TestTableCacheRetryAfterFailure(t *testing.T) {
	// Test a retry can succeed after a failure, i.e., errors are not cached.
	c, fs, err := newTableCacheContainerTest(nil, "")
	require.NoError(t, err)

	fs.setOpenError(true /* enabled */)
	if _, _, err := c.newIters(
		&fileMetadata{FileNum: 0},
		nil, /* iter options */
		internalIterOpts{}); err == nil {
		t.Fatalf("expected failure, but found success")
	}
	fs.setOpenError(false /* enabled */)
	var iter internalIterator
	iter, _, err = c.newIters(
		&fileMetadata{FileNum: 0},
		nil, /* iter options */
		internalIterOpts{})
	require.NoError(t, err)
	require.NoError(t, iter.Close())
	fs.validate(t, c, nil)
}

func TestTableCacheEvictClose(t *testing.T) {
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
	require.NoError(t, db.Compact(start, end, false))
	require.NoError(t, db.Close())
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestTableCacheClockPro(t *testing.T) {
	// Test data was generated from the python code. See also
	// internal/cache/clockpro_test.go:TestCache.
	f, err := os.Open("internal/cache/testdata/cache")
	require.NoError(t, err)

	mem := vfs.NewMem()
	makeTable := func(fileNum FileNum) {
		require.NoError(t, err)
		f, err := mem.Create(base.MakeFilepath(mem, "", fileTypeTable, fileNum))
		require.NoError(t, err)
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		require.NoError(t, w.Set([]byte("a"), nil))
		require.NoError(t, w.Close())
	}

	opts := &Options{
		Cache: NewCache(8 << 20), // 8 MB
	}
	opts.EnsureDefaults()
	defer opts.Cache.Unref()

	cache := &tableCacheShard{}
	// NB: The table cache size of 200 is required for the expected test values.
	cache.init(200)
	dbOpts := &tableCacheOpts{}
	dbOpts.logger = opts.Logger
	dbOpts.cacheID = 0
	dbOpts.dirname = ""
	dbOpts.fs = mem
	dbOpts.opts = opts.MakeReaderOptions()

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
			makeTable(FileNum(key))
			tables[key] = true
		}

		oldHits := atomic.LoadInt64(&cache.atomic.hits)
		v := cache.findNode(&fileMetadata{FileNum: FileNum(key)}, dbOpts)
		cache.unrefValue(v)

		hit := atomic.LoadInt64(&cache.atomic.hits) != oldHits
		wantHit := fields[1][0] == 'h'
		if hit != wantHit {
			t.Errorf("%d: cache hit mismatch: got %v, want %v\n", line, hit, wantHit)
		}
		line++
	}
}

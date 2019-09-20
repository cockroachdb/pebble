// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
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

	mu          sync.Mutex
	openCounts  map[string]int
	closeCounts map[string]int
}

func (fs *tableCacheTestFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fs.mu.Lock()
	if fs.openCounts != nil {
		fs.openCounts[name]++
	}
	fs.mu.Unlock()
	f, err := fs.FS.Open(name, opts...)
	if len(opts) < 1 || opts[0] != vfs.RandomReadsOption {
		return nil, fmt.Errorf("sstable file %s not opened with random reads option", name)
	}
	if err != nil {
		return nil, err
	}
	return &tableCacheTestFile{f, fs, name}, nil
}

func (fs *tableCacheTestFS) validate(t *testing.T, c *tableCache, f func(i, gotO, gotC int) error) {
	if err := fs.validateOpenTables(f); err != nil {
		t.Error(err)
		return
	}
	c.Close()
	if err := fs.validateNoneStillOpen(); err != nil {
		t.Error(err)
		return
	}
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
			filename := base.MakeFilename(fs, "", fileTypeTable, uint64(i))
			gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
			if gotO > gotC {
				numStillOpen++
			}
			if gotC != gotO && gotC != gotO-1 {
				return fmt.Errorf("i=%d: table closed too many or too few times: opened %d times, closed %d times",
					i, gotO, gotC)
			}
			if f != nil {
				if err := f(i, gotO, gotC); err != nil {
					return err
				}
			}
		}
		if numStillOpen > tableCacheTestCacheSize {
			return fmt.Errorf("numStillOpen is %d, want <= %d", numStillOpen, tableCacheTestCacheSize)
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
			filename := base.MakeFilename(fs, "", fileTypeTable, uint64(i))
			gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
			if gotO != gotC {
				return fmt.Errorf("i=%d: opened %d times, closed %d times", i, gotO, gotC)
			}
		}
		return nil
	})
}

const (
	tableCacheTestNumTables     = 300
	tableCacheTestCacheSize     = 100
	tableCacheTestHitBufferSize = 64
)

func newTableCache() (*tableCache, *tableCacheTestFS, error) {
	xxx := bytes.Repeat([]byte("x"), tableCacheTestNumTables)
	fs := &tableCacheTestFS{
		FS: vfs.NewMem(),
	}
	for i := 0; i < tableCacheTestNumTables; i++ {
		f, err := fs.Create(base.MakeFilename(fs, "", fileTypeTable, uint64(i)))
		if err != nil {
			return nil, nil, fmt.Errorf("fs.Create: %v", err)
		}
		tw := sstable.NewWriter(f, nil, LevelOptions{})
		ik := base.ParseInternalKey(fmt.Sprintf("k.SET.%d", i))
		if err := tw.Add(ik, xxx[:i]); err != nil {
			return nil, nil, fmt.Errorf("tw.Set: %v", err)
		}
		if err := tw.Close(); err != nil {
			return nil, nil, fmt.Errorf("tw.Close: %v", err)
		}
	}

	fs.mu.Lock()
	fs.openCounts = map[string]int{}
	fs.closeCounts = map[string]int{}
	fs.mu.Unlock()

	opts := &Options{}
	opts.EnsureDefaults()
	c := &tableCache{}
	c.init(0, "", fs, opts, tableCacheTestCacheSize, tableCacheTestHitBufferSize)
	return c, fs, nil
}

func testTableCacheRandomAccess(t *testing.T, concurrent bool) {
	const N = 2000
	c, fs, err := newTableCache()
	if err != nil {
		t.Fatal(err)
	}

	rngMu := sync.Mutex{}
	rng := rand.New(rand.NewSource(1))

	errc := make(chan error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			rngMu.Lock()
			fileNum, sleepTime := rng.Intn(tableCacheTestNumTables), rng.Intn(1000)
			rngMu.Unlock()
			iter, _, err := c.newIters(
				&fileMetadata{FileNum: uint64(fileNum)},
				nil, /* iter options */
				nil /* bytes iterated */)
			if err != nil {
				errc <- fmt.Errorf("i=%d, fileNum=%d: find: %v", i, fileNum, err)
				return
			}
			key, value := iter.SeekGE([]byte("k"))
			if concurrent {
				time.Sleep(time.Duration(sleepTime) * time.Microsecond)
			}
			if key == nil {
				errc <- fmt.Errorf("i=%d, fileNum=%d: valid.0: got false, want true", i, fileNum)
				return
			}
			if got := len(value); got != fileNum {
				errc <- fmt.Errorf("i=%d, fileNum=%d: value: got %d bytes, want %d", i, fileNum, got, fileNum)
				return
			}
			if key, _ := iter.Next(); key != nil {
				errc <- fmt.Errorf("i=%d, fileNum=%d: next.1: got true, want false", i, fileNum)
				return
			}
			if err := iter.Close(); err != nil {
				errc <- fmt.Errorf("i=%d, fileNum=%d: close: %v", i, fileNum, err)
				return
			}
			errc <- nil
		}(i)
		if !concurrent {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
	}
	if concurrent {
		for i := 0; i < N; i++ {
			if err := <-errc; err != nil {
				t.Fatal(err)
			}
		}
	}
	fs.validate(t, c, nil)
}

func TestTableCacheRandomAccessSequential(t *testing.T) { testTableCacheRandomAccess(t, false) }
func TestTableCacheRandomAccessConcurrent(t *testing.T) { testTableCacheRandomAccess(t, true) }

func TestTableCacheFrequentlyUsed(t *testing.T) {
	const (
		N       = 1000
		pinned0 = 7
		pinned1 = 11
	)
	c, fs, err := newTableCache()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		for _, j := range [...]int{pinned0, i % tableCacheTestNumTables, pinned1} {
			iter, _, err := c.newIters(
				&fileMetadata{FileNum: uint64(j)},
				nil, /* iter options */
				nil /* bytes iterated */)
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
				return fmt.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		} else if gotO == 1 {
			return fmt.Errorf("i=%d: table only opened once", i)
		}
		return nil
	})
}

func TestTableCacheEvictions(t *testing.T) {
	const (
		N      = 1000
		lo, hi = 10, 20
	)
	c, fs, err := newTableCache()
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(2))
	for i := 0; i < N; i++ {
		j := rng.Intn(tableCacheTestNumTables)
		iter, _, err := c.newIters(
			&fileMetadata{FileNum: uint64(j)},
			nil, /* iter options */
			nil /* bytes iterated */)
		if err != nil {
			t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
		}
		if err := iter.Close(); err != nil {
			t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
		}

		c.evict(uint64(lo + rng.Intn(hi-lo)))
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

func TestTableCacheIterLeak(t *testing.T) {
	c, _, err := newTableCache()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.newIters(
		&fileMetadata{FileNum: 0},
		nil, /* iter options */
		nil /* bytes iterated */); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
		t.Fatalf("expected leaked iterators, but found %+v", err)
	} else {
		t.Log(err.Error())
	}
}

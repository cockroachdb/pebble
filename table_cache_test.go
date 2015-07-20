// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memfs"
	"code.google.com/p/leveldb-go/leveldb/table"
)

type tableCacheTestFile struct {
	db.File
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
	db.FileSystem

	mu          sync.Mutex
	openCounts  map[string]int
	closeCounts map[string]int
}

func (fs *tableCacheTestFS) Open(name string) (db.File, error) {
	fs.mu.Lock()
	if fs.openCounts != nil {
		fs.openCounts[name]++
	}
	fs.mu.Unlock()
	f, err := fs.FileSystem.Open(name)
	if err != nil {
		return nil, err
	}
	return &tableCacheTestFile{f, fs, name}, nil
}

func (fs *tableCacheTestFS) validate(t *testing.T, c *tableCache, f func(i, gotO, gotC int)) {
	// Let any clean-up goroutines do their work.
	time.Sleep(1 * time.Millisecond)

	fs.mu.Lock()
	defer fs.mu.Unlock()
	numStillOpen := 0
	for i := 0; i < tableCacheTestNumTables; i++ {
		filename := dbFilename("", fileTypeTable, uint64(i))
		gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
		if gotO > gotC {
			numStillOpen++
		}
		if gotC != gotO && gotC != gotO-1 {
			t.Errorf("i=%d: table closed too many or too few times: opened %d times, closed %d times", i, gotO, gotC)
		}
		if f != nil {
			f(i, gotO, gotC)
		}
	}
	if numStillOpen > tableCacheTestCacheSize {
		t.Errorf("numStillOpen is %d, want <= %d", numStillOpen, tableCacheTestCacheSize)
	}

	// Close the tableCache and let any clean-up goroutines do their work.
	fs.mu.Unlock()
	c.Close()
	time.Sleep(1 * time.Millisecond)
	fs.mu.Lock()

	for i := 0; i < tableCacheTestNumTables; i++ {
		filename := dbFilename("", fileTypeTable, uint64(i))
		gotO, gotC := fs.openCounts[filename], fs.closeCounts[filename]
		if gotO != gotC {
			t.Errorf("i=%d: opened %d times, closed %d times", i, gotO, gotC)
		}
	}
}

const (
	tableCacheTestNumTables = 300
	tableCacheTestCacheSize = 100
)

func newTableCache() (*tableCache, *tableCacheTestFS, error) {
	xxx := bytes.Repeat([]byte("x"), tableCacheTestNumTables)
	fs := &tableCacheTestFS{
		FileSystem: memfs.New(),
	}
	for i := 0; i < tableCacheTestNumTables; i++ {
		f, err := fs.Create(dbFilename("", fileTypeTable, uint64(i)))
		if err != nil {
			return nil, nil, fmt.Errorf("fs.Create: %v", err)
		}
		tw := table.NewWriter(f, &db.Options{
			Comparer: internalKeyComparer{userCmp: db.DefaultComparer},
		})
		if err := tw.Set(makeIkey(fmt.Sprintf("k.SET.%d", i)), xxx[:i], nil); err != nil {
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

	c := &tableCache{}
	c.init("", fs, nil, tableCacheTestCacheSize)
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
			iter, err := c.find(uint64(fileNum), []byte("k"))
			if err != nil {
				errc <- fmt.Errorf("i=%d, fileNum=%d: find: %v", i, fileNum, err)
				return
			}
			if concurrent {
				time.Sleep(time.Duration(sleepTime) * time.Microsecond)
			}
			if !iter.Next() {
				errc <- fmt.Errorf("i=%d, fileNum=%d: next.0: got false, want true", i, fileNum)
				return
			}
			if got := len(iter.Value()); got != fileNum {
				errc <- fmt.Errorf("i=%d, fileNum=%d: value: got %d bytes, want %d", i, fileNum, got, fileNum)
				return
			}
			if iter.Next() {
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
			iter, err := c.find(uint64(j), nil)
			if err != nil {
				t.Fatalf("i=%d, j=%d: find: %v", i, j, err)
			}
			if err := iter.Close(); err != nil {
				t.Fatalf("i=%d, j=%d: close: %v", i, j, err)
			}
		}
	}

	fs.validate(t, c, func(i, gotO, gotC int) {
		if i == pinned0 || i == pinned1 {
			if gotO != 1 || gotC != 0 {
				t.Errorf("i=%d: pinned table: got %d, %d, want %d, %d", i, gotO, gotC, 1, 0)
			}
		} else if gotO == 1 {
			t.Errorf("i=%d: table only opened once", i)
		}
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
		iter, err := c.find(uint64(j), nil)
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
	fs.validate(t, c, func(i, gotO, gotC int) {
		if lo <= i && i < hi {
			sumEvicted += gotO
			nEvicted++
		} else {
			sumSafe += gotO
			nSafe++
		}
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

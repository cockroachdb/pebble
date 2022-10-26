// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"sort"
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

// try repeatedly calls f, sleeping between calls with exponential back-off,
// until f returns a nil error or the total sleep time is greater than or equal
// to maxTotalSleep. It always calls f at least once.
func try(initialSleep, maxTotalSleep time.Duration, f func() error) error {
	totalSleep := time.Duration(0)
	for d := initialSleep; ; d *= 2 {
		time.Sleep(d)
		totalSleep += d
		if err := f(); err == nil || totalSleep >= maxTotalSleep {
			return err
		}
	}
}

func TestTry(t *testing.T) {
	c := make(chan struct{})
	go func() {
		time.Sleep(1 * time.Millisecond)
		close(c)
	}()

	attemptsMu := sync.Mutex{}
	attempts := 0

	err := try(100*time.Microsecond, 20*time.Second, func() error {
		attemptsMu.Lock()
		attempts++
		attemptsMu.Unlock()

		select {
		default:
			return errors.New("timed out")
		case <-c:
			return nil
		}
	})
	require.NoError(t, err)

	attemptsMu.Lock()
	a := attempts
	attemptsMu.Unlock()

	if a == 0 {
		t.Fatalf("attempts: got 0, want > 0")
	}
}

func TestBasicReads(t *testing.T) {
	testCases := []struct {
		dirname string
		wantMap map[string]string
	}{
		{
			"db-stage-1",
			map[string]string{
				"aaa":  "",
				"bar":  "",
				"baz":  "",
				"foo":  "",
				"quux": "",
				"zzz":  "",
			},
		},
		{
			"db-stage-2",
			map[string]string{
				"aaa":  "",
				"bar":  "",
				"baz":  "three",
				"foo":  "four",
				"quux": "",
				"zzz":  "",
			},
		},
		{
			"db-stage-3",
			map[string]string{
				"aaa":  "",
				"bar":  "",
				"baz":  "three",
				"foo":  "four",
				"quux": "",
				"zzz":  "",
			},
		},
		{
			"db-stage-4",
			map[string]string{
				"aaa":  "",
				"bar":  "",
				"baz":  "",
				"foo":  "five",
				"quux": "six",
				"zzz":  "",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.dirname, func(t *testing.T) {
			fs := vfs.NewMem()
			_, err := vfs.Clone(vfs.Default, fs, filepath.Join("testdata", tc.dirname), tc.dirname)
			if err != nil {
				t.Fatalf("%s: cloneFileSystem failed: %v", tc.dirname, err)
			}
			d, err := Open(tc.dirname, testingRandomized(&Options{
				FS: fs,
			}))
			if err != nil {
				t.Fatalf("%s: Open failed: %v", tc.dirname, err)
			}
			for key, want := range tc.wantMap {
				got, closer, err := d.Get([]byte(key))
				if err != nil && err != ErrNotFound {
					t.Fatalf("%s: Get(%q) failed: %v", tc.dirname, key, err)
				}
				if string(got) != string(want) {
					t.Fatalf("%s: Get(%q): got %q, want %q", tc.dirname, key, got, want)
				}
				if closer != nil {
					closer.Close()
				}
			}
			err = d.Close()
			if err != nil {
				t.Fatalf("%s: Close failed: %v", tc.dirname, err)
			}
		})
	}
}

func TestBasicWrites(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)

	names := []string{
		"Alatar",
		"Gandalf",
		"Pallando",
		"Radagast",
		"Saruman",
		"Joe",
	}
	wantMap := map[string]string{}

	inBatch, batch, pending := false, &Batch{}, [][]string(nil)
	set0 := func(k, v string) error {
		return d.Set([]byte(k), []byte(v), nil)
	}
	del0 := func(k string) error {
		return d.Delete([]byte(k), nil)
	}
	set1 := func(k, v string) error {
		batch.Set([]byte(k), []byte(v), nil)
		return nil
	}
	del1 := func(k string) error {
		batch.Delete([]byte(k), nil)
		return nil
	}
	set, del := set0, del0

	testCases := []string{
		"set Gandalf Grey",
		"set Saruman White",
		"set Radagast Brown",
		"delete Saruman",
		"set Gandalf White",
		"batch",
		"  set Alatar AliceBlue",
		"apply",
		"delete Pallando",
		"set Alatar AntiqueWhite",
		"set Pallando PapayaWhip",
		"batch",
		"apply",
		"set Pallando PaleVioletRed",
		"batch",
		"  delete Alatar",
		"  set Gandalf GhostWhite",
		"  set Saruman Seashell",
		"  delete Saruman",
		"  set Saruman SeaGreen",
		"  set Radagast RosyBrown",
		"  delete Pallando",
		"apply",
		"delete Radagast",
		"delete Radagast",
		"delete Radagast",
		"set Gandalf Goldenrod",
		"set Pallando PeachPuff",
		"batch",
		"  delete Joe",
		"  delete Saruman",
		"  delete Radagast",
		"  delete Pallando",
		"  delete Gandalf",
		"  delete Alatar",
		"apply",
		"set Joe Plumber",
	}
	for i, tc := range testCases {
		s := strings.Split(strings.TrimSpace(tc), " ")
		switch s[0] {
		case "set":
			if err := set(s[1], s[2]); err != nil {
				t.Fatalf("#%d %s: %v", i, tc, err)
			}
			if inBatch {
				pending = append(pending, s)
			} else {
				wantMap[s[1]] = s[2]
			}
		case "delete":
			if err := del(s[1]); err != nil {
				t.Fatalf("#%d %s: %v", i, tc, err)
			}
			if inBatch {
				pending = append(pending, s)
			} else {
				delete(wantMap, s[1])
			}
		case "batch":
			inBatch, batch, set, del = true, &Batch{}, set1, del1
		case "apply":
			if err := d.Apply(batch, nil); err != nil {
				t.Fatalf("#%d %s: %v", i, tc, err)
			}
			for _, p := range pending {
				switch p[0] {
				case "set":
					wantMap[p[1]] = p[2]
				case "delete":
					delete(wantMap, p[1])
				}
			}
			inBatch, pending, set, del = false, nil, set0, del0
		default:
			t.Fatalf("#%d %s: bad test case: %q", i, tc, s)
		}

		fail := false
		for _, name := range names {
			g, closer, err := d.Get([]byte(name))
			if err != nil && err != ErrNotFound {
				t.Errorf("#%d %s: Get(%q): %v", i, tc, name, err)
				fail = true
			}
			got, gOK := string(g), err == nil
			want, wOK := wantMap[name]
			if got != want || gOK != wOK {
				t.Errorf("#%d %s: Get(%q): got %q, %t, want %q, %t",
					i, tc, name, got, gOK, want, wOK)
				fail = true
			}
			if closer != nil {
				closer.Close()
			}
		}
		if fail {
			return
		}
	}

	require.NoError(t, d.Close())
}

func TestRandomWrites(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS:           vfs.NewMem(),
		MemTableSize: 8 * 1024,
	}))
	require.NoError(t, err)

	keys := [64][]byte{}
	wants := [64]int{}
	for k := range keys {
		keys[k] = []byte(strconv.Itoa(k))
		wants[k] = -1
	}
	xxx := bytes.Repeat([]byte("x"), 512)

	rng := rand.New(rand.NewSource(123))
	const N = 1000
	for i := 0; i < N; i++ {
		k := rng.Intn(len(keys))
		if rng.Intn(20) != 0 {
			wants[k] = rng.Intn(len(xxx) + 1)
			if err := d.Set(keys[k], xxx[:wants[k]], nil); err != nil {
				t.Fatalf("i=%d: Set: %v", i, err)
			}
		} else {
			wants[k] = -1
			if err := d.Delete(keys[k], nil); err != nil {
				t.Fatalf("i=%d: Delete: %v", i, err)
			}
		}

		if i != N-1 || rng.Intn(50) != 0 {
			continue
		}
		for k := range keys {
			got := -1
			if v, closer, err := d.Get(keys[k]); err != nil {
				if err != ErrNotFound {
					t.Fatalf("Get: %v", err)
				}
			} else {
				got = len(v)
				closer.Close()
			}
			if got != wants[k] {
				t.Errorf("i=%d, k=%d: got %d, want %d", i, k, got, wants[k])
			}
		}
	}

	require.NoError(t, d.Close())
}

func TestLargeBatch(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS:                          vfs.NewMem(),
		MemTableSize:                1400,
		MemTableStopWritesThreshold: 100,
	}))
	require.NoError(t, err)

	verifyLSM := func(expected string) func() error {
		return func() error {
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			if expected != s {
				if testing.Verbose() {
					fmt.Println(strings.TrimSpace(s))
				}
				return errors.Errorf("expected %s, but found %s", expected, s)
			}
			return nil
		}
	}

	logNum := func() FileNum {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.log.queue[len(d.mu.log.queue)-1].fileNum
	}
	fileSize := func(fileNum FileNum) int64 {
		info, err := d.opts.FS.Stat(base.MakeFilepath(d.opts.FS, "", fileTypeLog, fileNum))
		require.NoError(t, err)
		return info.Size()
	}
	memTableCreationSeqNum := func() uint64 {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.mem.mutable.logSeqNum
	}

	startLogNum := logNum()
	startLogStartSize := fileSize(startLogNum)
	startSeqNum := atomic.LoadUint64(&d.mu.versions.atomic.logSeqNum)

	// Write a key with a value larger than the memtable size.
	require.NoError(t, d.Set([]byte("a"), bytes.Repeat([]byte("a"), 512), nil))

	// Verify that the large batch was written to the WAL that existed before it
	// was committed. We verify that WAL rotation occurred, where the large batch
	// was written to, and that the new WAL is empty.
	endLogNum := logNum()
	if startLogNum == endLogNum {
		t.Fatal("expected WAL rotation")
	}
	startLogEndSize := fileSize(startLogNum)
	if startLogEndSize == startLogStartSize {
		t.Fatalf("expected large batch to be written to %s.log, but file size unchanged at %d",
			startLogNum, startLogEndSize)
	}
	endLogSize := fileSize(endLogNum)
	if endLogSize != 0 {
		t.Fatalf("expected %s.log to be empty, but found %d", endLogNum, endLogSize)
	}
	if creationSeqNum := memTableCreationSeqNum(); creationSeqNum <= startSeqNum {
		t.Fatalf("expected memTable.logSeqNum=%d > largeBatch.seqNum=%d", creationSeqNum, startSeqNum)
	}

	// Verify this results in one L0 table being created.
	require.NoError(t, try(100*time.Microsecond, 20*time.Second,
		verifyLSM("0.0:\n  000005:[a#1,SET-a#1,SET]\n")))

	require.NoError(t, d.Set([]byte("b"), bytes.Repeat([]byte("b"), 512), nil))

	// Verify this results in a second L0 table being created.
	require.NoError(t, try(100*time.Microsecond, 20*time.Second,
		verifyLSM("0.0:\n  000005:[a#1,SET-a#1,SET]\n  000007:[b#2,SET-b#2,SET]\n")))

	// Allocate a bunch of batches to exhaust the batchPool. None of these
	// batches should have a non-zero count.
	for i := 0; i < 10; i++ {
		b := d.NewBatch()
		require.EqualValues(t, 0, b.Count())
	}

	require.NoError(t, d.Close())
}

func TestGetNoCache(t *testing.T) {
	cache := NewCache(0)
	defer cache.Unref()

	d, err := Open("", testingRandomized(&Options{
		Cache: cache,
		FS:    vfs.NewMem(),
	}))
	require.NoError(t, err)

	require.NoError(t, d.Set([]byte("a"), []byte("aa"), nil))
	require.NoError(t, d.Flush())
	verifyGet(t, d, []byte("a"), []byte("aa"))

	require.NoError(t, d.Close())
}

func TestGetMerge(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)

	key := []byte("a")
	verify := func(expected string) {
		val, closer, err := d.Get(key)
		require.NoError(t, err)

		if expected != string(val) {
			t.Fatalf("expected %s, but got %s", expected, val)
		}
		closer.Close()
	}

	const val = "1"
	for i := 1; i <= 3; i++ {
		require.NoError(t, d.Merge(key, []byte(val), nil))

		expected := strings.Repeat(val, i)
		verify(expected)

		require.NoError(t, d.Flush())
		verify(expected)
	}

	require.NoError(t, d.Close())
}

func TestMergeOrderSameAfterFlush(t *testing.T) {
	// Ensure compaction iterator (used by flush) and user iterator process merge
	// operands in the same order
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)

	key := []byte("a")
	verify := func(expected string) {
		iter := d.NewIter(nil)
		if !iter.SeekGE([]byte("a")) {
			t.Fatal("expected one value, but got empty iterator")
		}
		if expected != string(iter.Value()) {
			t.Fatalf("expected %s, but got %s", expected, string(iter.Value()))
		}
		if !iter.SeekLT([]byte("b")) {
			t.Fatal("expected one value, but got empty iterator")
		}
		if expected != string(iter.Value()) {
			t.Fatalf("expected %s, but got %s", expected, string(iter.Value()))
		}
		require.NoError(t, iter.Close())
	}

	require.NoError(t, d.Merge(key, []byte("0"), nil))
	require.NoError(t, d.Merge(key, []byte("1"), nil))

	verify("01")
	require.NoError(t, d.Flush())
	verify("01")

	require.NoError(t, d.Close())
}

type closableMerger struct {
	lastBuf []byte
	closed  bool
}

func (m *closableMerger) MergeNewer(value []byte) error {
	m.lastBuf = append(m.lastBuf[:0], value...)
	return nil
}

func (m *closableMerger) MergeOlder(value []byte) error {
	m.lastBuf = append(m.lastBuf[:0], value...)
	return nil
}

func (m *closableMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.lastBuf, m, nil
}

func (m *closableMerger) Close() error {
	m.closed = true
	return nil
}

func TestMergerClosing(t *testing.T) {
	m := &closableMerger{}

	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
		Merger: &Merger{
			Merge: func(key, value []byte) (base.ValueMerger, error) {
				return m, m.MergeNewer(value)
			},
		},
	}))
	require.NoError(t, err)

	defer func() {
		require.NoError(t, d.Close())
	}()

	err = d.Merge([]byte("a"), []byte("b"), nil)
	require.NoError(t, err)
	require.False(t, m.closed)

	val, closer, err := d.Get([]byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("b"), val)
	require.NotNil(t, closer)
	require.False(t, m.closed)
	_ = closer.Close()
	require.True(t, m.closed)
}

func TestLogData(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)

	defer func() {
		require.NoError(t, d.Close())
	}()

	require.NoError(t, d.LogData([]byte("foo"), Sync))
	require.NoError(t, d.LogData([]byte("bar"), Sync))
	// TODO(itsbilal): Confirm that we wrote some bytes to the WAL.
	// For now, LogData proceeding ahead without a panic is good enough.
}

func TestSingleDeleteGet(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := []byte("key")
	val := []byte("val")

	require.NoError(t, d.Set(key, val, nil))
	verifyGet(t, d, key, val)

	key2 := []byte("key2")
	val2 := []byte("val2")

	require.NoError(t, d.Set(key2, val2, nil))
	verifyGet(t, d, key2, val2)

	require.NoError(t, d.SingleDelete(key2, nil))
	verifyGetNotFound(t, d, key2)
}

func TestSingleDeleteFlush(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := []byte("key")
	valFirst := []byte("first")
	valSecond := []byte("second")
	key2 := []byte("key2")
	val2 := []byte("val2")

	require.NoError(t, d.Set(key, valFirst, nil))
	require.NoError(t, d.Set(key2, val2, nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.SingleDelete(key, nil))
	require.NoError(t, d.Set(key, valSecond, nil))
	require.NoError(t, d.Delete(key2, nil))
	require.NoError(t, d.Set(key2, val2, nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.SingleDelete(key, nil))
	require.NoError(t, d.Delete(key2, nil))
	require.NoError(t, d.Flush())

	verifyGetNotFound(t, d, key)
	verifyGetNotFound(t, d, key2)
}

func TestUnremovableSingleDelete(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS:                    vfs.NewMem(),
		L0CompactionThreshold: 8,
	}))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	key := []byte("key")
	valFirst := []byte("valFirst")
	valSecond := []byte("valSecond")

	require.NoError(t, d.Set(key, valFirst, nil))
	ss := d.NewSnapshot()
	defer ss.Close()
	require.NoError(t, d.SingleDelete(key, nil))
	require.NoError(t, d.Set(key, valSecond, nil))
	require.NoError(t, d.Flush())

	verifyGet(t, ss, key, valFirst)
	verifyGet(t, d, key, valSecond)

	require.NoError(t, d.SingleDelete(key, nil))

	verifyGet(t, ss, key, valFirst)
	verifyGetNotFound(t, d, key)

	require.NoError(t, d.Flush())

	verifyGet(t, ss, key, valFirst)
	verifyGetNotFound(t, d, key)
}

func TestIterLeak(t *testing.T) {
	for _, leak := range []bool{true, false} {
		t.Run(fmt.Sprintf("leak=%t", leak), func(t *testing.T) {
			for _, flush := range []bool{true, false} {
				t.Run(fmt.Sprintf("flush=%t", flush), func(t *testing.T) {
					d, err := Open("", testingRandomized(&Options{
						FS: vfs.NewMem(),
					}))
					require.NoError(t, err)

					require.NoError(t, d.Set([]byte("a"), []byte("a"), nil))
					if flush {
						require.NoError(t, d.Flush())
					}
					iter := d.NewIter(nil)
					iter.First()
					if !leak {
						require.NoError(t, iter.Close())
						require.NoError(t, d.Close())
					} else {
						defer iter.Close()
						if err := d.Close(); err == nil {
							t.Fatalf("expected failure, but found success")
						} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
							t.Fatalf("expected leaked iterators, but found %+v", err)
						} else {
							t.Log(err.Error())
						}
					}
				})
			}
		})
	}
}

// Make sure that we detect an iter leak when only one DB closes
// while the second db still holds a reference to the TableCache.
func TestIterLeakSharedCache(t *testing.T) {
	for _, leak := range []bool{true, false} {
		t.Run(fmt.Sprintf("leak=%t", leak), func(t *testing.T) {
			for _, flush := range []bool{true, false} {
				t.Run(fmt.Sprintf("flush=%t", flush), func(t *testing.T) {
					d1, err := Open("", &Options{
						FS: vfs.NewMem(),
					})
					require.NoError(t, err)

					d2, err := Open("", &Options{
						FS: vfs.NewMem(),
					})
					require.NoError(t, err)

					require.NoError(t, d1.Set([]byte("a"), []byte("a"), nil))
					if flush {
						require.NoError(t, d1.Flush())
					}

					require.NoError(t, d2.Set([]byte("a"), []byte("a"), nil))
					if flush {
						require.NoError(t, d2.Flush())
					}

					// Check if leak detection works with only one db closing.
					{
						iter1 := d1.NewIter(nil)
						iter1.First()
						if !leak {
							require.NoError(t, iter1.Close())
							require.NoError(t, d1.Close())
						} else {
							defer iter1.Close()
							if err := d1.Close(); err == nil {
								t.Fatalf("expected failure, but found success")
							} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
								t.Fatalf("expected leaked iterators, but found %+v", err)
							} else {
								t.Log(err.Error())
							}
						}
					}

					{
						iter2 := d2.NewIter(nil)
						iter2.First()
						if !leak {
							require.NoError(t, iter2.Close())
							require.NoError(t, d2.Close())
						} else {
							defer iter2.Close()
							if err := d2.Close(); err == nil {
								t.Fatalf("expected failure, but found success")
							} else if !strings.HasPrefix(err.Error(), "leaked iterators:") {
								t.Fatalf("expected leaked iterators, but found %+v", err)
							} else {
								t.Log(err.Error())
							}
						}
					}

				})
			}
		})
	}
}

func TestMemTableReservation(t *testing.T) {
	cache := NewCache(128 << 10 /* 128 KB */)
	defer cache.Unref()

	opts := &Options{
		Cache:        cache,
		MemTableSize: initialMemTableSize,
		FS:           vfs.NewMem(),
	}
	opts.testingRandomized()
	opts.EnsureDefaults()
	// We're going to be looking at and asserting the global memtable reservation
	// amount below so we don't want to race with any triggered stats collections.
	opts.private.disableTableStats = true

	// Add a block to the cache. Note that the memtable size is larger than the
	// cache size, so opening the DB should cause this block to be evicted.
	tmpID := opts.Cache.NewID()
	helloWorld := []byte("hello world")
	value := opts.Cache.Alloc(len(helloWorld))
	copy(value.Buf(), helloWorld)
	opts.Cache.Set(tmpID, 0, 0, value).Release()

	d, err := Open("", opts)
	require.NoError(t, err)

	checkReserved := func(expected int64) {
		t.Helper()
		if reserved := atomic.LoadInt64(&d.atomic.memTableReserved); expected != reserved {
			t.Fatalf("expected %d reserved, but found %d", expected, reserved)
		}
	}

	checkReserved(int64(opts.MemTableSize))
	if refs := atomic.LoadInt32(&d.mu.mem.queue[len(d.mu.mem.queue)-1].readerRefs); refs != 2 {
		t.Fatalf("expected 2 refs, but found %d", refs)
	}
	// Verify the memtable reservation has caused our test block to be evicted.
	if h := opts.Cache.Get(tmpID, 0, 0); h.Get() != nil {
		t.Fatalf("expected failure, but found success: %s", h.Get())
	}

	// Flush the memtable. The memtable reservation should be unchanged because a
	// new memtable will be allocated.
	require.NoError(t, d.Flush())
	checkReserved(int64(opts.MemTableSize))

	// Flush in the presence of an active iterator. The iterator will hold a
	// reference to a readState which will in turn hold a reader reference to the
	// memtable. While the iterator is open, there will be the memory for 2
	// memtables reserved.
	iter := d.NewIter(nil)
	require.NoError(t, d.Flush())
	checkReserved(2 * int64(opts.MemTableSize))
	require.NoError(t, iter.Close())
	checkReserved(int64(opts.MemTableSize))

	require.NoError(t, d.Close())
}

func TestMemTableReservationLeak(t *testing.T) {
	d, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)

	d.mu.Lock()
	last := d.mu.mem.queue[len(d.mu.mem.queue)-1]
	last.readerRef()
	defer last.readerUnref()
	d.mu.Unlock()
	if err := d.Close(); err == nil {
		t.Fatalf("expected failure, but found success")
	} else if !strings.HasPrefix(err.Error(), "leaked memtable reservation:") {
		t.Fatalf("expected leaked memtable reservation, but found %+v", err)
	} else {
		t.Log(err.Error())
	}
}

func TestCacheEvict(t *testing.T) {
	cache := NewCache(10 << 20)
	defer cache.Unref()

	d, err := Open("", &Options{
		Cache: cache,
		FS:    vfs.NewMem(),
	})
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%04d", i))
		require.NoError(t, d.Set(key, key, nil))
	}

	require.NoError(t, d.Flush())
	iter := d.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
	}
	require.NoError(t, iter.Close())

	if size := cache.Size(); size == 0 {
		t.Fatalf("expected non-zero cache size")
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%04d", i))
		require.NoError(t, d.Delete(key, nil))
	}

	require.NoError(t, d.Compact([]byte("0"), []byte("1"), false))

	require.NoError(t, d.Close())

	if size := cache.Size(); size != 0 {
		t.Fatalf("expected empty cache, but found %d", size)
	}
}

func TestFlushEmpty(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)

	// Flushing an empty memtable should not fail.
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())
}

func TestRollManifest(t *testing.T) {
	toPreserve := rand.Int31n(5) + 1
	opts := &Options{
		MaxManifestFileSize:   1,
		L0CompactionThreshold: 10,
		L0StopWritesThreshold: 1000,
		FS:                    vfs.NewMem(),
		NumPrevManifest:       int(toPreserve),
	}
	opts.DisableAutomaticCompactions = true
	opts.testingRandomized()
	d, err := Open("", opts)
	require.NoError(t, err)

	manifestFileNumber := func() FileNum {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.versions.manifestFileNum
	}
	sizeRolloverState := func() (int64, int64) {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.versions.lastSnapshotFileCount, d.mu.versions.editsSinceLastSnapshotFileCount
	}

	current := func() string {
		desc, err := Peek(d.dirname, d.opts.FS)
		require.NoError(t, err)
		return desc.ManifestFilename
	}

	lastManifestNum := manifestFileNumber()
	manifestNums := []base.FileNum{lastManifestNum}
	for i := 0; i < 5; i++ {
		// MaxManifestFileSize is 1, but the rollover logic also counts edits
		// since the last snapshot to decide on rollover, so do as many flushes as
		// it demands.
		lastSnapshotCount, editsSinceSnapshotCount := sizeRolloverState()
		var expectedLastSnapshotCount, expectedEditsSinceSnapshotCount int64
		switch i {
		case 0:
			// DB is empty.
			expectedLastSnapshotCount, expectedEditsSinceSnapshotCount = 0, 0
		case 1:
			// First edit that caused rollover is not in the snapshot.
			expectedLastSnapshotCount, expectedEditsSinceSnapshotCount = 0, 1
		case 2:
			// One flush is in the snapshot. One flush in the edit.
			expectedLastSnapshotCount, expectedEditsSinceSnapshotCount = 1, 1
		case 3:
			// Two flushes in the snapshot. One flush in the edit. Will need to do
			// two more flushes, the first of which will be in the next snapshot.
			expectedLastSnapshotCount, expectedEditsSinceSnapshotCount = 2, 1
		case 4:
			// Four flushes in the snapshot. One flush in the edit. Will need to do
			// four more flushes, three of which will be in the snapshot.
			expectedLastSnapshotCount, expectedEditsSinceSnapshotCount = 4, 1
		}
		require.Equal(t, expectedLastSnapshotCount, lastSnapshotCount)
		require.Equal(t, expectedEditsSinceSnapshotCount, editsSinceSnapshotCount)
		// Number of flushes to do to trigger the rollover.
		steps := int(lastSnapshotCount - editsSinceSnapshotCount + 1)
		// Steps can be <= 0, but we need to do at least one edit to trigger the
		// rollover logic.
		if steps <= 0 {
			steps = 1
		}
		for j := 0; j < steps; j++ {
			require.NoError(t, d.Set([]byte("a"), nil, nil))
			require.NoError(t, d.Flush())
		}
		num := manifestFileNumber()
		if lastManifestNum == num {
			t.Fatalf("manifest failed to roll %d: %d == %d", i, lastManifestNum, num)
		}

		manifestNums = append(manifestNums, num)
		lastManifestNum = num

		expectedCurrent := fmt.Sprintf("MANIFEST-%s", lastManifestNum)
		if v := current(); expectedCurrent != v {
			t.Fatalf("expected %s, but found %s", expectedCurrent, v)
		}
	}
	lastSnapshotCount, editsSinceSnapshotCount := sizeRolloverState()
	require.EqualValues(t, 8, lastSnapshotCount)
	require.EqualValues(t, 1, editsSinceSnapshotCount)

	files, err := d.opts.FS.List("")
	require.NoError(t, err)

	var manifests []string
	for _, filename := range files {
		fileType, _, ok := base.ParseFilename(d.opts.FS, filename)
		if !ok {
			continue
		}
		if fileType == fileTypeManifest {
			manifests = append(manifests, filename)
		}
	}

	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i] < manifests[j]
	})

	var expected []string
	for i := len(manifestNums) - int(toPreserve) - 1; i < len(manifestNums); i++ {
		expected = append(
			expected,
			fmt.Sprintf("MANIFEST-%s", manifestNums[i]),
		)
	}
	require.EqualValues(t, expected, manifests)

	// Test the logic that uses the future snapshot size to rollover.
	// Reminder: we have a snapshot with 8 files and the manifest has 1 edit
	// (flush) with 1 file.
	// Add 8 more files with a different key.
	lastManifestNum = manifestFileNumber()
	for j := 0; j < 8; j++ {
		require.NoError(t, d.Set([]byte("c"), nil, nil))
		require.NoError(t, d.Flush())
	}
	lastSnapshotCount, editsSinceSnapshotCount = sizeRolloverState()
	// Need 16 more files in edits to trigger a rollover.
	require.EqualValues(t, 16, lastSnapshotCount)
	require.EqualValues(t, 1, editsSinceSnapshotCount)
	require.NotEqual(t, manifestFileNumber(), lastManifestNum)
	lastManifestNum = manifestFileNumber()
	// Do a compaction that moves 8 of the files from L0 to 1 file in L6. This
	// adds 9 files in edits. We still need 6 more files in edits based on the
	// last snapshot. But the current version has only 9 L0 files and 1 L6 file,
	// for a total of 10 files. So 1 flush should push us over that threshold.
	d.Compact([]byte("c"), []byte("d"), false)
	lastSnapshotCount, editsSinceSnapshotCount = sizeRolloverState()
	require.EqualValues(t, 16, lastSnapshotCount)
	require.EqualValues(t, 10, editsSinceSnapshotCount)
	require.Equal(t, manifestFileNumber(), lastManifestNum)
	require.NoError(t, d.Set([]byte("c"), nil, nil))
	require.NoError(t, d.Flush())
	lastSnapshotCount, editsSinceSnapshotCount = sizeRolloverState()
	require.EqualValues(t, 10, lastSnapshotCount)
	require.EqualValues(t, 1, editsSinceSnapshotCount)
	require.NotEqual(t, manifestFileNumber(), lastManifestNum)

	require.NoError(t, d.Close())
}

func TestDBClosed(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	require.NoError(t, err)
	require.NoError(t, d.Close())

	catch := func(f func()) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		f()
		return nil
	}

	require.True(t, errors.Is(catch(func() { _ = d.Close() }), ErrClosed))

	require.True(t, errors.Is(catch(func() { _ = d.Compact(nil, nil, false) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.Flush() }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _, _ = d.AsyncFlush() }), ErrClosed))

	require.True(t, errors.Is(catch(func() { _, _, _ = d.Get(nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.Delete(nil, nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.DeleteRange(nil, nil, nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.Ingest(nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.LogData(nil, nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.Merge(nil, nil, nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.RatchetFormatMajorVersion(FormatNewest) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.Set(nil, nil, nil) }), ErrClosed))

	require.True(t, errors.Is(catch(func() { _ = d.NewSnapshot() }), ErrClosed))

	b := d.NewIndexedBatch()
	require.True(t, errors.Is(catch(func() { _ = b.Commit(nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = d.Apply(b, nil) }), ErrClosed))
	require.True(t, errors.Is(catch(func() { _ = b.NewIter(nil) }), ErrClosed))
}

func TestDBConcurrentCommitCompactFlush(t *testing.T) {
	d, err := Open("", testingRandomized(&Options{
		FS: vfs.NewMem(),
	}))
	require.NoError(t, err)

	// Concurrently commit, compact, and flush in order to stress the locking around
	// those operations.
	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_ = d.Set([]byte(fmt.Sprint(i)), nil, nil)
			var err error
			switch i % 3 {
			case 0:
				err = d.Compact(nil, []byte("\xff"), false)
			case 1:
				err = d.Flush()
			case 2:
				_, err = d.AsyncFlush()
			}
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	require.NoError(t, d.Close())
}

func TestDBConcurrentCompactClose(t *testing.T) {
	// Test closing while a compaction is ongoing. This ensures compaction code
	// detects the close and finishes cleanly.
	mem := vfs.NewMem()
	for i := 0; i < 100; i++ {
		opts := &Options{
			FS: mem,
			MaxConcurrentCompactions: func() int {
				return 2
			},
		}
		d, err := Open("", testingRandomized(opts))
		require.NoError(t, err)

		// Ingest a series of files containing a single key each. As the outer
		// loop progresses, these ingestions will build up compaction debt
		// causing compactions to be running concurrently with the close below.
		for j := 0; j < 10; j++ {
			path := fmt.Sprintf("ext%d", j)
			f, err := mem.Create(path)
			require.NoError(t, err)
			w := sstable.NewWriter(f, sstable.WriterOptions{
				TableFormat: d.FormatMajorVersion().MaxTableFormat(),
			})
			require.NoError(t, w.Set([]byte(fmt.Sprint(j)), nil))
			require.NoError(t, w.Close())
			require.NoError(t, d.Ingest([]string{path}))
		}

		require.NoError(t, d.Close())
	}
}

func TestDBApplyBatchNilDB(t *testing.T) {
	d, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)

	b1 := &Batch{}
	b1.Set([]byte("test"), nil, nil)

	b2 := &Batch{}
	b2.Apply(b1, nil)
	if b2.memTableSize != 0 {
		t.Fatalf("expected memTableSize to not be set")
	}
	require.NoError(t, d.Apply(b2, nil))
	if b1.memTableSize != b2.memTableSize {
		t.Fatalf("expected memTableSize %d, but found %d", b1.memTableSize, b2.memTableSize)
	}

	require.NoError(t, d.Close())
}

func TestDBApplyBatchMismatch(t *testing.T) {
	srcDB, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)

	applyDB, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)

	err = func() (err error) {
		defer func() {
			if v := recover(); v != nil {
				err = errors.Errorf("%v", v)
			}
		}()

		b := srcDB.NewBatch()
		b.Set([]byte("test"), nil, nil)
		return applyDB.Apply(b, nil)
	}()
	if err == nil || !strings.Contains(err.Error(), "pebble: batch db mismatch:") {
		t.Fatalf("expected error, but found %v", err)
	}

	require.NoError(t, srcDB.Close())
	require.NoError(t, applyDB.Close())
}

func TestCloseCleanerRace(t *testing.T) {
	mem := vfs.NewMem()
	for i := 0; i < 20; i++ {
		db, err := Open("", testingRandomized(&Options{FS: mem}))
		require.NoError(t, err)
		require.NoError(t, db.Set([]byte("a"), []byte("something"), Sync))
		require.NoError(t, db.Flush())
		// Ref the sstables so cannot be deleted.
		it := db.NewIter(nil)
		require.NotNil(t, it)
		require.NoError(t, db.DeleteRange([]byte("a"), []byte("b"), Sync))
		require.NoError(t, db.Compact([]byte("a"), []byte("b"), false))
		// Only the iterator is keeping the sstables alive.
		files, err := mem.List("/")
		require.NoError(t, err)
		var found bool
		for _, f := range files {
			if strings.HasSuffix(f, ".sst") {
				found = true
				break
			}
		}
		require.True(t, found)
		// Close the iterator and the db in succession so file cleaning races with DB.Close() --
		// latter should wait for file cleaning to finish.
		require.NoError(t, it.Close())
		require.NoError(t, db.Close())
		files, err = mem.List("/")
		require.NoError(t, err)
		for _, f := range files {
			if strings.HasSuffix(f, ".sst") {
				t.Fatalf("found sst: %s", f)
			}
		}
	}
}

func TestSSTables(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	require.NoError(t, err)
	defer func() {
		if d != nil {
			require.NoError(t, d.Close())
		}
	}()

	// Create two sstables.
	require.NoError(t, d.Set([]byte("hello"), nil, nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Set([]byte("world"), nil, nil))
	require.NoError(t, d.Flush())

	// by default returned table infos should not contain Properties
	tableInfos, err := d.SSTables()
	require.NoError(t, err)
	for _, levelTables := range tableInfos {
		for _, info := range levelTables {
			require.Nil(t, info.Properties)
		}
	}

	// with opt `WithProperties()` the `Properties` in table info should not be nil
	tableInfos, err = d.SSTables(WithProperties())
	require.NoError(t, err)
	for _, levelTables := range tableInfos {
		for _, info := range levelTables {
			require.NotNil(t, info.Properties)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	const keyCount = 10000
	var keys [keyCount][]byte
	for i := 0; i < keyCount; i++ {
		keys[i] = []byte(strconv.Itoa(rng.Int()))
	}
	val := bytes.Repeat([]byte("x"), 10)

	benchmark := func(b *testing.B, useSingleDelete bool) {
		d, err := Open(
			"",
			&Options{
				FS: vfs.NewMem(),
			})
		if err != nil {
			b.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				b.Fatal(err)
			}
		}()

		b.StartTimer()
		for _, key := range keys {
			_ = d.Set(key, val, nil)
			if useSingleDelete {
				_ = d.SingleDelete(key, nil)
			} else {
				_ = d.Delete(key, nil)
			}
		}
		// Manually flush as it is flushing/compaction where SingleDelete
		// performance shows up. With SingleDelete, we can elide all of the
		// SingleDelete and Set records.
		if err := d.Flush(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}

	b.Run("delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmark(b, false)
		}
	})

	b.Run("single-delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			benchmark(b, true)
		}
	})
}

func BenchmarkNewIterReadAmp(b *testing.B) {
	for _, readAmp := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(readAmp), func(b *testing.B) {
			opts := &Options{
				FS:                    vfs.NewMem(),
				L0StopWritesThreshold: 1000,
			}
			opts.DisableAutomaticCompactions = true

			d, err := Open("", opts)
			require.NoError(b, err)

			for i := 0; i < readAmp; i++ {
				require.NoError(b, d.Set([]byte("a"), []byte("b"), NoSync))
				require.NoError(b, d.Flush())
			}

			require.Equal(b, d.Metrics().ReadAmp(), readAmp)

			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StartTimer()
				iter := d.NewIter(nil)
				b.StopTimer()
				require.NoError(b, iter.Close())
			}

			require.NoError(b, d.Close())
		})
	}
}

func verifyGet(t *testing.T, r Reader, key, expected []byte) {
	val, closer, err := r.Get(key)
	require.NoError(t, err)
	if !bytes.Equal(expected, val) {
		t.Fatalf("expected %s, but got %s", expected, val)
	}
	closer.Close()
}

func verifyGetNotFound(t *testing.T, r Reader, key []byte) {
	val, _, err := r.Get(key)
	if err != base.ErrNotFound {
		t.Fatalf("expected nil, but got %s", val)
	}
}

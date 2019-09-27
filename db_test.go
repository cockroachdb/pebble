// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
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
	if err != nil {
		t.Fatal(err)
	}

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
			_, err := vfs.Clone(vfs.Default, fs, filepath.Join("testdata", tc.dirname), "")
			if err != nil {
				t.Fatalf("%s: cloneFileSystem failed: %v", tc.dirname, err)
			}
			d, err := Open(tc.dirname, &Options{
				FS: fs,
			})
			if err != nil {
				t.Fatalf("%s: Open failed: %v", tc.dirname, err)
			}
			for key, want := range tc.wantMap {
				got, err := d.Get([]byte(key))
				if err != nil && err != ErrNotFound {
					t.Fatalf("%s: Get(%q) failed: %v", tc.dirname, key, err)
				}
				if string(got) != string(want) {
					t.Fatalf("%s: Get(%q): got %q, want %q", tc.dirname, key, got, want)
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
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

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
			g, err := d.Get([]byte(name))
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
		}
		if fail {
			return
		}
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRandomWrites(t *testing.T) {
	d, err := Open("", &Options{
		FS:           vfs.NewMem(),
		MemTableSize: 8 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}

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
			if v, err := d.Get(keys[k]); err != nil {
				if err != ErrNotFound {
					t.Fatalf("Get: %v", err)
				}
			} else {
				got = len(v)
			}
			if got != wants[k] {
				t.Errorf("i=%d, k=%d: got %d, want %d", i, k, got, wants[k])
			}
		}
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLargeBatch(t *testing.T) {
	d, err := Open("", &Options{
		FS:                          vfs.NewMem(),
		MemTableSize:                1400,
		MemTableStopWritesThreshold: 100,
	})
	if err != nil {
		t.Fatal(err)
	}

	verifyLSM := func(expected string) func() error {
		return func() error {
			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			if expected != s {
				if testing.Verbose() {
					fmt.Println(strings.TrimSpace(s))
				}
				return fmt.Errorf("expected %s, but found %s", expected, s)
			}
			return nil
		}
	}

	logNum := func() uint64 {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.log.queue[len(d.mu.log.queue)-1]
	}
	fileSize := func(fileNum uint64) int64 {
		info, err := d.opts.FS.Stat(base.MakeFilename(d.opts.FS, "", fileTypeLog, fileNum))
		if err != nil {
			t.Fatal()
		}
		return info.Size()
	}

	startLogNum := logNum()
	startLogStartSize := fileSize(startLogNum)

	// Write two keys with values that are larger than the memtable size.
	if err := d.Set([]byte("a"), bytes.Repeat([]byte("a"), 512), nil); err != nil {
		t.Fatal(err)
	}

	// Verify that the large batch was written to the WAL that existed before it
	// was committed. We verify that WAL rotation occurred, where the large batch
	// was written to, and that the new WAL is empty.
	endLogNum := logNum()
	if startLogNum == endLogNum {
		t.Fatal("expected WAL rotation")
	}
	startLogEndSize := fileSize(startLogNum)
	if startLogEndSize == startLogStartSize {
		t.Fatalf("expected large batch to be written to %06d.log, but file size unchanged at %d",
			startLogNum, startLogEndSize)
	}
	endLogSize := fileSize(endLogNum)
	if endLogSize != 0 {
		t.Fatalf("expected %06d.log to be empty, but found %d", endLogNum, endLogSize)
	}

	// Verify this results in one L0 table being created.
	err = try(100*time.Microsecond, 20*time.Second, verifyLSM("0:\n  5:[a-a]\n"))
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Set([]byte("b"), bytes.Repeat([]byte("b"), 512), nil); err != nil {
		t.Fatal(err)
	}

	// Verify this results in a second L0 table being created.
	err = try(100*time.Microsecond, 20*time.Second, verifyLSM("0:\n  5:[a-a]\n  7:[b-b]\n"))
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetMerge(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("a")
	verify := func(expected string) {
		val, err := d.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if expected != string(val) {
			t.Fatalf("expected %s, but got %s", expected, val)
		}
	}

	const val = "1"
	for i := 1; i <= 3; i++ {
		if err := d.Merge(key, []byte(val), nil); err != nil {
			t.Fatal(err)
		}
		expected := strings.Repeat(val, i)
		verify(expected)

		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		verify(expected)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLogData(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	if err := d.LogData([]byte("foo"), Sync); err != nil {
		t.Fatal(err)
	}
	if err := d.LogData([]byte("bar"), Sync); err != nil {
		t.Fatal(err)
	}
	// TODO(itsbilal): Confirm that we wrote some bytes to the WAL.
	// For now, LogData proceeding ahead without a panic is good enough.
}

func TestSingleDeleteGet(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	key := []byte("key")
	val := []byte("val")

	d.Set(key, val, nil)
	verifyGet(t, d, key, val)

	key2 := []byte("key2")
	val2 := []byte("val2")

	d.Set(key2, val2, nil)
	verifyGet(t, d, key2, val2)

	d.SingleDelete(key2, nil)
	verifyGetNotFound(t, d, key2)
}

func TestSingleDeleteFlush(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	key := []byte("key")
	valFirst := []byte("first")
	valSecond := []byte("second")
	key2 := []byte("key2")
	val2 := []byte("val2")

	d.Set(key, valFirst, nil)
	d.Set(key2, val2, nil)
	d.Flush()

	d.SingleDelete(key, nil)
	d.Set(key, valSecond, nil)
	d.Delete(key2, nil)
	d.Set(key2, val2, nil)
	d.Flush()

	d.SingleDelete(key, nil)
	d.Delete(key2, nil)
	d.Flush()

	verifyGetNotFound(t, d, key)
	verifyGetNotFound(t, d, key2)
}

func TestUnremovableSingleDelete(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	key := []byte("key")
	valFirst := []byte("valFirst")
	valSecond := []byte("valSecond")

	d.Set(key, valFirst, nil)
	ss := d.NewSnapshot()
	d.SingleDelete(key, nil)
	d.Set(key, valSecond, nil)
	d.Flush()

	verifyGetSnapshot(t, ss, key, valFirst)
	verifyGet(t, d, key, valSecond)

	d.SingleDelete(key, nil)

	verifyGetSnapshot(t, ss, key, valFirst)
	verifyGetNotFound(t, d, key)

	d.Flush()

	verifyGetSnapshot(t, ss, key, valFirst)
	verifyGetNotFound(t, d, key)
}

func TestIterLeak(t *testing.T) {
	for _, leak := range []bool{true, false} {
		t.Run(fmt.Sprintf("leak=%t", leak), func(t *testing.T) {
			for _, flush := range []bool{true, false} {
				t.Run(fmt.Sprintf("flush=%t", flush), func(t *testing.T) {
					d, err := Open("", &Options{
						FS: vfs.NewMem(),
					})
					if err != nil {
						t.Fatal(err)
					}

					if err := d.Set([]byte("a"), []byte("a"), nil); err != nil {
						t.Fatal(err)
					}
					if flush {
						if err := d.Flush(); err != nil {
							t.Fatal(err)
						}
					}
					iter := d.NewIter(nil)
					iter.First()
					if !leak {
						if err := iter.Close(); err != nil {
							t.Fatal(err)
						}
						if err := d.Close(); err != nil {
							t.Fatal(err)
						}
					} else {
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

func TestCacheEvict(t *testing.T) {
	cache := NewCache(10 << 20)
	d, err := Open("", &Options{
		Cache: cache,
		FS:    vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%04d", i))
		if err := d.Set(key, key, nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	iter := d.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if size := cache.Size(); size == 0 {
		t.Fatalf("expected non-zero cache size")
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("%04d", i))
		if err := d.Delete(key, nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := d.Compact([]byte("0"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	if size := cache.Size(); size != 0 {
		t.Fatalf("expected empty cache, but found %d", size)
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFlushEmpty(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Flushing an empty memtable should not fail.
	if err := d.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRollManifest(t *testing.T) {
	d, err := Open("", &Options{
		MaxManifestFileSize:   1,
		L0CompactionThreshold: 10,
		FS:                    vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	manifestFileNumber := func() uint64 {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.mu.versions.manifestFileNum
	}

	current := func() string {
		f, err := d.opts.FS.Open(base.MakeFilename(d.opts.FS, d.dirname, fileTypeCurrent, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		stat, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		n := stat.Size()
		b := make([]byte, n)
		if _, err = f.ReadAt(b, 0); err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	lastManifestNum := manifestFileNumber()
	for i := 0; i < 5; i++ {
		if err := d.Set([]byte("a"), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		num := manifestFileNumber()
		if lastManifestNum == num {
			t.Fatalf("manifest failed to roll: %d == %d", lastManifestNum, num)
		}
		lastManifestNum = num

		expectedCurrent := fmt.Sprintf("MANIFEST-%06d\n", lastManifestNum)
		if v := current(); expectedCurrent != v {
			t.Fatalf("expected %s, but found %s", expectedCurrent, v)
		}
	}

	files, err := d.opts.FS.List("")
	if err != nil {
		t.Fatal(err)
	}
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
	expected := []string{fmt.Sprintf("MANIFEST-%06d", lastManifestNum)}
	require.EqualValues(t, expected, manifests)

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDBClosed(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	catch := func(f func()) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = r.(error)
			}
		}()
		f()
		return nil
	}

	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Close() }))

	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Compact(nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Flush() }))
	require.EqualValues(t, ErrClosed, catch(func() { _, _ = d.AsyncFlush() }))

	require.EqualValues(t, ErrClosed, catch(func() { _, _ = d.Get(nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Delete(nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.DeleteRange(nil, nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Ingest(nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.LogData(nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Merge(nil, nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Set(nil, nil, nil) }))

	require.EqualValues(t, ErrClosed, catch(func() { _ = d.NewSnapshot() }))

	b := d.NewIndexedBatch()
	require.EqualValues(t, ErrClosed, catch(func() { _ = b.Commit(nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Apply(b, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = b.NewIter(nil) }))
}

func TestDBConcurrentCommitCompactFlush(t *testing.T) {
	d, err := Open("", &Options{
		FS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

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
				err = d.Compact(nil, []byte("\xff"))
			case 1:
				err = d.Flush()
			case 2:
				_, err = d.AsyncFlush()
			}
			if err != nil {
				t.Fatal(err)
			}
		}(i)
	}
	wg.Wait()

	if err := d.Close(); err != nil {
		t.Fatal(err)
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
			d.Set(key, val, nil)
			if useSingleDelete {
				d.SingleDelete(key, nil)
			} else {
				d.Delete(key, nil)
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

func verifyGet(t *testing.T, d *DB, key, expected []byte) {
	val, err := d.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(expected, val) {
		t.Fatalf("expected %s, but got %s", expected, val)
	}
}

func verifyGetNotFound(t *testing.T, d *DB, key []byte) {
	val, err := d.Get(key)
	if err != base.ErrNotFound {
		t.Fatalf("expected nil, but got %s", val)
	}
}

func verifyGetSnapshot(t *testing.T, ss *Snapshot, key, expected []byte) {
	val, err := ss.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(expected, val) {
		t.Fatalf("expected %s, but got %s", expected, val)
	}
}

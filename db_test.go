// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/petermattis/pebble/cache"
	"github.com/petermattis/pebble/vfs"
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

// cloneFileSystem returns a new memory-backed file system whose root contains
// a copy of the directory dirname in the source file system srcFS. The copy
// is not recursive; directories under dirname are not copied.
//
// Changes to the resultant file system do not modify the source file system.
//
// For example, if srcFS contained:
//   - /bar
//   - /baz/0
//   - /foo/x
//   - /foo/y
//   - /foo/z/A
//   - /foo/z/B
// then calling cloneFileSystem(srcFS, "/foo") would result in a file system
// containing:
//   - /x
//   - /y
func cloneFileSystem(srcFS vfs.FS, dirname string) (vfs.FS, error) {
	if len(dirname) == 0 || dirname[len(dirname)-1] != os.PathSeparator {
		dirname += string(os.PathSeparator)
	}

	dstFS := vfs.NewMem()
	list, err := srcFS.List(dirname)
	if err != nil {
		return nil, err
	}
	for _, name := range list {
		srcFile, err := srcFS.Open(dirname + name)
		if err != nil {
			return nil, err
		}
		stat, err := srcFile.Stat()
		if err != nil {
			return nil, err
		}
		if stat.IsDir() {
			err = srcFile.Close()
			if err != nil {
				return nil, err
			}
			continue
		}
		data := make([]byte, stat.Size())
		_, err = io.ReadFull(srcFile, data)
		if err != nil {
			return nil, err
		}
		err = srcFile.Close()
		if err != nil {
			return nil, err
		}
		dstFile, err := dstFS.Create(name)
		if err != nil {
			return nil, err
		}
		_, err = dstFile.Write(data)
		if err != nil {
			return nil, err
		}
		err = dstFile.Close()
		if err != nil {
			return nil, err
		}
	}
	return dstFS, nil
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
		fs, err := cloneFileSystem(vfs.Default, "testdata/"+tc.dirname)
		if err != nil {
			t.Errorf("%s: cloneFileSystem failed: %v", tc.dirname, err)
			continue
		}
		d, err := Open("", &Options{
			FS: fs,
		})
		if err != nil {
			t.Errorf("%s: Open failed: %v", tc.dirname, err)
			continue
		}
		for key, want := range tc.wantMap {
			got, err := d.Get([]byte(key))
			if err != nil && err != ErrNotFound {
				t.Errorf("%s: Get(%q) failed: %v", tc.dirname, key, err)
				continue
			}
			if string(got) != string(want) {
				t.Errorf("%s: Get(%q): got %q, want %q", tc.dirname, key, got, want)
				continue
			}
		}
		err = d.Close()
		if err != nil {
			t.Errorf("%s: Close failed: %v", tc.dirname, err)
			continue
		}
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
		MinCompactionRate:           4 << 20,
		MinFlushRate:                4 << 20,
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

	// Write two keys with values that are larger than the memtable size.
	if err := d.Set([]byte("a"), bytes.Repeat([]byte("a"), 512), nil); err != nil {
		t.Fatal(err)
	}

	// Verify this results in one L0 table being created.
	err = try(100*time.Microsecond, 20*time.Second, verifyLSM("0: a-a\n"))
	if err != nil {
		t.Fatal(err)
	}

	if err := d.Set([]byte("b"), bytes.Repeat([]byte("b"), 512), nil); err != nil {
		t.Fatal(err)
	}

	// Verify this results in a second L0 table being created.
	err = try(100*time.Microsecond, 20*time.Second, verifyLSM("0: a-a b-b\n"))
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
	cache := cache.New(10 << 20)
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
		return d.mu.versions.manifestFileNumber
	}

	current := func() string {
		f, err := d.opts.FS.Open(dbFilename(d.dirname, fileTypeCurrent, 0))
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
		fileType, _, ok := parseDBFilename(filename)
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
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.AsyncFlush() }))

	require.EqualValues(t, ErrClosed, catch(func() { _, _ = d.Get(nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Delete(nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.DeleteRange(nil, nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.LogData(nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Merge(nil, nil, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Set(nil, nil, nil) }))

	require.EqualValues(t, ErrClosed, catch(func() { _ = d.NewSnapshot() }))

	b := d.NewIndexedBatch()
	require.EqualValues(t, ErrClosed, catch(func() { _ = b.Commit(nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = d.Apply(b, nil) }))
	require.EqualValues(t, ErrClosed, catch(func() { _ = b.NewIter(nil) }))
}

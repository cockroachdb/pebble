// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memfs"
)

func TestErrorIfDBExists(t *testing.T) {
	for _, b := range [...]bool{false, true} {
		fs := memfs.New()
		d0, err := Open("", &db.Options{
			FileSystem: fs,
		})
		if err != nil {
			t.Errorf("b=%v: d0 Open: %v", b, err)
			continue
		}
		if err := d0.Close(); err != nil {
			t.Errorf("b=%v: d0 Close: %v", b, err)
			continue
		}

		d1, err := Open("", &db.Options{
			FileSystem:      fs,
			ErrorIfDBExists: b,
		})
		if d1 != nil {
			defer d1.Close()
		}
		if got := err != nil; got != b {
			t.Errorf("b=%v: d1 Open: err is %v, got (err != nil) is %v, want %v", b, err, got, b)
			continue
		}
	}
}

func TestNewDBFilenames(t *testing.T) {
	fooBar := filepath.Join("foo", "bar")
	fs := memfs.New()
	d, err := Open(fooBar, &db.Options{
		FileSystem: fs,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := fs.List(fooBar)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)
	// TODO: should there be a LOCK file here?
	want := []string{
		"000003.log",
		"CURRENT",
		"MANIFEST-000002",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot  %v\nwant %v", got, want)
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
func cloneFileSystem(srcFS db.FileSystem, dirname string) (db.FileSystem, error) {
	if len(dirname) == 0 || dirname[len(dirname)-1] != os.PathSeparator {
		dirname += string(os.PathSeparator)
	}

	dstFS := memfs.New()
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
		fs, err := cloneFileSystem(db.DefaultFileSystem, "../testdata/"+tc.dirname)
		if err != nil {
			t.Errorf("%s: cloneFileSystem failed: %v", tc.dirname, err)
			continue
		}
		d, err := Open("", &db.Options{
			FileSystem: fs,
		})
		if err != nil {
			t.Errorf("%s: Open failed: %v", tc.dirname, err)
			continue
		}
		for key, want := range tc.wantMap {
			got, err := d.Get([]byte(key), nil)
			if err != nil && err != db.ErrNotFound {
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
	d, err := Open("", &db.Options{
		FileSystem: memfs.New(),
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
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

	inBatch, batch, pending := false, Batch{}, [][]string(nil)
	set0 := func(k, v string) error {
		return d.Set([]byte(k), []byte(v), nil)
	}
	del0 := func(k string) error {
		return d.Delete([]byte(k), nil)
	}
	set1 := func(k, v string) error {
		batch.Set([]byte(k), []byte(v))
		return nil
	}
	del1 := func(k string) error {
		batch.Delete([]byte(k))
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
			inBatch, batch, set, del = true, Batch{}, set1, del1
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
			g, err := d.Get([]byte(name), nil)
			if err != nil && err != db.ErrNotFound {
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
		t.Fatalf("Close failed: %v", err)
	}
}

func TestRandomWrites(t *testing.T) {
	d, err := Open("", &db.Options{
		FileSystem:      memfs.New(),
		WriteBufferSize: 8 * 1024,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
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
			if v, err := d.Get(keys[k], nil); err != nil {
				if err != db.ErrNotFound {
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
		t.Fatalf("db Close: %v", err)
	}
}

func TestOpenCloseOpenClose(t *testing.T) {
	opts := &db.Options{
		FileSystem: memfs.New(),
	}

	for _, startFromEmpty := range []bool{false, true} {
		for _, length := range []int{-1, 0, 1, 1000, 10000, 100000} {
			dirname := "sharedDatabase"
			if startFromEmpty {
				dirname = "startFromEmpty" + strconv.Itoa(length)
			}

			got, xxx := []byte(nil), ""
			if length >= 0 {
				xxx = strings.Repeat("x", length)
			}

			d0, err := Open(dirname, opts)
			if err != nil {
				t.Errorf("sfe=%t, length=%d: Open #0: %v",
					startFromEmpty, length, err)
				continue
			}
			if length >= 0 {
				err = d0.Set([]byte("key"), []byte(xxx), nil)
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Set: %v",
						startFromEmpty, length, err)
					continue
				}
			}
			err = d0.Close()
			if err != nil {
				t.Errorf("sfe=%t, length=%d: Close #0: %v",
					startFromEmpty, length, err)
				continue
			}

			// TODO: make the second Open recover (without a fatal "corrupt log
			// file" error) even if the d0 database was not closed but the xxx
			// value is large enough to write a partial record. Writing to the
			// database should not corrupt it even if the writer process was
			// killed part-way through.

			d1, err := Open(dirname, opts)
			if err != nil {
				t.Errorf("sfe=%t, length=%d: Open #1: %v",
					startFromEmpty, length, err)
				continue
			}
			if length >= 0 {
				got, err = d1.Get([]byte("key"), nil)
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Get: %v",
						startFromEmpty, length, err)
					continue
				}
			}
			err = d1.Close()
			if err != nil {
				t.Errorf("sfe=%t, length=%d: Close #1: %v",
					startFromEmpty, length, err)
				continue
			}

			if length >= 0 && string(got) != xxx {
				t.Errorf("sfe=%t, length=%d: got value differs from set value",
					startFromEmpty, length)
				continue
			}
		}
	}
}

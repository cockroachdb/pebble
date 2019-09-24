// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestErrorIfDBExists(t *testing.T) {
	for _, b := range [...]bool{false, true} {
		mem := vfs.NewMem()
		d0, err := Open("", &Options{
			FS: mem,
		})
		if err != nil {
			t.Errorf("b=%v: d0 Open: %v", b, err)
			continue
		}
		if err := d0.Close(); err != nil {
			t.Errorf("b=%v: d0 Close: %v", b, err)
			continue
		}

		d1, err := Open("", &Options{
			FS:              mem,
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
	mem := vfs.NewMem()
	fooBar := mem.PathJoin("foo", "bar")
	d, err := Open(fooBar, &Options{
		FS: mem,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := mem.List(fooBar)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(got)
	want := []string{
		"000002.log",
		"CURRENT",
		"LOCK",
		"MANIFEST-000001",
		"OPTIONS-000003",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot  %v\nwant %v", got, want)
	}
}

func testOpenCloseOpenClose(t *testing.T, fs vfs.FS, root string) {
	opts := &Options{
		FS: fs,
	}

	for _, startFromEmpty := range []bool{false, true} {
		for _, walDirname := range []string{"", "wal"} {
			for _, length := range []int{-1, 0, 1, 1000, 10000, 100000} {
				dirname := "sharedDatabase" + walDirname
				if startFromEmpty {
					dirname = "startFromEmpty" + walDirname + strconv.Itoa(length)
				}
				dirname = fs.PathJoin(root, dirname)
				if walDirname == "" {
					opts.WALDir = ""
				} else {
					opts.WALDir = fs.PathJoin(dirname, walDirname)
				}

				got, xxx := []byte(nil), ""
				if length >= 0 {
					xxx = strings.Repeat("x", length)
				}

				d0, err := Open(dirname, opts)
				if err != nil {
					t.Fatalf("sfe=%t, length=%d: Open #0: %v",
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

				d1, err := Open(dirname, opts)
				if err != nil {
					t.Errorf("sfe=%t, length=%d: Open #1: %v",
						startFromEmpty, length, err)
					continue
				}
				if length >= 0 {
					got, err = d1.Get([]byte("key"))
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

				{
					got, err := opts.FS.List(dirname)
					if err != nil {
						t.Fatalf("List: %v", err)
					}
					var optionsCount int
					for _, s := range got {
						if t, _, ok := base.ParseFilename(opts.FS, s); ok && t == fileTypeOptions {
							optionsCount++
						}
					}
					if optionsCount != 1 {
						t.Fatalf("expected 1 OPTIONS file, but found %d", optionsCount)
					}
				}
			}
		}
	}
}

func TestOpenCloseOpenClose(t *testing.T) {
	for _, fstype := range []string{"disk", "mem"} {
		t.Run(fstype, func(t *testing.T) {
			var fs vfs.FS
			var dir string
			switch fstype {
			case "disk":
				var err error
				dir, err = ioutil.TempDir("", "open-close")
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					_ = os.RemoveAll(dir)
				}()
				fs = vfs.Default
			case "mem":
				dir = ""
				fs = vfs.NewMem()
			}
			testOpenCloseOpenClose(t, fs, dir)
		})
	}
}

func TestOpenOptionsCheck(t *testing.T) {
	mem := vfs.NewMem()
	opts := &Options{FS: mem}

	d, err := Open("", opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	opts = &Options{
		Comparer: &Comparer{Name: "foo"},
		FS:       mem,
	}
	_, err = Open("", opts)
	require.Regexp(t, `comparer name from file.*!=.*`, err)

	opts = &Options{
		Merger: &Merger{Name: "bar"},
		FS:     mem,
	}
	_, err = Open("", opts)
	require.Regexp(t, `merger name from file.*!=.*`, err)
}

func TestOpenReadOnly(t *testing.T) {
	mem := vfs.NewMem()

	{
		// Opening a non-existent DB in read-only mode should result in no mutable
		// filesystem operations.
		var buf syncedBuffer
		_, err := Open("non-existent", &Options{
			FS:       loggingFS{mem, &buf},
			ReadOnly: true,
			WALDir:   "non-existent-waldir",
		})
		if err == nil {
			t.Fatalf("expected error, but found success")
		}
		const expected = `open-dir: non-existent`
		if trimmed := strings.TrimSpace(buf.String()); expected != trimmed {
			t.Fatalf("expected %q, but found %q", expected, trimmed)
		}
	}

	{
		// Opening a DB with a non-existent WAL dir in read-only mode should result
		// in no mutable filesystem operations other than the LOCK.
		var buf syncedBuffer
		_, err := Open("", &Options{
			FS:       loggingFS{mem, &buf},
			ReadOnly: true,
			WALDir:   "non-existent-waldir",
		})
		if err == nil {
			t.Fatalf("expected error, but found success")
		}
		const expected = "open-dir: \nopen-dir: non-existent-waldir"
		if trimmed := strings.TrimSpace(buf.String()); expected != trimmed {
			t.Fatalf("expected %q, but found %q", expected, trimmed)
		}
	}

	var contents []string
	{
		// Create a new DB and populate it with a small amount of data.
		d, err := Open("", &Options{
			FS: mem,
		})
		require.NoError(t, err)
		require.NoError(t, d.Set([]byte("test"), nil, nil))
		require.NoError(t, d.Close())
		contents, err = mem.List("")
		require.NoError(t, err)
		sort.Strings(contents)
	}

	{
		// Re-open the DB read-only. The directory contents should be unchanged.
		d, err := Open("", &Options{
			FS:       mem,
			ReadOnly: true,
		})
		if err != nil {
			t.Fatal(err)
		}

		// Verify various write operations fail in read-only mode.
		require.EqualValues(t, ErrReadOnly, d.Compact(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Flush())
		require.EqualValues(t, ErrReadOnly, func() error { _, err := d.AsyncFlush(); return err }())

		require.EqualValues(t, ErrReadOnly, d.Delete(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.DeleteRange(nil, nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Ingest(nil))
		require.EqualValues(t, ErrReadOnly, d.LogData(nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Merge(nil, nil, nil))
		require.EqualValues(t, ErrReadOnly, d.Set(nil, nil, nil))

		// Verify we can still read in read-only mode.
		require.NoError(t, func() error { _, err := d.Get([]byte("test")); return err }())

		checkIter := func(iter *Iterator) {
			t.Helper()

			var keys []string
			for valid := iter.First(); valid; valid = iter.Next() {
				keys = append(keys, string(iter.Key()))
			}
			require.NoError(t, iter.Close())
			expectedKeys := []string{"test"}
			if diff := pretty.Diff(keys, expectedKeys); diff != nil {
				t.Fatalf("%s\n%s", strings.Join(diff, "\n"), keys)
			}
		}

		checkIter(d.NewIter(nil))

		b := d.NewIndexedBatch()
		checkIter(b.NewIter(nil))
		require.EqualValues(t, ErrReadOnly, b.Commit(nil))
		require.EqualValues(t, ErrReadOnly, d.Apply(b, nil))

		s := d.NewSnapshot()
		checkIter(s.NewIter(nil))
		require.NoError(t, s.Close())

		require.NoError(t, d.Close())

		newContents, err := mem.List("")
		require.NoError(t, err)

		sort.Strings(newContents)
		if diff := pretty.Diff(contents, newContents); diff != nil {
			t.Fatalf("%s", strings.Join(diff, "\n"))
		}
	}
}

// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestErrorIfDBExists(t *testing.T) {
	for _, b := range [...]bool{false, true} {
		mem := vfs.NewMem()
		d0, err := Open("", &db.Options{
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

		d1, err := Open("", &db.Options{
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
	fooBar := filepath.Join("foo", "bar")
	mem := vfs.NewMem()
	d, err := Open(fooBar, &db.Options{
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
		"MANIFEST-000003",
		"OPTIONS-000004",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot  %v\nwant %v", got, want)
	}
}

func testOpenCloseOpenClose(t *testing.T, fs vfs.FS, root string) {
	opts := &db.Options{
		FS: fs,
	}

	for _, startFromEmpty := range []bool{false, true} {
		for _, length := range []int{-1, 0, 1, 1000, 10000, 100000} {
			dirname := "sharedDatabase"
			if startFromEmpty {
				dirname = "startFromEmpty" + strconv.Itoa(length)
			}
			dirname = filepath.Join(root, dirname)

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
					if t, _, ok := parseDBFilename(s); ok && t == fileTypeOptions {
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
	opts := &db.Options{FS: mem}

	d, err := Open("", opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	opts = &db.Options{
		Comparer: &db.Comparer{Name: "foo"},
		FS:       mem,
	}
	_, err = Open("", opts)
	require.Regexp(t, `comparer name from file.*!=.*`, err)

	opts = &db.Options{
		Merger: &db.Merger{Name: "bar"},
		FS:     mem,
	}
	_, err = Open("", opts)
	require.Regexp(t, `merger name from file.*!=.*`, err)
}

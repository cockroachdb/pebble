// Copyright 2012 The LevelDB-Go Authors. All rights reserved. Use of this
// source code is governed by a BSD-style license that can be found in the
// LICENSE file.

package pebble

import (
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/storage"
)

func TestErrorIfDBExists(t *testing.T) {
	for _, b := range [...]bool{false, true} {
		fs := storage.NewMem()
		d0, err := Open("", &db.Options{
			Storage: fs,
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
			Storage:         fs,
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
	fs := storage.NewMem()
	d, err := Open(fooBar, &db.Options{
		Storage: fs,
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
	// TODO(peter): should there be a LOCK file here?
	want := []string{
		"000003.log",
		"CURRENT",
		"MANIFEST-000002",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("\ngot  %v\nwant %v", got, want)
	}
}

func TestOpenCloseOpenClose(t *testing.T) {
	opts := &db.Options{
		Storage: storage.NewMem(),
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

			// TODO(peter): make the second Open recover (without a fatal "corrupt
			// log file" error) even if the d0 database was not closed but the xxx
			// value is large enough to write a partial record. Writing to the
			// database should not corrupt it even if the writer process was killed
			// part-way through.

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

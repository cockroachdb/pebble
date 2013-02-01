// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"io"
	"os"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/memfs"
)

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

func TestBasic(t *testing.T) {
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

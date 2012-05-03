// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memfs

import (
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
)

func normalize(name string) string {
	if os.PathSeparator == '/' {
		return name
	}
	return strings.Replace(name, "/", string(os.PathSeparator), -1)
}

func TestBasics(t *testing.T) {
	var (
		fs   = New()
		f, g db.File
		fi   os.FileInfo
		buf  [16]byte
		n    int
	)

	// Create a top-level file.
	_, err := fs.Create(normalize("/foo"))
	if err != nil {
		t.Fatalf("1a: Create: %v", err)
	}

	// Create a child of that file. It should fail, since /foo is not a directory.
	_, err = fs.Create(normalize("/foo/x"))
	if err == nil {
		t.Fatalf("2a: Create: got nil error, want non-nil")
	}

	// Create a third-level file. It should fail, since /bar has not been created.
	// Similarly, opening that file should fail.
	_, err = fs.Create(normalize("/bar/baz/y"))
	if err == nil {
		t.Fatalf("3a: Create: got nil error, want non-nil")
	}
	_, err = fs.Open(normalize("/bar/baz/y"))
	if err == nil {
		t.Fatalf("3b: Open: got nil error, want non-nil")
	}

	// Make the /bar/baz directory; create a third-level file. Creation should now succeed.
	err = fs.MkdirAll(normalize("/bar/baz/"), 0755)
	if err != nil {
		t.Fatalf("4a: MkdirAll: %v", err)
	}
	f, err = fs.Create(normalize("/bar/baz/y"))
	if err != nil {
		t.Fatalf("4b: Create: %v", err)
	}
	fi, err = f.Stat()
	if err != nil {
		t.Fatalf("4c: Stat: %v", err)
	}
	if got, want := fi.Name(), "y"; got != want {
		t.Fatalf("4d: Name: got %q, want %q", got, want)
	}

	// Write some data; read it back.
	_, err = f.Write([]byte("ABCDE"))
	if err != nil {
		t.Fatalf("5a: Write: %v", err)
	}
	err = f.Close()
	if err != nil {
		t.Fatalf("5b: Close: %v", err)
	}
	g, err = fs.Open(normalize("/bar/baz/y"))
	if err != nil {
		t.Fatalf("5c: Open: %v", err)
	}
	n, err = io.ReadFull(g, buf[:5])
	if err != nil {
		t.Fatalf("5d: ReadFull: %v", err)
	}
	if n != 5 {
		t.Fatalf("5e: ReadFull: got %d, want %d", n, 5)
	}
	n, err = g.ReadAt(buf[5:8], 0)
	if err != nil {
		t.Fatalf("5f: ReadAt: %v", err)
	}
	if n != 3 {
		t.Fatalf("5g: ReadAt: got %d, want %d", n, 3)
	}
	if got, want := string(buf[:8]), "ABCDEABC"; got != want {
		t.Fatalf("5h: ReadAt: got %q, want %q", got, want)
	}

	// Remove the file twice. The first should succeed, the second should fail.
	err = fs.Remove(normalize("/bar/baz/y"))
	if err != nil {
		t.Fatalf("6a: Remove: %v", err)
	}
	err = fs.Remove(normalize("/bar/baz/y"))
	if err == nil {
		t.Fatalf("6b: Remove: got nil error, want non-nil")
	}
	_, err = fs.Open(normalize("/bar/baz/y"))
	if err == nil {
		t.Fatalf("6c: Open: got nil error, want non-nil")
	}
}

func TestList(t *testing.T) {
	fs := New()

	dirnames := []string{
		"/bar",
		"/foo/2",
	}
	for _, dirname := range dirnames {
		err := fs.MkdirAll(dirname, 0755)
		if err != nil {
			t.Fatalf("MkdirAll %q: %v", dirname, err)
		}
	}

	filenames := []string{
		"/a",
		"/bar/baz",
		"/foo/0",
		"/foo/1",
		"/foo/2/a",
		"/foo/2/b",
		"/foo/3",
		"/foot",
	}
	for _, filename := range filenames {
		f, err := fs.Create(normalize(filename))
		if err != nil {
			t.Fatalf("Create %q: %v", filename, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close %q: %v", filename, err)
		}
	}

	{
		got := fs.(*fileSystem).String()
		want := normalize(`          /
       0    a
            bar/
       0      baz
            foo/
       0      0
       0      1
              2/
       0        a
       0        b
       0      3
       0    foot
`)
		if got != want {
			t.Fatalf("String:\n----got----\n%s----want----\n%s", got, want)
		}
	}

	testCases := []string{
		"/:a bar foo foot",
		"/bar:baz",
		"/bar/:baz",
		"/baz:",
		"/baz/:",
		"/foo:0 1 2 3",
		"/foo/:0 1 2 3",
		"/foo/1:",
		"/foo/1/:",
		"/foo/2:a b",
		"/foo/2/:a b",
		"/foot:",
		"/foot/:",
	}
	for _, tc := range testCases {
		s := strings.Split(tc, ":")
		list, _ := fs.List(normalize(s[0]))
		sort.Strings(list)
		got := strings.Join(list, " ")
		want := s[1]
		if got != want {
			t.Errorf("List %q: got %q, want %q", s[0], got, want)
		}
	}
}

// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestBasics(t *testing.T) {
	fs := NewMem()
	testCases := []string{
		// Create a top-level file.
		"1a: create /foo",
		// Create a child of that file. It should fail, since /foo is not a directory.
		"2a: create /foo/x fails",
		// Create a third-level file. It should fail, since /bar has not been created.
		// Similarly, opening that file should fail.
		"3a: create /bar/baz/y fails",
		"3b: open /bar/baz/y fails",
		// Make the /bar/baz directory; create a third-level file. Creation should now succeed.
		"4a: mkdirall /bar/baz",
		"4b: f = create /bar/baz/y",
		"4c: f.stat.name == y",
		// Write some data; read it back.
		"5a: f.write abcde",
		"5b: f.close",
		"5c: f = open /bar/baz/y",
		"5d: f.read 5 == abcde",
		"5e: f.readat 2 1 == bc",
		"5f: f.close",
		// Link /bar/baz/y to /bar/baz/z. We should be able to read from both files
		// and remove them independently.
		"6a: link /bar/baz/y /bar/baz/z",
		"6b: f = open /bar/baz/z",
		"6c: f.read 5 == abcde",
		"6d: f.close",
		"6e: remove /bar/baz/z",
		"6f: f = open /bar/baz/y",
		"6g: f.read 5 == abcde",
		"6h: f.close",
		// Remove the file twice. The first should succeed, the second should fail.
		"7a: remove /bar/baz/y",
		"7b: remove /bar/baz/y fails",
		"7c: open /bar/baz/y fails",
		// Rename /foo to /goo. Trying to open /foo should succeed before the rename and
		// fail afterwards, and vice versa for /goo.
		"8a: open /foo",
		"8b: open /goo fails",
		"8c: rename /foo /goo",
		"8d: open /foo fails",
		"8e: open /goo",
		// Create /bar/baz/z and rename /bar/baz to /bar/caz.
		"9a: create /bar/baz/z",
		"9b: open /bar/baz/z",
		"9c: open /bar/caz/z fails",
		"9d: rename /bar/baz /bar/caz",
		"9e: open /bar/baz/z fails",
		"9f: open /bar/caz/z",
	}
	var f File
	for _, tc := range testCases {
		s := strings.Split(tc, " ")[1:]

		saveF := s[0] == "f" && s[1] == "="
		if saveF {
			s = s[2:]
		}

		fails := s[len(s)-1] == "fails"
		if fails {
			s = s[:len(s)-1]
		}

		var (
			fi  os.FileInfo
			g   File
			err error
		)
		switch s[0] {
		case "create":
			g, err = fs.Create(s[1])
		case "link":
			err = fs.Link(s[1], s[2])
		case "open":
			g, err = fs.Open(s[1])
		case "mkdirall":
			err = fs.MkdirAll(s[1], 0755)
		case "remove":
			err = fs.Remove(s[1])
		case "rename":
			err = fs.Rename(s[1], s[2])
		case "f.write":
			_, err = f.Write([]byte(s[1]))
		case "f.read":
			n, _ := strconv.Atoi(s[1])
			buf := make([]byte, n)
			_, err = io.ReadFull(f, buf)
			if err != nil {
				break
			}
			if got, want := string(buf), s[3]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		case "f.readat":
			n, _ := strconv.Atoi(s[1])
			off, _ := strconv.Atoi(s[2])
			buf := make([]byte, n)
			_, err = f.ReadAt(buf, int64(off))
			if err != nil {
				break
			}
			if got, want := string(buf), s[4]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		case "f.close":
			f, err = nil, f.Close()
		case "f.stat.name":
			fi, err = f.Stat()
			if err != nil {
				break
			}
			if got, want := fi.Name(), s[2]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		default:
			t.Fatalf("bad test case: %q", tc)
		}

		if saveF {
			f, g = g, nil
		} else if g != nil {
			g.Close()
		}

		if fails {
			if err == nil {
				t.Fatalf("%q: got nil error, want non-nil", tc)
			}
		} else {
			if err != nil {
				t.Fatalf("%q: %v", tc, err)
			}
		}
	}
}

func TestList(t *testing.T) {
	fs := NewMem()

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
		f, err := fs.Create(filename)
		if err != nil {
			t.Fatalf("Create %q: %v", filename, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("Close %q: %v", filename, err)
		}
	}

	{
		got := fs.(*memFS).String()
		const want = `          /
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
`
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
		list, _ := fs.List(s[0])
		sort.Strings(list)
		got := strings.Join(list, " ")
		want := s[1]
		if got != want {
			t.Errorf("List %q: got %q, want %q", s[0], got, want)
		}
	}
}

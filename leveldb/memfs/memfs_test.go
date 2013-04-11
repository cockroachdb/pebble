// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memfs

import (
	"io"
	"os"
	"sort"
	"strconv"
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
	fs := New()
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
		// Remove the file twice. The first should succeed, the second should fail.
		"6a: remove /bar/baz/y",
		"6b: remove /bar/baz/y fails",
		"6c: open /bar/baz/y fails",
		// Rename /foo to /goo. Trying to open /foo should succeed before the rename and
		// fail afterwards, and vice versa for /goo.
		"7a: open /foo",
		"7b: open /goo fails",
		"7c: rename /foo /goo",
		"7d: open /foo fails",
		"7e: open /goo",
		// Create /bar/baz/z and rename /bar/baz to /bar/caz.
		"8a: create /bar/baz/z",
		"8b: open /bar/baz/z",
		"8c: open /bar/caz/z fails",
		"8d: rename /bar/baz /bar/caz",
		"8e: open /bar/baz/z fails",
		"8f: open /bar/caz/z",
	}
	var f db.File
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
			g   db.File
			err error
		)
		switch s[0] {
		case "create":
			g, err = fs.Create(normalize(s[1]))
		case "open":
			g, err = fs.Open(normalize(s[1]))
		case "mkdirall":
			err = fs.MkdirAll(normalize(s[1]), 0755)
		case "remove":
			err = fs.Remove(normalize(s[1]))
		case "rename":
			err = fs.Rename(normalize(s[1]), normalize(s[2]))
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
	fs := New()

	dirnames := []string{
		"/bar",
		"/foo/2",
	}
	for _, dirname := range dirnames {
		err := fs.MkdirAll(normalize(dirname), 0755)
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

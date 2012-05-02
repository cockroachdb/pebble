// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package memfs

import (
	"os"
	"sort"
	"strings"
	"testing"
)

func normalize(name string) string {
	if os.PathSeparator == '/' {
		return name
	}
	return strings.Replace(name, "/", string(os.PathSeparator), -1)
}

func TestList(t *testing.T) {
	fs := New()

	fullNames := []string{
		"/a",
		"/bar/baz",
		"/foo",
		"/foo/0",
		"/foo/1",
		"/foo/2/a",
		"/foo/2/b",
		"/foo/3",
		"/foot",
	}
	for _, fullName := range fullNames {
		f, err := fs.Create(normalize(fullName))
		if err != nil {
			t.Fatalf("create %q: %v", fullName, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %q: %v", fullName, err)
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
		list, err := fs.List(normalize(s[0]))
		if err != nil {
			t.Fatalf("list %q: %v", s[0], err)
		}
		sort.Strings(list)
		got := strings.Join(list, " ")
		want := s[1]
		if got != want {
			t.Errorf("list %q: got %q, want %q", s[0], got, want)
		}
	}
}

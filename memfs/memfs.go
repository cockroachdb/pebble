// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package memfs provides a memory-backed db.FileSystem implementation.
//
// It can be useful for tests, and also for LevelDB instances that should not
// ever touch persistent storage, such as a web browser's private browsing mode.
package memfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"code.google.com/p/leveldb-go/leveldb/db"
)

const sep = string(os.PathSeparator)

type nopCloser struct{}

func (nopCloser) Close() error {
	return nil
}

// New returns a new memory-backed db.FileSystem implementation.
func New() db.FileSystem {
	return &fileSystem{
		root: &node{
			children: make(map[string]*node),
			isDir:    true,
		},
	}
}

// fileSystem implements db.FileSystem.
type fileSystem struct {
	mu   sync.Mutex
	root *node
}

func (y *fileSystem) String() string {
	y.mu.Lock()
	defer y.mu.Unlock()

	s := new(bytes.Buffer)
	y.root.dump(s, 0)
	return s.String()
}

// walk walks the directory tree for the fullname, calling f at each step. If
// f returns an error, the walk will be aborted and return that same error.
//
// Each walk is atomic: y's mutex is held for the entire operation, including
// all calls to f.
//
// dir is the directory at that step, frag is the name fragment, and final is
// whether it is the final step. For example, walking "/foo/bar/x" will result
// in 3 calls to f:
//   - "/", "foo", false
//   - "/foo/", "bar", false
//   - "/foo/bar/", "x", true
// Similarly, walking "/y/z/", with a trailing slash, will result in 3 calls to f:
//   - "/", "y", false
//   - "/y/", "z", false
//   - "/y/z/", "", true
func (y *fileSystem) walk(fullname string, f func(dir *node, frag string, final bool) error) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	// For memfs, the current working directory is the same as the root directory,
	// so we strip off any leading "/"s to make fullname a relative path, and
	// the walk starts at y.root.
	for len(fullname) > 0 && fullname[0] == os.PathSeparator {
		fullname = fullname[1:]
	}
	dir := y.root

	for {
		frag, remaining := fullname, ""
		i := strings.IndexRune(fullname, os.PathSeparator)
		final := i < 0
		if !final {
			frag, remaining = fullname[:i], fullname[i+1:]
			for len(remaining) > 0 && remaining[0] == os.PathSeparator {
				remaining = remaining[1:]
			}
		}
		if err := f(dir, frag, final); err != nil {
			return err
		}
		if final {
			break
		}
		child := dir.children[frag]
		if child == nil {
			return errors.New("leveldb/memfs: no such directory")
		}
		if !child.isDir {
			return errors.New("leveldb/memfs: not a directory")
		}
		dir, fullname = child, remaining
	}
	return nil
}

func (y *fileSystem) Create(fullname string) (db.File, error) {
	var ret *file
	err := y.walk(fullname, func(dir *node, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			n := &node{name: frag}
			dir.children[frag] = n
			ret = &file{
				n:     n,
				write: true,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (y *fileSystem) Open(fullname string) (db.File, error) {
	var ret *file
	err := y.walk(fullname, func(dir *node, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			if n := dir.children[frag]; n != nil {
				ret = &file{
					n:    n,
					read: true,
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, &os.PathError{
			Op:   "open",
			Path: fullname,
			Err:  os.ErrNotExist,
		}
	}
	return ret, nil
}

func (y *fileSystem) Remove(fullname string) error {
	return y.walk(fullname, func(dir *node, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			_, ok := dir.children[frag]
			if !ok {
				return errors.New("leveldb/memfs: no such file or directory")
			}
			delete(dir.children, frag)
		}
		return nil
	})
}

func (y *fileSystem) Rename(oldname, newname string) error {
	var n *node
	err := y.walk(oldname, func(dir *node, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			n = dir.children[frag]
			delete(dir.children, frag)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if n == nil {
		return errors.New("leveldb/memfs: no such file or directory")
	}
	return y.walk(newname, func(dir *node, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			dir.children[frag] = n
		}
		return nil
	})
}

func (y *fileSystem) MkdirAll(dirname string, perm os.FileMode) error {
	return y.walk(dirname, func(dir *node, frag string, final bool) error {
		if frag == "" {
			if final {
				return nil
			}
			return errors.New("leveldb/memfs: empty file name")
		}
		child := dir.children[frag]
		if child == nil {
			dir.children[frag] = &node{
				name:     frag,
				children: make(map[string]*node),
				isDir:    true,
			}
			return nil
		}
		if !child.isDir {
			return errors.New("leveldb/memfs: not a directory")
		}
		return nil
	})
}

func (y *fileSystem) Lock(fullname string) (io.Closer, error) {
	// FileSystem.Lock excludes other processes, but other processes cannot
	// see this process' memory, so Lock is a no-op.
	return nopCloser{}, nil
}

func (y *fileSystem) List(dirname string) ([]string, error) {
	if !strings.HasSuffix(dirname, sep) {
		dirname += sep
	}
	var ret []string
	err := y.walk(dirname, func(dir *node, frag string, final bool) error {
		if final {
			if frag != "" {
				panic("unreachable")
			}
			ret = make([]string, 0, len(dir.children))
			for s := range dir.children {
				ret = append(ret, s)
			}
		}
		return nil
	})
	return ret, err
}

func (y *fileSystem) Stat(name string) (os.FileInfo, error) {
	f, err := y.Open(name)
	if err != nil {
		if pe, ok := err.(*os.PathError); ok {
			pe.Op = "stat"
		}
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

// node holds a file's data or a directory's children, and implements os.FileInfo.
type node struct {
	name     string
	data     []byte
	modTime  time.Time
	children map[string]*node
	isDir    bool
}

func (f *node) IsDir() bool {
	return f.isDir
}

func (f *node) ModTime() time.Time {
	return f.modTime
}

func (f *node) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | 0755
	}
	return 0755
}

func (f *node) Name() string {
	return f.name
}

func (f *node) Size() int64 {
	return int64(len(f.data))
}

func (f *node) Sys() interface{} {
	return nil
}

func (f *node) dump(w *bytes.Buffer, level int) {
	if f.isDir {
		w.WriteString("          ")
	} else {
		fmt.Fprintf(w, "%8d  ", len(f.data))
	}
	for i := 0; i < level; i++ {
		w.WriteString("  ")
	}
	w.WriteString(f.name)
	if !f.isDir {
		w.WriteByte('\n')
		return
	}
	w.WriteByte(os.PathSeparator)
	w.WriteByte('\n')
	names := make([]string, 0, len(f.children))
	for name := range f.children {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f.children[name].dump(w, level+1)
	}
}

// file is a reader or writer of a node's data, and implements db.File.
type file struct {
	n           *node
	rpos        int
	read, write bool
}

func (f *file) Close() error {
	return nil
}

func (f *file) Read(p []byte) (int, error) {
	if !f.read {
		return 0, errors.New("leveldb/memfs: file was not opened for reading")
	}
	if f.n.isDir {
		return 0, errors.New("leveldb/memfs: cannot read a directory")
	}
	if f.rpos >= len(f.n.data) {
		return 0, io.EOF
	}
	n := copy(p, f.n.data[f.rpos:])
	f.rpos += n
	return n, nil
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if !f.read {
		return 0, errors.New("leveldb/memfs: file was not opened for reading")
	}
	if f.n.isDir {
		return 0, errors.New("leveldb/memfs: cannot read a directory")
	}
	if off >= int64(len(f.n.data)) {
		return 0, io.EOF
	}
	return copy(p, f.n.data[off:]), nil
}

func (f *file) Write(p []byte) (int, error) {
	if !f.write {
		return 0, errors.New("leveldb/memfs: file was not created for writing")
	}
	if f.n.isDir {
		return 0, errors.New("leveldb/memfs: cannot write a directory")
	}
	f.n.modTime = time.Now()
	f.n.data = append(f.n.data, p...)
	return len(p), nil
}

func (f *file) Stat() (os.FileInfo, error) {
	return f.n, nil
}

func (f *file) Sync() error {
	return nil
}

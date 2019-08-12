// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs // import "github.com/petermattis/pebble/vfs"

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
)

const sep = string(os.PathSeparator)

type nopCloser struct{}

func (nopCloser) Close() error {
	return nil
}

// NewMem returns a new memory-backed FS implementation.
func NewMem() FS {
	return &memFS{
		root: &memNode{
			children: make(map[string]*memNode),
			isDir:    true,
		},
	}
}

// memFS implements FS.
type memFS struct {
	mu   sync.Mutex
	root *memNode
}

var _ FS = &memFS{}

func (y *memFS) String() string {
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
func (y *memFS) walk(fullname string, f func(dir *memNode, frag string, final bool) error) error {
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
			return errors.New("pebble/vfs: no such directory")
		}
		if !child.isDir {
			return errors.New("pebble/vfs: not a directory")
		}
		dir, fullname = child, remaining
	}
	return nil
}

func (y *memFS) Create(fullname string) (File, error) {
	var ret *memFile
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n := &memNode{name: frag}
			dir.children[frag] = n
			ret = &memFile{
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

func (y *memFS) Link(oldname, newname string) error {
	var n *memNode
	err := y.walk(oldname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n = dir.children[frag]
		}
		return nil
	})
	if err != nil {
		return err
	}
	if n == nil {
		return errors.New("pebble/vfs: no such file or directory")
	}
	return y.walk(newname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			dir.children[frag] = n
		}
		return nil
	})
}

func (y *memFS) open(fullname string, allowEmptyName bool) (File, error) {
	var ret *memFile
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				if !allowEmptyName {
					return errors.New("pebble/vfs: empty file name")
				}
				ret = &memFile{
					n: dir,
				}
				return nil
			}
			if n := dir.children[frag]; n != nil {
				ret = &memFile{
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

func (y *memFS) Open(fullname string, opts ...OpenOption) (File, error) {
	return y.open(fullname, false /* allowEmptyName */)
}

func (y *memFS) OpenDir(fullname string) (File, error) {
	return y.open(fullname, true /* allowEmptyName */)
}

func (y *memFS) Remove(fullname string) error {
	return y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			_, ok := dir.children[frag]
			if !ok {
				return os.ErrNotExist
			}
			delete(dir.children, frag)
		}
		return nil
	})
}

func (y *memFS) Rename(oldname, newname string) error {
	var n *memNode
	err := y.walk(oldname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
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
		return errors.New("pebble/vfs: no such file or directory")
	}
	return y.walk(newname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			dir.children[frag] = n
		}
		return nil
	})
}

func (y *memFS) MkdirAll(dirname string, perm os.FileMode) error {
	return y.walk(dirname, func(dir *memNode, frag string, final bool) error {
		if frag == "" {
			if final {
				return nil
			}
			return errors.New("pebble/vfs: empty file name")
		}
		child := dir.children[frag]
		if child == nil {
			dir.children[frag] = &memNode{
				name:     frag,
				children: make(map[string]*memNode),
				isDir:    true,
			}
			return nil
		}
		if !child.isDir {
			return errors.New("pebble/vfs: not a directory")
		}
		return nil
	})
}

func (y *memFS) Lock(fullname string) (io.Closer, error) {
	// FS.Lock excludes other processes, but other processes cannot see this
	// process' memory, so Lock is a no-op.
	return nopCloser{}, nil
}

func (y *memFS) List(dirname string) ([]string, error) {
	if !strings.HasSuffix(dirname, sep) {
		dirname += sep
	}
	var ret []string
	err := y.walk(dirname, func(dir *memNode, frag string, final bool) error {
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

func (y *memFS) Stat(name string) (os.FileInfo, error) {
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

// memNode holds a file's data or a directory's children, and implements os.FileInfo.
type memNode struct {
	name     string
	data     []byte
	modTime  time.Time
	children map[string]*memNode
	isDir    bool
}

func (f *memNode) IsDir() bool {
	return f.isDir
}

func (f *memNode) ModTime() time.Time {
	return f.modTime
}

func (f *memNode) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | 0755
	}
	return 0755
}

func (f *memNode) Name() string {
	return f.name
}

func (f *memNode) Size() int64 {
	return int64(len(f.data))
}

func (f *memNode) Sys() interface{} {
	return nil
}

func (f *memNode) dump(w *bytes.Buffer, level int) {
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

// memFile is a reader or writer of a node's data, and implements File.
type memFile struct {
	n           *memNode
	rpos        int
	read, write bool
}

func (f *memFile) Close() error {
	return nil
}

func (f *memFile) Read(p []byte) (int, error) {
	if !f.read {
		return 0, errors.New("pebble/vfs: file was not opened for reading")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot read a directory")
	}
	if f.rpos >= len(f.n.data) {
		return 0, io.EOF
	}
	n := copy(p, f.n.data[f.rpos:])
	f.rpos += n
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if !f.read {
		return 0, errors.New("pebble/vfs: file was not opened for reading")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot read a directory")
	}
	if off >= int64(len(f.n.data)) {
		return 0, io.EOF
	}
	return copy(p, f.n.data[off:]), nil
}

func (f *memFile) Write(p []byte) (int, error) {
	if !f.write {
		return 0, errors.New("pebble/vfs: file was not created for writing")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot write a directory")
	}
	f.n.modTime = time.Now()
	f.n.data = append(f.n.data, p...)
	return len(p), nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	return f.n, nil
}

func (f *memFile) Sync() error {
	return nil
}

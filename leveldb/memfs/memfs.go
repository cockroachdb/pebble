// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package memfs provides a memory-backed db.FileSystem implementation.
//
// It can be useful for tests, and also for LevelDB instances that should not
// ever touch persistent storage, such as a web browser's private browsing mode.
package memfs

import (
	"errors"
	"io"
	"os"
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
		root: &file{
			name:     sep,
			children: make(map[string]*file),
			isDir:    true,
		},
	}
}

// fileSystem implements db.FileSystem.
type fileSystem struct {
	mu   sync.Mutex
	root *file
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
func (y *fileSystem) walk(fullname string, f func(dir *file, frag string, final bool) error) error {
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
	err := y.walk(fullname, func(dir *file, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			ret = &file{name: frag}
			dir.children[frag] = ret
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
	err := y.walk(fullname, func(dir *file, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("leveldb/memfs: empty file name")
			}
			ret = dir.children[frag]
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, errors.New("leveldb/memfs: no such file")
	}
	return ret, nil
}

func (y *fileSystem) Remove(fullname string) error {
	return y.walk(fullname, func(dir *file, frag string, final bool) error {
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

func (y *fileSystem) MkdirAll(dirname string, perm os.FileMode) error {
	return y.walk(dirname, func(dir *file, frag string, final bool) error {
		if frag == "" {
			if final {
				return nil
			}
			return errors.New("leveldb/memfs: empty file name")
		}
		child := dir.children[frag]
		if child == nil {
			dir.children[frag] = &file{
				name:     frag,
				children: make(map[string]*file),
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
	err := y.walk(dirname, func(dir *file, frag string, final bool) error {
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

// file implements db.File and os.FileInfo.
type file struct {
	name     string
	data     []byte
	modTime  time.Time
	children map[string]*file
	isDir    bool
}

func (f *file) Close() error {
	return nil
}

func (f *file) IsDir() bool {
	return f.isDir
}

func (f *file) ModTime() time.Time {
	return f.modTime
}

func (f *file) Mode() os.FileMode {
	return os.FileMode(0755)
}

func (f *file) Name() string {
	return f.name
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if f.isDir {
		return 0, errors.New("leveldb/memfs: cannot read a directory")
	}
	return copy(p, f.data[off:]), nil
}

func (f *file) Size() int64 {
	return int64(len(f.data))
}

func (f *file) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *file) Sys() interface{} {
	return nil
}

func (f *file) Write(p []byte) (int, error) {
	if f.isDir {
		return 0, errors.New("leveldb/memfs: cannot write a directory")
	}
	f.modTime = time.Now()
	f.data = append(f.data, p...)
	return len(p), nil
}

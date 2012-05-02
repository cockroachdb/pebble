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

// New returns a new memory-backed db.FileSystem implementation.
func New() db.FileSystem {
	return &fileSystem{
		m: make(map[string]*file),
	}
}

// fileSystem implements db.FileSystem.
type fileSystem struct {
	mu sync.Mutex
	m  map[string]*file
}

func (y *fileSystem) Create(name string) (db.File, error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	f, ok := y.m[name]
	if !ok {
		f = &file{name: name}
		y.m[name] = f
	} else {
		f.data = nil
	}
	return f, nil
}

func (y *fileSystem) Open(name string) (db.File, error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	f, ok := y.m[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return f, nil
}

func (y *fileSystem) Remove(name string) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	_, ok := y.m[name]
	if !ok {
		return os.ErrNotExist
	}
	delete(y.m, name)
	return nil
}

func (y *fileSystem) Lock(name string) (io.Closer, error) {
	return nil, errors.New("leveldb/db: file locking is not implemented for memory-backed file systems")
}

func (y *fileSystem) List(dir string) ([]string, error) {
	y.mu.Lock()
	defer y.mu.Unlock()

	names := make(map[string]bool)
	if len(dir) == 0 || dir[len(dir)-1] != os.PathSeparator {
		dir += string(os.PathSeparator)
	}
	for fullName := range y.m {
		if !strings.HasPrefix(fullName, dir) {
			continue
		}
		name := fullName[len(dir):]
		if i := strings.IndexRune(name, os.PathSeparator); i >= 0 {
			name = name[:i]
		}
		names[name] = true
	}
	ret := make([]string, 0, len(names))
	for name := range names {
		ret = append(ret, name)
	}
	return ret, nil
}

// file implements db.File and os.FileInfo.
type file struct {
	name    string
	data    []byte
	modTime time.Time
}

func (f *file) Close() error {
	return nil
}

func (f *file) IsDir() bool {
	return false
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
	f.modTime = time.Now()
	f.data = append(f.data, p...)
	return len(p), nil
}

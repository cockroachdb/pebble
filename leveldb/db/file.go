// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// File is a readable, writable sequence of bytes.
//
// Typically, it will be an *os.File, but test code may choose to substitute
// memory-backed implementations.
type File interface {
	io.Closer
	io.ReaderAt
	io.Writer
	Stat() (stat os.FileInfo, err error)
}

// FileSystem is a namespace for files.
//
// The names are filepath names: they may be / separated or \ separated,
// depending on the underlying operating system.
type FileSystem interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error

	// Lock locks the given file. A nil Closer is returned if an error occurred.
	// Otherwise, close that Closer to release the lock.
	//
	// On Linux, a lock has the same semantics as fcntl(2)'s advisory locks.
	// In particular, closing any other file descriptor for the same file will
	// release the lock prematurely.
	//
	// Lock is not yet implemented on other operating systems, and calling it
	// will return an error.
	Lock(name string) (io.Closer, error)

	// List returns a listing of the given directory. The names returned are
	// relative to dir.
	List(dir string) ([]string, error)
}

// DefaultFileSystem is a FileSystem implementation backed by the underlying
// operating system's file system.
var DefaultFileSystem FileSystem = defFS{}

type defFS struct{}

func (defFS) Create(name string) (File, error) {
	return os.Create(name)
}

func (defFS) Open(name string) (File, error) {
	return os.Open(name)
}

func (defFS) Remove(name string) error {
	return os.Remove(name)
}

func (defFS) List(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

// NewMemFileSystem returns a new memory-backed FileSystem implementation.
func NewMemFileSystem() FileSystem {
	return new(memFS)
}

type memFS struct {
	mu sync.Mutex
	m  map[string]*memFile
}

func (m *memFS) Create(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = make(map[string]*memFile)
	}

	f, ok := m.m[name]
	if !ok {
		f = &memFile{name: name}
		m.m[name] = f
	} else {
		f.data = nil
	}
	return f, nil
}

func (m *memFS) Open(name string) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = make(map[string]*memFile)
	}

	f, ok := m.m[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return f, nil
}

func (m *memFS) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = make(map[string]*memFile)
	}

	_, ok := m.m[name]
	if !ok {
		return os.ErrNotExist
	}
	delete(m.m, name)
	return nil
}

func (m *memFS) Lock(name string) (io.Closer, error) {
	return nil, errors.New("leveldb/db: file locking is not implemented for memory-backed file systems")
}

func (m *memFS) List(dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = make(map[string]*memFile)
	}

	names := make(map[string]bool)
	if len(dir) == 0 || dir[len(dir)-1] != os.PathSeparator {
		dir += string(os.PathSeparator)
	}
	for fullName := range m.m {
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

// memFile is a memFS file.
type memFile struct {
	name    string
	data    []byte
	modTime time.Time
}

func (f *memFile) Close() error {
	return nil
}

func (f *memFile) IsDir() bool {
	return false
}

func (f *memFile) ModTime() time.Time {
	return f.modTime
}

func (f *memFile) Mode() os.FileMode {
	return os.FileMode(0755)
}

func (f *memFile) Name() string {
	return f.name
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	return copy(p, f.data[off:]), nil
}

func (f *memFile) Size() int64 {
	return int64(len(f.data))
}

func (f *memFile) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *memFile) Sys() interface{} {
	return nil
}

func (f *memFile) Write(p []byte) (int, error) {
	f.modTime = time.Now()
	f.data = append(f.data, p...)
	return len(p), nil
}

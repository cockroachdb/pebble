// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"io"
	"os"
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

// MemFileSystem is a memory-backed FileSystem implementation.
var MemFileSystem FileSystem = memFS{}

type memFS struct {
	mu sync.Mutex
	m  map[string]*memFile
}

func (m memFS) Create(name string) (File, error) {
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

func (m memFS) Open(name string) (File, error) {
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

func (m memFS) Remove(name string) error {
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

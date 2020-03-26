// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/pebble/vfs"
)

// OnIndex constructs an injector that returns an error on
// the (n+1)-th invocation of its MaybeError function. It
// may be passed to Wrap to inject an error into an FS.
func OnIndex(index int32) *InjectIndex {
	return &InjectIndex{index: index}
}

// InjectIndex implements Injector, injecting an error at a specific index.
type InjectIndex struct {
	index int32
}

// Index returns the index at which the error will be injected.
func (ii *InjectIndex) Index() int32 { return atomic.LoadInt32(&ii.index) }

// SetIndex sets the index at which the error will be injected.
func (ii *InjectIndex) SetIndex(v int32) { atomic.StoreInt32(&ii.index, v) }

// MaybeError implements the Injector interface.
func (ii *InjectIndex) MaybeError() error {
	if atomic.AddInt32(&ii.index, -1) == -1 {
		return fmt.Errorf("injected error")
	}
	return nil
}

// Injector injects errors into FS operations.
type Injector interface {
	MaybeError() error
}

// FS implements vfs.FS, injecting errors into
// its operations.
type FS struct {
	fs  vfs.FS
	inj Injector
}

// Wrap wraps an existing vfs.FS implementation, returning a new
// vfs.FS implementation that shadows operations to the provided FS.
// It uses the provided Injector for deciding when to inject errors.
// If an error is injected, FS propagates the error instead of
// shadowing the operation.
func Wrap(fs vfs.FS, inj Injector) *FS {
	return &FS{
		fs:  fs,
		inj: inj,
	}
}

// Create implements FS.Create.
func (fs *FS) Create(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	f, err := fs.fs.Create(name)
	if err != nil {
		return nil, err
	}
	return errorFile{f, fs}, nil
}

// Link implements FS.Link.
func (fs *FS) Link(oldname, newname string) error {
	if err := fs.inj.MaybeError(); err != nil {
		return err
	}
	return fs.fs.Link(oldname, newname)
}

// Open implements FS.Open.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	ef := errorFile{f, fs}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

// OpenDir implements FS.OpenDir.
func (fs *FS) OpenDir(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return errorFile{f, fs}, nil
}

// PathBase implements FS.PathBase.
func (fs *FS) PathBase(p string) string {
	return fs.fs.PathBase(p)
}

// PathDir implements FS.PathDir.
func (fs *FS) PathDir(p string) string {
	return fs.fs.PathDir(p)
}

// PathJoin implements FS.PathJoin.
func (fs *FS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

// Remove implements FS.Remove.
func (fs *FS) Remove(name string) error {
	if _, err := fs.fs.Stat(name); os.IsNotExist(err) {
		return nil
	}

	if err := fs.inj.MaybeError(); err != nil {
		return err
	}
	return fs.fs.Remove(name)
}

// RemoveAll implements FS.RemoveAll.
func (fs *FS) RemoveAll(fullname string) error {
	if err := fs.inj.MaybeError(); err != nil {
		return err
	}
	return fs.fs.RemoveAll(fullname)
}

// Rename implements FS.Rename.
func (fs *FS) Rename(oldname, newname string) error {
	if err := fs.inj.MaybeError(); err != nil {
		return err
	}
	return fs.fs.Rename(oldname, newname)
}

// ReuseForWrite implements FS.ReuseForWrite.
func (fs *FS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	return fs.fs.ReuseForWrite(oldname, newname)
}

// MkdirAll implements FS.MkdirAll.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.inj.MaybeError(); err != nil {
		return err
	}
	return fs.fs.MkdirAll(dir, perm)
}

// Lock implements FS.Lock.
func (fs *FS) Lock(name string) (io.Closer, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	return fs.fs.Lock(name)
}

// List implements FS.List.
func (fs *FS) List(dir string) ([]string, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	return fs.fs.List(dir)
}

// Stat implements FS.Stat.
func (fs *FS) Stat(name string) (os.FileInfo, error) {
	if err := fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	return fs.fs.Stat(name)
}

type errorFile struct {
	file vfs.File
	fs   *FS
}

func (f errorFile) Close() error {
	// We don't inject errors during close as those calls should never fail in
	// practice.
	return f.file.Close()
}

func (f errorFile) Read(p []byte) (int, error) {
	if err := f.fs.inj.MaybeError(); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.fs.inj.MaybeError(); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f errorFile) Write(p []byte) (int, error) {
	if err := f.fs.inj.MaybeError(); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f errorFile) Stat() (os.FileInfo, error) {
	if err := f.fs.inj.MaybeError(); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f errorFile) Sync() error {
	if err := f.fs.inj.MaybeError(); err != nil {
		return err
	}
	return f.file.Sync()
}

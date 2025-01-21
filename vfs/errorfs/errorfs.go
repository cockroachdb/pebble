// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/v2/internal/dsl"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// ErrInjected is an error artificially injected for testing fs error paths.
var ErrInjected = LabelledError{
	error: errors.New("injected error"),
	Label: "ErrInjected",
}

// Op describes a filesystem operation.
type Op struct {
	// Kind describes the particular kind of operation being performed.
	Kind OpKind
	// Path is the path of the file of the file being operated on.
	Path string
	// Offset is the offset of an operation. It's set for OpFileReadAt and
	// OpFileWriteAt operations.
	Offset int64
}

// OpKind is an enum describing the type of operation.
type OpKind int

const (
	// OpCreate describes a create file operation.
	OpCreate OpKind = iota
	// OpLink describes a hardlink operation.
	OpLink
	// OpOpen describes a file open operation.
	OpOpen
	// OpOpenDir describes a directory open operation.
	OpOpenDir
	// OpRemove describes a remove file operation.
	OpRemove
	// OpRemoveAll describes a recursive remove operation.
	OpRemoveAll
	// OpRename describes a rename operation.
	OpRename
	// OpReuseForWrite describes a reuse for write operation.
	OpReuseForWrite
	// OpMkdirAll describes a make directory including parents operation.
	OpMkdirAll
	// OpLock describes a lock file operation.
	OpLock
	// OpList describes a list directory operation.
	OpList
	// OpFilePreallocate describes a file preallocate operation.
	OpFilePreallocate
	// OpStat describes a path-based stat operation.
	OpStat
	// OpGetDiskUsage describes a disk usage operation.
	OpGetDiskUsage
	// OpFileClose describes a close file operation.
	OpFileClose
	// OpFileRead describes a file read operation.
	OpFileRead
	// OpFileReadAt describes a file seek read operation.
	OpFileReadAt
	// OpFileWrite describes a file write operation.
	OpFileWrite
	// OpFileWriteAt describes a file seek write operation.
	OpFileWriteAt
	// OpFileStat describes a file stat operation.
	OpFileStat
	// OpFileSync describes a file sync operation.
	OpFileSync
	// OpFileSyncData describes a file sync operation.
	OpFileSyncData
	// OpFileSyncTo describes a file sync operation.
	OpFileSyncTo
	// OpFileFlush describes a file flush operation.
	OpFileFlush
)

// ReadOrWrite returns the operation's kind.
func (o OpKind) ReadOrWrite() OpReadWrite {
	switch o {
	case OpOpen, OpOpenDir, OpList, OpStat, OpGetDiskUsage, OpFileRead, OpFileReadAt, OpFileStat:
		return OpIsRead
	case OpCreate, OpLink, OpRemove, OpRemoveAll, OpRename, OpReuseForWrite, OpMkdirAll, OpLock, OpFileClose, OpFileWrite, OpFileWriteAt, OpFileSync, OpFileSyncData, OpFileSyncTo, OpFileFlush, OpFilePreallocate:
		return OpIsWrite
	default:
		panic(fmt.Sprintf("unrecognized op %v\n", o))
	}
}

// OpReadWrite is an enum describing whether an operation is a read or write
// operation.
type OpReadWrite int

const (
	// OpIsRead describes read operations.
	OpIsRead OpReadWrite = iota
	// OpIsWrite describes write operations.
	OpIsWrite
)

// String implements fmt.Stringer.
func (kind OpReadWrite) String() string {
	switch kind {
	case OpIsRead:
		return "Reads"
	case OpIsWrite:
		return "Writes"
	default:
		panic(fmt.Sprintf("unrecognized OpKind %d", kind))
	}
}

// OnIndex is a convenience function for constructing a dsl.OnIndex for use with
// an error-injecting filesystem.
func OnIndex(index int32) *InjectIndex {
	return &InjectIndex{dsl.OnIndex[Op](index)}
}

// InjectIndex implements Injector, injecting an error at a specific index.
type InjectIndex struct {
	*dsl.Index[Op]
}

// MaybeError implements the Injector interface.
//
// TODO(jackson): Remove this implementation and update callers to compose it
// with other injectors.
func (ii *InjectIndex) MaybeError(op Op) error {
	if !ii.Evaluate(op) {
		return nil
	}
	return ErrInjected
}

// InjectorFunc implements the Injector interface for a function with
// MaybeError's signature.
type InjectorFunc func(Op) error

// String implements fmt.Stringer.
func (f InjectorFunc) String() string { return "<opaque func>" }

// MaybeError implements the Injector interface.
func (f InjectorFunc) MaybeError(op Op) error { return f(op) }

// Injector injects errors into FS operations.
type Injector interface {
	fmt.Stringer
	// MaybeError is invoked by an errorfs before an operation is executed. It
	// is passed an enum indicating the type of operation and a path of the
	// subject file or directory. If the operation takes two paths (eg,
	// Rename, Link), the original source path is provided.
	MaybeError(op Op) error
}

// Any returns an injector that injects an error if any of the provided
// injectors inject an error. The error returned by the first injector to return
// an error is used.
func Any(injectors ...Injector) Injector {
	return anyInjector(injectors)
}

type anyInjector []Injector

func (a anyInjector) String() string {
	var sb strings.Builder
	sb.WriteString("(Any")
	for _, inj := range a {
		sb.WriteString(" ")
		sb.WriteString(inj.String())
	}
	sb.WriteString(")")
	return sb.String()
}

func (a anyInjector) MaybeError(op Op) error {
	for _, inj := range a {
		if err := inj.MaybeError(op); err != nil {
			return err
		}
	}
	return nil
}

// Counter wraps an Injector, counting the number of errors injected. It may be
// used in tests to ensure that when an error is injected, the error is
// surfaced through the user interface.
type Counter struct {
	Injector
	mu struct {
		sync.Mutex
		v       uint64
		lastErr error
	}
}

// String implements fmt.Stringer.
func (c *Counter) String() string {
	return c.Injector.String()
}

// MaybeError implements Injector.
func (c *Counter) MaybeError(op Op) error {
	err := c.Injector.MaybeError(op)
	if err != nil {
		c.mu.Lock()
		c.mu.v++
		c.mu.lastErr = err
		c.mu.Unlock()
	}
	return err
}

// Load returns the number of errors injected.
func (c *Counter) Load() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.v
}

// LastError returns the last non-nil error injected.
func (c *Counter) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.lastErr
}

// Toggle wraps an Injector. By default, Toggle injects nothing. When toggled on
// through its On method, it begins injecting errors when the contained injector
// injects them. It may be returned to its original state through Off.
type Toggle struct {
	Injector
	on atomic.Bool
}

// String implements fmt.Stringer.
func (t *Toggle) String() string {
	return t.Injector.String()
}

// MaybeError implements Injector.
func (t *Toggle) MaybeError(op Op) error {
	if !t.on.Load() {
		return nil
	}
	return t.Injector.MaybeError(op)
}

// On enables error injection.
func (t *Toggle) On() { t.on.Store(true) }

// Off disables error injection.
func (t *Toggle) Off() { t.on.Store(false) }

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

// WrapFile wraps an existing vfs.File, returning a new vfs.File that shadows
// operations to the provided vfs.File. It uses the provided Injector for
// deciding when to inject errors. If an error is injected, the file
// propagates the error instead of shadowing the operation.
func WrapFile(f vfs.File, inj Injector) vfs.File {
	return &errorFile{file: f, inj: inj}
}

// Unwrap returns the FS implementation underlying fs.
// See pebble/vfs.Root.
func (fs *FS) Unwrap() vfs.FS {
	return fs.fs
}

// Create implements FS.Create.
func (fs *FS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpCreate, Path: name}); err != nil {
		return nil, err
	}
	f, err := fs.fs.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &errorFile{name, f, fs.inj}, nil
}

// Link implements FS.Link.
func (fs *FS) Link(oldname, newname string) error {
	if err := fs.inj.MaybeError(Op{Kind: OpLink, Path: oldname}); err != nil {
		return err
	}
	return fs.fs.Link(oldname, newname)
}

// Open implements FS.Open.
func (fs *FS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpOpen, Path: name}); err != nil {
		return nil, err
	}
	f, err := fs.fs.Open(name)
	if err != nil {
		return nil, err
	}
	ef := &errorFile{name, f, fs.inj}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

// OpenReadWrite implements FS.OpenReadWrite.
func (fs *FS) OpenReadWrite(
	name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption,
) (vfs.File, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpOpen, Path: name}); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenReadWrite(name, category)
	if err != nil {
		return nil, err
	}
	ef := &errorFile{name, f, fs.inj}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

// OpenDir implements FS.OpenDir.
func (fs *FS) OpenDir(name string) (vfs.File, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpOpenDir, Path: name}); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &errorFile{name, f, fs.inj}, nil
}

// GetDiskUsage implements FS.GetDiskUsage.
func (fs *FS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpGetDiskUsage, Path: path}); err != nil {
		return vfs.DiskUsage{}, err
	}
	return fs.fs.GetDiskUsage(path)
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
	if _, err := fs.fs.Stat(name); oserror.IsNotExist(err) {
		return nil
	}

	if err := fs.inj.MaybeError(Op{Kind: OpRemove, Path: name}); err != nil {
		return err
	}
	return fs.fs.Remove(name)
}

// RemoveAll implements FS.RemoveAll.
func (fs *FS) RemoveAll(fullname string) error {
	if err := fs.inj.MaybeError(Op{Kind: OpRemoveAll, Path: fullname}); err != nil {
		return err
	}
	return fs.fs.RemoveAll(fullname)
}

// Rename implements FS.Rename.
func (fs *FS) Rename(oldname, newname string) error {
	if err := fs.inj.MaybeError(Op{Kind: OpRename, Path: oldname}); err != nil {
		return err
	}
	return fs.fs.Rename(oldname, newname)
}

// ReuseForWrite implements FS.ReuseForWrite.
func (fs *FS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpReuseForWrite, Path: oldname}); err != nil {
		return nil, err
	}
	return fs.fs.ReuseForWrite(oldname, newname, category)
}

// MkdirAll implements FS.MkdirAll.
func (fs *FS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.inj.MaybeError(Op{Kind: OpMkdirAll, Path: dir}); err != nil {
		return err
	}
	return fs.fs.MkdirAll(dir, perm)
}

// Lock implements FS.Lock.
func (fs *FS) Lock(name string) (io.Closer, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpLock, Path: name}); err != nil {
		return nil, err
	}
	return fs.fs.Lock(name)
}

// List implements FS.List.
func (fs *FS) List(dir string) ([]string, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpList, Path: dir}); err != nil {
		return nil, err
	}
	return fs.fs.List(dir)
}

// Stat implements FS.Stat.
func (fs *FS) Stat(name string) (vfs.FileInfo, error) {
	if err := fs.inj.MaybeError(Op{Kind: OpStat, Path: name}); err != nil {
		return nil, err
	}
	return fs.fs.Stat(name)
}

// errorFile implements vfs.File. The interface is implemented on the pointer
// type to allow pointer equality comparisons.
type errorFile struct {
	path string
	file vfs.File
	inj  Injector
}

func (f *errorFile) Close() error {
	// We don't inject errors during close as those calls should never fail in
	// practice.
	return f.file.Close()
}

func (f *errorFile) Read(p []byte) (int, error) {
	if err := f.inj.MaybeError(Op{Kind: OpFileRead, Path: f.path}); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f *errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(Op{
		Kind:   OpFileReadAt,
		Path:   f.path,
		Offset: off,
	}); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f *errorFile) Write(p []byte) (int, error) {
	if err := f.inj.MaybeError(Op{Kind: OpFileWrite, Path: f.path}); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f *errorFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(Op{
		Kind:   OpFileWriteAt,
		Path:   f.path,
		Offset: off,
	}); err != nil {
		return 0, err
	}
	return f.file.WriteAt(p, off)
}

func (f *errorFile) Stat() (vfs.FileInfo, error) {
	if err := f.inj.MaybeError(Op{Kind: OpFileStat, Path: f.path}); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f *errorFile) Prefetch(offset, length int64) error {
	// TODO(radu): Consider error injection.
	return f.file.Prefetch(offset, length)
}

func (f *errorFile) Preallocate(offset, length int64) error {
	if err := f.inj.MaybeError(Op{Kind: OpFilePreallocate, Path: f.path}); err != nil {
		return err
	}
	return f.file.Preallocate(offset, length)
}

func (f *errorFile) Sync() error {
	if err := f.inj.MaybeError(Op{Kind: OpFileSync, Path: f.path}); err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *errorFile) SyncData() error {
	if err := f.inj.MaybeError(Op{Kind: OpFileSyncData, Path: f.path}); err != nil {
		return err
	}
	return f.file.SyncData()
}

func (f *errorFile) SyncTo(length int64) (fullSync bool, err error) {
	if err := f.inj.MaybeError(Op{Kind: OpFileSyncTo, Path: f.path}); err != nil {
		return false, err
	}
	return f.file.SyncTo(length)
}

func (f *errorFile) Fd() uintptr {
	return f.file.Fd()
}

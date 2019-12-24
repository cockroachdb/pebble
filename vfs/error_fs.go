package vfs

import (
	"errors"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
)

// ErrorFSMode is a bit field specifying the operation types for which error injection
// is enabled.
type ErrorFSMode int

// ErrorFSMsg is the error message for injected errors.
const ErrorFSMsg = "injected error"

const (
	// ErrorFSRead enables errors for filesystem read operations.
	ErrorFSRead ErrorFSMode = 0x1
	// ErrorFSWrite enables errors for filesystem write operations.
	ErrorFSWrite = 0x2
)

// NewErrorFS returns a new FS implementation that wraps another FS
// and injects an error for the operation at the specified index, or
// for operations with the specified probability.
func NewErrorFS(index *int32, prob float64, mode ErrorFSMode, fs FS) FS {
	return &errorFS{
		FS:    fs,
		index: index,
		prob:  prob,
		mode:  mode,
	}
}

type errorFS struct {
	FS
	index *int32
	prob  float64
	mode  ErrorFSMode
}

func (fs *errorFS) maybeError(mode ErrorFSMode) error {
	if fs.mode&mode == 0 {
		return nil
	}
	if fs.index != nil && atomic.AddInt32(fs.index, -1) == -1 {
		return errors.New(ErrorFSMsg)
	}
	if fs.prob > 0.0 && rand.Float64() < fs.prob {
		return errors.New(ErrorFSMsg)
	}
	return nil
}

func (fs *errorFS) Create(name string) (File, error) {
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return nil, err
	}
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return errorFile{f, fs}, nil
}

func (fs *errorFS) Link(oldname, newname string) error {
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return err
	}
	return fs.FS.Link(oldname, newname)
}

func (fs *errorFS) Open(name string, opts ...OpenOption) (File, error) {
	if err := fs.maybeError(ErrorFSRead); err != nil {
		return nil, err
	}
	f, err := fs.FS.Open(name)
	if err != nil {
		return nil, err
	}
	ef := errorFile{f, fs}
	for _, opt := range opts {
		opt.Apply(ef)
	}
	return ef, nil
}

func (fs *errorFS) OpenDir(name string) (File, error) {
	if err := fs.maybeError(ErrorFSRead); err != nil {
		return nil, err
	}
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return errorFile{f, fs}, nil
}

func (fs *errorFS) Remove(name string) error {
	if _, err := fs.FS.Stat(name); os.IsNotExist(err) {
		return nil
	}
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return err
	}
	return fs.FS.Remove(name)
}

func (fs *errorFS) Rename(oldname, newname string) error {
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return err
	}
	return fs.FS.Rename(oldname, newname)
}

func (fs *errorFS) ReuseForWrite(oldname, newname string) (File, error) {
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return nil, err
	}
	return fs.FS.ReuseForWrite(oldname, newname)
}

func (fs *errorFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return err
	}
	return fs.FS.MkdirAll(dir, perm)
}

func (fs *errorFS) Lock(name string) (io.Closer, error) {
	if err := fs.maybeError(ErrorFSWrite); err != nil {
		return nil, err
	}
	return fs.FS.Lock(name)
}

func (fs *errorFS) List(dir string) ([]string, error) {
	if err := fs.maybeError(ErrorFSRead); err != nil {
		return nil, err
	}
	return fs.FS.List(dir)
}

func (fs *errorFS) Stat(name string) (os.FileInfo, error) {
	if err := fs.maybeError(ErrorFSRead); err != nil {
		return nil, err
	}
	return fs.FS.Stat(name)
}

type errorFile struct {
	file File
	fs   *errorFS
}

func (f errorFile) Close() error {
	// We don't inject errors during close as those calls should never fail in
	// practice.
	return f.file.Close()
}

func (f errorFile) Read(p []byte) (int, error) {
	if err := f.fs.maybeError(ErrorFSRead); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.fs.maybeError(ErrorFSRead); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f errorFile) Write(p []byte) (int, error) {
	if err := f.fs.maybeError(ErrorFSWrite); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f errorFile) Stat() (os.FileInfo, error) {
	if err := f.fs.maybeError(ErrorFSRead); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f errorFile) Sync() error {
	if err := f.fs.maybeError(ErrorFSWrite); err != nil {
		return err
	}
	return f.file.Sync()
}

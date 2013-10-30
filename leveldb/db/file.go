// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"io"
	"os"
)

// File is a readable, writable sequence of bytes.
//
// Typically, it will be an *os.File, but test code may choose to substitute
// memory-backed implementations.
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	Stat() (os.FileInfo, error)
	Sync() error
}

// FileSystem is a namespace for files.
//
// The names are filepath names: they may be / separated or \ separated,
// depending on the underlying operating system.
type FileSystem interface {
	// Create creates the named file for writing, truncating it if it already
	// exists.
	Create(name string) (File, error)

	// Open opens the named file for reading.
	Open(name string) (File, error)

	// Remove removes the named file or directory.
	Remove(name string) error

	// Rename renames a file. It overwrites the file at newname if one exists,
	// the same as os.Rename.
	Rename(oldname, newname string) error

	// MkdirAll creates a directory and all necessary parents. The permission
	// bits perm have the same semantics as in os.MkdirAll. If the directory
	// already exists, MkdirAll does nothing and returns nil.
	MkdirAll(dir string, perm os.FileMode) error

	// Lock locks the given file, creating the file if necessary, and
	// truncating the file if it already exists. The lock is an exclusive lock
	// (a write lock), but locked files should neither be read from nor written
	// to. Such files should have zero size and only exist to co-ordinate
	// ownership across processes.
	//
	// A nil Closer is returned if an error occurred. Otherwise, close that
	// Closer to release the lock.
	//
	// On Linux and OSX, a lock has the same semantics as fcntl(2)'s advisory
	// locks.  In particular, closing any other file descriptor for the same
	// file will release the lock prematurely.
	//
	// Attempting to lock a file that is already locked by the current process
	// has undefined behavior.
	//
	// Lock is not yet implemented on other operating systems, and calling it
	// will return an error.
	Lock(name string) (io.Closer, error)

	// List returns a listing of the given directory. The names returned are
	// relative to dir.
	List(dir string) ([]string, error)

	// Stat returns an os.FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)
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

func (defFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (defFS) MkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
}

func (defFS) List(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

func (defFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

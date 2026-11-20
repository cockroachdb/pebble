// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build windows

package vfs

import (
	"io/fs"
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/windows"
)

func wrapOSFileImpl(f *os.File) File {
	return &windowsFile{f}
}

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &windowsDir{f}, nil
}

// Assert that windowsFile and windowsDir implement vfs.File.
var (
	_ File = (*windowsFile)(nil)
	_ File = (*windowsDir)(nil)
)

type windowsDir struct {
	*os.File
}

func (d *windowsDir) Stat() (FileInfo, error)                 { return maybeWrapFileInfo(d.File.Stat()) }
func (*windowsDir) Prefetch(offset int64, length int64) error { return nil }
func (*windowsDir) Preallocate(off, length int64) error       { return nil }

// Sync flushes the directory. On POSIX, OpenDir returns a file descriptor
// that callers fsync directly; on Windows the handle OpenDir holds is
// read-only, and FlushFileBuffers requires the handle to carry GENERIC_WRITE
// (a read-only handle fails with ERROR_ACCESS_DENIED), so a dedicated handle
// is opened for the duration of the call. The directory entry of a newly
// created file only becomes durable when the parent directory handle is
// flushed; without this, the Windows side silently reported success for the
// same contract the POSIX side honors. RocksDB's WinDirectory::Fsync
// (port/win/io_win.cc) stopped being a no-op in the same way.
func (d *windowsDir) Sync() error {
	path, err := syscall.UTF16PtrFromString(d.Name())
	if err != nil {
		return errors.WithStack(err)
	}
	handle, err := windows.CreateFile(path, windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := windows.FlushFileBuffers(handle); err != nil {
		_ = windows.CloseHandle(handle)
		return errors.WithStack(err)
	}
	if err := windows.CloseHandle(handle); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
func (*windowsDir) SyncData() error                                { return nil }
func (*windowsDir) SyncTo(length int64) (fullSync bool, err error) { return false, nil }

type windowsFile struct {
	*os.File
}

func (*windowsFile) Prefetch(offset int64, length int64) error { return nil }
func (*windowsFile) Preallocate(offset, length int64) error    { return nil }

func (f *windowsFile) Stat() (FileInfo, error) { return maybeWrapFileInfo(f.File.Stat()) }
func (f *windowsFile) SyncData() error         { return f.Sync() }
func (f *windowsFile) SyncTo(length int64) (fullSync bool, err error) {
	if err = f.Sync(); err != nil {
		return false, err
	}
	return true, nil
}

func deviceIDFromFileInfo(finfo fs.FileInfo) DeviceID {
	// Unsupported.
	return DeviceID{}
}

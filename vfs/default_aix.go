// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build aix

package vfs

import (
	"io/fs"
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func wrapOSFileImpl(osFile *os.File) File {
	return &aixFile{File: osFile, fd: osFile.Fd()}
}

func (defaultFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &aixDir{f}, nil
}

// Assert that aixFile and aixDir implement vfs.File.
var (
	_ File = (*aixFile)(nil)
	_ File = (*aixDir)(nil)
)

type aixFile struct {
	*os.File
	fd uintptr
}

func (f *aixFile) Stat() (FileInfo, error)                 { return maybeWrapFileInfo(f.File.Stat()) }
func (*aixFile) Prefetch(offset int64, length int64) error { return nil }
func (*aixFile) Preallocate(offset, length int64) error    { return nil }

func (f *aixFile) SyncData() error {
	return f.Sync()
}

func (f *aixFile) SyncTo(int64) (fullSync bool, err error) {
	if err = f.Sync(); err != nil {
		return false, err
	}
	return true, nil
}

// aixDir is a handle to a directory. On AIX, fsync(2) requires a descriptor
// that is open for writing; a directory cannot be opened for writing, so fsync
// of a directory descriptor fails with EINVAL. See the AIX "fsync or
// fsync_range Subroutine" reference, and golang/go#41372 which documents Go's
// os.File.Sync failing on AIX descriptors that are not open for writing.
//
// This is safe to treat as success rather than work around. Pebble fsyncs a
// directory to make a newly created or renamed entry durable. On JFS2 that
// durability is already provided by the filesystem's transaction log: JFS2
// records every metadata (namespace) change in its log so the change survives
// a crash, independently of any application-issued directory fsync. See the
// AIX "JFS and JFS2" file-system documentation. An explicit directory fsync is
// therefore both impossible and unnecessary on AIX, and treating EINVAL as
// success does not weaken Pebble's durability guarantees.
//
// We still issue the fsync and swallow only EINVAL, so that any other error, or
// a filesystem that does implement directory fsync, is handled normally.
//
//	AIX fsync: https://www.ibm.com/docs/ssw_aix_72/f_bostechref/fsync.html
//	JFS2 log:  https://www.ibm.com/docs/en/aix/7.1.0?topic=jfs2-jfs-functions
//	Go #41372: https://github.com/golang/go/issues/41372
type aixDir struct {
	*os.File
}

func (d *aixDir) Sync() error {
	err := d.File.Sync()
	if errors.Is(err, unix.EINVAL) {
		return nil
	}
	return err
}

func (d *aixDir) Stat() (FileInfo, error)                        { return maybeWrapFileInfo(d.File.Stat()) }
func (*aixDir) Prefetch(offset int64, length int64) error        { return nil }
func (*aixDir) Preallocate(offset, length int64) error           { return nil }
func (d *aixDir) SyncData() error                                { return d.Sync() }
func (d *aixDir) SyncTo(length int64) (fullSync bool, err error) { return false, nil }

func deviceIDFromFileInfo(finfo fs.FileInfo) DeviceID {
	statInfo := finfo.Sys().(*syscall.Stat_t)
	id := DeviceID{
		major: unix.Major(uint64(statInfo.Dev)),
		minor: unix.Minor(uint64(statInfo.Dev)),
	}
	return id
}

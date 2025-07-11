// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"io"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/vfs"
)

// AcquireOrValidateDirectoryLock attempts to acquire a lock on the
// provided directory, or validates a pre-acquired DirLock.
func AcquireOrValidateDirectoryLock(
	preAcquiredLock *DirLock, dirname string, fs vfs.FS,
) (*DirLock, error) {
	// If a pre-acquired lock is provided, check that it matches the directory
	// we're trying to open.
	if preAcquiredLock != nil {
		if err := preAcquiredLock.pathMatches(dirname); err != nil {
			return preAcquiredLock, err
		}
		return preAcquiredLock, preAcquiredLock.refForOpen()
	}

	// Otherwise, acquire the lock for the directory.
	return LockDirectory(dirname, fs)
}

// LockDirectory acquires the directory lock in the named directory, preventing
// another process from opening the database. LockDirectory returns a
// handle to the held lock that may be passed to Open, skipping lock acquisition
// during Open.
//
// LockDirectory may be used to expand the critical section protected by the
// database lock to include setup before the call to Open.
func LockDirectory(dirname string, fs vfs.FS) (*DirLock, error) {
	fileLock, err := fs.Lock(MakeFilepath(fs, dirname, FileTypeLock, DiskFileNum(0)))
	if err != nil {
		return nil, err
	}
	l := &DirLock{dirname: dirname, fileLock: fileLock}
	l.refs.Store(1)
	invariants.SetFinalizer(l, func(obj interface{}) {
		if refs := obj.(*DirLock).refs.Load(); refs > 0 {
			panic(errors.AssertionFailedf("lock for %q finalized with %d refs", dirname, refs))
		}
	})
	return l, nil
}

// DirLock represents a file lock on a directory. It may be passed to Open through
// Options.Lock to elide lock aquisition during Open.
type DirLock struct {
	dirname  string
	fileLock io.Closer
	// refs is a count of the number of handles on the lock. refs must be 0, 1
	// or 2.
	//
	// When acquired by the client and passed to Open, refs = 1 and the Open
	// call increments it to 2. When the database is closed, it's decremented to
	// 1. Finally when the original caller calls Close on the Lock, it's
	// decremented to zero and the underlying file lock is released.
	//
	// When Open acquires the file lock, refs remains at 1 until the database is
	// closed.
	refs atomic.Int32
}

func (l *DirLock) refForOpen() error {
	// During Open, when a user passed in a lock, the reference count must be
	// exactly 1. If it's zero, the lock is no longer held and is invalid. If
	// it's 2, the lock is already in use by another database within the
	// process.
	if !l.refs.CompareAndSwap(1, 2) {
		return errors.Errorf("pebble: unexpected %q DirLock reference count; is the lock already in use?", l.dirname)
	}
	return nil
}

func (l *DirLock) Refs() int {
	// Return the current reference count. This is used for testing purposes.
	return int(l.refs.Load())
}

// Close releases the lock, permitting another process to lock and open the
// database. Close must not be called until after a database using the Lock has
// been closed.
func (l *DirLock) Close() error {
	v := l.refs.Add(-1)
	if v > 0 {
		return nil
	} else if v < 0 {
		return errors.AssertionFailedf("pebble: unexpected %q DirLock reference count %d", l.dirname, v)
	}
	defer func() { l.fileLock = nil }()
	return l.fileLock.Close()
}

func (l *DirLock) pathMatches(dirname string) error {
	if dirname == l.dirname {
		return nil
	}
	// Check for relative paths, symlinks, etc. This isn't ideal because we're
	// circumventing the vfs.FS interface here.
	//
	// TODO(jackson): We could add support for retrieving file inodes through Stat
	// calls in the VFS interface on platforms where it's available and use that
	// to differentiate.
	dirStat, err1 := os.Stat(dirname)
	lockDirStat, err2 := os.Stat(l.dirname)
	if err1 == nil && err2 == nil && os.SameFile(dirStat, lockDirStat) {
		return nil
	}
	return errors.Join(
		errors.Newf("pebble: opts.Lock acquired in %q not %q", l.dirname, dirname),
		err1, err2)
}

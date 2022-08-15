// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build darwin && arm64
// +build darwin,arm64

package vfs

import (
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	F_BARRIERFSYNC = 85
	F_FULLFSYNC    = 51

	SYNC_MODE_FSYNC        = 1
	SYNC_MODE_FULLFSYNC    = 2
	SYNC_MODE_BARRIERFSYNC = 3
)

var (
	DarwinSyncMode = SYNC_MODE_FULLFSYNC
)

func init() {
	syncMode := os.Getenv("DARWIN_SYNC_MODE")
	syncMode = strings.ToUpper(syncMode)
	switch syncMode {
	case "FSYNC":
		DarwinSyncMode = SYNC_MODE_FSYNC
	case "BFSYNC", "BARRIERFSYNC":
		DarwinSyncMode = SYNC_MODE_BARRIERFSYNC
	case "FFSYNC", "FULLFSYNC":
		DarwinSyncMode = SYNC_MODE_FULLFSYNC
	}
}

func (f *syncingFile) init() {
	f.syncTo = f.syncToGeneric
	f.syncData = f.darwinSync
}

func (f *syncingFile) syncToGeneric(_ int64) error {
	return f.darwinSync()
}

func (f *syncingFile) darwinSync() error {
	switch DarwinSyncMode {
	case SYNC_MODE_FSYNC:
		return f.fsync()
	case SYNC_MODE_FULLFSYNC:
		return f.fcntlSync(F_FULLFSYNC)
	default:
		return f.fcntlSync(F_BARRIERFSYNC)
	}
}

func (f *syncingFile) fcntlSync(mode int) error {
	if f.fd == 0 {
		return f.File.Sync()
	}
	var err error
	f.timeDiskOp(func() {
		_, err = unix.FcntlInt(f.fd, mode, 0)
	})
	return err
}

func (f *syncingFile) fsync() error {
	if f.fd == 0 {
		return f.File.Sync()
	}
	var err error
	f.timeDiskOp(func() {
		err = syscall.Fsync(int(f.fd))
	})
	return err
}

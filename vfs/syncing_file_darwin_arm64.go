// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build darwin && arm64
// +build darwin,arm64

package vfs

import "golang.org/x/sys/unix"

const (
	F_BARRIERFSYNC = 85
)

func (f *syncingFile) init() {
	f.syncTo = f.syncToGeneric
	f.syncData = f.barrierSync
}

func (f *syncingFile) syncToGeneric(_ int64) error {
	return f.barrierSync()
}

func (f *syncingFile) barrierSync() error {
	if f.fd == 0 {
		return f.File.Sync()
	}
	var err error
	f.timeDiskOp(func() {
		_, err = unix.FcntlInt(f.fd, F_BARRIERFSYNC, 0)
	})
	return err
}

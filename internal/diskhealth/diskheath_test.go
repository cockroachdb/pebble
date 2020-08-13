// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package diskhealth

import (
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

type mockFile struct {
	syncDuration time.Duration
}

func (m mockFile) Close() error {
	return nil
}

func (m mockFile) Read(p []byte) (n int, err error) {
	panic("unimplemented")
}

func (m mockFile) ReadAt(p []byte, off int64) (n int, err error) {
	panic("unimplemented")
}

func (m mockFile) Write(p []byte) (n int, err error) {
	time.Sleep(m.syncDuration)
	return len(p), nil
}

func (m mockFile) Stat() (os.FileInfo, error) {
	panic("unimplemented")
}

func (m mockFile) Sync() error {
	time.Sleep(m.syncDuration)
	return nil
}

var _ vfs.File = &mockFile{}

func TestDiskStallDetector(t *testing.T) {
	diskStalled := make(chan struct{}, 1)
	diskSlow := make(chan struct{}, 100)

	dsDetector := NewDiskStallDetector(
		4 * time.Second, 1 * time.Second, func(d time.Duration) {
			diskStalled <- struct{}{}
		}, func(d time.Duration) {
			diskSlow <- struct{}{}
		})
	dsDetector.tickInterval = 500 * time.Millisecond
	file := &mockFile{syncDuration: 2 * time.Second}
	wrappedFile := dsDetector.WrapFile(file)
	defer wrappedFile.Close()

	wrappedFile.Sync()

	select {
	case <-diskSlow:
	case <-time.After(5 * time.Second):
		t.Fatal("disk stall detector did not detect slow disk operation")
	}

	file.syncDuration = 8 * time.Second
	wrappedFile.Sync()

	select {
	case <-diskStalled:
	case <-time.After(15 * time.Second):
		t.Fatal("disk stall detector did not detect stalled disk operation")
	}
}

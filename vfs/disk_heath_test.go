// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"testing"
	"time"
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

var _ File = &mockFile{}

type mockFS struct {
	syncDuration time.Duration
}

func (m mockFS) Create(name string) (File, error) {
	return mockFile{syncDuration: m.syncDuration}, nil
}

func (m mockFS) Link(oldname, newname string) error {
	panic("unimplemented")
}

func (m mockFS) Open(name string, opts ...OpenOption) (File, error) {
	panic("unimplemented")
}

func (m mockFS) OpenDir(name string) (File, error) {
	panic("unimplemented")
}

func (m mockFS) Remove(name string) error {
	panic("unimplemented")
}

func (m mockFS) RemoveAll(name string) error {
	panic("unimplemented")
}

func (m mockFS) Rename(oldname, newname string) error {
	panic("unimplemented")
}

func (m mockFS) ReuseForWrite(oldname, newname string) (File, error) {
	return mockFile{syncDuration: m.syncDuration}, nil
}

func (m mockFS) MkdirAll(dir string, perm os.FileMode) error {
	panic("unimplemented")
}

func (m mockFS) Lock(name string) (io.Closer, error) {
	panic("unimplemented")
}

func (m mockFS) List(dir string) ([]string, error) {
	panic("unimplemented")
}

func (m mockFS) Stat(name string) (os.FileInfo, error) {
	panic("unimplemented")
}

func (m mockFS) PathBase(path string) string {
	panic("unimplemented")
}

func (m mockFS) PathJoin(elem ...string) string {
	panic("unimplemented")
}

func (m mockFS) PathDir(path string) string {
	panic("unimplemented")
}

func (m mockFS) GetFreeSpace(path string) (uint64, error) {
	panic("unimplemented")
}

var _ FS = &mockFS{}

func TestDiskHealthChecking(t *testing.T) {
	diskSlow := make(chan time.Duration, 100)
	slowThreshold := 1 * time.Second
	mockFS := &mockFS{syncDuration: 3 * time.Second}
	fs := WithDiskHealthChecks(mockFS, slowThreshold, func(s string, duration time.Duration) {
		diskSlow <- duration
	})
	dhFile, _ := fs.Create("test")
	defer dhFile.Close()

	dhFile.Sync()

	select {
	case d := <-diskSlow:
		if d.Seconds() < slowThreshold.Seconds() {
			t.Fatalf("expected %0.1f to be greater than threshold %0.1f", d.Seconds(), slowThreshold.Seconds())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("disk stall detector did not detect slow disk operation")
	}
}

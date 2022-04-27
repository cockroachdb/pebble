// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	return mockFile(m), nil
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
	return mockFile(m), nil
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

func (m mockFS) GetDiskUsage(path string) (DiskUsage, error) {
	panic("unimplemented")
}

var _ FS = &mockFS{}

func TestDiskHealthChecking(t *testing.T) {
	const (
		slowThreshold = 1 * time.Second
		syncDuration  = 3 * time.Second
	)
	testCases := []struct {
		op OpType
		fn func(f File)
	}{
		{
			OpTypeWrite,
			func(f File) { f.Write([]byte("uh oh")) },
		},
		{
			OpTypeSync,
			func(f File) { f.Sync() },
		},
	}
	for _, tc := range testCases {
		t.Run(tc.op.String(), func(t *testing.T) {
			type info struct {
				op       OpType
				duration time.Duration
			}
			diskSlow := make(chan info, 1)
			mockFS := &mockFS{syncDuration: syncDuration}
			fs := WithDiskHealthChecks(mockFS, slowThreshold, func(s string, op OpType, duration time.Duration) {
				diskSlow <- info{
					op:       op,
					duration: duration,
				}
			})
			dhFile, _ := fs.Create("test")
			defer dhFile.Close()

			tc.fn(dhFile)
			select {
			case i := <-diskSlow:
				d := i.duration
				if d.Seconds() < slowThreshold.Seconds() {
					t.Fatalf("expected %0.1f to be greater than threshold %0.1f", d.Seconds(), slowThreshold.Seconds())
				}
				require.Equal(t, tc.op, i.op)
			case <-time.After(5 * time.Second):
				t.Fatal("disk stall detector did not detect slow disk operation")
			}
		})
	}
}

func TestDiskHealthChecking_Underflow(t *testing.T) {
	f := &mockFile{}
	hcFile := newDiskHealthCheckingFile(f, 1*time.Second, func(opType OpType, duration time.Duration) {
		// We expect to panic before sending the event.
		t.Fatalf("unexpected slow disk event")
	})
	defer hcFile.Close()

	// Set the file creation to the UNIX epoch, which is earlier than the max
	// offset of the health check.
	tEpoch := time.Unix(0, 0)
	hcFile.createTime = time.Unix(0, 0)

	// Assert that the time since the epoch (in nanoseconds) is indeed greater
	// than the max offset.
	require.True(t, time.Since(tEpoch).Nanoseconds() > 1<<nOffsetBits-1)

	// Attempting to start the clock for a new operation on the file should
	// trigger a panic, as the calculated offset from the file creation time would
	// result in integer overflow.
	require.Panics(t, func() { _, _ = hcFile.Write([]byte("uh oh")) })
}

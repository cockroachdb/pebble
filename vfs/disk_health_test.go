// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
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
	create        func(string) (File, error)
	link          func(string, string) error
	list          func(string) ([]string, error)
	lock          func(string) (io.Closer, error)
	mkdirAll      func(string, os.FileMode) error
	open          func(string, ...OpenOption) (File, error)
	openDir       func(string) (File, error)
	pathBase      func(string) string
	pathJoin      func(...string) string
	pathDir       func(string) string
	remove        func(string) error
	removeAll     func(string) error
	rename        func(string, string) error
	reuseForWrite func(string, string) (File, error)
	stat          func(string) (os.FileInfo, error)
	getDiskUsage  func(string) (DiskUsage, error)
}

func (m mockFS) Create(name string) (File, error) {
	if m.create == nil {
		panic("unimplemented")
	}
	return m.create(name)
}

func (m mockFS) Link(oldname, newname string) error {
	if m.link == nil {
		panic("unimplemented")
	}
	return m.link(oldname, newname)
}

func (m mockFS) Open(name string, opts ...OpenOption) (File, error) {
	if m.open == nil {
		panic("unimplemented")
	}
	return m.open(name, opts...)
}

func (m mockFS) OpenDir(name string) (File, error) {
	if m.openDir == nil {
		panic("unimplemented")
	}
	return m.openDir(name)
}

func (m mockFS) Remove(name string) error {
	if m.remove == nil {
		panic("unimplemented")
	}
	return m.remove(name)
}

func (m mockFS) RemoveAll(name string) error {
	if m.removeAll == nil {
		panic("unimplemented")
	}
	return m.removeAll(name)
}

func (m mockFS) Rename(oldname, newname string) error {
	if m.rename == nil {
		panic("unimplemented")
	}
	return m.rename(oldname, newname)
}

func (m mockFS) ReuseForWrite(oldname, newname string) (File, error) {
	if m.reuseForWrite == nil {
		panic("unimplemented")
	}
	return m.reuseForWrite(oldname, newname)
}

func (m mockFS) MkdirAll(dir string, perm os.FileMode) error {
	if m.mkdirAll == nil {
		panic("unimplemented")
	}
	return m.mkdirAll(dir, perm)
}

func (m mockFS) Lock(name string) (io.Closer, error) {
	if m.lock == nil {
		panic("unimplemented")
	}
	return m.lock(name)
}

func (m mockFS) List(dir string) ([]string, error) {
	if m.list == nil {
		panic("unimplemented")
	}
	return m.list(dir)
}

func (m mockFS) Stat(name string) (os.FileInfo, error) {
	if m.stat == nil {
		panic("unimplemented")
	}
	return m.stat(name)
}

func (m mockFS) PathBase(path string) string {
	if m.pathBase == nil {
		panic("unimplemented")
	}
	return m.pathBase(path)
}

func (m mockFS) PathJoin(elem ...string) string {
	if m.pathJoin == nil {
		panic("unimplemented")
	}
	return m.pathJoin(elem...)
}

func (m mockFS) PathDir(path string) string {
	if m.pathDir == nil {
		panic("unimplemented")
	}
	return m.pathDir(path)
}

func (m mockFS) GetDiskUsage(path string) (DiskUsage, error) {
	if m.getDiskUsage == nil {
		panic("unimplemented")
	}
	return m.getDiskUsage(path)
}

var _ FS = &mockFS{}

func TestDiskHealthChecking_Sync(t *testing.T) {
	diskSlow := make(chan time.Duration, 100)
	slowThreshold := 1 * time.Second
	mockFS := &mockFS{create: func(name string) (File, error) {
		return mockFile{syncDuration: 3 * time.Second}, nil
	}}
	fs, closer := WithDiskHealthChecks(mockFS, slowThreshold,
		func(s string, duration time.Duration) {
			diskSlow <- duration
		})
	defer closer.Close()
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

var (
	errInjected = errors.New("injected error")
)

func filesystemOpsMockFS(sleepDur time.Duration) *mockFS {
	return &mockFS{
		create: func(name string) (File, error) {
			time.Sleep(sleepDur)
			return nil, errInjected
		},
		link: func(oldname, newname string) error {
			time.Sleep(sleepDur)
			return errInjected
		},
		mkdirAll: func(string, os.FileMode) error {
			time.Sleep(sleepDur)
			return errInjected
		},
		remove: func(name string) error {
			time.Sleep(sleepDur)
			return errInjected
		},
		removeAll: func(name string) error {
			time.Sleep(sleepDur)
			return errInjected
		},
		rename: func(oldname, newname string) error {
			time.Sleep(sleepDur)
			return errInjected
		},
		reuseForWrite: func(oldname, newname string) (File, error) {
			time.Sleep(sleepDur)
			return nil, errInjected
		},
	}
}

func stallFilesystemOperations(fs FS) map[string]func() {
	return map[string]func(){
		"create":          func() { _, _ = fs.Create("foo") },
		"link":            func() { _ = fs.Link("foo", "bar") },
		"mkdir-all":       func() { _ = fs.MkdirAll("foo", os.ModePerm) },
		"remove":          func() { _ = fs.Remove("foo") },
		"remove-all":      func() { _ = fs.RemoveAll("foo") },
		"rename":          func() { _ = fs.Rename("foo", "bar") },
		"reuse-for-write": func() { _, _ = fs.ReuseForWrite("foo", "bar") },
	}
}

func TestDiskHealthChecking_Filesystem(t *testing.T) {
	const sleepDur = 50 * time.Millisecond
	const stallThreshold = 10 * time.Millisecond

	// Wrap with disk-health checking, counting each stall on stallCount.
	var stallCount uint64
	fs, closer := WithDiskHealthChecks(filesystemOpsMockFS(sleepDur), stallThreshold,
		func(name string, dur time.Duration) {
			atomic.AddUint64(&stallCount, 1)
		})
	defer closer.Close()
	fs.(*diskHealthCheckingFS).tickInterval = 5 * time.Millisecond
	ops := stallFilesystemOperations(fs)
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			before := atomic.LoadUint64(&stallCount)
			op()
			after := atomic.LoadUint64(&stallCount)
			require.Greater(t, int(after-before), 0)
		})
	}
}

// TestDiskHealthChecking_Filesystem_Close tests the behavior of repeatedly
// closing and reusing a filesystem wrapped by WithDiskHealthChecks. This is a
// permitted usage because it allows (*pebble.Options).EnsureDefaults to wrap
// with disk-health checking by default, and to clean up the long-running
// goroutine on (*pebble.DB).Close, while still allowing the FS to be used
// multiple times.
func TestDiskHealthChecking_Filesystem_Close(t *testing.T) {
	const stallThreshold = 10 * time.Millisecond
	mockFS := &mockFS{
		create: func(name string) (File, error) {
			time.Sleep(50 * time.Millisecond)
			return &mockFile{}, nil
		},
	}

	stalled := map[string]time.Duration{}
	fs, closer := WithDiskHealthChecks(mockFS, stallThreshold,
		func(name string, dur time.Duration) { stalled[name] = dur })
	fs.(*diskHealthCheckingFS).tickInterval = 5 * time.Millisecond

	files := []string{"foo", "bar", "bax"}
	for _, filename := range files {
		// Create will stall, and the detector should write to the stalled map
		// with the filename.
		_, _ = fs.Create(filename)
		// Invoke the closer. This will cause the long-running goroutine to
		// exit, but the fs should still be usable and should still detect
		// subsequent stalls on the next iteration.
		require.NoError(t, closer.Close())
		require.Contains(t, stalled, filename)
	}
}

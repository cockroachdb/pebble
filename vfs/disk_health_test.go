// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type mockFile struct {
	syncAndWriteDuration time.Duration
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
	time.Sleep(m.syncAndWriteDuration)
	return len(p), nil
}

func (m mockFile) Stat() (os.FileInfo, error) {
	panic("unimplemented")
}

func (m mockFile) Fd() uintptr {
	return InvalidFd
}

func (m mockFile) Sync() error {
	time.Sleep(m.syncAndWriteDuration)
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

func TestDiskHealthChecking_File(t *testing.T) {
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
				opType   OpType
				duration time.Duration
			}
			diskSlow := make(chan info, 1)
			mockFS := &mockFS{create: func(name string) (File, error) {
				return mockFile{syncAndWriteDuration: syncDuration}, nil
			}}
			fs, closer := WithDiskHealthChecks(mockFS, slowThreshold,
				func(s string, opType OpType, duration time.Duration) {
					diskSlow <- info{
						opType:   opType,
						duration: duration,
					}
				})
			defer closer.Close()
			dhFile, _ := fs.Create("test")
			defer dhFile.Close()

			tc.fn(dhFile)
			select {
			case i := <-diskSlow:
				d := i.duration
				if d.Seconds() < slowThreshold.Seconds() {
					t.Fatalf("expected %0.1f to be greater than threshold %0.1f", d.Seconds(), slowThreshold.Seconds())
				}
				require.Equal(t, tc.op, i.opType)
			case <-time.After(5 * time.Second):
				t.Fatal("disk stall detector did not detect slow disk operation")
			}
		})
	}
}

func TestDiskHealthChecking_NotTooManyOps(t *testing.T) {
	numBitsForOpType := 64 - nOffsetBits
	numOpTypesAllowed := int(math.Pow(2, float64(numBitsForOpType)))
	numOpTypes := int(opTypeMax)
	require.LessOrEqual(t, numOpTypes, numOpTypesAllowed)
}

func TestDiskHealthChecking_File_Underflow(t *testing.T) {
	f := &mockFile{}
	hcFile := newDiskHealthCheckingFile(f, 1*time.Second, func(opType OpType, duration time.Duration) {
		// We expect to panic before sending the event.
		t.Fatalf("unexpected slow disk event")
	})
	defer hcFile.Close()

	// Set the file creation to the UNIX epoch, which is earlier than the max
	// offset of the health check.
	tEpoch := time.Unix(0, 0)
	hcFile.createTime = tEpoch

	// Assert that the time since the epoch (in nanoseconds) is indeed greater
	// than the max offset.
	require.True(t, time.Since(tEpoch).Nanoseconds() > 1<<nOffsetBits-1)

	// Attempting to start the clock for a new operation on the file should
	// trigger a panic, as the calculated offset from the file creation time would
	// result in integer overflow.
	require.Panics(t, func() { _, _ = hcFile.Write([]byte("uh oh")) })
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

func stallFilesystemOperations(fs FS) []filesystemOperation {
	return []filesystemOperation{
		{
			"create", OpTypeCreate, func() { _, _ = fs.Create("foo") },
		},
		{
			"link", OpTypeLink, func() { _ = fs.Link("foo", "bar") },
		},
		{
			"mkdirall", OpTypeMkdirAll, func() { _ = fs.MkdirAll("foo", os.ModePerm) },
		},
		{
			"remove", OpTypeRemove, func() { _ = fs.Remove("foo") },
		},
		{
			"removeall", OpTypeRemoveAll, func() { _ = fs.RemoveAll("foo") },
		},
		{
			"rename", OpTypeRename, func() { _ = fs.Rename("foo", "bar") },
		},
		{
			"reuseforwrite", OpTypeReuseForWrite, func() { _, _ = fs.ReuseForWrite("foo", "bar") },
		},
	}
}

type filesystemOperation struct {
	name   string
	opType OpType
	f      func()
}

func TestDiskHealthChecking_Filesystem(t *testing.T) {
	const sleepDur = 50 * time.Millisecond
	const stallThreshold = 10 * time.Millisecond

	// Wrap with disk-health checking, counting each stall via stallCount.
	var mu sync.Mutex
	var expectedOpType OpType
	var stallCount uint64
	fs, closer := WithDiskHealthChecks(filesystemOpsMockFS(sleepDur), stallThreshold,
		func(name string, opType OpType, dur time.Duration) {
			mu.Lock()
			require.Equal(t, expectedOpType, opType)
			mu.Unlock()
			atomic.AddUint64(&stallCount, 1)
		})
	defer closer.Close()
	fs.(*diskHealthCheckingFS).tickInterval = 5 * time.Millisecond
	ops := stallFilesystemOperations(fs)
	for _, o := range ops {
		t.Run(o.name, func(t *testing.T) {
			mu.Lock()
			expectedOpType = o.opType
			mu.Unlock()
			before := atomic.LoadUint64(&stallCount)
			o.f()
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

	mu := sync.Mutex{}
	stalled := map[string]time.Duration{}
	onStalled := make(chan struct{}, 5)
	fs, closer := WithDiskHealthChecks(mockFS, stallThreshold,
		func(name string, opType OpType, dur time.Duration) {
			mu.Lock()
			stalled[name] = dur
			mu.Unlock()
			onStalled <- struct{}{}
		})
	fs.(*diskHealthCheckingFS).tickInterval = 5 * time.Millisecond

	files := []string{"foo", "bar", "bax"}
	for _, filename := range files {
		// Create will stall, and the detector should write to the stalled map
		// with the filename.
		_, _ = fs.Create(filename)
		<-onStalled
		// Invoke the closer. This will cause the long-running goroutine to
		// exit, but the fs should still be usable and should still detect
		// subsequent stalls on the next iteration.
		require.NoError(t, closer.Close())
		mu.Lock()
		require.Contains(t, stalled, filename)
		mu.Unlock()
	}
}

// TestDiskHealthChecking_FS_StalledEventListener tests the behaviour of
// diskHealthCheckingFS being able to react to slow disk operations even if the
// call to the event listener stalls.
func TestDiskHealthChecking_FS_StalledEventListener(t *testing.T) {
	const stallThreshold = 10 * time.Millisecond
	mockFS := &mockFS{
		create: func(name string) (File, error) {
			time.Sleep(50 * time.Millisecond)
			return &mockFile{}, nil
		},
	}

	stallListener := int32(0)
	stalledFile := make(chan string, 5)
	fs, closer := WithDiskHealthChecks(mockFS, stallThreshold,
		func(name string, opType OpType, dur time.Duration) {
			if atomic.CompareAndSwapInt32(&stallListener, 1, 0) {
				// Stall for a long time.
				time.Sleep(20 * time.Second)
			} else {
				stalledFile <- name
			}
		})
	fs.(*diskHealthCheckingFS).tickInterval = 5 * time.Millisecond

	files := []string{"foo", "bar"}
	for _, filename := range files {
		// Create will stall, and the detector should write to the stalled map
		// with the filename.
		atomic.CompareAndSwapInt32(&stallListener, 0, 1)
		_, _ = fs.Create(filename)
		_, _ = fs.Create(filename)
		exitLoop := false
		for !exitLoop {
			select {
			case stalledFile := <-stalledFile:
				if stalledFile == filename {
					exitLoop = true
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("waited over 3 seconds for file %s to show up in stalled map", filename)
			}
		}

		// Invoke the closer. This will cause the long-running goroutine to
		// exit, but the fs should still be usable and should still detect
		// subsequent stalls on the next iteration.
		require.NoError(t, closer.Close())
	}
}

// TestDiskHealthChecking_File_StalledEventListener tests the behaviour of
// diskHealthCheckingFile being able to react to slow disk operations even if the
// call to the event listener stalls.
func TestDiskHealthChecking_File_StalledEventListener(t *testing.T) {
	const stallThreshold = 10 * time.Millisecond
	mockFS := &mockFS{
		create: func(name string) (File, error) {
			return &mockFile{50 * time.Millisecond}, nil
		},
	}

	stallListener := int32(0)
	stalledFile := make(chan string, 5)
	fs, closer := WithDiskHealthChecks(mockFS, stallThreshold,
		func(name string, opType OpType, dur time.Duration) {
			if atomic.CompareAndSwapInt32(&stallListener, 1, 0) {
				// Stall for a long time.
				time.Sleep(20 * time.Second)
			} else {
				stalledFile <- name
			}
		})
	fs.(*diskHealthCheckingFS).tickInterval = 5 * time.Millisecond
	fs.(*diskHealthCheckingFS).testingHook = func(f File) {
		f.(*diskHealthCheckingFile).tickInterval = 5 * time.Millisecond
	}

	files := []string{"foo", "bar"}
	for _, filename := range files {
		f, err := fs.Create(filename)
		require.NoError(t, err)
		// Sync will stall, and the detector should write to the stalled map
		// with the filename
		atomic.CompareAndSwapInt32(&stallListener, 0, 1)
		_ = f.Sync()
		_ = f.Sync()
		exitLoop := false
		for !exitLoop {
			select {
			case stalledFile := <-stalledFile:
				if stalledFile == filename {
					exitLoop = true
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("waited over 3 seconds for file %s to show up in stalled map", filename)
			}
		}
	}
	require.NoError(t, closer.Close())
}

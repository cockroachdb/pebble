// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs // import "github.com/cockroachdb/pebble/vfs"

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func NewStatFS() *StatFS {
	fs := &StatFS{}
	fs.metrics = &fsMetrics{}
	return fs
}

func NewStatFile(data []byte) File {
	n := &memNode{refs: 1}
	n.mu.data = data
	n.mu.modTime = time.Now()
	return &memFile{
		n:    n,
		read: true,
	}
}

// StatFS implements FS. It wraps a defaultFS, while also
// maintaining some file system stats.
type StatFS struct {
	fs      defaultFS
	metrics *fsMetrics
}

type fsMetrics struct {
	atomic struct {
		// We use syncCount to make sure that we're only
		// sampling 1% of all syncs.
		syncCount uint64
	}

	mu struct {
		sync.RWMutex
		syncLatencyEWMA float64
		syncLatencyDev  float64
	}
}

type FSMetrics struct {
	SyncLatencyEWMA float64
	SyncLatencyDev  float64
}

var _ FS = &StatFS{}

func (fs *StatFS) Metrics() *FSMetrics {
	m := &FSMetrics{}
	fs.metrics.mu.Lock()
	m.SyncLatencyDev = fs.metrics.mu.syncLatencyDev
	m.SyncLatencyEWMA = fs.metrics.mu.syncLatencyEWMA
	fs.metrics.mu.Unlock()
	return m
}

// Create implements FS.Create.
func (fs *StatFS) Create(fullname string) (File, error) {
	f, err := fs.fs.Create(fullname)
	if err != nil {
		return nil, err
	}
	s := &statFile{f, fs.metrics}
	return s, nil

}

// Link implements FS.Link.
func (fs *StatFS) Link(oldname, newname string) error {
	return fs.fs.Link(oldname, newname)
}

// Open implements FS.Open.
func (fs *StatFS) Open(fullname string, opts ...OpenOption) (File, error) {
	f, err := fs.fs.Open(fullname, opts...)
	if err != nil {
		return nil, err
	}
	s := &statFile{f, fs.metrics}
	return s, nil
}

// OpenDir implements FS.OpenDir.
func (fs *StatFS) OpenDir(fullname string) (File, error) {
	f, err := fs.fs.OpenDir(fullname)
	if err != nil {
		return nil, err
	}
	s := &statFile{f, fs.metrics}
	return s, nil
}

// Remove implements FS.Remove.
func (fs *StatFS) Remove(fullname string) error {
	return fs.fs.Remove(fullname)
}

// RemoveAll implements FS.RemoveAll.
func (fs *StatFS) RemoveAll(fullname string) error {
	return fs.fs.RemoveAll(fullname)
}

// Rename implements FS.Rename.
func (fs *StatFS) Rename(oldname, newname string) error {
	return fs.fs.Rename(oldname, newname)
}

// ReuseForWrite implements FS.ReuseForWrite.
func (fs *StatFS) ReuseForWrite(oldname, newname string) (File, error) {
	f, err := fs.fs.ReuseForWrite(oldname, newname)
	if err != nil {
		return nil, err
	}
	s := &statFile{f, fs.metrics}
	return s, nil
}

// MkdirAll implements FS.MkdirAll.
func (fs *StatFS) MkdirAll(dirname string, perm os.FileMode) error {
	return fs.fs.MkdirAll(dirname, perm)
}

// Lock implements FS.Lock.
func (fs *StatFS) Lock(fullname string) (io.Closer, error) {
	return fs.fs.Lock(fullname)
}

// List implements FS.List.
func (fs *StatFS) List(dirname string) ([]string, error) {
	return fs.fs.List(dirname)
}

// Stat implements FS.Stat.
func (fs *StatFS) Stat(name string) (os.FileInfo, error) {
	return fs.fs.Stat(name)
}

// PathBase implements FS.PathBase.
func (fs *StatFS) PathBase(p string) string {
	return fs.fs.PathBase(p)
}

// PathJoin implements FS.PathJoin.
func (fs *StatFS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

// PathDir implements FS.PathDir.
func (fs *StatFS) PathDir(p string) string {
	return fs.fs.PathDir(p)
}

// GetDiskUsage implements FS.GetDiskUsage.
func (fs *StatFS) GetDiskUsage(path string) (DiskUsage, error) {
	return fs.fs.GetDiskUsage(path)
}

var _ File = &statFile{}

type statFile struct {
	file    File
	metrics *fsMetrics
}

func (f *statFile) Close() error {
	return f.file.Close()
}

func (f *statFile) Read(p []byte) (int, error) {
	return f.file.Read(p)
}

func (f *statFile) ReadAt(p []byte, off int64) (int, error) {
	return f.file.ReadAt(p, off)
}

func (f *statFile) Write(p []byte) (int, error) {
	return f.file.Write(p)
}

func (f *statFile) Stat() (os.FileInfo, error) {
	return f.file.Stat()
}

func abs(a float64) float64 {
	if a < 0 {
		return -a
	}
	return a
}

func (f *statFile) Sync() error {
	old := atomic.AddUint64(&f.metrics.atomic.syncCount, 1)

	var t1 time.Time
	if old%100 == 0 {
		t1 = time.Now()
	}

	// Note: Syncs times are dependant on the amount being synced,
	// so we might have to normalize for that.
	err := f.file.Sync()
	if old%100 == 0 && err == nil {
		newSample := float64(time.Since(t1).Milliseconds())
		f.metrics.mu.Lock()
		timeout := f.metrics.mu.syncLatencyEWMA + 2*f.metrics.mu.syncLatencyDev
		fmt.Println("timeout?", timeout >= f.metrics.mu.syncLatencyEWMA, f.metrics.mu.syncLatencyEWMA, f.metrics.mu.syncLatencyDev, timeout)
		f.metrics.mu.syncLatencyEWMA = 0.875*f.metrics.mu.syncLatencyEWMA + 0.125*newSample
		f.metrics.mu.syncLatencyDev = 0.75*f.metrics.mu.syncLatencyDev + 0.25*abs(newSample-f.metrics.mu.syncLatencyEWMA)
		f.metrics.mu.Unlock()
	}
	return err
}

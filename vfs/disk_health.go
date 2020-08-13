// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"sync/atomic"
	"time"
)

const (
	// defaultTickInterval is the default interval between two ticks of each
	// diskHealthChecker loop iteration.
	defaultTickInterval = 2 * time.Second
)

// diskHealthChecker is a utility to detect slow disk operations, and call
// onDiskStall if a wrapped disk operation is seen to exceed diskStallThreshold.
// Similarly, onSlowDisk is called if a disk operation is seen to exceed
// diskSlowThreshold.
//
// This struct creates a goroutine (in startTicker()) that, at every tick
// interval, sees if there's a disk operation taking longer than the specified
// durations. This setup is preferable to creating a new timer at every disk
// operation, as it reduces overhead per disk operation.
//
// Each diskHealthChecker instance is safe for single-goroutine use; instances
// should not be shared across goroutines.
type diskHealthChecker struct {
	onDiskStall        func(time.Duration)
	onSlowDisk         func(time.Duration)
	diskStallThreshold time.Duration
	diskSlowThreshold  time.Duration
	tickInterval       time.Duration

	stopper        chan struct{}
	lastWriteNanos int64
	inUse          int32
}

// newDiskHealthChecker instantiates a new disk health checker, with the
// specified time thresholds and event listeners.
func newDiskHealthChecker(
	diskStallThreshold, diskSlowThreshold time.Duration, onDiskStall, onSlowDisk func(time.Duration),
) *diskHealthChecker {
	return &diskHealthChecker{
		onDiskStall:        onDiskStall,
		onSlowDisk:         onSlowDisk,
		diskStallThreshold: diskStallThreshold,
		diskSlowThreshold:  diskSlowThreshold,
		tickInterval:       defaultTickInterval,

		stopper: make(chan struct{}, 1),
	}
}

// startTicker starts a new goroutine with a ticker to monitor disk operations.
// Can only be called if the ticker goroutine isn't running already.
func (d *diskHealthChecker) startTicker() {
	if d == nil || (d.diskStallThreshold == 0 && d.diskSlowThreshold == 0) {
		return
	}
	if !atomic.CompareAndSwapInt32(&d.inUse, 0, 1) {
		panic("disk health checker already in use")
	}

	go func() {
		ticker := time.NewTicker(d.tickInterval)
		defer ticker.Stop()
		defer func() {
			atomic.StoreInt32(&d.inUse, 0)
		}()

		for {
			select {
			case <-d.stopper:
				return

			case <-ticker.C:
				lastWriteNanos := atomic.LoadInt64(&d.lastWriteNanos)
				if lastWriteNanos == 0 {
					continue
				}
				lastWrite := time.Unix(0, lastWriteNanos)
				now := time.Now()
				if lastWrite.Add(d.diskStallThreshold).Before(now) {
					// diskStallThreshold was exceeded. Call the passed-in
					// listener.
					d.onDiskStall(now.Sub(lastWrite))
				} else if lastWrite.Add(d.diskSlowThreshold).Before(now) {
					// diskSlowThreshold was exceeded. Call the passed-in
					// listener.
					d.onSlowDisk(now.Sub(lastWrite))
				}
			}
		}
	}()
}

// stopTicker stops the goroutine started in startTicker.
func (d *diskHealthChecker) stopTicker() {
	if d == nil {
		return
	}
	d.stopper <- struct{}{}
}

// wrapFile wraps a File's disk writing/syncing methods for monitoring by
// the disk health checker. A monitoring goroutine is also started; that
// goroutine gets closed in the file's Close() method.
func (d *diskHealthChecker) wrapFile(f File) File {
	dsFile := diskHealthCheckingFile{File: f, d: d}
	d.startTicker()

	return dsFile
}

// timeDiskOp runs the specified closure and makes its timing visible to the
// monitoring goroutine, in case it exceeds one of the slow disk durations.
func (d *diskHealthChecker) timeDiskOp(op func()) {
	if d == nil {
		op()
		return
	}

	atomic.StoreInt64(&d.lastWriteNanos, time.Now().UnixNano())
	defer func() {
		atomic.StoreInt64(&d.lastWriteNanos, 0)
	}()
	op()
}

type diskHealthCheckingFS struct {
	defaultFS

	diskStalledThreshold, diskSlowThreshold time.Duration
	onDiskStall, onSlowDisk                 func(string, time.Duration)
}

// DefaultWithDiskHealthChecks wraps a defaultFS and ensures that all
// write-oriented created with that FS are wrapped with disk health detection
// checks. Disk operations that are observed to take longer than diskStallThreshold
// trigger an onDiskStall call, and operations observed to take longer than
// diskSlowThreshold trigger an onSlowDisk call.
func DefaultWithDiskHealthChecks(
	diskStallThreshold, diskSlowThreshold time.Duration, onDiskStall, onSlowDisk func(string, time.Duration),
) FS {
	return diskHealthCheckingFS{
		diskStalledThreshold: diskStallThreshold,
		diskSlowThreshold:    diskSlowThreshold,
		onDiskStall:          onDiskStall,
		onSlowDisk:           onSlowDisk,
	}
}

// Create implements the vfs.FS interface.
func (d diskHealthCheckingFS) Create(name string) (File, error) {
	f, err := d.defaultFS.Create(name)
	if err != nil {
		return f, err
	}
	checker := newDiskHealthChecker(
		d.diskStalledThreshold, d.diskSlowThreshold, func(duration time.Duration) {
			d.onDiskStall(name, duration)
		}, func(duration time.Duration) {
			d.onSlowDisk(name, duration)
		})
	return checker.wrapFile(f), nil
}

// ReuseForWrite implements the vfs.FS interface.
func (d diskHealthCheckingFS) ReuseForWrite(oldname, newname string) (File, error) {
	f, err := d.defaultFS.ReuseForWrite(oldname, newname)
	if err != nil {
		return f, err
	}
	checker := newDiskHealthChecker(
		d.diskStalledThreshold, d.diskSlowThreshold, func(duration time.Duration) {
			d.onDiskStall(newname, duration)
		}, func(duration time.Duration) {
			d.onSlowDisk(newname, duration)
		})
	return checker.wrapFile(f), nil
}

// diskHealthCheckingFile wraps a File's write/sync methods and monitors
// them for slow disk operations. The disk health checker's monitoring goroutine
// is stopped when the file is closed.
type diskHealthCheckingFile struct {
	File

	d *diskHealthChecker
}

// Write implements the io.Writer interface.
func (d diskHealthCheckingFile) Write(p []byte) (n int, err error) {
	d.d.timeDiskOp(func() {
		n, err = d.File.Write(p)
	})
	return n, err
}

// Close implements the io.Closer interface.
func (d diskHealthCheckingFile) Close() error {
	d.d.stopTicker()
	return d.File.Close()
}

// Sync implements the io.Syncer interface.
func (d diskHealthCheckingFile) Sync() (err error) {
	d.d.timeDiskOp(func() {
		err = d.File.Sync()
	})
	return err
}

// diskHealthChecker returns this file's instance of diskHealthChecker. Useful
// to time any additional disk operations added by files that wrap vfs.File.
func (d diskHealthCheckingFile) diskHealthChecker() *diskHealthChecker {
	return d.d
}

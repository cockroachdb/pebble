// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package diskhealth

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/vfs"
)

const (
	// defaultTickInterval is the default interval between two ticks of each
	// diskStallDetector loop iteration.
	defaultTickInterval = 10 * time.Second
)

// DiskStallDetector is a utility to detect slow disk operations, and call
// onDiskStall if a wrapped disk operation is seen to exceed maxSyncDuration.
// Similarly, onSlowDisk is called if a disk operation is seen to exceed
// slowDiskWarnThreshold.
//
// This struct creates a goroutine (in StartTicker()) that, at every tick
// interval, sees if there's a disk operation taking longer than the specified
// durations. This setup is preferable to creating a new timer at every disk
// operation, as it reduces overhead per disk operation.
//
// Each DiskStallDetector instance is safe for single-goroutine use; instances
// should not be shared across goroutines.
type DiskStallDetector struct {
	onDiskStall           func(time.Duration)
	onSlowDisk            func(time.Duration)
	maxSyncDuration       time.Duration
	slowDiskWarnThreshold time.Duration
	tickInterval          time.Duration

	stopper        chan struct{}
	lastWriteNanos int64
}

// NewDiskStallDetector instantiates a new disk stall detector, with the
// specified time thresholds and event listeners.
func NewDiskStallDetector(
	maxSyncDuration, slowDiskWarnThreshold time.Duration, onDiskStall, onSlowDisk func(time.Duration),
) *DiskStallDetector {
	return &DiskStallDetector{
		onDiskStall:           onDiskStall,
		onSlowDisk:            onSlowDisk,
		maxSyncDuration:       maxSyncDuration,
		slowDiskWarnThreshold: slowDiskWarnThreshold,
		tickInterval:          defaultTickInterval,

		stopper: make(chan struct{}, 1),
	}
}

// StartTicker starts a new goroutine with a ticket to monitor disk operations.
func (d *DiskStallDetector) StartTicker() {
	if d == nil || (d.maxSyncDuration == 0 && d.slowDiskWarnThreshold == 0) {
		return
	}

	go func() {
		ticker := time.NewTicker(d.tickInterval)
		defer ticker.Stop()

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
				if lastWrite.Add(d.maxSyncDuration).Before(now) {
					// maxSyncDuration was exceeded. Call the passed-in
					// listener.
					d.onDiskStall(now.Sub(lastWrite))
				} else if lastWrite.Add(d.slowDiskWarnThreshold).Before(now) {
					// slowDiskWarnThreshold was exceeded. Call the passed-in
					// listener.
					d.onSlowDisk(now.Sub(lastWrite))
				}
			}
		}
	}()
}

// StopTicker stops the goroutine started in StartTicker.
func (d *DiskStallDetector) StopTicker() {
	if d == nil {
		return
	}
	d.stopper <- struct{}{}
}

// WrapFile wraps a vfs.File's disk writing/syncing methods for monitoring by
// the disk stall detector. A monitoring goroutine is also started; that
// goroutine gets closed in the file's Close() method.
func (d *DiskStallDetector) WrapFile(f vfs.File) vfs.File {
	dsFile := diskStallDetectingFile{File: f, d: d}
	d.StartTicker()

	return dsFile
}

// TimeDiskOp runs the specified closure and makes its timing visible to the
// monitoring goroutine, in case it exceeds one of the slow disk durations.
func (d *DiskStallDetector) TimeDiskOp(op func()) {
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

// diskStallDetectingFile wraps a vfs.File's write/sync methods and monitors
// them for slow disk operations. The disk stall detector's monitoring goroutine
// is stopped when the file is closed.
type diskStallDetectingFile struct {
	vfs.File

	d *DiskStallDetector
}

// Write implements the io.Writer interface.
func (d diskStallDetectingFile) Write(p []byte) (n int, err error) {
	d.d.TimeDiskOp(func() {
		n, err = d.File.Write(p)
	})
	return n, err
}

// Close implements the io.Closer interface.
func (d diskStallDetectingFile) Close() error {
	d.d.StopTicker()
	return d.File.Close()
}

// Sync implements the io.Syncer interface.
func (d diskStallDetectingFile) Sync() (err error) {
	d.d.TimeDiskOp(func() {
		err = d.File.Sync()
	})
	return err
}

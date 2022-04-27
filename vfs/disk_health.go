// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"fmt"
	"sync/atomic"
	"time"
)

const (
	// defaultTickInterval is the default interval between two ticks of each
	// diskHealthCheckingFile loop iteration.
	defaultTickInterval = 2 * time.Second

	// nOffsetBits is the number of bits in the packed 64-bit integer used for
	// identifying an offset from the file creation time (in nanoseconds).
	nOffsetBits = 60
)

// OpType is the type of IO operation being monitored by a
// diskHealthCheckingFile.
type OpType uint8

// The following OpTypes is limited to the subset of file system operations that
// a diskHealthCheckingFile supports (namely writes and syncs).
const (
	OpTypeUnknown OpType = iota
	OpTypeWrite
	OpTypeSync
	opTypeMax = OpTypeSync
)

// String implements fmt.Stringer.
func (o OpType) String() string {
	switch o {
	case OpTypeWrite:
		return "write"
	case OpTypeSync:
		return "sync"
	case OpTypeUnknown:
		return "unknown"
	default:
		panic(fmt.Sprintf("vfs: unknown op type: %d", o))
	}
}

// diskHealthCheckingFile is a File wrapper to detect slow disk operations, and
// call onSlowDisk if a disk operation is seen to exceed diskSlowThreshold.
//
// This struct creates a goroutine (in startTicker()) that, at every tick
// interval, sees if there's a disk operation taking longer than the specified
// duration. This setup is preferable to creating a new timer at every disk
// operation, as it reduces overhead per disk operation.
type diskHealthCheckingFile struct {
	File

	onSlowDisk        func(OpType, time.Duration)
	diskSlowThreshold time.Duration
	tickInterval      time.Duration

	stopper chan struct{}
	// lastWritePacked is a 64-bit unsigned int, with the most significant 7.5
	// bytes (60 bits) representing an offset (in nanoseconds) from the file
	// creation time. The least significant four bits contains the OpType.
	//
	// The use of 60 bits for an offset provides ~36.5 years of effective
	// monitoring time before the uint wraps around. 36.5 years of process uptime
	// "ought to be enough for anybody". This also allows for 16 operation types.
	//
	// NB: this packing scheme is not persisted, and is therefore safe to adjust
	// across process boundaries.
	lastWritePacked uint64
	createTime      time.Time
}

// newDiskHealthCheckingFile instantiates a new diskHealthCheckingFile, with the
// specified time threshold and event listener.
func newDiskHealthCheckingFile(
	file File, diskSlowThreshold time.Duration, onSlowDisk func(OpType, time.Duration),
) *diskHealthCheckingFile {
	return &diskHealthCheckingFile{
		File:              file,
		onSlowDisk:        onSlowDisk,
		diskSlowThreshold: diskSlowThreshold,
		tickInterval:      defaultTickInterval,

		stopper:    make(chan struct{}),
		createTime: time.Now(),
	}
}

// startTicker starts a new goroutine with a ticker to monitor disk operations.
// Can only be called if the ticker goroutine isn't running already.
func (d *diskHealthCheckingFile) startTicker() {
	if d.diskSlowThreshold == 0 {
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
				packed := atomic.LoadUint64(&d.lastWritePacked)
				if packed == 0 {
					continue
				}
				offsetNanos, op := int64(packed>>(64-nOffsetBits)), OpType(packed&0xf)
				lastWrite := d.createTime.Add(time.Duration(offsetNanos))
				now := time.Now()
				if lastWrite.Add(d.diskSlowThreshold).Before(now) {
					// diskSlowThreshold was exceeded. Call the passed-in
					// listener.
					d.onSlowDisk(op, now.Sub(lastWrite))
				}
			}
		}
	}()
}

// stopTicker stops the goroutine started in startTicker.
func (d *diskHealthCheckingFile) stopTicker() {
	close(d.stopper)
}

// Write implements the io.Writer interface.
func (d *diskHealthCheckingFile) Write(p []byte) (n int, err error) {
	d.timeDiskOp(OpTypeWrite, func() {
		n, err = d.File.Write(p)
	})
	return n, err
}

// Close implements the io.Closer interface.
func (d *diskHealthCheckingFile) Close() error {
	d.stopTicker()
	return d.File.Close()
}

// Sync implements the io.Syncer interface.
func (d *diskHealthCheckingFile) Sync() (err error) {
	d.timeDiskOp(OpTypeSync, func() {
		err = d.File.Sync()
	})
	return err
}

// timeDiskOp runs the specified closure and makes its timing visible to the
// monitoring goroutine, in case it exceeds one of the slow disk durations.
func (d *diskHealthCheckingFile) timeDiskOp(o OpType, op func()) {
	if d == nil {
		op()
		return
	}

	offsetNanos := time.Since(d.createTime).Nanoseconds()
	if offsetNanos < 0 || (offsetNanos > 1<<nOffsetBits-1) {
		panic("vfs: last write offset would result in integer wraparound")
	}
	packed := uint64(offsetNanos)<<(64-nOffsetBits) | uint64(o)
	atomic.StoreUint64(&d.lastWritePacked, packed)
	defer func() {
		atomic.StoreUint64(&d.lastWritePacked, 0)
	}()
	op()
}

type diskHealthCheckingFS struct {
	FS

	diskSlowThreshold time.Duration
	onSlowDisk        func(string, OpType, time.Duration)
}

// WithDiskHealthChecks wraps an FS and ensures that all
// write-oriented created with that FS are wrapped with disk health detection
// checks. Disk operations that are observed to take longer than
// diskSlowThreshold trigger an onSlowDisk call.
func WithDiskHealthChecks(
	fs FS, diskSlowThreshold time.Duration, onSlowDisk func(string, OpType, time.Duration),
) FS {
	return diskHealthCheckingFS{
		FS:                fs,
		diskSlowThreshold: diskSlowThreshold,
		onSlowDisk:        onSlowDisk,
	}
}

// Create implements the vfs.FS interface.
func (d diskHealthCheckingFS) Create(name string) (File, error) {
	f, err := d.FS.Create(name)
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(op OpType, duration time.Duration) {
		d.onSlowDisk(name, op, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

// ReuseForWrite implements the vfs.FS interface.
func (d diskHealthCheckingFS) ReuseForWrite(oldname, newname string) (File, error) {
	f, err := d.FS.ReuseForWrite(oldname, newname)
	if err != nil {
		return f, err
	}
	if d.diskSlowThreshold == 0 {
		return f, nil
	}
	checkingFile := newDiskHealthCheckingFile(f, d.diskSlowThreshold, func(op OpType, duration time.Duration) {
		d.onSlowDisk(newname, op, duration)
	})
	checkingFile.startTicker()
	return WithFd(f, checkingFile), nil
}

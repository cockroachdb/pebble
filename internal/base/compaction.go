// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "context"

type CompactionSlot interface {
	// CompactionSelected is called when a compaction is selected for execution in
	// this compaction slot. The firstInputLevel is the highest input level participating
	// in this compaction, and the outputLevel is the level that outputs are being
	// written to from this compaction. inputSize is the cumulative size of inputs participating
	// in this compaction, including the size of the memtable for flushes.
	CompactionSelected(firstInputLevel, outputLevel int, inputSize uint64)
	// UpdateMetrics is called periodically to update the number of disk bytes read and written
	// for this compaction slot. The metrics passed in are cumulative, not incremental
	// since the last UpdateMetrics call. The implementation of this method could
	// calculate deltas as necessary. For flushes, bytesRead will be bytes read from
	// the memtable.
	UpdateMetrics(bytesRead, bytesWritten uint64)
	// Release returns a compaction slot. Must be called once a compaction is
	// complete. After this method, no more calls must be made to other interface
	// methods on CompactionSlot.
	Release(totalBytesWritten uint64)
}

type CompactionLimiter interface {
	// TookWithoutPermission is called when a compaction is performed without
	// asking for permission. Pebble calls into this method for flushes as well
	// as for the first compaction in an instance, as those slots will always be
	// granted even in the case of slot exhaustion or overload.
	TookWithoutPermission(ctx context.Context) CompactionSlot
	// RequestSlot is called when a compaction is about to be scheduled. If the
	// compaction is allowed, the method returns a non-nil slot that must be released
	// after the compaction is complete. If a nil value is returned, the compaction is
	// disallowed due to possible overload.
	RequestSlot(ctx context.Context) (CompactionSlot, error)
}

type DefaultCompactionSlot struct{}

func (d *DefaultCompactionSlot) CompactionSelected(
	firstInputLevel, outputLevel int, inputSize uint64,
) {
}

func (d *DefaultCompactionSlot) UpdateMetrics(bytesRead, bytesWritten uint64) {
}

func (d *DefaultCompactionSlot) Release(totalBytesWritten uint64) {
}

type DefaultCompactionLimiter struct{}

func (d *DefaultCompactionLimiter) TookWithoutPermission(ctx context.Context) CompactionSlot {
	return &DefaultCompactionSlot{}
}

func (d *DefaultCompactionLimiter) RequestSlot(ctx context.Context) (CompactionSlot, error) {
	return &DefaultCompactionSlot{}, nil
}

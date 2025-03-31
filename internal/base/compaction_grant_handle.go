// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// CompactionGrantHandleStats contains stats provided to a CompactionGrantHandle.
type CompactionGrantHandleStats struct {
	// CumWriteBytes is the cumulative bytes written to disk.
	CumWriteBytes uint64
	// TODO(sumeer): add more stats like:
	// cumReadBytes uint64
	// cumReadBytesInCache uint64
}

// CompactionGrantHandle is used to frequently update the CompactionScheduler
// about resource consumption. The MeasureCPU and CumulativeStats methods must
// be called frequently.
type CompactionGrantHandle interface {
	// Started is called once and must precede calls to MeasureCPU and
	// CumulativeStats.
	Started()
	CPUMeasurer
	// CumulativeStats reports the current cumulative stats. This method may
	// block if the scheduler wants to pace the compaction (say to moderate its
	// consumption of disk write bandwidth).
	CumulativeStats(stats CompactionGrantHandleStats)
	// Done must be called when the compaction completes (whether success or
	// failure). It may synchronously result in a call to
	// DBForCompaction.Schedule so this must be called without holding any
	// locks, *and* after the new version (if the compaction was successful) has
	// been installed.
	Done()
}

// CompactionGoroutineKind identifies the kind of compaction goroutine.
type CompactionGoroutineKind uint8

const (
	// CompactionGoroutinePrimary is the primary compaction goroutine that
	// iterates over key-value pairs in the input and calls the current sstable
	// writer and blob file writer.
	CompactionGoroutinePrimary CompactionGoroutineKind = iota
	// CompactionGoroutineSSTableSecondary is the secondary goroutine in the
	// current sstable writer that writes blocks to the sstable.
	CompactionGoroutineSSTableSecondary
	// CompactionGoroutineBlobFileSecondary is the secondary goroutine in the
	// current blob file writer that writes blocks to the blob file.
	CompactionGoroutineBlobFileSecondary
)

// CPUMeasurer is used to measure the CPU consumption of goroutines involved
// in a compaction.
type CPUMeasurer interface {
	// MeasureCPU allows the measurer to keep track of CPU usage while a
	// compaction is ongoing. It is to be called regularly from the compaction
	// goroutine corresponding to the argument. The first call from a goroutine
	// must be done before any significant CPU consumption, since it is used to
	// initialize the measurer for the goroutine making the call. If a
	// compaction is not using a certain kind of goroutine, it can skip calling
	// this method with the corresponding argument.
	MeasureCPU(CompactionGoroutineKind)
}

type NoopCPUMeasurer struct{}

func (NoopCPUMeasurer) MeasureCPU(CompactionGoroutineKind) {}

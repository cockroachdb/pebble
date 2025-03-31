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
// about resource consumption. The MeasureCPU* and CumulativeStats methods
// must be called frequently.
type CompactionGrantHandle interface {
	// Started is called once and must precede calls to MeasureCPU* and
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

// CPUMeasurer is used to measure the CPU consumption of goroutines involved
// in a compaction.
//
// MeasureCPU* methods must be called regularly from each of the three
// goroutines that consume CPU during a compaction, and the first call must be
// before any significant work is done, since the first call is used to
// initialize the measurer for the goroutine. If a compaction is not using a
// certain kind of goroutine, it can skip calling that particular MeasureCPU*
// method.
type CPUMeasurer interface {
	// MeasureCPUPrimary measures CPU for the primary goroutine that
	// synchronously does the work in the compaction.
	MeasureCPUPrimary()
	// MeasureCPUSSTableSecondary measures the CPU for the secondary goroutine
	// that writes blocks to the SSTable.
	MeasureCPUSSTableSecondary()
	// MeasureCPUBlobFileSecondary measures the CPU for the secondary goroutine
	// that writes blocks to a blob file.
	MeasureCPUBlobFileSecondary()
}

type NoopCPUMeasurer struct{}

func (NoopCPUMeasurer) MeasureCPUPrimary()           {}
func (NoopCPUMeasurer) MeasureCPUSSTableSecondary()  {}
func (NoopCPUMeasurer) MeasureCPUBlobFileSecondary() {}

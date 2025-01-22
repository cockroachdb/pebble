// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// CompactionGrantHandleStats contains stats provided to a CompactionGrantHandle.
type CompactionGrantHandleStats struct {
	// CumWriteBytes is the cumulative bytes written to disk.
	CumWriteBytes uint64
	// TODO(sumeer): add more stats.
	// cumReadBytes uint64
	// cumReadBytesInCache uint64
}

// CompactionGrantHandle is used to frequently update the CompactionScheduler
// about resource consumption. The MeasureCPU and CumulativeStats methods must
// be called frequently.
type CompactionGrantHandle interface {
	// Started is called once and must precede calls to MeasureCPU and
	// CumulativeStats.
	//
	// TODO(sumeer): consider eliminating Started given there isn't a
	// requirement to call it on the same goroutine that runs the compaction.
	Started()
	// MeasureCPU is called from each of the two goroutines that consume CPU
	// during a compaction. The parameter g must be 0 or 1, to differentiate
	// between the goroutines.
	MeasureCPU(g int)
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

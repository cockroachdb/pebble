// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "sync"

const maxStatsSamples = 20

type fileStatsSampler struct {
	loadStats     func(*fileMetadata, *fileStats) error
	eventListener EventListener

	mu            sync.Mutex
	accum         fileStats
	accumFileSize uint64
	samples       [maxStatsSamples]sample // scratch
}

type sample struct {
	*fileMetadata
	stats fileStats
}

// fileStats holds statistics for an on-disk table.
type fileStats struct {
	entries       uint64
	deletions     uint64
	rawKeyBytes   uint64
	rawValueBytes uint64
}

func (s *fileStatsSampler) init(loadFunc func(*fileMetadata, *fileStats) error, el EventListener) {
	s.loadStats = loadFunc
	s.eventListener = el
}

// sample loads file stats from the properties of added and updates its
// accumulated totals. It updates sampled files to have non-zero compensated
// sizes. This method will perform IO, so call it without holding d.mu.
func (s *fileStatsSampler) sample(added [numLevels][]*fileMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	samples := s.collectLocked(added)

	// Compute average value size with the updated accumulated stats.
	var averageValueSize uint64
	nonDeletions := s.accum.entries - s.accum.deletions
	if nonDeletions != 0 {
		averageValueSize = s.accum.rawValueBytes / nonDeletions *
			s.accumFileSize /
			(s.accum.rawKeyBytes + s.accum.rawValueBytes)
	}

	// Calculate compensated sizes for sampled files.
	for _, f := range samples {
		// NB: This is a different compensated size calculation than
		// RocksDB's. RocksDB's only adjusts the size if deletion entries
		// exceed non-deletion entries in a file. RocksDB also adds additional
		// weight to each deletion.
		// TODO(jackson): compensate for range deletion tombstones
		v := f.Size + f.stats.deletions*averageValueSize
		f.SetCompensatedSize(v)
	}
}

func (s *fileStatsSampler) collectLocked(added [numLevels][]*fileMetadata) []sample {
	samples := s.samples[:0] // reuse samples array

	for _, ff := range added {
		for _, f := range ff {
			if _, ok := f.CompensatedSize(); ok {
				// Skip files with already compensated sizes.
				continue
			}

			var stats fileStats
			err := s.loadStats(f, &stats)
			if err != nil {
				s.eventListener.BackgroundError(err)
				continue
			}
			s.accum.entries += stats.entries
			s.accum.deletions += stats.deletions
			s.accum.rawKeyBytes += stats.rawKeyBytes
			s.accum.rawValueBytes += stats.rawValueBytes
			s.accumFileSize += f.Size
			samples = append(samples, sample{fileMetadata: f, stats: stats})

			if len(samples) == maxStatsSamples {
				return samples
			}
		}
	}
	return samples
}

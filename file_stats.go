// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync"
)

const maxStatsSamples = 20

type fileStatsSampler struct {
	loadStats     func(*fileMetadata, *fileStats) error
	eventListener EventListener

	mu            sync.Mutex
	accum         fileStats
	accumFileSize uint64
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
	nonDeletions := s.accum.Entries - s.accum.Deletions
	if nonDeletions != 0 {
		averageValueSize = s.accum.RawValueBytes / nonDeletions *
			s.accumFileSize /
			(s.accum.RawKeyBytes + s.accum.RawValueBytes)
	}

	// Calculate compensated sizes for sampled files.
	for _, f := range samples {
		// NB: This is a different compensated size calculation than
		// RocksDB's. RocksDB's only adjusts the size if deletion entries
		// exceed non-deletion entries in a file. RocksDB also adds additional
		// weight to each deletion.
		// TODO(jackson): compensate for range deletion tombstones
		stats := f.MaybeStats()
		if stats == nil {
			panic("programming error")
		}
		v := f.Size + stats.Deletions*averageValueSize
		f.SetCompensatedSize(v)
	}
}

func (s *fileStatsSampler) collectLocked(added [numLevels][]*fileMetadata) []*fileMetadata {
	samples := make([]*fileMetadata, 0, maxStatsSamples)
	for _, ff := range added {
		for _, f := range ff {
			if stats := f.MaybeStats(); stats != nil {
				// Skip files that already have collected stats.
				continue
			}

			stats, err := f.Stats(s.loadStats)
			if err != nil {
				s.eventListener.BackgroundError(err)
				continue
			}
			s.accum.Entries += stats.Entries
			s.accum.Deletions += stats.Deletions
			s.accum.RawKeyBytes += stats.RawKeyBytes
			s.accum.RawValueBytes += stats.RawValueBytes
			s.accumFileSize += f.Size
			samples = append(samples, f)

			if len(samples) == maxStatsSamples {
				return samples
			}
		}
	}
	return samples
}

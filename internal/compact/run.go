// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"sort"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
)

// Result stores the result of a compaction - more specifically, the "data" part
// where we use the compaction iterator to write output tables.
type Result struct {
	// Err is the result of the compaction.
	//
	// On success, Err is nil and Tables stores the output tables.
	//
	// On failure, Err is set and Tables stores the tables created so far (and
	// which need to be cleaned up).
	Err error

	Tables []OutputTable
}

// OutputTable contains metadata about a table that was created during a compaction.
type OutputTable struct {
	CreationTime time.Time
	// ObjMeta is metadata for the object backing the table.
	ObjMeta objstorage.ObjectMetadata
	// WriterMeta is populated once the table is fully written. On compaction
	// failure (see Result), WriterMeta might not be set.
	WriterMeta sstable.WriterMetadata
}

// RunnerConfig contains the parameters needed for the Runner.
type RunnerConfig struct {
	// L0SplitKeys is only set for flushes and it contains the flush split keys
	// (see L0Sublevels.FlushSplitKeys). These are split points enforced for the
	// output tables.
	L0SplitKeys [][]byte

	// Grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that
	// the actual files in the grandparent when this compaction finishes will be
	// the same.
	Grandparents manifest.LevelSlice

	// MaxGrandparentOverlapBytes is the maximum number of bytes of overlap
	// allowed for a single output table with the tables in the grandparent level.
	MaxGrandparentOverlapBytes uint64

	// TargetOutputFileSize is the desired size of an individual table created
	// during compaction. In practice, the sizes can vary between 50%-200% of this
	// value.
	TargetOutputFileSize uint64
}

// Runner is a helper for running the "data" part of a compaction (where we use
// the compaction iterator to write output tables).
type Runner struct {
	cmp  base.Compare
	cfg  RunnerConfig
	iter *Iter
}

// NewRunner creates a new Runner.
// TODO(radu): document usage once we have more functionality.
func NewRunner(cfg RunnerConfig, iter *Iter) *Runner {
	r := &Runner{
		cmp:  iter.cmp,
		cfg:  cfg,
		iter: iter,
	}
	return r
}

// TableSplitLimit returns a hard split limit for an output table that starts at
// startKey (which must be strictly greater than startKey), or nil if there is
// no limit.
func (r *Runner) TableSplitLimit(startKey []byte) []byte {
	var limitKey []byte

	// Enforce the MaxGrandparentOverlapBytes limit: find the user key to which
	// that table can extend without excessively overlapping the grandparent
	// level. If no limit is needed considering the grandparent, limitKey stays
	// nil.
	//
	// This is done in order to prevent a table at level N from overlapping too
	// much data at level N+1. We want to avoid such large overlaps because they
	// translate into large compactions. The current heuristic stops output of a
	// table if the addition of another key would cause the table to overlap more
	// than 10x the target file size at level N. See
	// compaction.maxGrandparentOverlapBytes.
	iter := r.cfg.Grandparents.Iter()
	var overlappedBytes uint64
	f := iter.SeekGE(r.cmp, startKey)
	// Handle an overlapping table.
	if f != nil && r.cmp(f.Smallest.UserKey, startKey) <= 0 {
		overlappedBytes += f.Size
		f = iter.Next()
	}
	for ; f != nil; f = iter.Next() {
		overlappedBytes += f.Size
		if overlappedBytes > r.cfg.MaxGrandparentOverlapBytes {
			limitKey = f.Smallest.UserKey
			break
		}
	}

	if len(r.cfg.L0SplitKeys) != 0 {
		// Find the first split key that is greater than startKey.
		index := sort.Search(len(r.cfg.L0SplitKeys), func(i int) bool {
			return r.cmp(r.cfg.L0SplitKeys[i], startKey) > 0
		})
		if index < len(r.cfg.L0SplitKeys) {
			limitKey = base.MinUserKey(r.cmp, limitKey, r.cfg.L0SplitKeys[index])
		}
	}

	return limitKey
}

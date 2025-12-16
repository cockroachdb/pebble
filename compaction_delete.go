// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/tombspan"
)

// tryScheduleDeleteOnlyCompaction tries to kick off a delete-only compaction
// for all files that can be deleted as suggested by wide tombstones.
//
// Requires d.mu to be held. Updates d.mu.compact.wideTombstones.
//
// Returns true iff a compaction was started.
func (d *DB) tryScheduleDeleteOnlyCompaction() bool {
	if d.opts.private.disableDeleteOnlyCompactions ||
		d.opts.DisableAutomaticCompactions {
		return false
	}
	if _, maxConcurrency := d.opts.CompactionConcurrencyRange(); d.mu.compact.compactingCount >= maxConcurrency {
		return false
	}
	v := d.mu.versions.currentVersion()
	isExciseAllowed := d.FormatMajorVersion() >= FormatVirtualSSTables &&
		d.opts.Experimental.EnableDeleteOnlyCompactionExcises != nil &&
		d.opts.Experimental.EnableDeleteOnlyCompactionExcises()

	picked, ok := d.mu.compact.wideTombstones.PickCompaction(v, isExciseAllowed)
	if !ok {
		return false
	}
	c := newDeleteOnlyCompaction(d.opts, v, picked, d.opts.private.timeNow())
	d.mu.compact.compactingCount++
	d.mu.compact.compactProcesses++
	c.AddInProgressLocked(d)
	go d.compact(c, nil)
	return true
}

// newDeleteOnlyCompaction constructs a delete-only compaction from the provided
// inputs.
//
// The compaction is created with a reference to its version that must be
// released when the compaction is complete.
func newDeleteOnlyCompaction(
	opts *Options, cur *manifest.Version, picked tombspan.DeleteOnlyCompaction, beganAt time.Time,
) *tableCompaction {
	c := &tableCompaction{
		kind:     compactionKindDeleteOnly,
		comparer: opts.Comparer,
		logger:   opts.Logger,
		version:  cur,
		inputs: []compactionLevel{{
			level: picked.Level,
			files: picked.Table.Slice(),
		}},
		deleteOnly:  picked,
		grantHandle: noopGrantHandle{},
		metrics: compactionMetrics{
			beganAt: beganAt,
		},
	}
	c.outputLevel = &c.inputs[0]
	// Acquire a reference to the version to ensure that files and in-memory
	// version state necessary for reading files remain available. Ignoring
	// excises, this isn't strictly necessary for reading the sstables that are
	// inputs to the compaction because those files are 'marked as compacting'
	// and shouldn't be subject to any competing compactions. However with
	// excises, a concurrent excise may remove a compaction's file from the
	// Version and then cancel the compaction. The file shouldn't be physically
	// removed until the cancelled compaction stops reading it.
	//
	// Additionally, we need any blob files referenced by input sstables to
	// remain available, even if the blob file is rewritten. Maintaining a
	// reference ensures that all these files remain available for the
	// compaction's reads.
	c.version.Ref()
	c.bounds = picked.Table.UserKeyBounds()
	return c
}

// Runs a delete-only compaction.
//
// d.mu must *not* be held when calling this.
func (d *DB) runDeleteOnlyCompaction(
	c *tableCompaction,
) (ve *manifest.VersionEdit, stats compact.Stats, blobs []compact.OutputBlob, retErr error) {
	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	if c.deleteOnly.Excise && d.FormatMajorVersion() < FormatVirtualSSTables {
		panic(errors.AssertionFailedf("excise is not supported at this format major version"))
	}

	lm := c.metrics.perLevel.level(c.deleteOnly.Level)
	ve = &manifest.VersionEdit{
		DeletedTables: make(map[manifest.DeletedTableEntry]*manifest.TableMetadata, 1),
	}

	if !c.deleteOnly.Excise {
		ve.DeletedTables[manifest.DeletedTableEntry{
			Level:   c.deleteOnly.Level,
			FileNum: c.deleteOnly.Table.TableNum,
		}] = c.deleteOnly.Table.TableMetadata
		lm.TablesDeleted++
	} else {
		left, right, err := d.exciseTable(
			context.TODO(), c.deleteOnly.Bounds, c.deleteOnly.Table.TableMetadata,
			c.deleteOnly.Level, tightExciseBounds)
		if err != nil {
			return nil, stats, blobs, errors.Wrap(err, "error when running excise for delete-only compaction")
		}
		if left != nil && right != nil {
			return nil, stats, blobs, errors.AssertionFailedf(
				"delete-only compaction excised the middle of a table")
		}
		ve.NewTables = applyExciseToVersionEdit(ve, c.deleteOnly.Table.TableMetadata,
			left, right, c.deleteOnly.Level)
		lm.TablesExcised++

		c.annotations = append(c.annotations, "[excise]")
	}

	// Refresh the disk available statistic whenever a compaction/flush
	// completes, before re-acquiring the mutex.
	d.calculateDiskAvailableBytes()
	return ve, stats, blobs, nil
}

// tombstoneTypeFromKeys returns a manifest.KeyType given a slice of
// keyspan.Keys.
func tombstoneKeyTypeFromKeys(keys []keyspan.Key) manifest.KeyType {
	var pointKeys, rangeKeys bool
	for _, k := range keys {
		switch k.Kind() {
		case base.InternalKeyKindRangeDelete:
			pointKeys = true
		case base.InternalKeyKindRangeKeyDelete:
			rangeKeys = true
		default:
			panic(errors.AssertionFailedf("unsupported key kind: %s", k.Kind()))
		}
	}
	switch {
	case pointKeys && rangeKeys:
		return manifest.KeyTypePointAndRange
	case pointKeys:
		return manifest.KeyTypePoint
	case rangeKeys:
		return manifest.KeyTypeRange
	default:
		panic(errors.AssertionFailedf("no keys"))
	}
}

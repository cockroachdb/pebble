// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"code.google.com/p/leveldb-go/leveldb/db"
)

const (
	targetFileSize = 2 * 1024 * 1024

	// expandedCompactionByteSizeLimit is the maximum number of bytes in
	// all compacted files. We avoid expanding the lower level file set of
	// a compaction if it would make the total compaction cover more than
	// this many bytes.
	expandedCompactionByteSizeLimit = 25 * targetFileSize
)

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	version *version

	// level is the level that is being compacted. Inputs from level and
	// level+1 will be merged to produce a set of level+1 files.
	level int

	// inputs are the tables to be compacted.
	inputs [3][]fileMetadata
}

// pickCompaction picks the best compaction, if any, for vs' current version.
func pickCompaction(vs *versionSet) (c *compaction) {
	cur := vs.currentVersion()

	// Pick a compaction based on size. If none exist, pick one based on seeks.
	if cur.compactionScore >= 1 {
		c = &compaction{
			version: cur,
			level:   cur.compactionLevel,
		}
		// TODO: Pick the first file that comes after the compaction pointer for c.level.
		c.inputs[0] = []fileMetadata{cur.files[c.level][0]}

	} else if false {
		// TODO: look for a compaction triggered by seeks.

	} else {
		return nil
	}

	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if c.level == 0 {
		smallest, largest := ikeyRange(vs.icmp, c.inputs[0], nil)
		c.inputs[0] = cur.overlaps(0, vs.ucmp, smallest.ukey(), largest.ukey())
		if len(c.inputs) == 0 {
			panic("leveldb: empty compaction")
		}
	}

	c.setupOtherInputs(vs)
	return c
}

// TODO: user initiated compactions.

// setupOtherInputs fills in the rest of the compaction inputs, regardless of
// whether the compaction was automatically scheduled or user initiated.
func (c *compaction) setupOtherInputs(vs *versionSet) {
	smallest0, largest0 := ikeyRange(vs.icmp, c.inputs[0], nil)
	c.inputs[1] = c.version.overlaps(c.level+1, vs.ucmp, smallest0.ukey(), largest0.ukey())
	smallest01, largest01 := ikeyRange(vs.icmp, c.inputs[0], c.inputs[1])

	// Grow the inputs if it doesn't affect the number of level+1 files.
	if c.grow(vs, smallest01, largest01) {
		smallest01, largest01 = ikeyRange(vs.icmp, c.inputs[0], c.inputs[1])
	}

	// Compute the set of level+2 files that overlap this compaction.
	if c.level+2 < numLevels {
		c.inputs[2] = c.version.overlaps(c.level+2, vs.ucmp, smallest01.ukey(), largest01.ukey())
	}

	// TODO: update the compaction pointer for c.level.
}

// grow grows the number of inputs at c.level without changing the number of
// c.level+1 files in the compaction, and returns whether the inputs grew. sm
// and la are the smallest and largest internalKeys in all of the inputs.
func (c *compaction) grow(vs *versionSet, sm, la internalKey) bool {
	if len(c.inputs[1]) == 0 {
		return false
	}
	grow0 := c.version.overlaps(c.level, vs.ucmp, sm.ukey(), la.ukey())
	if len(grow0) <= len(c.inputs[0]) {
		return false
	}
	if totalSize(grow0)+totalSize(c.inputs[1]) >= expandedCompactionByteSizeLimit {
		return false
	}
	sm1, la1 := ikeyRange(vs.icmp, grow0, nil)
	grow1 := c.version.overlaps(c.level+1, vs.ucmp, sm1, la1)
	if len(grow1) != len(c.inputs[1]) {
		return false
	}
	c.inputs[0] = grow0
	c.inputs[1] = grow1
	return true
}

// isBaseLevelForUkey reports whether it is guaranteed that there are no
// key/value pairs at c.level+2 or higher that have the user key ukey.
func (c *compaction) isBaseLevelForUkey(userCmp db.Comparer, ukey []byte) bool {
	// TODO: this can be faster if ukey is always increasing between successive
	// isBaseLevelForUkey calls and we can keep some state in between calls.
	for level := c.level + 2; level < numLevels; level++ {
		for _, f := range c.version.files[level] {
			if userCmp.Compare(ukey, f.largest.ukey()) <= 0 {
				if userCmp.Compare(ukey, f.smallest.ukey()) >= 0 {
					return false
				}
				// For levels above level 0, the files within a level are in
				// increasing ikey order, so we can break early.
				break
			}
		}
	}
	return true
}

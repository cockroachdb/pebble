// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math"
)

// compactionPicker holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPicker struct {
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int

	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64

	// These fields are the level that should be compacted next and its
	// compaction score. A score < 1 means that compaction is not strictly
	// needed.
	score float64
	level int
	file  int
}

func newCompactionPicker(v *version, opts *Options) *compactionPicker {
	p := &compactionPicker{
		vers: v,
	}
	p.initLevelMaxBytes(v, opts)
	p.initTarget(v, opts)
	return p
}

func (p *compactionPicker) compactionNeeded() bool {
	if p == nil {
		return false
	}
	return p.score >= 1
}

func (p *compactionPicker) initLevelMaxBytes(v *version, opts *Options) {
	// Determine the first non-empty level and the maximum size of any level.
	firstNonEmptyLevel := -1
	var botLevelSize int64
	for level := 1; level < numLevels; level++ {
		levelSize := int64(totalSize(v.files[level]))
		if levelSize > 0 {
			if firstNonEmptyLevel == -1 {
				firstNonEmptyLevel = level
			}
			botLevelSize = levelSize
		}
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	if botLevelSize == 0 {
		// No levels for L1 and up contain any data. Target L0 compactions for the
		// last level.
		p.baseLevel = numLevels - 1
		return
	}

	levelMultiplier := 10.0

	baseBytesMax := opts.LBaseMaxBytes
	baseBytesMin := int64(float64(baseBytesMax) / levelMultiplier)

	curLevelSize := botLevelSize
	for level := numLevels - 2; level >= firstNonEmptyLevel; level-- {
		curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
	}

	if curLevelSize <= baseBytesMin {
		// If we make target size of last level to be botLevelSize, target size of
		// the first non-empty level would be smaller than baseBytesMin. We set it
		// be baseBytesMin.
		p.baseLevel = firstNonEmptyLevel
	} else {
		// Compute base level (where L0 data is compacted to).
		p.baseLevel = firstNonEmptyLevel
		for p.baseLevel > 1 && curLevelSize > baseBytesMax {
			p.baseLevel--
			curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
		}
	}

	var smoothedLevelMultiplier float64
	if p.baseLevel < numLevels-1 {
		smoothedLevelMultiplier = math.Pow(
			float64(botLevelSize)/float64(baseBytesMax),
			1.0/float64(numLevels-p.baseLevel-1))
	} else {
		smoothedLevelMultiplier = 1.0
	}

	levelSize := baseBytesMax
	for level := p.baseLevel; level < numLevels; level++ {
		if level > p.baseLevel {
			if levelSize > 0 && float64(math.MaxInt64/levelSize) >= smoothedLevelMultiplier {
				levelSize = int64(float64(levelSize) * smoothedLevelMultiplier)
			}
		}
		p.levelMaxBytes[level] = levelSize
	}
}

// initTarget initializes the compaction score and level. If the compaction
// score indicates compaction is needed, a target table within the target level
// is selected for compaction.
func (p *compactionPicker) initTarget(v *version, opts *Options) {
	// We treat level-0 specially by bounding the number of files instead of
	// number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too many
	// level-0 compactions.
	//
	// (2) The files in level-0 are merged on every read and therefore we
	// wish to avoid too many files when the individual file size is small
	// (perhaps because of a small write-buffer setting, or very high
	// compression ratios, or lots of overwrites/deletions).
	p.score = float64(len(v.files[0])) / float64(opts.L0CompactionThreshold)
	p.level = 0

	for level := 1; level < numLevels-1; level++ {
		score := float64(totalSize(v.files[level])) / float64(p.levelMaxBytes[level])
		if p.score < score {
			p.score = score
			p.level = level
		}
	}

	if p.score >= 1 {
		// TODO(peter): Select the file within the level to compact. See the
		// kMinOverlappingRatio heuristic in RocksDB which chooses the file with the
		// minimum overlapping ratio with the next level. This minimizes write
		// amplification. We also want to computed a "compensated size" which adjusts
		// the size of a table based on the number of deletions it contains.
		//
		// We want to minimize write amplification, but also ensure that deletes
		// are propagated to the bottom level in a timely fashion so as to reclaim
		// disk space. A table's smallest sequence number provides a measure of its
		// age. The ratio of overlapping-bytes / table-size gives an indication of
		// write amplification (a smaller ratio is preferrable).
		//
		// Simulate various workloads:
		// - Uniform random write
		// - Uniform random write+delete
		// - Skewed random write
		// - Skewed random write+delete
		// - Sequential write
		// - Sequential write+delete (queue)

		// The current heuristic matches the RocksDB kOldestSmallestSeqFirst
		// heuristic.
		smallestSeqNum := uint64(math.MaxUint64)
		files := v.files[p.level]
		for i := range files {
			f := &files[i]
			if smallestSeqNum > f.smallestSeqNum {
				smallestSeqNum = f.smallestSeqNum
				p.file = i
			}
		}
		return
	}

	// No levels exceeded their size threshold. Check for forced compactions.
	for level := 0; level < numLevels-1; level++ {
		files := v.files[p.level]
		for i := range files {
			f := &files[i]
			if f.markedForCompaction {
				p.score = 1.0
				p.level = level
				p.file = i
				return
			}
		}
	}

	// TODO(peter): When a snapshot is released, we may need to compact tables at
	// the bottom level in order to free up entries that were pinned by the
	// snapshot.
}

// pickAuto picks the best compaction, if any.
func (p *compactionPicker) pickAuto(opts *Options) (c *compaction) {
	if !p.compactionNeeded() {
		return nil
	}

	vers := p.vers
	c = newCompaction(opts, vers, p.level, p.baseLevel)
	c.inputs[0] = vers.files[c.startLevel][p.file : p.file+1]

	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if c.startLevel == 0 {
		cmp := opts.Comparer.Compare
		smallest, largest := ikeyRange(cmp, c.inputs[0], nil)
		c.inputs[0] = vers.overlaps(0, cmp, smallest.UserKey, largest.UserKey)
		if len(c.inputs) == 0 {
			panic("pebble: empty compaction")
		}
	}

	c.setupOtherInputs()
	return c
}

func (p *compactionPicker) pickManual(opts *Options, manual *manualCompaction) (c *compaction) {
	if p == nil {
		return nil
	}

	// TODO(peter): The logic here is untested and possibly incomplete.
	cur := p.vers
	c = newCompaction(opts, cur, manual.level, p.baseLevel)
	manual.outputLevel = c.outputLevel
	cmp := opts.Comparer.Compare
	c.inputs[0] = cur.overlaps(manual.level, cmp, manual.start.UserKey, manual.end.UserKey)
	if len(c.inputs[0]) == 0 {
		return nil
	}
	c.setupOtherInputs()
	return c
}

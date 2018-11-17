// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "github.com/petermattis/pebble/db"

// compactionPicker holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPicker struct {
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int

	// These fields are the level that should be compacted next and its
	// compaction score. A score < 1 means that compaction is not strictly
	// needed.
	score float64
	level int
}

func newCompactionPicker(v *version, opts *db.Options) *compactionPicker {
	p := &compactionPicker{
		vers: v,
	}
	p.initBaseLevel(v, opts)
	p.initScore(v, opts)
	return p
}

func (p *compactionPicker) compactionNeeded() bool {
	if p == nil {
		return false
	}
	return p.score >= 1
}

func (p *compactionPicker) initBaseLevel(v *version, opts *db.Options) {
	p.baseLevel = numLevels - 1
	for level := 1; level < p.baseLevel; level++ {
		if len(v.files) > 0 {
			p.baseLevel = level
			break
		}
	}
}

// initScore initializes the compaction score and level.
func (p *compactionPicker) initScore(v *version, opts *db.Options) {
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
		score := float64(totalSize(v.files[level])) / float64(opts.Level(level).MaxBytes)
		if score > p.score {
			p.score = score
			p.level = level
		}
	}
}

// pick picks the best compaction, if any.
func (p *compactionPicker) pick(opts *db.Options) (c *compaction) {
	if !p.compactionNeeded() {
		return nil
	}

	// TODO(peter): Flesh out the compaction heuristics. Need to first determine
	// the level to compact, then the file within the level. We need to iterate
	// from the higest score level to the lowest score level, choosing the first
	// level that needs a compaction and has a table available for compaction
	// (i.e. not already being compacted).

	vers := p.vers
	c = newCompaction(opts, vers, p.level)
	c.inputs[0] = []fileMetadata{vers.files[c.level][0]}

	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if c.level == 0 {
		cmp := opts.Comparer.Compare
		smallest, largest := ikeyRange(cmp, c.inputs[0], nil)
		c.inputs[0] = vers.overlaps(0, cmp, smallest.UserKey, largest.UserKey)
		if len(c.inputs) == 0 {
			panic("pebble: empty compaction")
		}
	}

	c.setupOtherInputs(opts)
	return c
}

func (p *compactionPicker) pickManual(opts *db.Options, manual *manualCompaction) (c *compaction) {
	if p == nil {
		return nil
	}

	// TODO(peter): The logic here is untested and likely incomplete.
	cur := p.vers
	c = newCompaction(opts, cur, manual.level)
	cmp := opts.Comparer.Compare
	c.inputs[0] = cur.overlaps(manual.level, cmp, manual.start.UserKey, manual.end.UserKey)
	if len(c.inputs[0]) == 0 {
		return nil
	}
	c.setupOtherInputs(opts)
	return c
}

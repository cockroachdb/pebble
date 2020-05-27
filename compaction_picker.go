// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"math"
	"sort"

	"github.com/cockroachdb/pebble/internal/manifest"
)

// The minimum count for an intra-L0 compaction. This matches the RocksDB
// heuristic.
const minIntraL0Count = 4

type compactionEnv struct {
	bytesCompacted          *uint64
	earliestUnflushedSeqNum uint64
	inProgressCompactions   []compactionInfo
}

type compactionPicker interface {
	getScores([]compactionInfo) [numLevels]float64
	getBaseLevel() int
	getEstimatedMaxWAmp() float64
	estimatedCompactionDebt(l0ExtraSize uint64) uint64
	pickAuto(env compactionEnv) (c *compaction)
	pickManual(env compactionEnv, manual *manualCompaction) (c *compaction, retryLater bool)

	forceBaseLevel1()
}

// Information about in-progress compactions provided to the compaction picker. These are used to
// constrain the new compactions that will be picked.
type compactionInfo struct {
	startLevel  int
	outputLevel int
	inputs      [2][]*fileMetadata
}

type sortCompactionLevelsDecreasingScore []pickedCompactionInfo

func (s sortCompactionLevelsDecreasingScore) Len() int {
	return len(s)
}
func (s sortCompactionLevelsDecreasingScore) Less(i, j int) bool {
	if s[i].score != s[j].score {
		return s[i].score > s[j].score
	}
	return s[i].level < s[j].level
}
func (s sortCompactionLevelsDecreasingScore) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func newCompactionPicker(
	v *version, opts *Options, inProgressCompactions []compactionInfo,
) compactionPicker {
	p := &compactionPickerByScore{
		opts: opts,
		vers: v,
	}
	p.initLevelMaxBytes(inProgressCompactions)
	return p
}

// Information about a candidate compaction that has been identified by the
// compaction picker.
type pickedCompactionInfo struct {
	// The score of the level to be compacted.
	score float64
	level int
	// The level to compact to.
	outputLevel int
	// The file in level that will be compacted. Additional files may be picked by the
	// compaction.
	file int
}

// compactionPickerByScore holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPickerByScore struct {
	opts *Options
	vers *version

	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int

	// estimatedMaxWAmp is the estimated maximum write amp per byte that is
	// added to L0.
	estimatedMaxWAmp float64

	// smoothedLevelMultiplier is the size ratio between one level and the next.
	smoothedLevelMultiplier float64

	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64
}

var _ compactionPicker = &compactionPickerByScore{}

func (p *compactionPickerByScore) getScores(inProgress []compactionInfo) [numLevels]float64 {
	var scores [numLevels]float64
	for _, info := range p.calculateScores(inProgress) {
		scores[info.level] = info.score
	}
	return scores
}

func (p *compactionPickerByScore) getBaseLevel() int {
	if p == nil {
		return 1
	}
	return p.baseLevel
}

func (p *compactionPickerByScore) getEstimatedMaxWAmp() float64 {
	return p.estimatedMaxWAmp
}

// estimatedCompactionDebt estimates the number of bytes which need to be
// compacted before the LSM tree becomes stable.
func (p *compactionPickerByScore) estimatedCompactionDebt(l0ExtraSize uint64) uint64 {
	if p == nil {
		return 0
	}

	// We assume that all the bytes in L0 need to be compacted to Lbase. This is
	// unlike the RocksDB logic that figures out whether L0 needs compaction.
	bytesAddedToNextLevel := l0ExtraSize + totalSize(p.vers.Files[0])
	nextLevelSize := totalSize(p.vers.Files[p.baseLevel])

	var compactionDebt uint64
	if bytesAddedToNextLevel > 0 && nextLevelSize > 0 {
		// We only incur compaction debt if both L0 and Lbase contain data. If L0
		// is empty, no compaction is necessary. If Lbase is empty, a move-based
		// compaction from L0 would occur.
		compactionDebt += bytesAddedToNextLevel + nextLevelSize
	}

	for level := p.baseLevel; level < numLevels-1; level++ {
		levelSize := nextLevelSize + bytesAddedToNextLevel
		nextLevelSize = totalSize(p.vers.Files[level+1])
		if levelSize > uint64(p.levelMaxBytes[level]) {
			bytesAddedToNextLevel = levelSize - uint64(p.levelMaxBytes[level])
			if nextLevelSize > 0 {
				// We only incur compaction debt if the next level contains data. If the
				// next level is empty, a move-based compaction would be used.
				levelRatio := float64(nextLevelSize) / float64(levelSize)
				// The current level contributes bytesAddedToNextLevel to compactions.
				// The next level contributes levelRatio * bytesAddedToNextLevel.
				compactionDebt += uint64(float64(bytesAddedToNextLevel) * (levelRatio + 1))
			}
		}
	}

	return compactionDebt
}

func (p *compactionPickerByScore) initLevelMaxBytes(inProgressCompactions []compactionInfo) {
	// Determine the first non-empty level and the maximum size of any level.
	firstNonEmptyLevel := -1
	var bottomLevelSize int64
	for level := 1; level < numLevels; level++ {
		levelSize := int64(totalSize(p.vers.Files[level]))
		if levelSize > 0 {
			if firstNonEmptyLevel == -1 {
				firstNonEmptyLevel = level
			}
			bottomLevelSize = levelSize
		}
	}
	for _, c := range inProgressCompactions {
		if c.outputLevel == 0 {
			continue
		}
		if c.startLevel == 0 && (firstNonEmptyLevel == -1 || c.outputLevel < firstNonEmptyLevel) {
			firstNonEmptyLevel = c.outputLevel
		}
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	if bottomLevelSize == 0 {
		// No levels for L1 and up contain any data. Target L0 compactions for the
		// last level or to the level to which there is an ongoing L0 compaction.
		p.baseLevel = numLevels - 1
		if firstNonEmptyLevel >= 0 {
			p.baseLevel = firstNonEmptyLevel
		}
		return
	}

	levelMultiplier := 10.0

	baseBytesMax := p.opts.LBaseMaxBytes
	baseBytesMin := int64(float64(baseBytesMax) / levelMultiplier)

	curLevelSize := bottomLevelSize
	for level := numLevels - 2; level >= firstNonEmptyLevel; level-- {
		curLevelSize = int64(float64(curLevelSize) / levelMultiplier)
	}

	if curLevelSize <= baseBytesMin {
		// If we make target size of last level to be bottomLevelSize, target size of
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

	if p.baseLevel < numLevels-1 {
		p.smoothedLevelMultiplier = math.Pow(
			float64(bottomLevelSize)/float64(baseBytesMax),
			1.0/float64(numLevels-p.baseLevel-1))
	} else {
		p.smoothedLevelMultiplier = 1.0
	}

	p.estimatedMaxWAmp = float64(numLevels-p.baseLevel) * (p.smoothedLevelMultiplier + 1)

	levelSize := float64(baseBytesMax)
	for level := p.baseLevel; level < numLevels; level++ {
		if level > p.baseLevel && levelSize > 0 {
			levelSize *= p.smoothedLevelMultiplier
		}
		// Round the result since test cases use small target level sizes, which
		// can be impacted by floating-point imprecision + integer truncation.
		roundedLevelSize := math.Round(levelSize)
		if roundedLevelSize > float64(math.MaxInt64) {
			p.levelMaxBytes[level] = math.MaxInt64
		} else {
			p.levelMaxBytes[level] = int64(roundedLevelSize)
		}
	}
}

func calculateSizeAdjust(inProgressCompactions []compactionInfo) [numLevels]int64 {
	// Compute a size adjustment for each level based on the in-progress
	// compactions. We subtract the size of the start level inputs from the start
	// level, and add the size of the start level inputs to the output
	// level. This is slightly different from RocksDB's behavior, which simply
	// elides compacting files from the level size calculation.
	var sizeAdjust [numLevels]int64
	for i := range inProgressCompactions {
		c := &inProgressCompactions[i]
		size := int64(totalSize(c.inputs[0]))
		sizeAdjust[c.startLevel] -= size
		sizeAdjust[c.outputLevel] += size
	}
	return sizeAdjust
}

func (p *compactionPickerByScore) calculateScores(inProgressCompactions []compactionInfo) [numLevels]pickedCompactionInfo {
	var scores [numLevels]pickedCompactionInfo
	for i := range scores {
		scores[i].level = i
		scores[i].outputLevel = i + 1
	}
	scores[0] = p.calculateL0Score(inProgressCompactions)

	sizeAdjust := calculateSizeAdjust(inProgressCompactions)
	for level := 1; level < numLevels-1; level++ {
		size := int64(totalSize(p.vers.Files[level])) + sizeAdjust[level]
		scores[level].score = float64(size) / float64(p.levelMaxBytes[level])
	}
	sort.Sort(sortCompactionLevelsDecreasingScore(scores[:]))
	return scores
}

func (p *compactionPickerByScore) calculateL0Score(inProgressCompactions []compactionInfo) pickedCompactionInfo {
	var info pickedCompactionInfo
	info.outputLevel = p.baseLevel

	if p.opts.Experimental.L0SublevelCompactions {
		// If L0SubLevels are present, we use the sublevel count as opposed to
		// the L0 file count to score this level. The base vs intra-L0
		// compaction determination happens in pickAuto, not here.
		info.score = float64(p.vers.L0SubLevels.MaxDepthAfterOngoingCompactions()) /
			float64(p.opts.L0CompactionThreshold)
		return info
	}
	// TODO(peter): The current scoring logic precludes concurrent L0->Lbase
	// compactions in most cases because if there is an in-progress L0->Lbase
	// compaction we'll instead preferentially score an intra-L0 compaction. One
	// possible way out is to score both by increasing the size of the "scores"
	// array by one and adding entries for both L0->Lbase and intra-L0
	// compactions.

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

	// Score an L0->Lbase compaction by counting the number of idle
	// (non-compacting) files in L0.
	var idleL0Count int
	for _, f := range p.vers.Files[0] {
		if !f.Compacting {
			idleL0Count++
		}
	}
	info.score = float64(idleL0Count) / float64(p.opts.L0CompactionThreshold)

	// Only start an intra-L0 compaction if there is an existing L0->Lbase
	// compaction.
	var l0Compaction bool
	for i := range inProgressCompactions {
		if inProgressCompactions[i].startLevel == 0 &&
			inProgressCompactions[i].outputLevel != 0 {
			l0Compaction = true
			break
		}
	}
	if !l0Compaction {
		return info
	}

	l0Files := p.vers.Files[0]
	if len(l0Files) < p.opts.L0CompactionThreshold+2 {
		// If L0 isn't accumulating many files beyond the regular L0 trigger,
		// don't resort to an intra-L0 compaction yet. This matches the RocksDB
		// heuristic.
		return info
	}
	var end = len(l0Files)
	for ; end >= 1; end-- {
		if l0Files[end-1].Compacting {
			break
		}
	}

	intraL0Count := len(l0Files) - end
	if intraL0Count < minIntraL0Count {
		// Not enough idle L0 files to perform an intra-L0 compaction. This
		// matches the RocksDB heuristic. Note that if another file is flushed
		// or ingested to L0, a new compaction picker will be created and we'll
		// reexamine the intra-L0 score.
		return info
	}

	// Score the intra-L0 compaction using the number of files that are
	// possibly in the compaction.
	info.score = float64(intraL0Count) / float64(p.opts.L0CompactionThreshold)
	info.outputLevel = 0
	return info
}

func (p *compactionPickerByScore) pickFile(level int) int {
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
	//
	// TODO(peter): For concurrent compactions, we may want to try harder to pick
	// a seed file whose resulting compaction bounds do not overlap with an
	// in-progress compaction.
	smallestSeqNum := uint64(math.MaxUint64)
	file := -1
	for i, f := range p.vers.Files[level] {
		if f.Compacting {
			continue
		}
		if smallestSeqNum > f.SmallestSeqNum {
			smallestSeqNum = f.SmallestSeqNum
			file = i
		}
	}
	return file
}

// pickAuto picks the best compaction, if any.
//
// On each call, pickAuto computes per-level size adjustments based on
// in-progress compactions, and computes a per-level score. The levels are
// iterated over in decreasing score order trying to find a valid compaction
// anchored at that level.
//
// If a score-based compaction cannot be found, pickAuto falls back to looking
// for a forced compaction (identified by FileMetadata.MarkedForCompaction).
func (p *compactionPickerByScore) pickAuto(env compactionEnv) (c *compaction) {
	const highPriorityThreshold = 1.5

	scores := p.calculateScores(env.inProgressCompactions)

	// Check for a score-based compaction. "scores" has been sorted in order of
	// decreasing score. For each level with a score >= 1, we attempt to find a
	// compaction anchored at at that level.
	for i := range scores {
		info := &scores[i]
		if len(env.inProgressCompactions) > 0 && info.score < highPriorityThreshold {
			// Don't start a low priority compaction if there is already a compaction
			// running.
			return nil
		}
		if info.score < 1 {
			break
		}

		if info.level == 0 && p.opts.Experimental.L0SublevelCompactions {
			c = pickL0(env, p.opts, p.vers, p.baseLevel)
			// Fail-safe to protect against compacting the same sstable
			// concurrently.
			if c != nil && !inputAlreadyCompacting(c) {
				c.score = info.score
				return c
			}
			continue
		}

		info.file = p.pickFile(info.level)
		if info.file == -1 {
			continue
		}

		c := pickAutoHelper(env, p.opts, p.vers, *info, p.baseLevel)
		// Fail-safe to protect against compacting the same sstable concurrently.
		if c != nil && !inputAlreadyCompacting(c) {
			c.score = info.score
			return c
		}
	}

	// Check for forced compactions. These are lower priority than score-based
	// compactions. Note that this loop only runs if we haven't already found a
	// score-based compaction.
	//
	// TODO(peter): MarkedForCompaction is almost never set, making this
	// extremely wasteful in the common case. Could we maintain a
	// MarkedForCompaction map from fileNum to level?
	for level := 0; level < numLevels-1; level++ {
		for file, f := range p.vers.Files[level] {
			if !f.MarkedForCompaction {
				continue
			}
			for i := range scores {
				if scores[i].level != level {
					continue
				}
				info := &scores[i]
				info.file = file
				c := pickAutoHelper(env, p.opts, p.vers, *info, p.baseLevel)
				// Fail-safe to protect against compacting the same sstable concurrently.
				if c != nil && !inputAlreadyCompacting(c) {
					c.score = info.score
					return c
				}
				break
			}
			break
		}
	}

	// TODO(peter): When a snapshot is released, we may need to compact tables at
	// the bottom level in order to free up entries that were pinned by the
	// snapshot.
	return nil
}

func pickAutoHelper(
	env compactionEnv, opts *Options, vers *version, cInfo pickedCompactionInfo, baseLevel int,
) (c *compaction) {
	if cInfo.outputLevel == 0 {
		return pickIntraL0(env, opts, vers)
	}

	c = newCompaction(opts, vers, cInfo.level, baseLevel, env.bytesCompacted)
	if c.outputLevel != cInfo.outputLevel {
		panic("pebble: compaction picked unexpected output level")
	}
	c.inputs[0] = vers.Files[c.startLevel][cInfo.file : cInfo.file+1]
	// Files in level 0 may overlap each other, so pick up all overlapping ones.
	if c.startLevel == 0 {
		cmp := opts.Comparer.Compare
		smallest, largest := manifest.KeyRange(cmp, c.inputs[0], nil)
		c.inputs[0] = vers.Overlaps(0, cmp, smallest.UserKey, largest.UserKey)
		if len(c.inputs[0]) == 0 {
			panic("pebble: empty compaction")
		}
	}

	c.setupInputs()
	return c
}

// Helper method to pick compactions originating from L0. Uses information about
// sublevels to generate a compaction.
func pickL0(env compactionEnv, opts *Options, vers *version, baseLevel int) (c *compaction) {
	// It is important to pass information about Lbase files to L0SubLevels
	// so it can pick a compaction that does not conflict with an Lbase => Lbase+1
	// compaction. Without this, we observed reduced concurrency of L0=>Lbase
	// compactions, and increasing read amplification in L0.
	lcf, err := vers.L0SubLevels.PickBaseCompaction(
		opts.L0CompactionThreshold, vers.Files[baseLevel])
	if err != nil {
		opts.Logger.Infof("error when picking base compaction: %s", err)
		return
	}
	if lcf != nil {
		// Manually build the compaction as opposed to calling
		// pickAutoHelper. This is because L0SubLevels has already added
		// any overlapping L0 SSTables that need to be added, and
		// because compactions built by L0SSTables do not necessarily
		// pick contiguous sequences of files in p.vers.Files[0].
		c = newCompaction(opts, vers, 0, baseLevel, env.bytesCompacted)
		c.lcf = lcf
		if c.outputLevel != baseLevel {
			opts.Logger.Fatalf("compaction picked unexpected output level: %d != %d", c.outputLevel, baseLevel)
		}
		c.inputs[0] = make([]*manifest.FileMetadata, 0, len(lcf.Files))
		for j := range lcf.FilesIncluded {
			if lcf.FilesIncluded[j] {
				c.inputs[0] = append(c.inputs[0], vers.Files[0][j])
			}
		}
		c.setupInputs()
		if len(c.inputs[0]) == 0 {
			opts.Logger.Fatalf("empty compaction chosen")
		}
		return c
	}

	// Couldn't choose a base compaction. Try choosing an intra-L0
	// compaction.
	lcf, err = vers.L0SubLevels.PickIntraL0Compaction(env.earliestUnflushedSeqNum, opts.L0CompactionThreshold)
	if err != nil {
		opts.Logger.Infof("error when picking base compaction: %s", err)
		return
	}
	if lcf != nil {
		c = newCompaction(opts, vers, 0, 0, env.bytesCompacted)
		c.lcf = lcf
		c.inputs[0] = make([]*manifest.FileMetadata, 0, len(lcf.Files))
		for j := range lcf.FilesIncluded {
			if lcf.FilesIncluded[j] {
				c.inputs[0] = append(c.inputs[0], vers.Files[0][j])
			}
		}
		if len(c.inputs[0]) == 0 {
			opts.Logger.Fatalf("empty compaction chosen")
		}
		c.smallest, c.largest = manifest.KeyRange(c.cmp, c.inputs[0], nil)
		c.setupInuseKeyRanges()
		// Output only a single sstable for intra-L0 compactions.
		// Now that we have the ability to split flushes, we could conceivably
		// split the output of intra-L0 compactions too. This may be unnecessary
		// complexity -- the inputs to intra-L0 should be narrow in the key space
		// (unlike flushes), so writing a single sstable should be ok.
		c.maxOutputFileSize = math.MaxUint64
		c.maxOverlapBytes = math.MaxUint64
		c.maxExpandedBytes = math.MaxUint64
	}
	return c
}

func pickIntraL0(env compactionEnv, opts *Options, vers *version) (c *compaction) {
	l0Files := vers.Files[0]
	end := len(l0Files)
	for ; end >= 1; end-- {
		m := l0Files[end-1]
		if m.Compacting {
			return nil
		}
		if m.LargestSeqNum < env.earliestUnflushedSeqNum {
			break
		}
		// Don't compact an L0 file which contains a seqnum greater than the
		// earliest unflushed seqnum (we continue the loop, rather than existing,
		// see conditional above). This can happen when a file is ingested into L0
		// yet doesn't overlap with the memtable. Consider the scenario:
		//
		//   ingest a#2 -> 000001:[a#2-a#2]
		//   ingest a#3 -> 000002:[a#3-a#3]
		//   ingest a#4 -> 000003:[a#4-a#4]
		//   put a#5
		//   ingest b#6 -> 000004:[b#6-b#6]
		//   compact 000001,000002,000003,000004 -> 000005:[a#4-b#6]
		//   flush -> 000006:[a#5-a#5]
		//
		// At this point, the LSM will look like:
		//
		//   L0
		//     000006:[a#5-a#5]
		//     000005:[a#4-b#6]
		//
		// Because 000006's largest sequence number is smaller than 000005's it
		// is ordered before 000005. When performing reads, we’ll check 000005
		// first which is wrong as 000006 contains the newest value of
		// "a". Furthermore, the next L0->Lbase compaction can compact 000006
		// without compacting 000005, further violating the level sequence number
		// invariant.
		//
		// The solution to this problem is to exclude 000004 from the L0->L0
		// compaction. Doing so, will result in an LSM like:
		//
		//   L0
		//     000005:[a#4-a#4]
		//     000006:[a#5-a#5]
		//     000004:[b#6-b#6]
		//
		// And now everything is copacetic.
		//
		// See https://github.com/facebook/rocksdb/pull/5958.
	}
	if end < minIntraL0Count {
		return nil
	}

	compactTotalSize := l0Files[end-1].Size
	compactSizePerFile := uint64(math.MaxUint64)

	// The compaction will be in the range [begin,end). We add files to the
	// compaction until the amount of compaction work per file begins increasing.
	begin := end - 1
	for ; begin >= 1; begin-- {
		m := l0Files[begin-1]
		if m.Compacting {
			break
		}
		newCompactTotalSize := compactTotalSize + m.Size
		newCompactSizePerFile := newCompactTotalSize / uint64(end-(begin-1))
		if newCompactSizePerFile > compactSizePerFile {
			break
		}
		compactTotalSize = newCompactTotalSize
		compactSizePerFile = newCompactSizePerFile
	}

	if end-begin < minIntraL0Count {
		return nil
	}

	c = newCompaction(opts, vers, 0, 0, env.bytesCompacted)
	c.inputs[0] = l0Files[begin:end]
	c.smallest, c.largest = manifest.KeyRange(c.cmp, c.inputs[0], nil)
	c.setupInuseKeyRanges()
	// Output only a single sstable for intra-L0 compactions. There is no current
	// benefit to outputting multiple tables, because other parts of the code
	// (i.e. iterators and comapction) expect L0 sstables to overlap and will
	// thus read all of the L0 sstables anyways, even if they are partitioned.
	c.maxOutputFileSize = math.MaxUint64
	c.maxOverlapBytes = math.MaxUint64
	c.maxExpandedBytes = math.MaxUint64
	return c
}

func (p *compactionPickerByScore) pickManual(
	env compactionEnv, manual *manualCompaction,
) (c *compaction, retryLater bool) {
	if p == nil {
		return nil, false
	}

	outputLevel := manual.level + 1
	if manual.level == 0 {
		outputLevel = p.baseLevel
	} else if manual.level < p.baseLevel {
		// The start level for a compaction must be >= Lbase. A manual
		// compaction could have been created adhering to that condition, and
		// then an automatic compaction came in and compacted all of the
		// sstables in Lbase to Lbase+1 which caused Lbase to change. Simply
		// ignore this manual compaction as there is nothing to do (manual.level
		// points to an empty level).
		return nil, false
	}
	// TODO(peter): The conflictsWithInProgress call should no longer be
	// necessary, but TestManualCompaction currently expects it.
	if conflictsWithInProgress(manual.level, outputLevel, env.inProgressCompactions) {
		return nil, true
	}
	c = pickManualHelper(env, p.opts, manual, p.vers, p.baseLevel)
	if c == nil {
		return nil, false
	}
	if c.outputLevel != outputLevel {
		panic("pebble: compaction picked unexpected output level")
	}
	// Fail-safe to protect against compacting the same sstable concurrently.
	if inputAlreadyCompacting(c) {
		return nil, true
	}
	return c, false
}

func pickManualHelper(
	env compactionEnv, opts *Options, manual *manualCompaction, vers *version, baseLevel int,
) (c *compaction) {
	c = newCompaction(opts, vers, manual.level, baseLevel, env.bytesCompacted)
	manual.outputLevel = c.outputLevel
	cmp := opts.Comparer.Compare
	c.inputs[0] = vers.Overlaps(manual.level, cmp, manual.start.UserKey, manual.end.UserKey)
	if len(c.inputs[0]) == 0 {
		// Nothing to do
		return nil
	}
	c.setupInputs()
	return c
}

func (p *compactionPickerByScore) forceBaseLevel1() {
	p.baseLevel = 1
}

func inputAlreadyCompacting(c *compaction) bool {
	for _, inputs := range c.inputs {
		for _, f := range inputs {
			if f.Compacting {
				return true
			}
		}
	}
	return false
}

func conflictsWithInProgress(
	level int, outputLevel int, inProgressCompactions []compactionInfo,
) bool {
	for _, c := range inProgressCompactions {
		if level == c.startLevel ||
			level == c.outputLevel ||
			outputLevel == c.startLevel ||
			outputLevel == c.outputLevel {
			return true
		}
	}
	return false
}

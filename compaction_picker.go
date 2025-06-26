// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"cmp"
	"fmt"
	"iter"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/problemspans"
)

// The minimum count for an intra-L0 compaction. This matches the RocksDB
// heuristic.
const minIntraL0Count = 4

type compactionEnv struct {
	// diskAvailBytes holds a statistic on the number of bytes available on
	// disk, as reported by the filesystem. It's used to be more restrictive in
	// expanding compactions if available disk space is limited.
	//
	// The cached value (d.diskAvailBytes) is updated whenever a file is deleted
	// and whenever a compaction or flush completes. Since file removal is the
	// primary means of reclaiming space, there is a rough bound on the
	// statistic's staleness when available bytes is growing. Compactions and
	// flushes are longer, slower operations and provide a much looser bound
	// when available bytes is decreasing.
	diskAvailBytes          uint64
	earliestUnflushedSeqNum base.SeqNum
	earliestSnapshotSeqNum  base.SeqNum
	inProgressCompactions   []compactionInfo
	readCompactionEnv       readCompactionEnv
	// problemSpans is checked by the compaction picker to avoid compactions that
	// overlap an active "problem span". It can be nil when there are no problem
	// spans.
	problemSpans *problemspans.ByLevel
}

type compactionPickerMetrics struct {
	levels [numLevels]struct {
		score                 float64
		fillFactor            float64
		compensatedFillFactor float64
	}
}

type compactionPicker interface {
	getMetrics([]compactionInfo) compactionPickerMetrics
	getBaseLevel() int
	estimatedCompactionDebt() uint64
	pickAutoScore(env compactionEnv) (pc pickedCompaction)
	pickAutoNonScore(env compactionEnv) (pc pickedCompaction)
	forceBaseLevel1()
}

// A pickedCompaction describes a potential compaction that the compaction
// picker has selected, based on its heuristics. When a compaction begins to
// execute, it is converted into a compaction struct by ConstructCompaction.
type pickedCompaction interface {
	// ManualID returns the ID of the manual compaction, or 0 if the picked
	// compaction is not a result of a manual compaction.
	ManualID() uint64
	// ConstructCompaction creates a compaction from the picked compaction.
	ConstructCompaction(*DB, CompactionGrantHandle) compaction
	// WaitingCompaction returns a WaitingCompaction description of this
	// compaction for consumption by the compaction scheduler.
	WaitingCompaction() WaitingCompaction
}

// readCompactionEnv is used to hold data required to perform read compactions
type readCompactionEnv struct {
	rescheduleReadCompaction *bool
	readCompactions          *readCompactionQueue
	flushing                 bool
}

// Information about in-progress compactions provided to the compaction picker.
// These are used to constrain the new compactions that will be picked.
type compactionInfo struct {
	// versionEditApplied is true if this compaction's version edit has already
	// been committed. The compaction may still be in-progress deleting newly
	// obsolete files.
	versionEditApplied bool
	// kind indicates the kind of compaction.
	kind        compactionKind
	inputs      []compactionLevel
	outputLevel int
	// bounds may be nil if the compaction does not involve sstables
	// (specifically, a blob file rewrite).
	bounds *base.UserKeyBounds
}

func (info compactionInfo) String() string {
	var buf bytes.Buffer
	var largest int
	for i, in := range info.inputs {
		if i > 0 {
			fmt.Fprintf(&buf, " -> ")
		}
		fmt.Fprintf(&buf, "L%d", in.level)
		for f := range in.files.All() {
			fmt.Fprintf(&buf, " %s", f.TableNum)
		}
		if largest < in.level {
			largest = in.level
		}
	}
	if largest != info.outputLevel || len(info.inputs) == 1 {
		fmt.Fprintf(&buf, " -> L%d", info.outputLevel)
	}
	return buf.String()
}

// sublevelInfo is used to tag a LevelSlice for an L0 sublevel with the
// sublevel.
type sublevelInfo struct {
	manifest.LevelSlice
	sublevel manifest.Layer
}

func (cl sublevelInfo) Clone() sublevelInfo {
	return sublevelInfo{
		sublevel:   cl.sublevel,
		LevelSlice: cl.LevelSlice,
	}
}
func (cl sublevelInfo) String() string {
	return fmt.Sprintf(`Sublevel %s; Levels %s`, cl.sublevel, cl.LevelSlice)
}

// generateSublevelInfo will generate the level slices for each of the sublevels
// from the level slice for all of L0.
func generateSublevelInfo(cmp base.Compare, levelFiles manifest.LevelSlice) []sublevelInfo {
	sublevelMap := make(map[uint64][]*manifest.TableMetadata)
	for f := range levelFiles.All() {
		sublevelMap[uint64(f.SubLevel)] = append(sublevelMap[uint64(f.SubLevel)], f)
	}

	var sublevels []int
	for level := range sublevelMap {
		sublevels = append(sublevels, int(level))
	}
	sort.Ints(sublevels)

	var levelSlices []sublevelInfo
	for _, sublevel := range sublevels {
		metas := sublevelMap[uint64(sublevel)]
		levelSlices = append(
			levelSlices,
			sublevelInfo{
				manifest.NewLevelSliceKeySorted(cmp, metas),
				manifest.L0Sublevel(sublevel),
			},
		)
	}
	return levelSlices
}

// pickedCompactionMetrics holds metrics related to the compaction picking process
type pickedCompactionMetrics struct {
	// scores contains candidateLevelInfo.scores.
	scores                      []float64
	singleLevelOverlappingRatio float64
	multiLevelOverlappingRatio  float64
}

// pickedTableCompaction contains information about a compaction of sstables
// that has already been chosen, and is being constructed. Compaction
// construction info lives in this struct, and is copied over into the
// compaction struct in constructCompaction.
type pickedTableCompaction struct {
	// score of the chosen compaction (candidateLevelInfo.score).
	score float64
	// kind indicates the kind of compaction.
	kind compactionKind
	// manualID > 0 iff this is a manual compaction. It exists solely for
	// internal bookkeeping.
	manualID uint64
	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel *compactionLevel
	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when:
	//    - if startLevel is 0, the output level equals compactionPicker.baseLevel().
	//    - in multilevel compaction, the output level is the lowest level involved in
	//      the compaction
	outputLevel *compactionLevel
	// inputs contain levels involved in the compaction in ascending order
	inputs []compactionLevel
	// LBase at the time of compaction picking. Might be uninitialized for
	// intra-L0 compactions.
	baseLevel int
	// L0-specific compaction info. Set to a non-nil value for all compactions
	// where startLevel == 0 that were generated by L0Sublevels.
	lcf *manifest.L0CompactionFiles
	// maxOutputFileSize is the maximum size of an individual table created
	// during compaction.
	maxOutputFileSize uint64
	// maxOverlapBytes is the maximum number of bytes of overlap allowed for a
	// single output table with the tables in the grandparent level.
	maxOverlapBytes uint64
	// maxReadCompactionBytes is the maximum bytes a read compaction is allowed to
	// overlap in its output level with. If the overlap is greater than
	// maxReadCompaction bytes, then we don't proceed with the compaction.
	maxReadCompactionBytes uint64

	// The boundaries of the input data.
	bounds        base.UserKeyBounds
	version       *manifest.Version
	l0Organizer   *manifest.L0Organizer
	pickerMetrics pickedCompactionMetrics
}

// Assert that *pickedTableCompaction implements pickedCompaction.
var _ pickedCompaction = (*pickedTableCompaction)(nil)

// ManualID returns the ID of the manual compaction, or 0 if the picked
// compaction is not a result of a manual compaction.
func (pc *pickedTableCompaction) ManualID() uint64 { return pc.manualID }

// Kind returns the kind of compaction.
func (pc *pickedTableCompaction) Kind() compactionKind { return pc.kind }

// Score returns the score of the level at the time the compaction was picked.
func (pc *pickedTableCompaction) Score() float64 { return pc.score }

// ConstructCompaction creates a compaction struct from the
// pickedTableCompaction.
func (pc *pickedTableCompaction) ConstructCompaction(
	d *DB, grantHandle CompactionGrantHandle,
) compaction {
	return newCompaction(
		pc,
		d.opts,
		d.timeNow(),
		d.ObjProvider(),
		grantHandle,
		d.TableFormat(),
		d.determineCompactionValueSeparation)
}

// WaitingCompaction returns a WaitingCompaction description of this compaction
// for consumption by the compaction scheduler.
func (pc *pickedTableCompaction) WaitingCompaction() WaitingCompaction {
	if pc.manualID > 0 {
		return WaitingCompaction{Priority: manualCompactionPriority, Score: pc.score}
	}
	entry, ok := scheduledCompactionMap[pc.kind]
	if !ok {
		panic(errors.AssertionFailedf("unexpected compactionKind %s", pc.kind))
	}
	return WaitingCompaction{
		Optional: entry.optional,
		Priority: entry.priority,
		Score:    pc.score,
	}
}

func defaultOutputLevel(startLevel, baseLevel int) int {
	outputLevel := startLevel + 1
	if startLevel == 0 {
		outputLevel = baseLevel
	}
	if outputLevel >= numLevels-1 {
		outputLevel = numLevels - 1
	}
	return outputLevel
}

func newPickedTableCompaction(
	opts *Options,
	cur *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	startLevel, outputLevel, baseLevel int,
) *pickedTableCompaction {
	if outputLevel > 0 && baseLevel == 0 {
		panic("base level cannot be 0")
	}
	if startLevel > 0 && startLevel < baseLevel {
		panic(fmt.Sprintf("invalid compaction: start level %d should not be empty (base level %d)",
			startLevel, baseLevel))
	}

	targetFileSize := opts.TargetFileSize(outputLevel, baseLevel)
	pc := &pickedTableCompaction{
		version:                cur,
		l0Organizer:            l0Organizer,
		baseLevel:              baseLevel,
		inputs:                 []compactionLevel{{level: startLevel}, {level: outputLevel}},
		maxOutputFileSize:      uint64(targetFileSize),
		maxOverlapBytes:        maxGrandparentOverlapBytes(targetFileSize),
		maxReadCompactionBytes: maxReadCompactionBytes(targetFileSize),
	}
	pc.startLevel = &pc.inputs[0]
	pc.outputLevel = &pc.inputs[1]
	return pc
}

// adjustedOutputLevel is the output level used for the purpose of
// determining the target output file size, overlap bytes, and expanded
// bytes, taking into account the base level.
func adjustedOutputLevel(outputLevel int, baseLevel int) int {
	if outputLevel == 0 {
		return 0
	}
	if baseLevel == 0 {
		panic("base level cannot be 0")
	}
	// Output level is in the range [baseLevel, numLevels). For the purpose of
	// determining the target output file size, overlap bytes, and expanded
	// bytes, we want to adjust the range to [1, numLevels).
	return 1 + outputLevel - baseLevel
}

func newPickedCompactionFromL0(
	lcf *manifest.L0CompactionFiles,
	opts *Options,
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	baseLevel int,
	isBase bool,
) *pickedTableCompaction {
	outputLevel := baseLevel
	if !isBase {
		outputLevel = 0 // Intra L0
	}

	pc := newPickedTableCompaction(opts, vers, l0Organizer, 0, outputLevel, baseLevel)
	pc.lcf = lcf
	pc.outputLevel.level = outputLevel

	// Manually build the compaction as opposed to calling
	// pickAutoHelper. This is because L0Sublevels has already added
	// any overlapping L0 SSTables that need to be added, and
	// because compactions built by L0SSTables do not necessarily
	// pick contiguous sequences of files in pc.version.Levels[0].
	pc.startLevel.files = manifest.NewLevelSliceSeqSorted(lcf.Files)
	return pc
}

func (pc *pickedTableCompaction) String() string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(`Score=%f, `, pc.score))
	builder.WriteString(fmt.Sprintf(`Kind=%s, `, pc.kind))
	builder.WriteString(fmt.Sprintf(`AdjustedOutputLevel=%d, `, adjustedOutputLevel(pc.outputLevel.level, pc.baseLevel)))
	builder.WriteString(fmt.Sprintf(`maxOutputFileSize=%d, `, pc.maxOutputFileSize))
	builder.WriteString(fmt.Sprintf(`maxReadCompactionBytes=%d, `, pc.maxReadCompactionBytes))
	builder.WriteString(fmt.Sprintf(`bounds=%s, `, pc.bounds))
	builder.WriteString(fmt.Sprintf(`version=%s, `, pc.version))
	builder.WriteString(fmt.Sprintf(`inputs=%s, `, pc.inputs))
	builder.WriteString(fmt.Sprintf(`startlevel=%s, `, pc.startLevel))
	builder.WriteString(fmt.Sprintf(`outputLevel=%s, `, pc.outputLevel))
	builder.WriteString(fmt.Sprintf(`l0SublevelInfo=%s, `, pc.startLevel.l0SublevelInfo))
	builder.WriteString(fmt.Sprintf(`lcf=%s`, pc.lcf))
	return builder.String()
}

// Clone creates a deep copy of the pickedCompaction
func (pc *pickedTableCompaction) clone() *pickedTableCompaction {

	// Quickly copy over fields that do not require special deep copy care, and
	// set all fields that will require a deep copy to nil.
	newPC := &pickedTableCompaction{
		score:                  pc.score,
		kind:                   pc.kind,
		baseLevel:              pc.baseLevel,
		maxOutputFileSize:      pc.maxOutputFileSize,
		maxOverlapBytes:        pc.maxOverlapBytes,
		maxReadCompactionBytes: pc.maxReadCompactionBytes,
		bounds:                 pc.bounds.Clone(),

		// TODO(msbutler): properly clone picker metrics
		pickerMetrics: pc.pickerMetrics,

		// Both copies see the same manifest, therefore, it's ok for them to share
		// the same pc.version and pc.l0Organizer.
		version:     pc.version,
		l0Organizer: pc.l0Organizer,
	}

	newPC.inputs = make([]compactionLevel, len(pc.inputs))
	for i := range pc.inputs {
		newPC.inputs[i] = pc.inputs[i].Clone()
		if i == 0 {
			newPC.startLevel = &newPC.inputs[i]
		} else if i == len(pc.inputs)-1 {
			newPC.outputLevel = &newPC.inputs[i]
		}
	}

	if len(pc.startLevel.l0SublevelInfo) > 0 {
		newPC.startLevel.l0SublevelInfo = make([]sublevelInfo, len(pc.startLevel.l0SublevelInfo))
		for i := range pc.startLevel.l0SublevelInfo {
			newPC.startLevel.l0SublevelInfo[i] = pc.startLevel.l0SublevelInfo[i].Clone()
		}
	}
	if pc.lcf != nil {
		newPC.lcf = pc.lcf.Clone()
	}
	return newPC
}

// setupInputs returns true if a compaction has been set up using the provided inputLevel and
// pc.outputLevel. It returns false if a concurrent compaction is occurring on the start or
// output level files. Note that inputLevel is not necessarily pc.startLevel. In multiLevel
// compactions, inputs are set by calling setupInputs once for each adjacent pair of levels.
// This will preserve level invariants when expanding the compaction. pc.smallest and pc.largest
// will be updated to reflect the key range of the inputs.
func (pc *pickedTableCompaction) setupInputs(
	opts *Options,
	diskAvailBytes uint64,
	inputLevel *compactionLevel,
	problemSpans *problemspans.ByLevel,
) bool {
	cmp := opts.Comparer.Compare
	if !canCompactTables(inputLevel.files, inputLevel.level, problemSpans) {
		return false
	}
	pc.bounds = manifest.ExtendKeyRange(cmp, pc.bounds, inputLevel.files.All())

	// Setup output files and attempt to grow the inputLevel files with
	// the expanded key range. No need to do this for intra-L0 compactions;
	// outputLevel.files is left empty for those.
	if inputLevel.level != pc.outputLevel.level {
		// Determine the sstables in the output level which overlap with the compaction
		// key range.
		pc.outputLevel.files = pc.version.Overlaps(pc.outputLevel.level, pc.bounds)
		if !canCompactTables(pc.outputLevel.files, pc.outputLevel.level, problemSpans) {
			return false
		}
		pc.bounds = manifest.ExtendKeyRange(cmp, pc.bounds, pc.outputLevel.files.All())

		// maxExpandedBytes is the maximum size of an expanded compaction. If
		// growing a compaction results in a larger size, the original compaction
		// is used instead.
		targetFileSize := opts.TargetFileSize(pc.outputLevel.level, pc.baseLevel)
		maxExpandedBytes := expandedCompactionByteSizeLimit(opts, targetFileSize, diskAvailBytes)

		// Grow the sstables in inputLevel.level as long as it doesn't affect the number
		// of sstables included from pc.outputLevel.level.
		if pc.lcf != nil && inputLevel.level == 0 {
			pc.growL0ForBase(cmp, maxExpandedBytes)
		} else if pc.grow(cmp, pc.bounds, maxExpandedBytes, inputLevel, problemSpans) {
			// inputLevel was expanded, adjust key range if necessary.
			pc.bounds = manifest.ExtendKeyRange(cmp, pc.bounds, inputLevel.files.All())
		}
	}

	if inputLevel.level == 0 {
		// If L0 is involved, it should always be the startLevel of the compaction.
		pc.startLevel.l0SublevelInfo = generateSublevelInfo(cmp, pc.startLevel.files)
	}
	return true
}

// grow grows the number of inputs at startLevel without changing the number of
// pc.outputLevel files in the compaction, and returns whether the inputs grew. sm
// and la are the smallest and largest InternalKeys in all of the inputs.
func (pc *pickedTableCompaction) grow(
	cmp base.Compare,
	bounds base.UserKeyBounds,
	maxExpandedBytes uint64,
	inputLevel *compactionLevel,
	problemSpans *problemspans.ByLevel,
) bool {
	if pc.outputLevel.files.Empty() {
		return false
	}
	expandedInputLevel := pc.version.Overlaps(inputLevel.level, bounds)
	if !canCompactTables(expandedInputLevel, inputLevel.level, problemSpans) {
		return false
	}
	if expandedInputLevel.Len() <= inputLevel.files.Len() {
		return false
	}
	if expandedInputLevel.AggregateSizeSum()+pc.outputLevel.files.AggregateSizeSum() >= maxExpandedBytes {
		return false
	}
	// Check that expanding the input level does not change the number of overlapping files in output level.
	// We need to include the outputLevel iter because without it, in a multiLevel scenario,
	// expandedInputLevel's key range not fully cover all files currently in pc.outputLevel,
	// since pc.outputLevel was created using the entire key range which includes higher levels.
	expandedOutputLevel := pc.version.Overlaps(pc.outputLevel.level,
		manifest.KeyRange(cmp, expandedInputLevel.All(), pc.outputLevel.files.All()))
	if expandedOutputLevel.Len() != pc.outputLevel.files.Len() {
		return false
	}
	if !canCompactTables(expandedOutputLevel, pc.outputLevel.level, problemSpans) {
		return false
	}
	inputLevel.files = expandedInputLevel
	return true
}

// Similar logic as pc.grow. Additional L0 files are optionally added to the
// compaction at this step. Note that the bounds passed in are not the bounds
// of the compaction, but rather the smallest and largest internal keys that
// the compaction cannot include from L0 without pulling in more Lbase
// files. Consider this example:
//
// L0:        c-d e+f g-h
// Lbase: a-b     e+f     i-j
//
//	a b c d e f g h i j
//
// The e-f files have already been chosen in the compaction. As pulling
// in more LBase files is undesirable, the logic below will pass in
// smallest = b and largest = i to ExtendL0ForBaseCompactionTo, which
// will expand the compaction to include c-d and g-h from L0. The
// bounds passed in are exclusive; the compaction cannot be expanded
// to include files that "touch" it.
func (pc *pickedTableCompaction) growL0ForBase(cmp base.Compare, maxExpandedBytes uint64) bool {
	if invariants.Enabled {
		if pc.startLevel.level != 0 {
			panic(fmt.Sprintf("pc.startLevel.level is %d, expected 0", pc.startLevel.level))
		}
	}

	if pc.outputLevel.files.Empty() {
		// If there are no overlapping fields in the output level, we do not
		// attempt to expand the compaction to encourage move compactions.
		return false
	}

	smallestBaseKey := base.InvalidInternalKey
	largestBaseKey := base.InvalidInternalKey
	// NB: We use Reslice to access the underlying level's files, but
	// we discard the returned slice. The pc.outputLevel.files slice
	// is not modified.
	_ = pc.outputLevel.files.Reslice(func(start, end *manifest.LevelIterator) {
		if sm := start.Prev(); sm != nil {
			smallestBaseKey = sm.Largest()
		}
		if la := end.Next(); la != nil {
			largestBaseKey = la.Smallest()
		}
	})
	oldLcf := pc.lcf.Clone()
	if !pc.l0Organizer.ExtendL0ForBaseCompactionTo(smallestBaseKey, largestBaseKey, pc.lcf) {
		return false
	}

	var newStartLevelFiles []*manifest.TableMetadata
	iter := pc.version.Levels[0].Iter()
	var sizeSum uint64
	for j, f := 0, iter.First(); f != nil; j, f = j+1, iter.Next() {
		if pc.lcf.FilesIncluded[f.L0Index] {
			newStartLevelFiles = append(newStartLevelFiles, f)
			sizeSum += f.Size
		}
	}

	if sizeSum+pc.outputLevel.files.AggregateSizeSum() >= maxExpandedBytes {
		*pc.lcf = *oldLcf
		return false
	}

	pc.startLevel.files = manifest.NewLevelSliceSeqSorted(newStartLevelFiles)
	pc.bounds = manifest.ExtendKeyRange(cmp, pc.bounds,
		pc.startLevel.files.All(), pc.outputLevel.files.All())
	return true
}

// estimatedInputSize returns an estimate of the size of the compaction's
// inputs, including the estimated physical size of input tables' blob
// references.
func (pc *pickedTableCompaction) estimatedInputSize() uint64 {
	var bytesToCompact uint64
	for i := range pc.inputs {
		bytesToCompact += pc.inputs[i].files.AggregateSizeSum()
	}
	return bytesToCompact
}

// setupMultiLevelCandidate returns true if it successfully added another level
// to the compaction.
func (pc *pickedTableCompaction) setupMultiLevelCandidate(
	opts *Options, diskAvailBytes uint64,
) bool {
	pc.inputs = append(pc.inputs, compactionLevel{level: pc.outputLevel.level + 1})

	// Recalibrate startLevel and outputLevel:
	//  - startLevel and outputLevel pointers may be obsolete after appending to pc.inputs.
	//  - push outputLevel to extraLevels and move the new level to outputLevel
	pc.startLevel = &pc.inputs[0]
	pc.outputLevel = &pc.inputs[2]
	return pc.setupInputs(opts, diskAvailBytes, &pc.inputs[1], nil /* TODO(radu) */)
}

// canCompactTables returns true if the tables in the level slice are not
// compacting already and don't intersect any problem spans.
func canCompactTables(
	inputs manifest.LevelSlice, level int, problemSpans *problemspans.ByLevel,
) bool {
	for f := range inputs.All() {
		if f.IsCompacting() {
			return false
		}
		if problemSpans != nil && problemSpans.Overlaps(level, f.UserKeyBounds()) {
			return false
		}
	}
	return true
}

// newCompactionPickerByScore creates a compactionPickerByScore associated with
// the newest version. The picker is used under logLock (until a new version is
// installed).
func newCompactionPickerByScore(
	v *manifest.Version,
	lvs *latestVersionState,
	opts *Options,
	inProgressCompactions []compactionInfo,
) *compactionPickerByScore {
	p := &compactionPickerByScore{
		opts:               opts,
		vers:               v,
		latestVersionState: lvs,
	}
	p.initLevelMaxBytes(inProgressCompactions)
	return p
}

// Information about a candidate compaction level that has been identified by
// the compaction picker.
type candidateLevelInfo struct {
	// The fill factor of the level, calculated using uncompensated file sizes and
	// without any adjustments. A factor > 1 means that the level has more data
	// than the ideal size for that level.
	//
	// For L0, the fill factor is calculated based on the number of sublevels
	// (see calculateL0FillFactor).
	//
	// For L1+, the fill factor is the ratio between the total uncompensated file
	// size and the ideal size of the level (based on the total size of the DB).
	fillFactor float64

	// The score of the level, used to rank levels.
	//
	// If the level doesn't require compaction, the score is 0. Otherwise:
	//  - for L6 the score is equal to the fillFactor;
	//  - for L0-L5:
	//    - if the fillFactor is < 1: the score is equal to the fillFactor;
	//    - if the fillFactor is >= 1: the score is the ratio between the
	//                                 fillFactor and the next level's fillFactor.
	score float64

	// The fill factor of the level after accounting for level size compensation.
	//
	// For L0, the compensatedFillFactor is equal to the fillFactor as we don't
	// account for level size compensation in L0.
	//
	// For l1+, the compensatedFillFactor takes into account the estimated
	// savings in the lower levels because of deletions.
	//
	// The compensated fill factor is used to determine if the level should be
	// compacted (see calculateLevelScores).
	compensatedFillFactor float64

	level int
	// The level to compact to.
	outputLevel int
	// The file in level that will be compacted. Additional files may be
	// picked by the compaction, and a pickedCompaction created for the
	// compaction.
	file manifest.LevelFile
}

func (c *candidateLevelInfo) shouldCompact() bool {
	return c.score > 0
}

func tableTombstoneCompensation(t *manifest.TableMetadata) uint64 {
	return t.Stats.PointDeletionsBytesEstimate + t.Stats.RangeDeletionsBytesEstimate
}

// tableCompensatedSize returns t's size, including an estimate of the physical
// size of its external references, and inflated according to compaction
// priorities.
func tableCompensatedSize(t *manifest.TableMetadata) uint64 {
	// Add in the estimate of disk space that may be reclaimed by compacting the
	// table's tombstones.
	return t.Size + t.EstimatedReferenceSize() + tableTombstoneCompensation(t)
}

// totalCompensatedSize computes the compensated size over a table metadata
// iterator. Note that this function is linear in the files available to the
// iterator. Use the compensatedSizeAnnotator if querying the total
// compensated size of a level.
func totalCompensatedSize(iter iter.Seq[*manifest.TableMetadata]) uint64 {
	var sz uint64
	for f := range iter {
		sz += tableCompensatedSize(f)
	}
	return sz
}

// compactionPickerByScore holds the state and logic for picking a compaction. A
// compaction picker is associated with a single version. A new compaction
// picker is created and initialized every time a new version is installed.
type compactionPickerByScore struct {
	opts *Options
	vers *manifest.Version
	// Unlike vers, which is immutable and the latest version when this picker
	// is created, latestVersionState represents the mutable state of the latest
	// version. This means that at some point in the future a
	// compactionPickerByScore created in the past will have mutually
	// inconsistent state in vers and latestVersionState. This is not a problem
	// since (a) a new picker is created in UpdateVersionLocked when a new
	// version is installed, and (b) only the latest picker is used for picking
	// compactions. This is ensured by holding versionSet.logLock for both (a)
	// and (b).
	latestVersionState *latestVersionState
	// The level to target for L0 compactions. Levels L1 to baseLevel must be
	// empty.
	baseLevel int
	// levelMaxBytes holds the dynamically adjusted max bytes setting for each
	// level.
	levelMaxBytes [numLevels]int64
	dbSizeBytes   uint64
}

var _ compactionPicker = &compactionPickerByScore{}

func (p *compactionPickerByScore) getMetrics(inProgress []compactionInfo) compactionPickerMetrics {
	var m compactionPickerMetrics
	for _, info := range p.calculateLevelScores(inProgress) {
		m.levels[info.level].score = info.score
		m.levels[info.level].fillFactor = info.fillFactor
		m.levels[info.level].compensatedFillFactor = info.compensatedFillFactor
	}
	return m
}

func (p *compactionPickerByScore) getBaseLevel() int {
	if p == nil {
		return 1
	}
	return p.baseLevel
}

// estimatedCompactionDebt estimates the number of bytes which need to be
// compacted before the LSM tree becomes stable.
func (p *compactionPickerByScore) estimatedCompactionDebt() uint64 {
	if p == nil {
		return 0
	}

	// We assume that all the bytes in L0 need to be compacted to Lbase. This is
	// unlike the RocksDB logic that figures out whether L0 needs compaction.
	bytesAddedToNextLevel := p.vers.Levels[0].AggregateSize()
	lbaseSize := p.vers.Levels[p.baseLevel].AggregateSize()

	var compactionDebt uint64
	if bytesAddedToNextLevel > 0 && lbaseSize > 0 {
		// We only incur compaction debt if both L0 and Lbase contain data. If L0
		// is empty, no compaction is necessary. If Lbase is empty, a move-based
		// compaction from L0 would occur.
		compactionDebt += bytesAddedToNextLevel + lbaseSize
	}

	// loop invariant: At the beginning of the loop, bytesAddedToNextLevel is the
	// bytes added to `level` in the loop.
	for level := p.baseLevel; level < numLevels-1; level++ {
		levelSize := p.vers.Levels[level].AggregateSize() + bytesAddedToNextLevel
		nextLevelSize := p.vers.Levels[level+1].AggregateSize()
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
		} else {
			// We're not moving any bytes to the next level.
			bytesAddedToNextLevel = 0
		}
	}
	return compactionDebt
}

func (p *compactionPickerByScore) initLevelMaxBytes(inProgressCompactions []compactionInfo) {
	// The levelMaxBytes calculations here differ from RocksDB in two ways:
	//
	// 1. The use of dbSize vs maxLevelSize. RocksDB uses the size of the maximum
	//    level in L1-L6, rather than determining the size of the bottom level
	//    based on the total amount of data in the dB. The RocksDB calculation is
	//    problematic if L0 contains a significant fraction of data, or if the
	//    level sizes are roughly equal and thus there is a significant fraction
	//    of data outside of the largest level.
	//
	// 2. Not adjusting the size of Lbase based on L0. RocksDB computes
	//    baseBytesMax as the maximum of the configured LBaseMaxBytes and the
	//    size of L0. This is problematic because baseBytesMax is used to compute
	//    the max size of lower levels. A very large baseBytesMax will result in
	//    an overly large value for the size of lower levels which will caused
	//    those levels not to be compacted even when they should be
	//    compacted. This often results in "inverted" LSM shapes where Ln is
	//    larger than Ln+1.

	// Determine the first non-empty level and the total DB size.
	firstNonEmptyLevel := -1
	var dbSize uint64
	for level := 1; level < numLevels; level++ {
		if p.vers.Levels[level].AggregateSize() > 0 {
			if firstNonEmptyLevel == -1 {
				firstNonEmptyLevel = level
			}
			dbSize += p.vers.Levels[level].AggregateSize()
		}
	}
	for _, c := range inProgressCompactions {
		if c.outputLevel == 0 || c.outputLevel == -1 {
			continue
		}
		if c.inputs[0].level == 0 && (firstNonEmptyLevel == -1 || c.outputLevel < firstNonEmptyLevel) {
			firstNonEmptyLevel = c.outputLevel
		}
	}

	// Initialize the max-bytes setting for each level to "infinity" which will
	// disallow compaction for that level. We'll fill in the actual value below
	// for levels we want to allow compactions from.
	for level := 0; level < numLevels; level++ {
		p.levelMaxBytes[level] = math.MaxInt64
	}

	dbSizeBelowL0 := dbSize
	dbSize += p.vers.Levels[0].AggregateSize()
	p.dbSizeBytes = dbSize
	if dbSizeBelowL0 == 0 {
		// No levels for L1 and up contain any data. Target L0 compactions for the
		// last level or to the level to which there is an ongoing L0 compaction.
		p.baseLevel = numLevels - 1
		if firstNonEmptyLevel >= 0 {
			p.baseLevel = firstNonEmptyLevel
		}
		return
	}

	bottomLevelSize := dbSize - dbSize/uint64(p.opts.Experimental.LevelMultiplier)

	curLevelSize := bottomLevelSize
	for level := numLevels - 2; level >= firstNonEmptyLevel; level-- {
		curLevelSize = uint64(float64(curLevelSize) / float64(p.opts.Experimental.LevelMultiplier))
	}

	// Compute base level (where L0 data is compacted to).
	baseBytesMax := uint64(p.opts.LBaseMaxBytes)
	p.baseLevel = firstNonEmptyLevel
	for p.baseLevel > 1 && curLevelSize > baseBytesMax {
		p.baseLevel--
		curLevelSize = uint64(float64(curLevelSize) / float64(p.opts.Experimental.LevelMultiplier))
	}

	smoothedLevelMultiplier := 1.0
	if p.baseLevel < numLevels-1 {
		smoothedLevelMultiplier = math.Pow(
			float64(bottomLevelSize)/float64(baseBytesMax),
			1.0/float64(numLevels-p.baseLevel-1))
	}

	levelSize := float64(baseBytesMax)
	for level := p.baseLevel; level < numLevels; level++ {
		if level > p.baseLevel && levelSize > 0 {
			levelSize *= smoothedLevelMultiplier
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

type levelSizeAdjust struct {
	incomingActualBytes      uint64
	outgoingActualBytes      uint64
	outgoingCompensatedBytes uint64
}

func (a levelSizeAdjust) compensated() uint64 {
	return a.incomingActualBytes - a.outgoingCompensatedBytes
}

func (a levelSizeAdjust) actual() uint64 {
	return a.incomingActualBytes - a.outgoingActualBytes
}

func calculateSizeAdjust(inProgressCompactions []compactionInfo) [numLevels]levelSizeAdjust {
	// Compute size adjustments for each level based on the in-progress
	// compactions. We sum the file sizes of all files leaving and entering each
	// level in in-progress compactions. For outgoing files, we also sum a
	// separate sum of 'compensated file sizes', which are inflated according
	// to deletion estimates.
	//
	// When we adjust a level's size according to these values during score
	// calculation, we subtract the compensated size of start level inputs to
	// account for the fact that score calculation uses compensated sizes.
	//
	// Since compensated file sizes may be compensated because they reclaim
	// space from the output level's files, we only add the real file size to
	// the output level.
	//
	// This is slightly different from RocksDB's behavior, which simply elides
	// compacting files from the level size calculation.
	var sizeAdjust [numLevels]levelSizeAdjust
	for i := range inProgressCompactions {
		c := &inProgressCompactions[i]
		// If this compaction's version edit has already been applied, there's
		// no need to adjust: The LSM we'll examine will already reflect the
		// new LSM state.
		if c.versionEditApplied {
			continue
		}

		for _, input := range c.inputs {
			actualSize := input.files.AggregateSizeSum()
			compensatedSize := totalCompensatedSize(input.files.All())

			if input.level != c.outputLevel {
				sizeAdjust[input.level].outgoingCompensatedBytes += compensatedSize
				sizeAdjust[input.level].outgoingActualBytes += actualSize
				if c.outputLevel != -1 {
					sizeAdjust[c.outputLevel].incomingActualBytes += actualSize
				}
			}
		}
	}
	return sizeAdjust
}

// calculateLevelScores calculates the candidateLevelInfo for all levels and
// returns them in decreasing score order.
func (p *compactionPickerByScore) calculateLevelScores(
	inProgressCompactions []compactionInfo,
) [numLevels]candidateLevelInfo {
	var scores [numLevels]candidateLevelInfo
	for i := range scores {
		scores[i].level = i
		scores[i].outputLevel = i + 1
	}
	l0FillFactor := calculateL0FillFactor(p.vers, p.latestVersionState.l0Organizer, p.opts, inProgressCompactions)
	scores[0] = candidateLevelInfo{
		outputLevel:           p.baseLevel,
		fillFactor:            l0FillFactor,
		compensatedFillFactor: l0FillFactor, // No compensation for L0.
	}
	sizeAdjust := calculateSizeAdjust(inProgressCompactions)
	for level := 1; level < numLevels; level++ {
		compensatedLevelSize :=
			// Actual file size.
			p.vers.Levels[level].AggregateSize() +
				// Point deletions.
				*pointDeletionsBytesEstimateAnnotator.LevelAnnotation(p.vers.Levels[level]) +
				// Range deletions.
				*rangeDeletionsBytesEstimateAnnotator.LevelAnnotation(p.vers.Levels[level]) +
				// Adjustments for in-progress compactions.
				sizeAdjust[level].compensated()
		scores[level].compensatedFillFactor = float64(compensatedLevelSize) / float64(p.levelMaxBytes[level])
		scores[level].fillFactor = float64(p.vers.Levels[level].AggregateSize()+sizeAdjust[level].actual()) / float64(p.levelMaxBytes[level])
	}

	// Adjust each level's fill factor by the fill factor of the next level to get
	// an (uncompensated) score; and each level's compensated fill factor by the
	// fill factor of the next level to get a compensated score.
	//
	// The compensated score is used to determine if the level should be compacted
	// at all. The (uncompensated) score is used as the value used to rank levels.
	//
	// If the next level has a high fill factor, and is thus a priority for
	// compaction, this reduces the priority for compacting the current level. If
	// the next level has a low fill factor (i.e. it is below its target size),
	// this increases the priority for compacting the current level.
	//
	// The effect of this adjustment is to help prioritize compactions in lower
	// levels. The following example shows the scores and the fill factors. In this
	// scenario, L0 has 68 sublevels. L3 (a.k.a. Lbase) is significantly above its
	// target size. The original score prioritizes compactions from those two
	// levels, but doing so ends up causing a future problem: data piles up in the
	// higher levels, starving L5->L6 compactions, and to a lesser degree starving
	// L4->L5 compactions.
	//
	// Note that in the example shown there is no level size compensation so the
	// compensatedFillFactor and fillFactor are the same for each level.
	//
	//        score   fillFactor   compensatedFillFactor   size   max-size
	//   L0     3.2         68.0                    68.0  2.2 G          -
	//   L3     3.2         21.1                    21.1  1.3 G       64 M
	//   L4     3.4          6.7                     6.7  3.1 G      467 M
	//   L5     3.4          2.0                     2.0  6.6 G      3.3 G
	//   L6       0          0.6                     0.6   14 G       24 G
	//
	// TODO(radu): the way compensation works needs some rethinking. For example,
	// if compacting L5 can free up a lot of space in L6, the score of L5 should
	// go *up* with the fill factor of L6, not the other way around.
	for level := 0; level < numLevels; level++ {
		if level > 0 && level < p.baseLevel {
			continue
		}
		const compensatedFillFactorThreshold = 1.0
		if scores[level].compensatedFillFactor < compensatedFillFactorThreshold {
			// No need to compact this level; score stays 0.
			continue
		}
		score := scores[level].fillFactor
		compensatedScore := scores[level].compensatedFillFactor
		if level < numLevels-1 {
			nextLevel := scores[level].outputLevel
			// Avoid absurdly large scores by placing a floor on the factor that we'll
			// adjust a level by. The value of 0.01 was chosen somewhat arbitrarily.
			denominator := max(0.01, scores[nextLevel].fillFactor)
			score /= denominator
			compensatedScore /= denominator
		}
		// The level requires compaction iff both compensatedFillFactor and
		// compensatedScore are >= 1.0.
		//
		// TODO(radu): this seems ad-hoc. In principle, the state of other levels
		// should not come into play when we're determining this level's eligibility
		// for compaction. The score should take care of correctly prioritizing the
		// levels.
		const compensatedScoreThreshold = 1.0
		if compensatedScore < compensatedScoreThreshold {
			// No need to compact this level; score stays 0.
			continue
		}
		scores[level].score = score
	}
	// Sort by score (decreasing) and break ties by level (increasing).
	slices.SortFunc(scores[:], func(a, b candidateLevelInfo) int {
		if a.score != b.score {
			return cmp.Compare(b.score, a.score)
		}
		return cmp.Compare(a.level, b.level)
	})
	return scores
}

// calculateL0FillFactor calculates a float value representing the relative
// priority of compacting L0. A value less than 1 indicates that L0 does not
// need any compactions.
//
// L0 is special in that files within L0 may overlap one another, so a different
// set of heuristics that take into account read amplification apply.
func calculateL0FillFactor(
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	opts *Options,
	inProgressCompactions []compactionInfo,
) float64 {
	// Use the sublevel count to calculate the score. The base vs intra-L0
	// compaction determination happens in pickAuto, not here.
	score := float64(2*l0Organizer.MaxDepthAfterOngoingCompactions()) /
		float64(opts.L0CompactionThreshold)

	// Also calculate a score based on the file count but use it only if it
	// produces a higher score than the sublevel-based one. This heuristic is
	// designed to accommodate cases where L0 is accumulating non-overlapping
	// files in L0. Letting too many non-overlapping files accumulate in few
	// sublevels is undesirable, because:
	// 1) we can produce a massive backlog to compact once files do overlap.
	// 2) constructing L0 sublevels has a runtime that grows superlinearly with
	//    the number of files in L0 and must be done while holding D.mu.
	noncompactingFiles := vers.Levels[0].Len()
	for _, c := range inProgressCompactions {
		for _, cl := range c.inputs {
			if cl.level == 0 {
				noncompactingFiles -= cl.files.Len()
			}
		}
	}
	fileScore := float64(noncompactingFiles) / float64(opts.L0CompactionFileThreshold)
	if score < fileScore {
		score = fileScore
	}
	return score
}

// pickCompactionSeedFile picks a file from `level` in the `vers` to build a
// compaction around. Currently, this function implements a heuristic similar to
// RocksDB's kMinOverlappingRatio, seeking to minimize write amplification. This
// function is linear with respect to the number of files in `level` and
// `outputLevel`.
func pickCompactionSeedFile(
	vers *manifest.Version,
	virtualBackings *manifest.VirtualBackings,
	opts *Options,
	level, outputLevel int,
	earliestSnapshotSeqNum base.SeqNum,
	problemSpans *problemspans.ByLevel,
) (manifest.LevelFile, bool) {
	// Select the file within the level to compact. We want to minimize write
	// amplification, but also ensure that (a) deletes are propagated to the
	// bottom level in a timely fashion, and (b) virtual sstables that are
	// pinning backing sstables where most of the data is garbage are compacted
	// away. Doing (a) and (b) reclaims disk space. A table's smallest sequence
	// number provides a measure of its age. The ratio of overlapping-bytes /
	// table-size gives an indication of write amplification (a smaller ratio is
	// preferrable).
	//
	// The current heuristic is based off the RocksDB kMinOverlappingRatio
	// heuristic. It chooses the file with the minimum overlapping ratio with
	// the target level, which minimizes write amplification.
	//
	// The heuristic uses a "compensated size" for the denominator, which is the
	// file size inflated by (a) an estimate of the space that may be reclaimed
	// through compaction, and (b) a fraction of the amount of garbage in the
	// backing sstable pinned by this (virtual) sstable.
	//
	// TODO(peter): For concurrent compactions, we may want to try harder to
	// pick a seed file whose resulting compaction bounds do not overlap with
	// an in-progress compaction.

	cmp := opts.Comparer.Compare
	startIter := vers.Levels[level].Iter()
	outputIter := vers.Levels[outputLevel].Iter()

	var file manifest.LevelFile
	smallestRatio := uint64(math.MaxUint64)

	outputFile := outputIter.First()

	for f := startIter.First(); f != nil; f = startIter.Next() {
		var overlappingBytes uint64
		if f.IsCompacting() {
			// Move on if this file is already being compacted. We'll likely
			// still need to move past the overlapping output files regardless,
			// but in cases where all start-level files are compacting we won't.
			continue
		}
		if problemSpans != nil && problemSpans.Overlaps(level, f.UserKeyBounds()) {
			// File touches problem span which temporarily disallows auto compactions.
			continue
		}

		// Trim any output-level files smaller than f.
		for outputFile != nil && sstableKeyCompare(cmp, outputFile.Largest(), f.Smallest()) < 0 {
			outputFile = outputIter.Next()
		}

		skip := false
		for outputFile != nil && sstableKeyCompare(cmp, outputFile.Smallest(), f.Largest()) <= 0 {
			overlappingBytes += outputFile.Size
			if outputFile.IsCompacting() {
				// If one of the overlapping files is compacting, we're not going to be
				// able to compact f anyway, so skip it.
				skip = true
				break
			}
			if problemSpans != nil && problemSpans.Overlaps(outputLevel, outputFile.UserKeyBounds()) {
				// Overlapping file touches problem span which temporarily disallows auto compactions.
				skip = true
				break
			}

			// For files in the bottommost level of the LSM, the
			// Stats.RangeDeletionsBytesEstimate field is set to the estimate
			// of bytes /within/ the file itself that may be dropped by
			// recompacting the file. These bytes from obsolete keys would not
			// need to be rewritten if we compacted `f` into `outputFile`, so
			// they don't contribute to write amplification. Subtracting them
			// out of the overlapping bytes helps prioritize these compactions
			// that are cheaper than their file sizes suggest.
			if outputLevel == numLevels-1 && outputFile.LargestSeqNum < earliestSnapshotSeqNum {
				overlappingBytes -= outputFile.Stats.RangeDeletionsBytesEstimate
			}

			// If the file in the next level extends beyond f's largest key,
			// break out and don't advance outputIter because f's successor
			// might also overlap.
			//
			// Note, we stop as soon as we encounter an output-level file with a
			// largest key beyond the input-level file's largest bound. We
			// perform a simple user key comparison here using sstableKeyCompare
			// which handles the potential for exclusive largest key bounds.
			// There's some subtlety when the bounds are equal (eg, equal and
			// inclusive, or equal and exclusive). Current Pebble doesn't split
			// user keys across sstables within a level (and in format versions
			// FormatSplitUserKeysMarkedCompacted and later we guarantee no
			// split user keys exist within the entire LSM). In that case, we're
			// assured that neither the input level nor the output level's next
			// file shares the same user key, so compaction expansion will not
			// include them in any compaction compacting `f`.
			//
			// NB: If we /did/ allow split user keys, or we're running on an
			// old database with an earlier format major version where there are
			// existing split user keys, this logic would be incorrect. Consider
			//    L1: [a#120,a#100] [a#80,a#60]
			//    L2: [a#55,a#45] [a#35,a#25] [a#15,a#5]
			// While considering the first file in L1, [a#120,a#100], we'd skip
			// past all of the files in L2. When considering the second file in
			// L1, we'd improperly conclude that the second file overlaps
			// nothing in the second level and is cheap to compact, when in
			// reality we'd need to expand the compaction to include all 5
			// files.
			if sstableKeyCompare(cmp, outputFile.Largest(), f.Largest()) > 0 {
				break
			}
			outputFile = outputIter.Next()
		}
		if skip {
			continue
		}

		compSz := tableCompensatedSize(f) + responsibleForGarbageBytes(virtualBackings, f)
		scaledRatio := overlappingBytes * 1024 / compSz
		if scaledRatio < smallestRatio {
			smallestRatio = scaledRatio
			file = startIter.Take()
		}
	}
	return file, file.TableMetadata != nil
}

// responsibleForGarbageBytes returns the amount of garbage in the backing
// sstable that we consider the responsibility of this virtual sstable. For
// non-virtual sstables, this is of course 0. For virtual sstables, we equally
// distribute the responsibility of the garbage across all the virtual
// sstables that are referencing the same backing sstable. One could
// alternatively distribute this in proportion to the virtual sst sizes, but
// it isn't clear that more sophisticated heuristics are worth it, given that
// the garbage cannot be reclaimed until all the referencing virtual sstables
// are compacted.
func responsibleForGarbageBytes(
	virtualBackings *manifest.VirtualBackings, m *manifest.TableMetadata,
) uint64 {
	if !m.Virtual {
		return 0
	}
	useCount, virtualizedSize := virtualBackings.Usage(m.TableBacking.DiskFileNum)
	// Since virtualizedSize is the sum of the estimated size of all virtual
	// ssts, we allow for the possibility that virtualizedSize could exceed
	// m.TableBacking.Size.
	totalGarbage := int64(m.TableBacking.Size) - int64(virtualizedSize)
	if totalGarbage <= 0 {
		return 0
	}
	if useCount == 0 {
		// This cannot happen if m exists in the latest version. The call to
		// ResponsibleForGarbageBytes during compaction picking ensures that m
		// exists in the latest version by holding versionSet.logLock.
		panic(errors.AssertionFailedf("%s has zero useCount", m.String()))
	}
	return uint64(totalGarbage) / uint64(useCount)
}

func (p *compactionPickerByScore) getCompactionConcurrency() int {
	lower, upper := p.opts.CompactionConcurrencyRange()
	if lower >= upper {
		return upper
	}
	// Compaction concurrency is controlled by L0 read-amp. We allow one
	// additional compaction per L0CompactionConcurrency sublevels, as well as
	// one additional compaction per CompactionDebtConcurrency bytes of
	// compaction debt. Compaction concurrency is tied to L0 sublevels as that
	// signal is independent of the database size. We tack on the compaction
	// debt as a second signal to prevent compaction concurrency from dropping
	// significantly right after a base compaction finishes, and before those
	// bytes have been compacted further down the LSM.
	//
	// Let n be the number of in-progress compactions.
	//
	// l0ReadAmp >= ccSignal1 then can run another compaction, where
	// ccSignal1 = n * p.opts.Experimental.L0CompactionConcurrency
	// Rearranging,
	// n <= l0ReadAmp / p.opts.Experimental.L0CompactionConcurrency.
	// So we can run up to
	// l0ReadAmp / p.opts.Experimental.L0CompactionConcurrency extra compactions.
	l0ReadAmpCompactions := 0
	if p.opts.Experimental.L0CompactionConcurrency > 0 {
		l0ReadAmp := p.latestVersionState.l0Organizer.MaxDepthAfterOngoingCompactions()
		l0ReadAmpCompactions = (l0ReadAmp / p.opts.Experimental.L0CompactionConcurrency)
	}
	// compactionDebt >= ccSignal2 then can run another compaction, where
	// ccSignal2 = uint64(n) * p.opts.Experimental.CompactionDebtConcurrency
	// Rearranging,
	// n <= compactionDebt / p.opts.Experimental.CompactionDebtConcurrency
	// So we can run up to
	// compactionDebt / p.opts.Experimental.CompactionDebtConcurrency extra
	// compactions.
	compactionDebtCompactions := 0
	if p.opts.Experimental.CompactionDebtConcurrency > 0 {
		compactionDebt := p.estimatedCompactionDebt()
		compactionDebtCompactions = int(compactionDebt / p.opts.Experimental.CompactionDebtConcurrency)
	}

	compactableGarbageCompactions := 0
	garbageFractionLimit := p.opts.Experimental.CompactionGarbageFractionForMaxConcurrency()
	if garbageFractionLimit > 0 && p.dbSizeBytes > 0 {
		compactableGarbageBytes :=
			*pointDeletionsBytesEstimateAnnotator.MultiLevelAnnotation(p.vers.Levels[:]) +
				*rangeDeletionsBytesEstimateAnnotator.MultiLevelAnnotation(p.vers.Levels[:])
		garbageFraction := float64(compactableGarbageBytes) / float64(p.dbSizeBytes)
		compactableGarbageCompactions =
			int((garbageFraction / garbageFractionLimit) * float64(upper-lower))
	}

	extraCompactions := max(l0ReadAmpCompactions, compactionDebtCompactions, compactableGarbageCompactions, 0)

	return min(lower+extraCompactions, upper)
}

// TODO(sumeer): remove unless someone actually finds this useful.
func (p *compactionPickerByScore) logCompactionForTesting(
	env compactionEnv, scores [numLevels]candidateLevelInfo, pc *pickedTableCompaction,
) {
	var buf bytes.Buffer
	for i := 0; i < numLevels; i++ {
		if i != 0 && i < p.baseLevel {
			continue
		}

		var info *candidateLevelInfo
		for j := range scores {
			if scores[j].level == i {
				info = &scores[j]
				break
			}
		}

		marker := " "
		if pc.startLevel.level == info.level {
			marker = "*"
		}
		fmt.Fprintf(&buf, "  %sL%d: score:%5.1f  fillFactor:%5.1f  compensatedFillFactor:%5.1f %8s  %8s",
			marker, info.level, info.score, info.fillFactor, info.compensatedFillFactor,
			humanize.Bytes.Int64(int64(totalCompensatedSize(
				p.vers.Levels[info.level].All(),
			))),
			humanize.Bytes.Int64(p.levelMaxBytes[info.level]),
		)

		count := 0
		for i := range env.inProgressCompactions {
			c := &env.inProgressCompactions[i]
			if c.inputs[0].level != info.level {
				continue
			}
			count++
			if count == 1 {
				fmt.Fprintf(&buf, "  [")
			} else {
				fmt.Fprintf(&buf, " ")
			}
			fmt.Fprintf(&buf, "L%d->L%d", c.inputs[0].level, c.outputLevel)
		}
		if count > 0 {
			fmt.Fprintf(&buf, "]")
		}
		fmt.Fprintf(&buf, "\n")
	}
	p.opts.Logger.Infof("pickAuto: L%d->L%d\n%s",
		pc.startLevel.level, pc.outputLevel.level, buf.String())
}

// pickAutoScore picks the best score-based compaction, if any.
//
// On each call, pickAutoScore computes per-level size adjustments based on
// in-progress compactions, and computes a per-level score. The levels are
// iterated over in decreasing score order trying to find a valid compaction
// anchored at that level.
//
// If a score-based compaction cannot be found, pickAuto falls back to looking
// for an elision-only compaction to remove obsolete keys.
func (p *compactionPickerByScore) pickAutoScore(env compactionEnv) pickedCompaction {
	scores := p.calculateLevelScores(env.inProgressCompactions)

	// Check for a score-based compaction. candidateLevelInfos are first sorted
	// by whether they should be compacted, so if we find a level which shouldn't
	// be compacted, we can break early.
	for i := range scores {
		info := &scores[i]
		if !info.shouldCompact() {
			break
		}
		if info.level == numLevels-1 {
			continue
		}

		if info.level == 0 {
			ptc := pickL0(env, p.opts, p.vers, p.latestVersionState.l0Organizer, p.baseLevel)
			// Fail-safe to protect against compacting the same sstable
			// concurrently.
			if ptc != nil && !inputRangeAlreadyCompacting(p.opts.Comparer.Compare, env, ptc) {
				p.addScoresToPickedCompactionMetrics(ptc, scores)
				ptc.score = info.score
				if false {
					p.logCompactionForTesting(env, scores, ptc)
				}
				return ptc
			}
			continue
		}

		// info.level > 0
		var ok bool
		info.file, ok = pickCompactionSeedFile(p.vers, &p.latestVersionState.virtualBackings, p.opts, info.level, info.outputLevel, env.earliestSnapshotSeqNum, env.problemSpans)
		if !ok {
			continue
		}

		pc := pickAutoLPositive(env, p.opts, p.vers, p.latestVersionState.l0Organizer, *info, p.baseLevel)
		// Fail-safe to protect against compacting the same sstable concurrently.
		if pc != nil && !inputRangeAlreadyCompacting(p.opts.Comparer.Compare, env, pc) {
			p.addScoresToPickedCompactionMetrics(pc, scores)
			pc.score = info.score
			if false {
				p.logCompactionForTesting(env, scores, pc)
			}
			return pc
		}
	}
	return nil
}

// pickAutoNonScore picks the best non-score-based compaction, if any.
func (p *compactionPickerByScore) pickAutoNonScore(env compactionEnv) (pc pickedCompaction) {
	// Check for files which contain excessive point tombstones that could slow
	// down reads. Unlike elision-only compactions, these compactions may select
	// a file at any level rather than only the lowest level.
	if pc := p.pickTombstoneDensityCompaction(env); pc != nil {
		return pc
	}

	// Check for L6 files with tombstones that may be elided. These files may
	// exist if a snapshot prevented the elision of a tombstone or because of
	// a move compaction. These are low-priority compactions because they
	// don't help us keep up with writes, just reclaim disk space.
	if pc := p.pickElisionOnlyCompaction(env); pc != nil {
		return pc
	}

	// Check for blob file rewrites. These are low-priority compactions because
	// they don't help us keep up with writes, just reclaim disk space.
	if pc := p.pickBlobFileRewriteCompaction(env); pc != nil {
		return pc
	}

	if pc := p.pickReadTriggeredCompaction(env); pc != nil {
		return pc
	}

	// NB: This should only be run if a read compaction wasn't
	// scheduled.
	//
	// We won't be scheduling a read compaction right now, and in
	// read heavy workloads, compactions won't be scheduled frequently
	// because flushes aren't frequent. So we need to signal to the
	// iterator to schedule a compaction when it adds compactions to
	// the read compaction queue.
	//
	// We need the nil check here because without it, we have some
	// tests which don't set that variable fail. Since there's a
	// chance that one of those tests wouldn't want extra compactions
	// to be scheduled, I added this check here, instead of
	// setting rescheduleReadCompaction in those tests.
	if env.readCompactionEnv.rescheduleReadCompaction != nil {
		*env.readCompactionEnv.rescheduleReadCompaction = true
	}

	// At the lowest possible compaction-picking priority, look for files marked
	// for compaction. Pebble will mark files for compaction if they have atomic
	// compaction units that span multiple files. While current Pebble code does
	// not construct such sstables, RocksDB and earlier versions of Pebble may
	// have created them. These split user keys form sets of files that must be
	// compacted together for correctness (referred to as "atomic compaction
	// units" within the code). Rewrite them in-place.
	//
	// It's also possible that a file may have been marked for compaction by
	// even earlier versions of Pebble code, since TableMetadata's
	// MarkedForCompaction field is persisted in the manifest. That's okay. We
	// previously would've ignored the designation, whereas now we'll re-compact
	// the file in place.
	if p.vers.Stats.MarkedForCompaction > 0 {
		if pc := p.pickRewriteCompaction(env); pc != nil {
			return pc
		}
	}

	return nil
}

func (p *compactionPickerByScore) addScoresToPickedCompactionMetrics(
	pc *pickedTableCompaction, candInfo [numLevels]candidateLevelInfo,
) {

	// candInfo is sorted by score, not by compaction level.
	infoByLevel := [numLevels]candidateLevelInfo{}
	for i := range candInfo {
		level := candInfo[i].level
		infoByLevel[level] = candInfo[i]
	}
	// Gather the compaction scores for the levels participating in the compaction.
	pc.pickerMetrics.scores = make([]float64, len(pc.inputs))
	inputIdx := 0
	for i := range infoByLevel {
		if pc.inputs[inputIdx].level == infoByLevel[i].level {
			pc.pickerMetrics.scores[inputIdx] = infoByLevel[i].score
			inputIdx++
		}
		if inputIdx == len(pc.inputs) {
			break
		}
	}
}

// elisionOnlyAnnotator is a manifest.Annotator that annotates B-Tree
// nodes with the *fileMetadata of a file meeting the obsolete keys criteria
// for an elision-only compaction within the subtree. If multiple files meet
// the criteria, it chooses whichever file has the lowest LargestSeqNum. The
// lowest LargestSeqNum file will be the first eligible for an elision-only
// compaction once snapshots less than or equal to its LargestSeqNum are closed.
var elisionOnlyAnnotator = &manifest.Annotator[manifest.TableMetadata]{
	Aggregator: manifest.PickFileAggregator{
		Filter: func(f *manifest.TableMetadata) (eligible bool, cacheOK bool) {
			if f.IsCompacting() {
				return false, true
			}
			if !f.StatsValid() {
				return false, false
			}
			// Bottommost files are large and not worthwhile to compact just
			// to remove a few tombstones. Consider a file eligible only if
			// either its own range deletions delete at least 10% of its data or
			// its deletion tombstones make at least 10% of its entries.
			//
			// TODO(jackson): This does not account for duplicate user keys
			// which may be collapsed. Ideally, we would have 'obsolete keys'
			// statistics that would include tombstones, the keys that are
			// dropped by tombstones and duplicated user keys. See #847.
			//
			// Note that tables that contain exclusively range keys (i.e. no point keys,
			// `NumEntries` and `RangeDeletionsBytesEstimate` are both zero) are excluded
			// from elision-only compactions.
			// TODO(travers): Consider an alternative heuristic for elision of range-keys.
			return f.Stats.RangeDeletionsBytesEstimate*10 >= f.Size || f.Stats.NumDeletions*10 > f.Stats.NumEntries, true
		},
		Compare: func(f1 *manifest.TableMetadata, f2 *manifest.TableMetadata) bool {
			return f1.LargestSeqNum < f2.LargestSeqNum
		},
	},
}

// markedForCompactionAnnotator is a manifest.Annotator that annotates B-Tree
// nodes with the *fileMetadata of a file that is marked for compaction
// within the subtree. If multiple files meet the criteria, it chooses
// whichever file has the lowest LargestSeqNum.
var markedForCompactionAnnotator = &manifest.Annotator[manifest.TableMetadata]{
	Aggregator: manifest.PickFileAggregator{
		Filter: func(f *manifest.TableMetadata) (eligible bool, cacheOK bool) {
			return f.MarkedForCompaction, true
		},
		Compare: func(f1 *manifest.TableMetadata, f2 *manifest.TableMetadata) bool {
			return f1.LargestSeqNum < f2.LargestSeqNum
		},
	},
}

// pickedCompactionFromCandidateFile creates a pickedCompaction from a *fileMetadata
// with various checks to ensure that the file still exists in the expected level
// and isn't already being compacted.
func (p *compactionPickerByScore) pickedCompactionFromCandidateFile(
	candidate *manifest.TableMetadata,
	env compactionEnv,
	startLevel int,
	outputLevel int,
	kind compactionKind,
) *pickedTableCompaction {
	if candidate == nil || candidate.IsCompacting() {
		return nil
	}

	var inputs manifest.LevelSlice
	if startLevel == 0 {
		// Overlapping L0 files must also be compacted alongside the candidate.
		inputs = p.vers.Overlaps(0, candidate.UserKeyBounds())
	} else {
		inputs = p.vers.Levels[startLevel].Find(p.opts.Comparer.Compare, candidate)
	}
	if invariants.Enabled {
		found := false
		for f := range inputs.All() {
			if f.TableNum == candidate.TableNum {
				found = true
			}
		}
		if !found {
			panic(fmt.Sprintf("file %s not found in level %d as expected", candidate.TableNum, startLevel))
		}
	}

	pc := newPickedTableCompaction(p.opts, p.vers, p.latestVersionState.l0Organizer,
		startLevel, outputLevel, p.baseLevel)
	pc.kind = kind
	pc.startLevel.files = inputs
	pc.bounds = manifest.KeyRange(p.opts.Comparer.Compare, pc.startLevel.files.All())

	// Fail-safe to protect against compacting the same sstable concurrently.
	if inputRangeAlreadyCompacting(p.opts.Comparer.Compare, env, pc) {
		return nil
	}

	if !pc.setupInputs(p.opts, env.diskAvailBytes, pc.startLevel, env.problemSpans) {
		return nil
	}

	return pc
}

// pickElisionOnlyCompaction looks for compactions of sstables in the
// bottommost level containing obsolete records that may now be dropped.
func (p *compactionPickerByScore) pickElisionOnlyCompaction(
	env compactionEnv,
) (pc *pickedTableCompaction) {
	if p.opts.private.disableElisionOnlyCompactions {
		return nil
	}
	candidate := elisionOnlyAnnotator.LevelAnnotation(p.vers.Levels[numLevels-1])
	if candidate == nil {
		return nil
	}
	if candidate.LargestSeqNum >= env.earliestSnapshotSeqNum {
		return nil
	}
	return p.pickedCompactionFromCandidateFile(candidate, env, numLevels-1, numLevels-1, compactionKindElisionOnly)
}

// pickRewriteCompaction attempts to construct a compaction that
// rewrites a file marked for compaction. pickRewriteCompaction will
// pull in adjacent files in the file's atomic compaction unit if
// necessary. A rewrite compaction outputs files to the same level as
// the input level.
func (p *compactionPickerByScore) pickRewriteCompaction(
	env compactionEnv,
) (pc *pickedTableCompaction) {
	if p.vers.Stats.MarkedForCompaction == 0 {
		return nil
	}
	for l := numLevels - 1; l >= 0; l-- {
		candidate := markedForCompactionAnnotator.LevelAnnotation(p.vers.Levels[l])
		if candidate == nil {
			// Try the next level.
			continue
		}
		pc := p.pickedCompactionFromCandidateFile(candidate, env, l, l, compactionKindRewrite)
		if pc != nil {
			return pc
		}
	}
	return nil
}

// pickBlobFileRewriteCompaction looks for compactions of blob files that
// can be rewritten to reclaim disk space.
func (p *compactionPickerByScore) pickBlobFileRewriteCompaction(
	env compactionEnv,
) (pc *pickedBlobFileCompaction) {
	aggregateStats, heuristicStats := p.latestVersionState.blobFiles.Stats()
	if heuristicStats.CountFilesEligible == 0 && heuristicStats.CountFilesTooRecent == 0 {
		// No blob files with any garbage to rewrite.
		return nil
	}
	policy := p.opts.Experimental.ValueSeparationPolicy()
	if policy.TargetGarbageRatio >= 1.0 {
		// Blob file rewrite compactions are disabled.
		return nil
	}
	garbagePct := float64(aggregateStats.ValueSize-aggregateStats.ReferencedValueSize) /
		float64(aggregateStats.ValueSize)
	if garbagePct <= policy.TargetGarbageRatio {
		// Not enough garbage to warrant a rewrite compaction.
		return nil
	}

	// Check if there is an ongoing blob file rewrite compaction. If there is,
	// don't schedule a new one.
	for _, c := range env.inProgressCompactions {
		if c.kind == compactionKindBlobFileRewrite {
			return nil
		}
	}

	candidate, ok := p.latestVersionState.blobFiles.ReplacementCandidate()
	if !ok {
		// None meet the heuristic.
		return nil
	}
	return &pickedBlobFileCompaction{
		vers:              p.vers,
		file:              candidate,
		referencingTables: p.latestVersionState.blobFiles.ReferencingTables(candidate.FileID),
	}
}

// pickTombstoneDensityCompaction looks for a compaction that eliminates
// regions of extremely high point tombstone density. For each level, it picks
// a file where the ratio of tombstone-dense blocks is at least
// options.Experimental.MinTombstoneDenseRatio, prioritizing compaction of
// files with higher ratios of tombstone-dense blocks.
func (p *compactionPickerByScore) pickTombstoneDensityCompaction(
	env compactionEnv,
) (pc *pickedTableCompaction) {
	if p.opts.Experimental.TombstoneDenseCompactionThreshold <= 0 {
		// Tombstone density compactions are disabled.
		return nil
	}

	var candidate *manifest.TableMetadata
	var level int
	// If a candidate file has a very high overlapping ratio, point tombstones
	// in it are likely sparse in keyspace even if the sstable itself is tombstone
	// dense. These tombstones likely wouldn't be slow to iterate over, so we exclude
	// these files from tombstone density compactions. The threshold of 40.0 is
	// chosen somewhat arbitrarily, after some observations around excessively large
	// tombstone density compactions.
	const maxOverlappingRatio = 40.0
	// NB: we don't consider the lowest level because elision-only compactions
	// handle that case.
	lastNonEmptyLevel := numLevels - 1
	for l := numLevels - 2; l >= 0; l-- {
		iter := p.vers.Levels[l].Iter()
		for f := iter.First(); f != nil; f = iter.Next() {
			if f.IsCompacting() || !f.StatsValid() || f.Size == 0 {
				continue
			}
			if f.Stats.TombstoneDenseBlocksRatio < p.opts.Experimental.TombstoneDenseCompactionThreshold {
				continue
			}
			overlaps := p.vers.Overlaps(lastNonEmptyLevel, f.UserKeyBounds())
			if float64(overlaps.AggregateSizeSum())/float64(f.Size) > maxOverlappingRatio {
				continue
			}
			if candidate == nil || candidate.Stats.TombstoneDenseBlocksRatio < f.Stats.TombstoneDenseBlocksRatio {
				candidate = f
				level = l
			}
		}
		// We prefer lower level (ie. L5) candidates over higher level (ie. L4) ones.
		if candidate != nil {
			break
		}
		if !p.vers.Levels[l].Empty() {
			lastNonEmptyLevel = l
		}
	}

	return p.pickedCompactionFromCandidateFile(candidate, env, level, defaultOutputLevel(level, p.baseLevel), compactionKindTombstoneDensity)
}

// pickAutoLPositive picks an automatic compaction for the candidate
// file in a positive-numbered level. This function must not be used for
// L0.
func pickAutoLPositive(
	env compactionEnv,
	opts *Options,
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	cInfo candidateLevelInfo,
	baseLevel int,
) (pc *pickedTableCompaction) {
	if cInfo.level == 0 {
		panic("pebble: pickAutoLPositive called for L0")
	}

	pc = newPickedTableCompaction(opts, vers, l0Organizer, cInfo.level, defaultOutputLevel(cInfo.level, baseLevel), baseLevel)
	if pc.outputLevel.level != cInfo.outputLevel {
		panic("pebble: compaction picked unexpected output level")
	}
	pc.startLevel.files = cInfo.file.Slice()

	if !pc.setupInputs(opts, env.diskAvailBytes, pc.startLevel, env.problemSpans) {
		return nil
	}
	return pc.maybeAddLevel(opts, env.diskAvailBytes)
}

// maybeAddLevel maybe adds a level to the picked compaction.
func (pc *pickedTableCompaction) maybeAddLevel(
	opts *Options, diskAvailBytes uint64,
) *pickedTableCompaction {
	pc.pickerMetrics.singleLevelOverlappingRatio = pc.overlappingRatio()
	if pc.outputLevel.level == numLevels-1 {
		// Don't add a level if the current output level is in L6.
		return pc
	}
	if !opts.Experimental.MultiLevelCompactionHeuristic.allowL0() && pc.startLevel.level == 0 {
		return pc
	}
	targetFileSize := opts.TargetFileSize(pc.outputLevel.level, pc.baseLevel)
	if pc.estimatedInputSize() > expandedCompactionByteSizeLimit(opts, targetFileSize, diskAvailBytes) {
		// Don't add a level if the current compaction exceeds the compaction size limit
		return pc
	}
	return opts.Experimental.MultiLevelCompactionHeuristic.pick(pc, opts, diskAvailBytes)
}

// MultiLevelHeuristic evaluates whether to add files from the next level into the compaction.
type MultiLevelHeuristic interface {
	// Evaluate returns the preferred compaction.
	pick(pc *pickedTableCompaction, opts *Options, diskAvailBytes uint64) *pickedTableCompaction

	// Returns if the heuristic allows L0 to be involved in ML compaction
	allowL0() bool

	// String implements fmt.Stringer.
	String() string
}

// NoMultiLevel will never add an additional level to the compaction.
type NoMultiLevel struct{}

var _ MultiLevelHeuristic = (*NoMultiLevel)(nil)

func (nml NoMultiLevel) pick(
	pc *pickedTableCompaction, opts *Options, diskAvailBytes uint64,
) *pickedTableCompaction {
	return pc
}

func (nml NoMultiLevel) allowL0() bool  { return false }
func (nml NoMultiLevel) String() string { return "none" }

func (pc *pickedTableCompaction) predictedWriteAmp() float64 {
	var bytesToCompact uint64
	var higherLevelBytes uint64
	for i := range pc.inputs {
		levelSize := pc.inputs[i].files.AggregateSizeSum()
		bytesToCompact += levelSize
		if i != len(pc.inputs)-1 {
			higherLevelBytes += levelSize
		}
	}
	return float64(bytesToCompact) / float64(higherLevelBytes)
}

func (pc *pickedTableCompaction) overlappingRatio() float64 {
	var higherLevelBytes uint64
	var lowestLevelBytes uint64
	for i := range pc.inputs {
		levelSize := pc.inputs[i].files.AggregateSizeSum()
		if i == len(pc.inputs)-1 {
			lowestLevelBytes += levelSize
			continue
		}
		higherLevelBytes += levelSize
	}
	return float64(lowestLevelBytes) / float64(higherLevelBytes)
}

// WriteAmpHeuristic defines a multi level compaction heuristic which will add
// an additional level to the picked compaction if it reduces predicted write
// amp of the compaction + the addPropensity constant.
type WriteAmpHeuristic struct {
	// addPropensity is a constant that affects the propensity to conduct multilevel
	// compactions. If positive, a multilevel compaction may get picked even if
	// the single level compaction has lower write amp, and vice versa.
	AddPropensity float64

	// AllowL0 if true, allow l0 to be involved in a ML compaction.
	AllowL0 bool
}

var _ MultiLevelHeuristic = (*WriteAmpHeuristic)(nil)

// TODO(msbutler): microbenchmark the extent to which multilevel compaction
// picking slows down the compaction picking process.  This should be as fast as
// possible since Compaction-picking holds d.mu, which prevents WAL rotations,
// in-progress flushes and compactions from completing, etc. Consider ways to
// deduplicate work, given that setupInputs has already been called.
func (wa WriteAmpHeuristic) pick(
	pcOrig *pickedTableCompaction, opts *Options, diskAvailBytes uint64,
) *pickedTableCompaction {
	pcMulti := pcOrig.clone()
	if !pcMulti.setupMultiLevelCandidate(opts, diskAvailBytes) {
		return pcOrig
	}
	// We consider the addition of a level as an "expansion" of the compaction.
	// If pcMulti is past the expanded compaction byte size limit already,
	// we don't consider it.
	targetFileSize := opts.TargetFileSize(pcMulti.outputLevel.level, pcMulti.baseLevel)
	if pcMulti.estimatedInputSize() >= expandedCompactionByteSizeLimit(opts, targetFileSize, diskAvailBytes) {
		return pcOrig
	}
	picked := pcOrig
	if pcMulti.predictedWriteAmp() <= pcOrig.predictedWriteAmp()+wa.AddPropensity {
		picked = pcMulti
	}
	// Regardless of what compaction was picked, log the multilevelOverlapping ratio.
	picked.pickerMetrics.multiLevelOverlappingRatio = pcMulti.overlappingRatio()
	return picked
}

func (wa WriteAmpHeuristic) allowL0() bool {
	return wa.AllowL0
}

// String implements fmt.Stringer.
func (wa WriteAmpHeuristic) String() string {
	return fmt.Sprintf("wamp(%.2f, %t)", wa.AddPropensity, wa.AllowL0)
}

// Helper method to pick compactions originating from L0. Uses information about
// sublevels to generate a compaction.
func pickL0(
	env compactionEnv,
	opts *Options,
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	baseLevel int,
) *pickedTableCompaction {
	// It is important to pass information about Lbase files to L0Sublevels
	// so it can pick a compaction that does not conflict with an Lbase => Lbase+1
	// compaction. Without this, we observed reduced concurrency of L0=>Lbase
	// compactions, and increasing read amplification in L0.
	//
	// TODO(bilal) Remove the minCompactionDepth parameter once fixing it at 1
	// has been shown to not cause a performance regression.
	lcf := l0Organizer.PickBaseCompaction(opts.Logger, 1, vers.Levels[baseLevel].Slice(), baseLevel, env.problemSpans)
	if lcf != nil {
		pc := newPickedCompactionFromL0(lcf, opts, vers, l0Organizer, baseLevel, true)
		if pc.setupInputs(opts, env.diskAvailBytes, pc.startLevel, env.problemSpans) {
			if pc.startLevel.files.Empty() {
				opts.Logger.Errorf("%v", base.AssertionFailedf("empty compaction chosen"))
			}
			return pc.maybeAddLevel(opts, env.diskAvailBytes)
		}
		// TODO(radu): investigate why this happens.
		// opts.Logger.Errorf("%v", base.AssertionFailedf("setupInputs failed"))
	}

	// Couldn't choose a base compaction. Try choosing an intra-L0
	// compaction. Note that we pass in L0CompactionThreshold here as opposed to
	// 1, since choosing a single sublevel intra-L0 compaction is
	// counterproductive.
	lcf = l0Organizer.PickIntraL0Compaction(env.earliestUnflushedSeqNum, minIntraL0Count, env.problemSpans)
	if lcf != nil {
		pc := newPickedCompactionFromL0(lcf, opts, vers, l0Organizer, baseLevel, false)
		if pc.setupInputs(opts, env.diskAvailBytes, pc.startLevel, env.problemSpans) {
			if pc.startLevel.files.Empty() {
				opts.Logger.Fatalf("empty compaction chosen")
			}
			// A single-file intra-L0 compaction is unproductive.
			if iter := pc.startLevel.files.Iter(); iter.First() != nil && iter.Next() != nil {
				pc.bounds = manifest.KeyRange(opts.Comparer.Compare, pc.startLevel.files.All())
				return pc
			}
		} else {
			// TODO(radu): investigate why this happens.
			// opts.Logger.Errorf("%v", base.AssertionFailedf("setupInputs failed"))
		}
	}
	return nil
}

func newPickedManualCompaction(
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	opts *Options,
	env compactionEnv,
	baseLevel int,
	manual *manualCompaction,
) (pc *pickedTableCompaction, retryLater bool) {
	outputLevel := manual.level + 1
	if manual.level == 0 {
		outputLevel = baseLevel
	} else if manual.level < baseLevel {
		// The start level for a compaction must be >= Lbase. A manual
		// compaction could have been created adhering to that condition, and
		// then an automatic compaction came in and compacted all of the
		// sstables in Lbase to Lbase+1 which caused Lbase to change. Simply
		// ignore this manual compaction as there is nothing to do (manual.level
		// points to an empty level).
		return nil, false
	}
	// This conflictsWithInProgress call is necessary for the manual compaction to
	// be retried when it conflicts with an ongoing automatic compaction. Without
	// it, the compaction is dropped due to pc.setupInputs returning false since
	// the input/output range is already being compacted, and the manual
	// compaction ends with a non-compacted LSM.
	if conflictsWithInProgress(manual, outputLevel, env.inProgressCompactions, opts.Comparer.Compare) {
		return nil, true
	}
	pc = newPickedTableCompaction(opts, vers, l0Organizer, manual.level, defaultOutputLevel(manual.level, baseLevel), baseLevel)
	pc.manualID = manual.id
	manual.outputLevel = pc.outputLevel.level
	pc.startLevel.files = vers.Overlaps(manual.level, base.UserKeyBoundsInclusive(manual.start, manual.end))
	if pc.startLevel.files.Empty() {
		// Nothing to do
		return nil, false
	}
	// We use nil problemSpans because we don't want problem spans to prevent
	// manual compactions.
	if !pc.setupInputs(opts, env.diskAvailBytes, pc.startLevel, nil /* problemSpans */) {
		// setupInputs returned false indicating there's a conflicting
		// concurrent compaction.
		return nil, true
	}
	if pc = pc.maybeAddLevel(opts, env.diskAvailBytes); pc == nil {
		return nil, false
	}
	if pc.outputLevel.level != outputLevel {
		if len(pc.inputs) > 2 {
			// Multilevel compactions relax this invariant.
		} else {
			panic("pebble: compaction picked unexpected output level")
		}
	}
	// Fail-safe to protect against compacting the same sstable concurrently.
	if inputRangeAlreadyCompacting(opts.Comparer.Compare, env, pc) {
		return nil, true
	}
	return pc, false
}

// pickDownloadCompaction picks a download compaction for the downloadSpan,
// which could be specified as being performed either by a copy compaction of
// the backing file or a rewrite compaction.
func pickDownloadCompaction(
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	opts *Options,
	env compactionEnv,
	baseLevel int,
	kind compactionKind,
	level int,
	file *manifest.TableMetadata,
) (pc *pickedTableCompaction) {
	// Check if the file is compacting already.
	if file.CompactionState == manifest.CompactionStateCompacting {
		return nil
	}
	if kind != compactionKindCopy && kind != compactionKindRewrite {
		panic("invalid download/rewrite compaction kind")
	}
	pc = newPickedTableCompaction(opts, vers, l0Organizer, level, level, baseLevel)
	pc.kind = kind
	pc.startLevel.files = manifest.NewLevelSliceKeySorted(opts.Comparer.Compare, []*manifest.TableMetadata{file})
	if !pc.setupInputs(opts, env.diskAvailBytes, pc.startLevel, nil /* problemSpans */) {
		// setupInputs returned false indicating there's a conflicting
		// concurrent compaction.
		return nil
	}
	if pc.outputLevel.level != level {
		panic("pebble: download compaction picked unexpected output level")
	}
	// Fail-safe to protect against compacting the same sstable concurrently.
	if inputRangeAlreadyCompacting(opts.Comparer.Compare, env, pc) {
		return nil
	}
	return pc
}

func (p *compactionPickerByScore) pickReadTriggeredCompaction(
	env compactionEnv,
) (pc *pickedTableCompaction) {
	// If a flush is in-progress or expected to happen soon, it means more writes are taking place. We would
	// soon be scheduling more write focussed compactions. In this case, skip read compactions as they are
	// lower priority.
	if env.readCompactionEnv.flushing || env.readCompactionEnv.readCompactions == nil {
		return nil
	}
	for env.readCompactionEnv.readCompactions.size > 0 {
		rc := env.readCompactionEnv.readCompactions.remove()
		if pc = pickReadTriggeredCompactionHelper(p, rc, env); pc != nil {
			break
		}
	}
	return pc
}

func pickReadTriggeredCompactionHelper(
	p *compactionPickerByScore, rc *readCompaction, env compactionEnv,
) (pc *pickedTableCompaction) {
	overlapSlice := p.vers.Overlaps(rc.level, base.UserKeyBoundsInclusive(rc.start, rc.end))
	var fileMatches bool
	for f := range overlapSlice.All() {
		if f.TableNum == rc.tableNum {
			fileMatches = true
			break
		}
	}
	if !fileMatches {
		return nil
	}

	pc = newPickedTableCompaction(p.opts, p.vers, p.latestVersionState.l0Organizer,
		rc.level, defaultOutputLevel(rc.level, p.baseLevel), p.baseLevel)

	pc.startLevel.files = overlapSlice
	if !pc.setupInputs(p.opts, env.diskAvailBytes, pc.startLevel, env.problemSpans) {
		return nil
	}
	if inputRangeAlreadyCompacting(p.opts.Comparer.Compare, env, pc) {
		return nil
	}
	pc.kind = compactionKindRead

	// Prevent read compactions which are too wide.
	outputOverlaps := pc.version.Overlaps(pc.outputLevel.level, pc.bounds)
	if outputOverlaps.AggregateSizeSum() > pc.maxReadCompactionBytes {
		return nil
	}

	// Prevent compactions which start with a small seed file X, but overlap
	// with over allowedCompactionWidth * X file sizes in the output layer.
	const allowedCompactionWidth = 35
	if outputOverlaps.AggregateSizeSum() > overlapSlice.AggregateSizeSum()*allowedCompactionWidth {
		return nil
	}

	return pc
}

func (p *compactionPickerByScore) forceBaseLevel1() {
	p.baseLevel = 1
}

func inputRangeAlreadyCompacting(
	cmp base.Compare, env compactionEnv, pc *pickedTableCompaction,
) bool {
	for _, cl := range pc.inputs {
		for f := range cl.files.All() {
			if f.IsCompacting() {
				return true
			}
		}
	}

	// Look for active compactions outputting to the same region of the key
	// space in the same output level. Two potential compactions may conflict
	// without sharing input files if there are no files in the output level
	// that overlap with the intersection of the compactions' key spaces.
	//
	// Consider an active L0->Lbase compaction compacting two L0 files one
	// [a-f] and the other [t-z] into Lbase.
	//
	// L0
	//     ↦ 000100  ↤                           ↦  000101   ↤
	// L1
	//     ↦ 000004  ↤
	//     a b c d e f g h i j k l m n o p q r s t u v w x y z
	//
	// If a new file 000102 [j-p] is flushed while the existing compaction is
	// still ongoing, new file would not be in any compacting sublevel
	// intervals and would not overlap with any Lbase files that are also
	// compacting. However, this compaction cannot be picked because the
	// compaction's output key space [j-p] would overlap the existing
	// compaction's output key space [a-z].
	//
	// L0
	//     ↦ 000100* ↤       ↦   000102  ↤       ↦  000101*  ↤
	// L1
	//     ↦ 000004* ↤
	//     a b c d e f g h i j k l m n o p q r s t u v w x y z
	//
	// * - currently compacting
	if pc.outputLevel != nil && pc.outputLevel.level != 0 {
		for _, c := range env.inProgressCompactions {
			if pc.outputLevel.level != c.outputLevel {
				continue
			}
			if !c.bounds.Overlaps(cmp, &pc.bounds) {
				continue
			}
			// The picked compaction and the in-progress compaction c are
			// outputting to the same region of the key space of the same
			// level.
			return true
		}
	}
	return false
}

// conflictsWithInProgress checks if there are any in-progress compactions with overlapping keyspace.
func conflictsWithInProgress(
	manual *manualCompaction, outputLevel int, inProgressCompactions []compactionInfo, cmp Compare,
) bool {
	for _, c := range inProgressCompactions {
		if (c.outputLevel == manual.level || c.outputLevel == outputLevel) &&
			areUserKeysOverlapping(manual.start, manual.end, c.bounds.Start, c.bounds.End.Key, cmp) {
			return true
		}
		for _, in := range c.inputs {
			if in.files.Empty() {
				continue
			}
			iter := in.files.Iter()
			smallest := iter.First().Smallest().UserKey
			largest := iter.Last().Largest().UserKey
			if (in.level == manual.level || in.level == outputLevel) &&
				areUserKeysOverlapping(manual.start, manual.end, smallest, largest, cmp) {
				return true
			}
		}
	}
	return false
}

func areUserKeysOverlapping(x1, x2, y1, y2 []byte, cmp Compare) bool {
	return cmp(x1, y2) <= 0 && cmp(y1, x2) <= 0
}

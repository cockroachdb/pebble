// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"math"
	"runtime/pprof"
	"slices"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/keyspan/keyspanimpl"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

var errEmptyTable = errors.New("pebble: empty table")

// ErrCancelledCompaction is returned if a compaction is cancelled by a
// concurrent excise or ingest-split operation.
var ErrCancelledCompaction = errors.New("pebble: compaction cancelled by a concurrent operation, will retry compaction")

var flushLabels = pprof.Labels("pebble", "flush", "output-level", "L0")
var gcLabels = pprof.Labels("pebble", "gc")

// expandedCompactionByteSizeLimit is the maximum number of bytes in all
// compacted files. We avoid expanding the lower level file set of a compaction
// if it would make the total compaction cover more than this many bytes.
func expandedCompactionByteSizeLimit(opts *Options, level int, availBytes uint64) uint64 {
	v := uint64(25 * opts.Level(level).TargetFileSize)

	// Never expand a compaction beyond half the available capacity, divided
	// by the maximum number of concurrent compactions. Each of the concurrent
	// compactions may expand up to this limit, so this attempts to limit
	// compactions to half of available disk space. Note that this will not
	// prevent compaction picking from pursuing compactions that are larger
	// than this threshold before expansion.
	//
	// NB: this heuristic is an approximation since we may run more compactions
	// than MaxConcurrentCompactions.
	diskMax := (availBytes / 2) / uint64(opts.MaxConcurrentCompactions())
	if v > diskMax {
		v = diskMax
	}
	return v
}

// maxGrandparentOverlapBytes is the maximum bytes of overlap with level+1
// before we stop building a single file in a level-1 to level compaction.
func maxGrandparentOverlapBytes(opts *Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// maxReadCompactionBytes is used to prevent read compactions which
// are too wide.
func maxReadCompactionBytes(opts *Options, level int) uint64 {
	return uint64(10 * opts.Level(level).TargetFileSize)
}

// noCloseIter wraps around a FragmentIterator, intercepting and eliding
// calls to Close. It is used during compaction to ensure that rangeDelIters
// are not closed prematurely.
type noCloseIter struct {
	keyspan.FragmentIterator
}

func (i *noCloseIter) Close() {}

type compactionLevel struct {
	level int
	files manifest.LevelSlice
	// l0SublevelInfo contains information about L0 sublevels being compacted.
	// It's only set for the start level of a compaction starting out of L0 and
	// is nil for all other compactions.
	l0SublevelInfo []sublevelInfo
}

func (cl compactionLevel) Clone() compactionLevel {
	newCL := compactionLevel{
		level: cl.level,
		files: cl.files,
	}
	return newCL
}
func (cl compactionLevel) String() string {
	return fmt.Sprintf(`Level %d, Files %s`, cl.level, cl.files)
}

// compactionWritable is a objstorage.Writable wrapper that, on every write,
// updates a metric in `versions` on bytes written by in-progress compactions so
// far. It also increments a per-compaction `written` int.
type compactionWritable struct {
	objstorage.Writable

	versions *versionSet
	written  *int64
}

// Write is part of the objstorage.Writable interface.
func (c *compactionWritable) Write(p []byte) error {
	if err := c.Writable.Write(p); err != nil {
		return err
	}

	*c.written += int64(len(p))
	c.versions.incrementCompactionBytes(int64(len(p)))
	return nil
}

type compactionKind int

const (
	compactionKindDefault compactionKind = iota
	compactionKindFlush
	// compactionKindMove denotes a move compaction where the input file is
	// retained and linked in a new level without being obsoleted.
	compactionKindMove
	// compactionKindCopy denotes a copy compaction where the input file is
	// copied byte-by-byte into a new file with a new FileNum in the output level.
	compactionKindCopy
	// compactionKindDeleteOnly denotes a compaction that only deletes input
	// files. It can occur when wide range tombstones completely contain sstables.
	compactionKindDeleteOnly
	compactionKindElisionOnly
	compactionKindRead
	compactionKindTombstoneDensity
	compactionKindRewrite
	compactionKindIngestedFlushable
)

func (k compactionKind) String() string {
	switch k {
	case compactionKindDefault:
		return "default"
	case compactionKindFlush:
		return "flush"
	case compactionKindMove:
		return "move"
	case compactionKindDeleteOnly:
		return "delete-only"
	case compactionKindElisionOnly:
		return "elision-only"
	case compactionKindRead:
		return "read"
	case compactionKindTombstoneDensity:
		return "tombstone-density"
	case compactionKindRewrite:
		return "rewrite"
	case compactionKindIngestedFlushable:
		return "ingested-flushable"
	case compactionKindCopy:
		return "copy"
	}
	return "?"
}

// compactingOrFlushing returns "flushing" if the compaction kind is a flush,
// otherwise it returns "compacting".
func (k compactionKind) compactingOrFlushing() string {
	if k == compactionKindFlush {
		return "flushing"
	}
	return "compacting"
}

// compaction is a table compaction from one level to the next, starting from a
// given version.
type compaction struct {
	// cancel is a bool that can be used by other goroutines to signal a compaction
	// to cancel, such as if a conflicting excise operation raced it to manifest
	// application. Only holders of the manifest lock will write to this atomic.
	cancel atomic.Bool

	kind compactionKind
	// isDownload is true if this compaction was started as part of a Download
	// operation. In this case kind is compactionKindCopy or
	// compactionKindRewrite.
	isDownload bool

	cmp       Compare
	equal     Equal
	comparer  *base.Comparer
	formatKey base.FormatKey
	logger    Logger
	version   *version
	stats     base.InternalIteratorStats
	beganAt   time.Time
	// versionEditApplied is set to true when a compaction has completed and the
	// resulting version has been installed (if successful), but the compaction
	// goroutine is still cleaning up (eg, deleting obsolete files).
	versionEditApplied bool
	bufferPool         sstable.BufferPool
	// getValueSeparation constructs a compact.ValueSeparation for use in a
	// compaction. It implements heuristics around choosing whether a compaction
	// should:
	//
	// a) preserve existing blob references: The compaction does not write any
	// new blob files, but propagates existing references to blob files.This
	// conserves write bandwidth by avoiding rewriting the referenced values. It
	// also reduces the locality of the referenced values which can reduce scan
	// performance because a scan must load values from more unique blob files.
	// It can also delay reclamation of disk space if some of the references to
	// blob values are elided by the compaction, increasing space amplification.
	//
	// b) rewrite blob files: The compaction will write eligible values to new
	// blob files. This consumes more write bandwidth because all values are
	// rewritten. However it restores locality.
	getValueSeparation func(JobID, *compaction, sstable.TableFormat) compact.ValueSeparation
	// valueFetcher is used to fetch values from blob files. It's propagated
	// down the iterator tree through the internal iterator options.
	valueFetcher blob.ValueFetcher

	// startLevel is the level that is being compacted. Inputs from startLevel
	// and outputLevel will be merged to produce a set of outputLevel files.
	startLevel *compactionLevel

	// outputLevel is the level that files are being produced in. outputLevel is
	// equal to startLevel+1 except when:
	//    - if startLevel is 0, the output level equals compactionPicker.baseLevel().
	//    - in multilevel compaction, the output level is the lowest level involved in
	//      the compaction
	// A compaction's outputLevel is nil for delete-only compactions.
	outputLevel *compactionLevel

	// extraLevels point to additional levels in between the input and output
	// levels that get compacted in multilevel compactions
	extraLevels []*compactionLevel

	inputs []compactionLevel

	// maxOutputFileSize is the maximum size of an individual table created
	// during compaction.
	maxOutputFileSize uint64
	// maxOverlapBytes is the maximum number of bytes of overlap allowed for a
	// single output table with the tables in the grandparent level.
	maxOverlapBytes uint64

	// flushing contains the flushables (aka memtables) that are being flushed.
	flushing flushableList
	// bytesWritten contains the number of bytes that have been written to outputs.
	bytesWritten int64

	// The boundaries of the input data.
	smallest InternalKey
	largest  InternalKey

	// A list of fragment iterators to close when the compaction finishes. Used by
	// input iteration to keep rangeDelIters open for the lifetime of the
	// compaction, and only close them when the compaction finishes.
	closers []*noCloseIter

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that the actual files
	// in the grandparent when this compaction finishes will be the same.
	grandparents manifest.LevelSlice

	// Boundaries at which flushes to L0 should be split. Determined by
	// L0Sublevels. If nil, flushes aren't split.
	l0Limits [][]byte

	delElision      compact.TombstoneElision
	rangeKeyElision compact.TombstoneElision

	// allowedZeroSeqNum is true if seqnums can be zeroed if there are no
	// snapshots requiring them to be kept. This determination is made by
	// looking for an sstable which overlaps the bounds of the compaction at a
	// lower level in the LSM during runCompaction.
	allowedZeroSeqNum bool

	// deletionHints are set if this is a compactionKindDeleteOnly. Used to figure
	// out whether an input must be deleted in its entirety, or excised into
	// virtual sstables.
	deletionHints []deleteCompactionHint

	// exciseEnabled is set to true if this is a compactionKindDeleteOnly and
	// this compaction is allowed to excise files.
	exciseEnabled bool

	metrics levelMetricsDelta

	pickerMetrics pickedCompactionMetrics

	grantHandle CompactionGrantHandle
}

// inputLargestSeqNumAbsolute returns the maximum LargestSeqNumAbsolute of any
// input sstables.
func (c *compaction) inputLargestSeqNumAbsolute() base.SeqNum {
	var seqNum base.SeqNum
	for _, cl := range c.inputs {
		for m := range cl.files.All() {
			seqNum = max(seqNum, m.LargestSeqNumAbsolute)
		}
	}
	return seqNum
}

func (c *compaction) makeInfo(jobID JobID) CompactionInfo {
	info := CompactionInfo{
		JobID:       int(jobID),
		Reason:      c.kind.String(),
		Input:       make([]LevelInfo, 0, len(c.inputs)),
		Annotations: []string{},
	}
	if c.isDownload {
		info.Reason = "download," + info.Reason
	}
	for _, cl := range c.inputs {
		inputInfo := LevelInfo{Level: cl.level, Tables: nil}
		for m := range cl.files.All() {
			inputInfo.Tables = append(inputInfo.Tables, m.TableInfo())
		}
		info.Input = append(info.Input, inputInfo)
	}
	if c.outputLevel != nil {
		info.Output.Level = c.outputLevel.level

		// If there are no inputs from the output level (eg, a move
		// compaction), add an empty LevelInfo to info.Input.
		if len(c.inputs) > 0 && c.inputs[len(c.inputs)-1].level != c.outputLevel.level {
			info.Input = append(info.Input, LevelInfo{Level: c.outputLevel.level})
		}
	} else {
		// For a delete-only compaction, set the output level to L6. The
		// output level is not meaningful here, but complicating the
		// info.Output interface with a pointer doesn't seem worth the
		// semantic distinction.
		info.Output.Level = numLevels - 1
	}

	for i, score := range c.pickerMetrics.scores {
		info.Input[i].Score = score
	}
	info.SingleLevelOverlappingRatio = c.pickerMetrics.singleLevelOverlappingRatio
	info.MultiLevelOverlappingRatio = c.pickerMetrics.multiLevelOverlappingRatio
	if len(info.Input) > 2 {
		info.Annotations = append(info.Annotations, "multilevel")
	}
	return info
}

func (c *compaction) userKeyBounds() base.UserKeyBounds {
	return base.UserKeyBoundsFromInternal(c.smallest, c.largest)
}

type getValueSeparation func(JobID, *compaction, sstable.TableFormat) compact.ValueSeparation

func newCompaction(
	pc *pickedCompaction,
	opts *Options,
	beganAt time.Time,
	provider objstorage.Provider,
	grantHandle CompactionGrantHandle,
	getValueSeparation getValueSeparation,
) *compaction {
	c := &compaction{
		kind:               compactionKindDefault,
		cmp:                pc.cmp,
		equal:              opts.Comparer.Equal,
		comparer:           opts.Comparer,
		formatKey:          opts.Comparer.FormatKey,
		inputs:             pc.inputs,
		smallest:           pc.smallest,
		largest:            pc.largest,
		logger:             opts.Logger,
		version:            pc.version,
		beganAt:            beganAt,
		getValueSeparation: getValueSeparation,
		maxOutputFileSize:  pc.maxOutputFileSize,
		maxOverlapBytes:    pc.maxOverlapBytes,
		pickerMetrics:      pc.pickerMetrics,
		grantHandle:        grantHandle,
	}
	c.startLevel = &c.inputs[0]
	if pc.startLevel.l0SublevelInfo != nil {
		c.startLevel.l0SublevelInfo = pc.startLevel.l0SublevelInfo
	}
	c.outputLevel = &c.inputs[1]

	if len(pc.extraLevels) > 0 {
		c.extraLevels = pc.extraLevels
		c.outputLevel = &c.inputs[len(c.inputs)-1]
	}
	// Compute the set of outputLevel+1 files that overlap this compaction (these
	// are the grandparent sstables).
	if c.outputLevel.level+1 < numLevels {
		c.grandparents = c.version.Overlaps(c.outputLevel.level+1, c.userKeyBounds())
	}
	c.delElision, c.rangeKeyElision = compact.SetupTombstoneElision(
		c.cmp, c.version, pc.l0Organizer, c.outputLevel.level, base.UserKeyBoundsFromInternal(c.smallest, c.largest),
	)
	c.kind = pc.kind

	if c.kind == compactionKindDefault && c.outputLevel.files.Empty() && !c.hasExtraLevelData() &&
		c.startLevel.files.Len() == 1 && c.grandparents.SizeSum() <= c.maxOverlapBytes {
		// This compaction can be converted into a move or copy from one level
		// to the next. We avoid such a move if there is lots of overlapping
		// grandparent data. Otherwise, the move could create a parent file
		// that will require a very expensive merge later on.
		iter := c.startLevel.files.Iter()
		meta := iter.First()
		isRemote := false
		// We should always be passed a provider, except in some unit tests.
		if provider != nil {
			isRemote = !objstorage.IsLocalTable(provider, meta.FileBacking.DiskFileNum)
		}
		// Avoid a trivial move or copy if all of these are true, as rewriting a
		// new file is better:
		//
		// 1) The source file is a virtual sstable
		// 2) The existing file `meta` is on non-remote storage
		// 3) The output level prefers shared storage
		mustCopy := !isRemote && remote.ShouldCreateShared(opts.Experimental.CreateOnShared, c.outputLevel.level)
		if mustCopy {
			// If the source is virtual, it's best to just rewrite the file as all
			// conditions in the above comment are met.
			if !meta.Virtual {
				c.kind = compactionKindCopy
			}
		} else {
			c.kind = compactionKindMove
		}
	}
	return c
}

func newDeleteOnlyCompaction(
	opts *Options,
	cur *version,
	inputs []compactionLevel,
	beganAt time.Time,
	hints []deleteCompactionHint,
	exciseEnabled bool,
) *compaction {
	c := &compaction{
		kind:          compactionKindDeleteOnly,
		cmp:           opts.Comparer.Compare,
		equal:         opts.Comparer.Equal,
		comparer:      opts.Comparer,
		formatKey:     opts.Comparer.FormatKey,
		logger:        opts.Logger,
		version:       cur,
		beganAt:       beganAt,
		inputs:        inputs,
		deletionHints: hints,
		exciseEnabled: exciseEnabled,
		grantHandle:   noopGrantHandle{},
	}

	// Set c.smallest, c.largest.
	files := make([]iter.Seq[*manifest.TableMetadata], 0, len(inputs))
	for _, in := range inputs {
		files = append(files, in.files.All())
	}
	c.smallest, c.largest = manifest.KeyRange(opts.Comparer.Compare, files...)
	return c
}

func adjustGrandparentOverlapBytesForFlush(c *compaction, flushingBytes uint64) {
	// Heuristic to place a lower bound on compaction output file size
	// caused by Lbase. Prior to this heuristic we have observed an L0 in
	// production with 310K files of which 290K files were < 10KB in size.
	// Our hypothesis is that it was caused by L1 having 2600 files and
	// ~10GB, such that each flush got split into many tiny files due to
	// overlapping with most of the files in Lbase.
	//
	// The computation below is general in that it accounts
	// for flushing different volumes of data (e.g. we may be flushing
	// many memtables). For illustration, we consider the typical
	// example of flushing a 64MB memtable. So 12.8MB output,
	// based on the compression guess below. If the compressed bytes
	// guess is an over-estimate we will end up with smaller files,
	// and if an under-estimate we will end up with larger files.
	// With a 2MB target file size, 7 files. We are willing to accept
	// 4x the number of files, if it results in better write amplification
	// when later compacting to Lbase, i.e., ~450KB files (target file
	// size / 4).
	//
	// Note that this is a pessimistic heuristic in that
	// fileCountUpperBoundDueToGrandparents could be far from the actual
	// number of files produced due to the grandparent limits. For
	// example, in the extreme, consider a flush that overlaps with 1000
	// files in Lbase f0...f999, and the initially calculated value of
	// maxOverlapBytes will cause splits at f10, f20,..., f990, which
	// means an upper bound file count of 100 files. Say the input bytes
	// in the flush are such that acceptableFileCount=10. We will fatten
	// up maxOverlapBytes by 10x to ensure that the upper bound file count
	// drops to 10. However, it is possible that in practice, even without
	// this change, we would have produced no more than 10 files, and that
	// this change makes the files unnecessarily wide. Say the input bytes
	// are distributed such that 10% are in f0...f9, 10% in f10...f19, ...
	// 10% in f80...f89 and 10% in f990...f999. The original value of
	// maxOverlapBytes would have actually produced only 10 sstables. But
	// by increasing maxOverlapBytes by 10x, we may produce 1 sstable that
	// spans f0...f89, i.e., a much wider sstable than necessary.
	//
	// We could produce a tighter estimate of
	// fileCountUpperBoundDueToGrandparents if we had knowledge of the key
	// distribution of the flush. The 4x multiplier mentioned earlier is
	// a way to try to compensate for this pessimism.
	//
	// TODO(sumeer): we don't have compression info for the data being
	// flushed, but it is likely that existing files that overlap with
	// this flush in Lbase are representative wrt compression ratio. We
	// could store the uncompressed size in TableMetadata and estimate
	// the compression ratio.
	const approxCompressionRatio = 0.2
	approxOutputBytes := approxCompressionRatio * float64(flushingBytes)
	approxNumFilesBasedOnTargetSize :=
		int(math.Ceil(approxOutputBytes / float64(c.maxOutputFileSize)))
	acceptableFileCount := float64(4 * approxNumFilesBasedOnTargetSize)
	// The byte calculation is linear in numGrandparentFiles, but we will
	// incur this linear cost in compact.Runner.TableSplitLimit() too, so we are
	// also willing to pay it now. We could approximate this cheaply by using the
	// mean file size of Lbase.
	grandparentFileBytes := c.grandparents.SizeSum()
	fileCountUpperBoundDueToGrandparents :=
		float64(grandparentFileBytes) / float64(c.maxOverlapBytes)
	if fileCountUpperBoundDueToGrandparents > acceptableFileCount {
		c.maxOverlapBytes = uint64(
			float64(c.maxOverlapBytes) *
				(fileCountUpperBoundDueToGrandparents / acceptableFileCount))
	}
}

func newFlush(
	opts *Options,
	cur *version,
	l0Organizer *manifest.L0Organizer,
	baseLevel int,
	flushing flushableList,
	beganAt time.Time,
	getValueSeparation getValueSeparation,
) (*compaction, error) {
	c := &compaction{
		kind:               compactionKindFlush,
		cmp:                opts.Comparer.Compare,
		equal:              opts.Comparer.Equal,
		comparer:           opts.Comparer,
		formatKey:          opts.Comparer.FormatKey,
		logger:             opts.Logger,
		version:            cur,
		beganAt:            beganAt,
		inputs:             []compactionLevel{{level: -1}, {level: 0}},
		getValueSeparation: getValueSeparation,
		maxOutputFileSize:  math.MaxUint64,
		maxOverlapBytes:    math.MaxUint64,
		flushing:           flushing,
		grantHandle:        noopGrantHandle{},
	}
	c.startLevel = &c.inputs[0]
	c.outputLevel = &c.inputs[1]

	if len(flushing) > 0 {
		if _, ok := flushing[0].flushable.(*ingestedFlushable); ok {
			if len(flushing) != 1 {
				panic("pebble: ingestedFlushable must be flushed one at a time.")
			}
			c.kind = compactionKindIngestedFlushable
			return c, nil
		}
	}

	// Make sure there's no ingestedFlushable after the first flushable in the
	// list.
	for _, f := range flushing {
		if _, ok := f.flushable.(*ingestedFlushable); ok {
			panic("pebble: flushing shouldn't contain ingestedFlushable flushable")
		}
	}

	c.l0Limits = l0Organizer.FlushSplitKeys()

	smallestSet, largestSet := false, false
	updatePointBounds := func(iter internalIterator) {
		if kv := iter.First(); kv != nil {
			if !smallestSet ||
				base.InternalCompare(c.cmp, c.smallest, kv.K) > 0 {
				smallestSet = true
				c.smallest = kv.K.Clone()
			}
		}
		if kv := iter.Last(); kv != nil {
			if !largestSet ||
				base.InternalCompare(c.cmp, c.largest, kv.K) < 0 {
				largestSet = true
				c.largest = kv.K.Clone()
			}
		}
	}

	updateRangeBounds := func(iter keyspan.FragmentIterator) error {
		// File bounds require s != nil && !s.Empty(). We only need to check for
		// s != nil here, as the memtable's FragmentIterator would never surface
		// empty spans.
		if s, err := iter.First(); err != nil {
			return err
		} else if s != nil {
			if key := s.SmallestKey(); !smallestSet ||
				base.InternalCompare(c.cmp, c.smallest, key) > 0 {
				smallestSet = true
				c.smallest = key.Clone()
			}
		}
		if s, err := iter.Last(); err != nil {
			return err
		} else if s != nil {
			if key := s.LargestKey(); !largestSet ||
				base.InternalCompare(c.cmp, c.largest, key) < 0 {
				largestSet = true
				c.largest = key.Clone()
			}
		}
		return nil
	}

	var flushingBytes uint64
	for i := range flushing {
		f := flushing[i]
		updatePointBounds(f.newIter(nil))
		if rangeDelIter := f.newRangeDelIter(nil); rangeDelIter != nil {
			if err := updateRangeBounds(rangeDelIter); err != nil {
				return nil, err
			}
		}
		if rangeKeyIter := f.newRangeKeyIter(nil); rangeKeyIter != nil {
			if err := updateRangeBounds(rangeKeyIter); err != nil {
				return nil, err
			}
		}
		flushingBytes += f.inuseBytes()
	}

	if opts.FlushSplitBytes > 0 {
		c.maxOutputFileSize = uint64(opts.Level(0).TargetFileSize)
		c.maxOverlapBytes = maxGrandparentOverlapBytes(opts, 0)
		c.grandparents = c.version.Overlaps(baseLevel, c.userKeyBounds())
		adjustGrandparentOverlapBytesForFlush(c, flushingBytes)
	}

	// We don't elide tombstones for flushes.
	c.delElision, c.rangeKeyElision = compact.NoTombstoneElision(), compact.NoTombstoneElision()
	return c, nil
}

func (c *compaction) hasExtraLevelData() bool {
	if len(c.extraLevels) == 0 {
		// not a multi level compaction
		return false
	} else if c.extraLevels[0].files.Empty() {
		// a multi level compaction without data in the intermediate input level;
		// e.g. for a multi level compaction with levels 4,5, and 6, this could
		// occur if there is no files to compact in 5, or in 5 and 6 (i.e. a move).
		return false
	}
	return true
}

// errorOnUserKeyOverlap returns an error if the last two written sstables in
// this compaction have revisions of the same user key present in both sstables,
// when it shouldn't (eg. when splitting flushes).
func (c *compaction) errorOnUserKeyOverlap(ve *versionEdit) error {
	if n := len(ve.NewTables); n > 1 {
		meta := ve.NewTables[n-1].Meta
		prevMeta := ve.NewTables[n-2].Meta
		if !prevMeta.Largest().IsExclusiveSentinel() &&
			c.cmp(prevMeta.Largest().UserKey, meta.Smallest().UserKey) >= 0 {
			return errors.Errorf("pebble: compaction split user key across two sstables: %s in %s and %s",
				prevMeta.Largest().Pretty(c.formatKey),
				prevMeta.FileNum,
				meta.FileNum)
		}
	}
	return nil
}

// allowZeroSeqNum returns true if seqnum's can be zeroed if there are no
// snapshots requiring them to be kept. It performs this determination by
// looking at the TombstoneElision values which are set up based on sstables
// which overlap the bounds of the compaction at a lower level in the LSM.
func (c *compaction) allowZeroSeqNum() bool {
	// TODO(peter): we disable zeroing of seqnums during flushing to match
	// RocksDB behavior and to avoid generating overlapping sstables during
	// DB.replayWAL. When replaying WAL files at startup, we flush after each
	// WAL is replayed building up a single version edit that is
	// applied. Because we don't apply the version edit after each flush, this
	// code doesn't know that L0 contains files and zeroing of seqnums should
	// be disabled. That is fixable, but it seems safer to just match the
	// RocksDB behavior for now.
	return len(c.flushing) == 0 && c.delElision.ElidesEverything() && c.rangeKeyElision.ElidesEverything()
}

// newInputIters returns an iterator over all the input tables in a compaction.
func (c *compaction) newInputIters(
	newIters tableNewIters, newRangeKeyIter keyspanimpl.TableNewSpanIter, iiopts internalIterOpts,
) (
	pointIter internalIterator,
	rangeDelIter, rangeKeyIter keyspan.FragmentIterator,
	retErr error,
) {
	// Validate the ordering of compaction input files for defense in depth.
	if len(c.flushing) == 0 {
		if c.startLevel.level >= 0 {
			err := manifest.CheckOrdering(c.cmp, c.formatKey,
				manifest.Level(c.startLevel.level), c.startLevel.files.Iter())
			if err != nil {
				return nil, nil, nil, err
			}
		}
		err := manifest.CheckOrdering(c.cmp, c.formatKey,
			manifest.Level(c.outputLevel.level), c.outputLevel.files.Iter())
		if err != nil {
			return nil, nil, nil, err
		}
		if c.startLevel.level == 0 {
			if c.startLevel.l0SublevelInfo == nil {
				panic("l0SublevelInfo not created for compaction out of L0")
			}
			for _, info := range c.startLevel.l0SublevelInfo {
				err := manifest.CheckOrdering(c.cmp, c.formatKey,
					info.sublevel, info.Iter())
				if err != nil {
					return nil, nil, nil, err
				}
			}
		}
		if len(c.extraLevels) > 0 {
			if len(c.extraLevels) > 1 {
				panic("n>2 multi level compaction not implemented yet")
			}
			interLevel := c.extraLevels[0]
			err := manifest.CheckOrdering(c.cmp, c.formatKey,
				manifest.Level(interLevel.level), interLevel.files.Iter())
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}

	// There are three classes of keys that a compaction needs to process: point
	// keys, range deletion tombstones and range keys. Collect all iterators for
	// all these classes of keys from all the levels. We'll aggregate them
	// together farther below.
	//
	// numInputLevels is an approximation of the number of iterator levels. Due
	// to idiosyncrasies in iterator construction, we may (rarely) exceed this
	// initial capacity.
	numInputLevels := max(len(c.flushing), len(c.inputs))
	iters := make([]internalIterator, 0, numInputLevels)
	rangeDelIters := make([]keyspan.FragmentIterator, 0, numInputLevels)
	rangeKeyIters := make([]keyspan.FragmentIterator, 0, numInputLevels)

	// If construction of the iterator inputs fails, ensure that we close all
	// the consitutent iterators.
	defer func() {
		if retErr != nil {
			for _, iter := range iters {
				if iter != nil {
					_ = iter.Close()
				}
			}
			for _, rangeDelIter := range rangeDelIters {
				rangeDelIter.Close()
			}
		}
	}()
	iterOpts := IterOptions{
		Category: categoryCompaction,
		logger:   c.logger,
	}

	// Populate iters, rangeDelIters and rangeKeyIters with the appropriate
	// constituent iterators. This depends on whether this is a flush or a
	// compaction.
	if len(c.flushing) != 0 {
		// If flushing, we need to build the input iterators over the memtables
		// stored in c.flushing.
		for i := range c.flushing {
			f := c.flushing[i]
			iters = append(iters, f.newFlushIter(nil))
			rangeDelIter := f.newRangeDelIter(nil)
			if rangeDelIter != nil {
				rangeDelIters = append(rangeDelIters, rangeDelIter)
			}
			if rangeKeyIter := f.newRangeKeyIter(nil); rangeKeyIter != nil {
				rangeKeyIters = append(rangeKeyIters, rangeKeyIter)
			}
		}
	} else {
		addItersForLevel := func(level *compactionLevel, l manifest.Layer) error {
			// Add a *levelIter for point iterators. Because we don't call
			// initRangeDel, the levelIter will close and forget the range
			// deletion iterator when it steps on to a new file. Surfacing range
			// deletions to compactions are handled below.
			iters = append(iters, newLevelIter(context.Background(),
				iterOpts, c.comparer, newIters, level.files.Iter(), l, iiopts))
			// TODO(jackson): Use keyspanimpl.LevelIter to avoid loading all the range
			// deletions into memory upfront. (See #2015, which reverted this.) There
			// will be no user keys that are split between sstables within a level in
			// Cockroach 23.1, which unblocks this optimization.

			// Add the range deletion iterator for each file as an independent level
			// in mergingIter, as opposed to making a levelIter out of those. This
			// is safer as levelIter expects all keys coming from underlying
			// iterators to be in order. Due to compaction / tombstone writing
			// logic in finishOutput(), it is possible for range tombstones to not
			// be strictly ordered across all files in one level.
			//
			// Consider this example from the metamorphic tests (also repeated in
			// finishOutput()), consisting of three L3 files with their bounds
			// specified in square brackets next to the file name:
			//
			// ./000240.sst   [tmgc#391,MERGE-tmgc#391,MERGE]
			// tmgc#391,MERGE [786e627a]
			// tmgc-udkatvs#331,RANGEDEL
			//
			// ./000241.sst   [tmgc#384,MERGE-tmgc#384,MERGE]
			// tmgc#384,MERGE [666c7070]
			// tmgc-tvsalezade#383,RANGEDEL
			// tmgc-tvsalezade#331,RANGEDEL
			//
			// ./000242.sst   [tmgc#383,RANGEDEL-tvsalezade#72057594037927935,RANGEDEL]
			// tmgc-tvsalezade#383,RANGEDEL
			// tmgc#375,SET [72646c78766965616c72776865676e79]
			// tmgc-tvsalezade#356,RANGEDEL
			//
			// Here, the range tombstone in 000240.sst falls "after" one in
			// 000241.sst, despite 000240.sst being ordered "before" 000241.sst for
			// levelIter's purposes. While each file is still consistent before its
			// bounds, it's safer to have all rangedel iterators be visible to
			// mergingIter.
			iter := level.files.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				rangeDelIter, err := c.newRangeDelIter(newIters, iter.Take(), iterOpts, iiopts, l)
				if err != nil {
					// The error will already be annotated with the BackingFileNum, so
					// we annotate it with the FileNum.
					return errors.Wrapf(err, "pebble: could not open table %s", errors.Safe(f.FileNum))
				}
				if rangeDelIter == nil {
					continue
				}
				rangeDelIters = append(rangeDelIters, rangeDelIter)
				c.closers = append(c.closers, rangeDelIter)
			}

			// Check if this level has any range keys.
			hasRangeKeys := false
			for f := iter.First(); f != nil; f = iter.Next() {
				if f.HasRangeKeys {
					hasRangeKeys = true
					break
				}
			}
			if hasRangeKeys {
				newRangeKeyIterWrapper := func(ctx context.Context, file *manifest.TableMetadata, iterOptions keyspan.SpanIterOptions) (keyspan.FragmentIterator, error) {
					rangeKeyIter, err := newRangeKeyIter(ctx, file, iterOptions)
					if err != nil {
						return nil, err
					} else if rangeKeyIter == nil {
						return emptyKeyspanIter, nil
					}
					// Ensure that the range key iter is not closed until the compaction is
					// finished. This is necessary because range key processing
					// requires the range keys to be held in memory for up to the
					// lifetime of the compaction.
					noCloseIter := &noCloseIter{rangeKeyIter}
					c.closers = append(c.closers, noCloseIter)

					// We do not need to truncate range keys to sstable boundaries, or
					// only read within the file's atomic compaction units, unlike with
					// range tombstones. This is because range keys were added after we
					// stopped splitting user keys across sstables, so all the range keys
					// in this sstable must wholly lie within the file's bounds.
					return noCloseIter, err
				}
				li := keyspanimpl.NewLevelIter(
					context.Background(), keyspan.SpanIterOptions{}, c.cmp,
					newRangeKeyIterWrapper, level.files.Iter(), l, manifest.KeyTypeRange,
				)
				rangeKeyIters = append(rangeKeyIters, li)
			}
			return nil
		}

		for i := range c.inputs {
			// If the level is annotated with l0SublevelInfo, expand it into one
			// level per sublevel.
			// TODO(jackson): Perform this expansion even earlier when we pick the
			// compaction?
			if len(c.inputs[i].l0SublevelInfo) > 0 {
				for _, info := range c.startLevel.l0SublevelInfo {
					sublevelCompactionLevel := &compactionLevel{0, info.LevelSlice, nil}
					if err := addItersForLevel(sublevelCompactionLevel, info.sublevel); err != nil {
						return nil, nil, nil, err
					}
				}
				continue
			}
			if err := addItersForLevel(&c.inputs[i], manifest.Level(c.inputs[i].level)); err != nil {
				return nil, nil, nil, err
			}
		}
	}

	// If there's only one constituent point iterator, we can avoid the overhead
	// of a *mergingIter. This is possible, for example, when performing a flush
	// of a single memtable. Otherwise, combine all the iterators into a merging
	// iter.
	pointIter = iters[0]
	if len(iters) > 1 {
		pointIter = newMergingIter(c.logger, &c.stats, c.cmp, nil, iters...)
	}

	// In normal operation, levelIter iterates over the point operations in a
	// level, and initializes a rangeDelIter pointer for the range deletions in
	// each table. During compaction, we want to iterate over the merged view of
	// point operations and range deletions. In order to do this we create one
	// levelIter per level to iterate over the point operations, and collect up
	// all the range deletion files.
	//
	// The range deletion levels are combined with a keyspanimpl.MergingIter. The
	// resulting merged rangedel iterator is then included using an
	// InterleavingIter.
	// TODO(jackson): Consider using a defragmenting iterator to stitch together
	// logical range deletions that were fragmented due to previous file
	// boundaries.
	if len(rangeDelIters) > 0 {
		mi := &keyspanimpl.MergingIter{}
		mi.Init(c.comparer, keyspan.NoopTransform, new(keyspanimpl.MergingBuffers), rangeDelIters...)
		rangeDelIter = mi
	}

	// If there are range key iterators, we need to combine them using
	// keyspanimpl.MergingIter, and then interleave them among the points.
	if len(rangeKeyIters) > 0 {
		mi := &keyspanimpl.MergingIter{}
		mi.Init(c.comparer, keyspan.NoopTransform, new(keyspanimpl.MergingBuffers), rangeKeyIters...)
		// TODO(radu): why do we have a defragmenter here but not above?
		di := &keyspan.DefragmentingIter{}
		di.Init(c.comparer, mi, keyspan.DefragmentInternal, keyspan.StaticDefragmentReducer, new(keyspan.DefragmentingBuffers))
		rangeKeyIter = di
	}
	return pointIter, rangeDelIter, rangeKeyIter, nil
}

func (c *compaction) newRangeDelIter(
	newIters tableNewIters,
	f manifest.LevelFile,
	opts IterOptions,
	iiopts internalIterOpts,
	l manifest.Layer,
) (*noCloseIter, error) {
	opts.layer = l
	iterSet, err := newIters(context.Background(), f.TableMetadata, &opts,
		internalIterOpts{
			compaction: true,
			readEnv:    sstable.ReadEnv{Block: block.ReadEnv{BufferPool: &c.bufferPool}},
		}, iterRangeDeletions)
	if err != nil {
		return nil, err
	} else if iterSet.rangeDeletion == nil {
		// The file doesn't contain any range deletions.
		return nil, nil
	}
	// Ensure that rangeDelIter is not closed until the compaction is
	// finished. This is necessary because range tombstone processing
	// requires the range tombstones to be held in memory for up to the
	// lifetime of the compaction.
	return &noCloseIter{iterSet.rangeDeletion}, nil
}

func (c *compaction) String() string {
	if len(c.flushing) != 0 {
		return "flush\n"
	}

	var buf bytes.Buffer
	for level := c.startLevel.level; level <= c.outputLevel.level; level++ {
		i := level - c.startLevel.level
		fmt.Fprintf(&buf, "%d:", level)
		for f := range c.inputs[i].files.All() {
			fmt.Fprintf(&buf, " %s:%s-%s", f.FileNum, f.Smallest(), f.Largest())
		}
		fmt.Fprintf(&buf, "\n")
	}
	return buf.String()
}

type manualCompaction struct {
	// id is for internal bookkeeping.
	id uint64
	// Count of the retries due to concurrent compaction to overlapping levels.
	retries     int
	level       int
	outputLevel int
	done        chan error
	start       []byte
	end         []byte
	split       bool
}

type readCompaction struct {
	level int
	// [start, end] key ranges are used for de-duping.
	start []byte
	end   []byte

	// The file associated with the compaction.
	// If the file no longer belongs in the same
	// level, then we skip the compaction.
	fileNum base.FileNum
}

func (d *DB) addInProgressCompaction(c *compaction) {
	d.mu.compact.inProgress[c] = struct{}{}
	var isBase, isIntraL0 bool
	for _, cl := range c.inputs {
		for f := range cl.files.All() {
			if f.IsCompacting() {
				d.opts.Logger.Fatalf("L%d->L%d: %s already being compacted", c.startLevel.level, c.outputLevel.level, f.FileNum)
			}
			f.SetCompactionState(manifest.CompactionStateCompacting)
			if c.startLevel != nil && c.outputLevel != nil && c.startLevel.level == 0 {
				if c.outputLevel.level == 0 {
					f.IsIntraL0Compacting = true
					isIntraL0 = true
				} else {
					isBase = true
				}
			}
		}
	}

	if isIntraL0 || isBase {
		l0Inputs := []manifest.LevelSlice{c.startLevel.files}
		if isIntraL0 {
			l0Inputs = append(l0Inputs, c.outputLevel.files)
		}
		if err := d.mu.versions.l0Organizer.UpdateStateForStartedCompaction(l0Inputs, isBase); err != nil {
			d.opts.Logger.Fatalf("could not update state for compaction: %s", err)
		}
	}
}

// Removes compaction markers from files in a compaction. The rollback parameter
// indicates whether the compaction state should be rolled back to its original
// state in the case of an unsuccessful compaction.
//
// DB.mu must be held when calling this method, however this method can drop and
// re-acquire that mutex. All writes to the manifest for this compaction should
// have completed by this point.
func (d *DB) clearCompactingState(c *compaction, rollback bool) {
	c.versionEditApplied = true
	for _, cl := range c.inputs {
		for f := range cl.files.All() {
			if !f.IsCompacting() {
				d.opts.Logger.Fatalf("L%d->L%d: %s not being compacted", c.startLevel.level, c.outputLevel.level, f.FileNum)
			}
			if !rollback {
				// On success all compactions other than move and delete-only compactions
				// transition the file into the Compacted state. Move-compacted files
				// become eligible for compaction again and transition back to NotCompacting.
				// Delete-only compactions could, on rare occasion, leave files untouched
				// (eg. if files have a loose bound), so we revert them all to NotCompacting
				// just in case they need to be compacted again.
				if c.kind != compactionKindMove && c.kind != compactionKindDeleteOnly {
					f.SetCompactionState(manifest.CompactionStateCompacted)
				} else {
					f.SetCompactionState(manifest.CompactionStateNotCompacting)
				}
			} else {
				// Else, on rollback, all input files unconditionally transition back to
				// NotCompacting.
				f.SetCompactionState(manifest.CompactionStateNotCompacting)
			}
			f.IsIntraL0Compacting = false
		}
	}
	l0InProgress := inProgressL0Compactions(d.getInProgressCompactionInfoLocked(c))
	func() {
		// InitCompactingFileInfo requires that no other manifest writes be
		// happening in parallel with it, i.e. we're not in the midst of installing
		// another version. Otherwise, it's possible that we've created another
		// L0Sublevels instance, but not added it to the versions list, causing
		// all the indices in TableMetadata to be inaccurate. To ensure this,
		// grab the manifest lock.
		d.mu.versions.logLock()
		// It is a bit peculiar that we are fiddling with th current version state
		// in a separate critical section from when this version was installed.
		// But this fiddling is necessary if the compaction failed. When the
		// compaction succeeded, we've already done this in UpdateVersionLocked, so
		// this seems redundant. Anyway, we clear the pickedCompactionCache since we
		// may be able to pick a better compaction (though when this compaction
		// succeeded we've also cleared the cache in UpdateVersionLocked).
		defer d.mu.versions.logUnlockAndInvalidatePickedCompactionCache()
		d.mu.versions.l0Organizer.InitCompactingFileInfo(l0InProgress)
	}()
}

func (d *DB) calculateDiskAvailableBytes() uint64 {
	space, err := d.opts.FS.GetDiskUsage(d.dirname)
	if err != nil {
		if !errors.Is(err, vfs.ErrUnsupported) {
			d.opts.EventListener.BackgroundError(err)
		}
		// Return the last value we managed to obtain.
		return d.diskAvailBytes.Load()
	}

	d.lowDiskSpaceReporter.Report(space.AvailBytes, space.TotalBytes, d.opts.EventListener)
	d.diskAvailBytes.Store(space.AvailBytes)
	return space.AvailBytes
}

// maybeScheduleFlush schedules a flush if necessary.
//
// d.mu must be held when calling this.
func (d *DB) maybeScheduleFlush() {
	if d.mu.compact.flushing || d.closed.Load() != nil || d.opts.ReadOnly {
		return
	}
	if len(d.mu.mem.queue) <= 1 {
		return
	}

	if !d.passedFlushThreshold() {
		return
	}

	d.mu.compact.flushing = true
	go d.flush()
}

func (d *DB) passedFlushThreshold() bool {
	var n int
	var size uint64
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
		if d.mu.mem.queue[n].flushForced {
			// A flush was forced. Pretend the memtable size is the configured
			// size. See minFlushSize below.
			size += d.opts.MemTableSize
		} else {
			size += d.mu.mem.queue[n].totalBytes()
		}
	}
	if n == 0 {
		// None of the immutable memtables are ready for flushing.
		return false
	}

	// Only flush once the sum of the queued memtable sizes exceeds half the
	// configured memtable size. This prevents flushing of memtables at startup
	// while we're undergoing the ramp period on the memtable size. See
	// DB.newMemTable().
	minFlushSize := d.opts.MemTableSize / 2
	return size >= minFlushSize
}

func (d *DB) maybeScheduleDelayedFlush(tbl *memTable, dur time.Duration) {
	var mem *flushableEntry
	for _, m := range d.mu.mem.queue {
		if m.flushable == tbl {
			mem = m
			break
		}
	}
	if mem == nil || mem.flushForced {
		return
	}
	deadline := d.timeNow().Add(dur)
	if !mem.delayedFlushForcedAt.IsZero() && deadline.After(mem.delayedFlushForcedAt) {
		// Already scheduled to flush sooner than within `dur`.
		return
	}
	mem.delayedFlushForcedAt = deadline
	go func() {
		timer := time.NewTimer(dur)
		defer timer.Stop()

		select {
		case <-d.closedCh:
			return
		case <-mem.flushed:
			return
		case <-timer.C:
			d.commit.mu.Lock()
			defer d.commit.mu.Unlock()
			d.mu.Lock()
			defer d.mu.Unlock()

			// NB: The timer may fire concurrently with a call to Close.  If a
			// Close call beat us to acquiring d.mu, d.closed holds ErrClosed,
			// and it's too late to flush anything. Otherwise, the Close call
			// will block on locking d.mu until we've finished scheduling the
			// flush and set `d.mu.compact.flushing` to true. Close will wait
			// for the current flush to complete.
			if d.closed.Load() != nil {
				return
			}

			if d.mu.mem.mutable == tbl {
				_ = d.makeRoomForWrite(nil)
			} else {
				mem.flushForced = true
			}
			d.maybeScheduleFlush()
		}
	}()
}

func (d *DB) flush() {
	pprof.Do(context.Background(), flushLabels, func(context.Context) {
		flushingWorkStart := crtime.NowMono()
		d.mu.Lock()
		defer d.mu.Unlock()
		idleDuration := flushingWorkStart.Sub(d.mu.compact.noOngoingFlushStartTime)
		var bytesFlushed uint64
		var err error
		if bytesFlushed, err = d.flush1(); err != nil {
			// TODO(peter): count consecutive flush errors and backoff.
			d.opts.EventListener.BackgroundError(err)
		}
		d.mu.compact.flushing = false
		d.mu.compact.noOngoingFlushStartTime = crtime.NowMono()
		workDuration := d.mu.compact.noOngoingFlushStartTime.Sub(flushingWorkStart)
		d.mu.compact.flushWriteThroughput.Bytes += int64(bytesFlushed)
		d.mu.compact.flushWriteThroughput.WorkDuration += workDuration
		d.mu.compact.flushWriteThroughput.IdleDuration += idleDuration
		// More flush work may have arrived while we were flushing, so schedule
		// another flush if needed.
		d.maybeScheduleFlush()
		// Let the CompactionScheduler know, so that it can react immediately to
		// an increase in DB.GetAllowedWithoutPermission.
		d.opts.Experimental.CompactionScheduler.UpdateGetAllowedWithoutPermission()
		// The flush may have produced too many files in a level, so schedule a
		// compaction if needed.
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
	})
}

// runIngestFlush is used to generate a flush version edit for sstables which
// were ingested as flushables. Both DB.mu and the manifest lock must be held
// while runIngestFlush is called.
func (d *DB) runIngestFlush(c *compaction) (*manifest.VersionEdit, error) {
	if len(c.flushing) != 1 {
		panic("pebble: ingestedFlushable must be flushed one at a time.")
	}

	// Finding the target level for ingestion must use the latest version
	// after the logLock has been acquired.
	c.version = d.mu.versions.currentVersion()

	baseLevel := d.mu.versions.picker.getBaseLevel()
	ve := &versionEdit{}
	var ingestSplitFiles []ingestSplitFile
	ingestFlushable := c.flushing[0].flushable.(*ingestedFlushable)

	updateLevelMetricsOnExcise := func(m *tableMetadata, level int, added []newTableEntry) {
		levelMetrics := c.metrics[level]
		if levelMetrics == nil {
			levelMetrics = &LevelMetrics{}
			c.metrics[level] = levelMetrics
		}
		levelMetrics.TablesCount--
		levelMetrics.TablesSize -= int64(m.Size)
		for i := range added {
			levelMetrics.TablesCount++
			levelMetrics.TablesSize += int64(added[i].Meta.Size)
		}
	}

	suggestSplit := d.opts.Experimental.IngestSplit != nil && d.opts.Experimental.IngestSplit() &&
		d.FormatMajorVersion() >= FormatVirtualSSTables

	if suggestSplit || ingestFlushable.exciseSpan.Valid() {
		// We could add deleted files to ve.
		ve.DeletedTables = make(map[manifest.DeletedTableEntry]*manifest.TableMetadata)
	}

	ctx := context.Background()
	overlapChecker := &overlapChecker{
		comparer: d.opts.Comparer,
		newIters: d.newIters,
		opts: IterOptions{
			logger:   d.opts.Logger,
			Category: categoryIngest,
		},
		v: c.version,
	}
	replacedFiles := make(map[base.FileNum][]newTableEntry)
	for _, file := range ingestFlushable.files {
		var fileToSplit *tableMetadata
		var level int

		// This file fits perfectly within the excise span, so we can slot it at L6.
		if ingestFlushable.exciseSpan.Valid() &&
			ingestFlushable.exciseSpan.Contains(d.cmp, file.Smallest()) &&
			ingestFlushable.exciseSpan.Contains(d.cmp, file.Largest()) {
			level = 6
		} else {
			// TODO(radu): this can perform I/O; we should not do this while holding DB.mu.
			lsmOverlap, err := overlapChecker.DetermineLSMOverlap(ctx, file.UserKeyBounds())
			if err != nil {
				return nil, err
			}
			level, fileToSplit, err = ingestTargetLevel(
				ctx, d.cmp, lsmOverlap, baseLevel, d.mu.compact.inProgress, file, suggestSplit,
			)
			if err != nil {
				return nil, err
			}
		}

		// Add the current flushableIngest file to the version.
		ve.NewTables = append(ve.NewTables, newTableEntry{Level: level, Meta: file})
		if fileToSplit != nil {
			ingestSplitFiles = append(ingestSplitFiles, ingestSplitFile{
				ingestFile: file,
				splitFile:  fileToSplit,
				level:      level,
			})
		}
		levelMetrics := c.metrics[level]
		if levelMetrics == nil {
			levelMetrics = &LevelMetrics{}
			c.metrics[level] = levelMetrics
		}
		levelMetrics.BytesIngested += file.Size
		levelMetrics.TablesIngested++
	}
	if ingestFlushable.exciseSpan.Valid() {
		exciseBounds := ingestFlushable.exciseSpan.UserKeyBounds()
		// Iterate through all levels and find files that intersect with exciseSpan.
		for layer, ls := range c.version.AllLevelsAndSublevels() {
			for m := range ls.Overlaps(d.cmp, ingestFlushable.exciseSpan.UserKeyBounds()).All() {
				leftTable, rightTable, err := d.exciseTable(context.TODO(), exciseBounds, m, layer.Level(), tightExciseBounds)
				if err != nil {
					return nil, err
				}
				newFiles := applyExciseToVersionEdit(ve, m, leftTable, rightTable, layer.Level())
				replacedFiles[m.FileNum] = newFiles
				updateLevelMetricsOnExcise(m, layer.Level(), newFiles)
			}
		}
	}

	if len(ingestSplitFiles) > 0 {
		if err := d.ingestSplit(context.TODO(), ve, updateLevelMetricsOnExcise, ingestSplitFiles, replacedFiles); err != nil {
			return nil, err
		}
	}

	return ve, nil
}

// flush runs a compaction that copies the immutable memtables from memory to
// disk.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) flush1() (bytesFlushed uint64, err error) {
	// NB: The flushable queue can contain flushables of type ingestedFlushable.
	// The sstables in ingestedFlushable.files must be placed into the appropriate
	// level in the lsm. Let's say the flushable queue contains a prefix of
	// regular immutable memtables, then an ingestedFlushable, and then the
	// mutable memtable. When the flush of the ingestedFlushable is performed,
	// it needs an updated view of the lsm. That is, the prefix of immutable
	// memtables must have already been flushed. Similarly, if there are two
	// contiguous ingestedFlushables in the queue, then the first flushable must
	// be flushed, so that the second flushable can see an updated view of the
	// lsm.
	//
	// Given the above, we restrict flushes to either some prefix of regular
	// memtables, or a single flushable of type ingestedFlushable. The DB.flush
	// function will call DB.maybeScheduleFlush again, so a new flush to finish
	// the remaining flush work should be scheduled right away.
	//
	// NB: Large batches placed in the flushable queue share the WAL with the
	// previous memtable in the queue. We must ensure the property that both the
	// large batch and the memtable with which it shares a WAL are flushed
	// together. The property ensures that the minimum unflushed log number
	// isn't incremented incorrectly. Since a flushableBatch.readyToFlush always
	// returns true, and since the large batch will always be placed right after
	// the memtable with which it shares a WAL, the property is naturally
	// ensured. The large batch will always be placed after the memtable with
	// which it shares a WAL because we ensure it in DB.commitWrite by holding
	// the commitPipeline.mu and then holding DB.mu. As an extra defensive
	// measure, if we try to flush the memtable without also flushing the
	// flushable batch in the same flush, since the memtable and flushableBatch
	// have the same logNum, the logNum invariant check below will trigger.
	var n, inputs int
	var inputBytes uint64
	var ingest bool
	for ; n < len(d.mu.mem.queue)-1; n++ {
		if f, ok := d.mu.mem.queue[n].flushable.(*ingestedFlushable); ok {
			if n == 0 {
				// The first flushable is of type ingestedFlushable. Since these
				// must be flushed individually, we perform a flush for just
				// this.
				if !f.readyForFlush() {
					// This check is almost unnecessary, but we guard against it
					// just in case this invariant changes in the future.
					panic("pebble: ingestedFlushable should always be ready to flush.")
				}
				// By setting n = 1, we ensure that the first flushable(n == 0)
				// is scheduled for a flush. The number of tables added is equal to the
				// number of files in the ingest operation.
				n = 1
				inputs = len(f.files)
				ingest = true
				break
			} else {
				// There was some prefix of flushables which weren't of type
				// ingestedFlushable. So, perform a flush for those.
				break
			}
		}
		if !d.mu.mem.queue[n].readyForFlush() {
			break
		}
		inputBytes += d.mu.mem.queue[n].inuseBytes()
	}
	if n == 0 {
		// None of the immutable memtables are ready for flushing.
		return 0, nil
	}
	if !ingest {
		// Flushes of memtables add the prefix of n memtables from the flushable
		// queue.
		inputs = n
	}

	// Require that every memtable being flushed has a log number less than the
	// new minimum unflushed log number.
	minUnflushedLogNum := d.mu.mem.queue[n].logNum
	if !d.opts.DisableWAL {
		for i := 0; i < n; i++ {
			if logNum := d.mu.mem.queue[i].logNum; logNum >= minUnflushedLogNum {
				panic(errors.AssertionFailedf("logNum invariant violated: flushing %d items; %d:type=%T,logNum=%d; %d:type=%T,logNum=%d",
					n,
					i, d.mu.mem.queue[i].flushable, logNum,
					n, d.mu.mem.queue[n].flushable, minUnflushedLogNum))
			}
		}
	}

	c, err := newFlush(d.opts, d.mu.versions.currentVersion(), d.mu.versions.l0Organizer,
		d.mu.versions.picker.getBaseLevel(), d.mu.mem.queue[:n], d.timeNow(), d.determineCompactionValueSeparation)
	if err != nil {
		return 0, err
	}
	d.addInProgressCompaction(c)

	jobID := d.newJobIDLocked()
	info := FlushInfo{
		JobID:      int(jobID),
		Input:      inputs,
		InputBytes: inputBytes,
		Ingest:     ingest,
	}
	d.opts.EventListener.FlushBegin(info)

	startTime := d.timeNow()

	var ve *manifest.VersionEdit
	var stats compact.Stats
	// To determine the target level of the files in the ingestedFlushable, we
	// need to acquire the logLock, and not release it for that duration. Since
	// UpdateVersionLocked acquires it anyway, we create the VersionEdit for
	// ingestedFlushable outside runCompaction. For all other flush cases, we
	// construct the VersionEdit inside runCompaction.
	var compactionErr error
	if c.kind != compactionKindIngestedFlushable {
		ve, stats, compactionErr = d.runCompaction(jobID, c)
	}

	err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
		err := compactionErr
		if c.kind == compactionKindIngestedFlushable {
			ve, err = d.runIngestFlush(c)
		}
		info.Duration = d.timeNow().Sub(startTime)
		if err != nil {
			return versionUpdate{}, err
		}

		validateVersionEdit(ve, d.opts.Comparer.ValidateKey, d.opts.Comparer.FormatKey, d.opts.Logger)
		for i := range ve.NewTables {
			e := &ve.NewTables[i]
			info.Output = append(info.Output, e.Meta.TableInfo())
			// Ingested tables are not necessarily flushed to L0. Record the level of
			// each ingested file explicitly.
			if ingest {
				info.IngestLevels = append(info.IngestLevels, e.Level)
			}
		}

		// The flush succeeded or it produced an empty sstable. In either case we
		// want to bump the minimum unflushed log number to the log number of the
		// oldest unflushed memtable.
		ve.MinUnflushedLogNum = minUnflushedLogNum
		if c.kind != compactionKindIngestedFlushable {
			l0Metrics := c.metrics[0]
			if d.opts.DisableWAL {
				// If the WAL is disabled, every flushable has a zero [logSize],
				// resulting in zero bytes in. Instead, use the number of bytes we
				// flushed as the BytesIn. This ensures we get a reasonable w-amp
				// calculation even when the WAL is disabled.
				l0Metrics.BytesIn = l0Metrics.BytesFlushed
			} else {
				for i := 0; i < n; i++ {
					l0Metrics.BytesIn += d.mu.mem.queue[i].logSize
				}
			}
		} else {
			// c.kind == compactionKindIngestedFlushable && we could have deleted files due
			// to ingest-time splits or excises.
			ingestFlushable := c.flushing[0].flushable.(*ingestedFlushable)
			for c2 := range d.mu.compact.inProgress {
				// Check if this compaction overlaps with the excise span. Note that just
				// checking if the inputs individually overlap with the excise span
				// isn't sufficient; for instance, a compaction could have [a,b] and [e,f]
				// as inputs and write it all out as [a,b,e,f] in one sstable. If we're
				// doing a [c,d) excise at the same time as this compaction, we will have
				// to error out the whole compaction as we can't guarantee it hasn't/won't
				// write a file overlapping with the excise span.
				if ingestFlushable.exciseSpan.OverlapsInternalKeyRange(d.cmp, c2.smallest, c2.largest) {
					c2.cancel.Store(true)
				}
			}

			if len(ve.DeletedTables) > 0 {
				// Iterate through all other compactions, and check if their inputs have
				// been replaced due to an ingest-time split or excise. In that case,
				// cancel the compaction.
				for c2 := range d.mu.compact.inProgress {
					for i := range c2.inputs {
						for f := range c2.inputs[i].files.All() {
							if _, ok := ve.DeletedTables[deletedFileEntry{FileNum: f.FileNum, Level: c2.inputs[i].level}]; ok {
								c2.cancel.Store(true)
								break
							}
						}
					}
				}
			}
		}
		return versionUpdate{
			VE:                      ve,
			JobID:                   jobID,
			Metrics:                 c.metrics,
			InProgressCompactionsFn: func() []compactionInfo { return d.getInProgressCompactionInfoLocked(c) },
		}, nil
	})

	// If err != nil, then the flush will be retried, and we will recalculate
	// these metrics.
	if err == nil {
		d.mu.snapshots.cumulativePinnedCount += stats.CumulativePinnedKeys
		d.mu.snapshots.cumulativePinnedSize += stats.CumulativePinnedSize
		d.mu.versions.metrics.Keys.MissizedTombstonesCount += stats.CountMissizedDels
	}

	d.clearCompactingState(c, err != nil)
	delete(d.mu.compact.inProgress, c)
	d.mu.versions.incrementCompactions(c.kind, c.extraLevels, c.pickerMetrics)

	var flushed flushableList
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewTables)
		if ingest {
			d.mu.versions.metrics.Flush.AsIngestCount++
			for _, l := range c.metrics {
				if l != nil {
					d.mu.versions.metrics.Flush.AsIngestBytes += l.BytesIngested
					d.mu.versions.metrics.Flush.AsIngestTableCount += l.TablesIngested
				}
			}
		}
		d.maybeTransitionSnapshotsToFileOnlyLocked()
	}
	// Signal FlushEnd after installing the new readState. This helps for unit
	// tests that use the callback to trigger a read using an iterator with
	// IterOptions.OnlyReadGuaranteedDurable.
	info.Err = err
	if info.Err == nil && len(ve.NewTables) == 0 {
		info.Err = errEmptyTable
	}
	info.Done = true
	info.TotalDuration = d.timeNow().Sub(startTime)
	d.opts.EventListener.FlushEnd(info)

	// The order of these operations matters here for ease of testing.
	// Removing the reader reference first allows tests to be guaranteed that
	// the memtable reservation has been released by the time a synchronous
	// flush returns. readerUnrefLocked may also produce obsolete files so the
	// call to deleteObsoleteFiles must happen after it.
	for i := range flushed {
		flushed[i].readerUnrefLocked(true)
	}

	d.deleteObsoleteFiles(jobID)

	// Mark all the memtables we flushed as flushed.
	for i := range flushed {
		close(flushed[i].flushed)
	}

	return inputBytes, err
}

// maybeTransitionSnapshotsToFileOnlyLocked transitions any "eventually
// file-only" snapshots to be file-only if all their visible state has been
// flushed to sstables.
//
// REQUIRES: d.mu.
func (d *DB) maybeTransitionSnapshotsToFileOnlyLocked() {
	earliestUnflushedSeqNum := d.getEarliestUnflushedSeqNumLocked()
	currentVersion := d.mu.versions.currentVersion()
	for s := d.mu.snapshots.root.next; s != &d.mu.snapshots.root; {
		if s.efos == nil {
			s = s.next
			continue
		}
		overlapsFlushable := false
		if base.Visible(earliestUnflushedSeqNum, s.efos.seqNum, base.SeqNumMax) {
			// There are some unflushed keys that are still visible to the EFOS.
			// Check if any memtables older than the EFOS contain keys within a
			// protected range of the EFOS. If no, we can transition.
			protectedRanges := make([]bounded, len(s.efos.protectedRanges))
			for i := range s.efos.protectedRanges {
				protectedRanges[i] = s.efos.protectedRanges[i]
			}
			for i := range d.mu.mem.queue {
				if !base.Visible(d.mu.mem.queue[i].logSeqNum, s.efos.seqNum, base.SeqNumMax) {
					// All keys in this memtable are newer than the EFOS. Skip this
					// memtable.
					continue
				}
				// NB: computePossibleOverlaps could have false positives, such as if
				// the flushable is a flushable ingest and not a memtable. In that
				// case we don't open the sstables to check; we just pessimistically
				// assume an overlap.
				d.mu.mem.queue[i].computePossibleOverlaps(func(b bounded) shouldContinue {
					overlapsFlushable = true
					return stopIteration
				}, protectedRanges...)
				if overlapsFlushable {
					break
				}
			}
		}
		if overlapsFlushable {
			s = s.next
			continue
		}
		currentVersion.Ref()

		// NB: s.efos.transitionToFileOnlySnapshot could close s, in which
		// case s.next would be nil. Save it before calling it.
		next := s.next
		_ = s.efos.transitionToFileOnlySnapshot(currentVersion)
		s = next
	}
}

// maybeScheduleCompactionAsync should be used when
// we want to possibly schedule a compaction, but don't
// want to eat the cost of running maybeScheduleCompaction.
// This method should be launched in a separate goroutine.
// d.mu must not be held when this is called.
func (d *DB) maybeScheduleCompactionAsync() {
	defer d.compactionSchedulers.Done()

	d.mu.Lock()
	d.maybeScheduleCompaction()
	d.mu.Unlock()
}

// maybeScheduleCompaction schedules a compaction if necessary.
//
// WARNING: maybeScheduleCompaction and Schedule must be the only ways that
// any compactions are run. These ensure that the pickedCompactionCache is
// used and not stale (by ensuring invalidation is done).
//
// Even compactions that are not scheduled by the CompactionScheduler must be
// run using maybeScheduleCompaction, since starting those compactions needs
// to invalidate the pickedCompactionCache.
//
// Requires d.mu to be held.
func (d *DB) maybeScheduleCompaction() {
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()
	env := d.makeCompactionEnvLocked()
	if env == nil {
		return
	}
	// env.inProgressCompactions will become stale once we pick a compaction, so
	// it needs to be kept fresh. Also, the pickedCompaction in the
	// pickedCompactionCache is not valid if we pick a compaction before using
	// it, since those earlier compactions can mark the same file as compacting.

	// Delete-only compactions are expected to be cheap and reduce future
	// compaction work, so schedule them directly instead of using the
	// CompactionScheduler.
	if d.tryScheduleDeleteOnlyCompaction() {
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
		d.mu.versions.pickedCompactionCache.invalidate()
	}
	// Download compactions have their own concurrency and do not currently
	// interact with CompactionScheduler.
	//
	// TODO(sumeer): integrate with CompactionScheduler, since these consume
	// disk write bandwidth.
	if d.tryScheduleDownloadCompactions(*env, d.opts.MaxConcurrentDownloads()) {
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
		d.mu.versions.pickedCompactionCache.invalidate()
	}
	// The remaining compactions are scheduled by the CompactionScheduler.
	if d.mu.versions.pickedCompactionCache.isWaiting() {
		// CompactionScheduler already knows that the DB is waiting to run a
		// compaction.
		return
	}
	// INVARIANT: !pickedCompactionCache.isWaiting. The following loop will
	// either exit after successfully starting all the compactions it can pick,
	// or will exit with one pickedCompaction in the cache, and isWaiting=true.
	for {
		// Do not have a pickedCompaction in the cache.
		pc := d.pickAnyCompaction(*env)
		if pc == nil {
			return
		}
		success, grantHandle := d.opts.Experimental.CompactionScheduler.TrySchedule()
		if !success {
			// Can't run now, but remember this pickedCompaction in the cache.
			d.mu.versions.pickedCompactionCache.add(pc)
			return
		}
		d.runPickedCompaction(pc, grantHandle)
		env.inProgressCompactions = d.getInProgressCompactionInfoLocked(nil)
	}
}

// makeCompactionEnv attempts to create a compactionEnv necessary during
// compaction picking. If the DB is closed or marked as read-only,
// makeCompactionEnv returns nil to indicate that compactions may not be
// performed. Else, a new compactionEnv is constructed using the current DB
// state.
//
// Compaction picking needs a coherent view of a Version. For example, we need
// to exclude concurrent ingestions from making a decision on which level to
// ingest into that conflicts with our compaction decision.
//
// A pickedCompaction constructed using a compactionEnv must only be used if
// the latest Version has not changed.
//
// REQUIRES: d.mu and d.mu.versions.logLock are held.
func (d *DB) makeCompactionEnvLocked() *compactionEnv {
	if d.closed.Load() != nil || d.opts.ReadOnly {
		return nil
	}
	env := &compactionEnv{
		diskAvailBytes:          d.diskAvailBytes.Load(),
		earliestSnapshotSeqNum:  d.mu.snapshots.earliest(),
		earliestUnflushedSeqNum: d.getEarliestUnflushedSeqNumLocked(),
		inProgressCompactions:   d.getInProgressCompactionInfoLocked(nil),
		readCompactionEnv: readCompactionEnv{
			readCompactions:          &d.mu.compact.readCompactions,
			flushing:                 d.mu.compact.flushing || d.passedFlushThreshold(),
			rescheduleReadCompaction: &d.mu.compact.rescheduleReadCompaction,
		},
	}
	if !d.problemSpans.IsEmpty() {
		env.problemSpans = &d.problemSpans
	}
	return env
}

// pickAnyCompaction tries to pick a manual or automatic compaction.
func (d *DB) pickAnyCompaction(env compactionEnv) (pc *pickedCompaction) {
	// Pick a score-based compaction first, since a misshapen LSM is bad.
	if !d.opts.DisableAutomaticCompactions {
		if pc = d.mu.versions.picker.pickAutoScore(env); pc != nil {
			return pc
		}
	}
	// Pick a manual compaction, if any.
	if pc = d.pickManualCompaction(env); pc != nil {
		return pc
	}
	if !d.opts.DisableAutomaticCompactions {
		return d.mu.versions.picker.pickAutoNonScore(env)
	}
	return nil
}

// runPickedCompaction kicks off the provided pickedCompaction. In case the
// pickedCompaction is a manual compaction, the corresponding manualCompaction
// is removed from d.mu.compact.manual.
//
// REQUIRES: d.mu and d.mu.versions.logLock is held.
func (d *DB) runPickedCompaction(pc *pickedCompaction, grantHandle CompactionGrantHandle) {
	var doneChannel chan error
	if pc.manualID > 0 {
		for i := range d.mu.compact.manual {
			if d.mu.compact.manual[i].id == pc.manualID {
				doneChannel = d.mu.compact.manual[i].done
				d.mu.compact.manual = slices.Delete(d.mu.compact.manual, i, i+1)
				d.mu.compact.manualLen.Store(int32(len(d.mu.compact.manual)))
				break
			}
		}
		if doneChannel == nil {
			panic(errors.AssertionFailedf("did not find manual compaction with id %d", pc.manualID))
		}
	}

	d.mu.compact.compactingCount++
	compaction := newCompaction(pc, d.opts, d.timeNow(), d.ObjProvider(), grantHandle, d.determineCompactionValueSeparation)
	d.addInProgressCompaction(compaction)
	go func() {
		d.compact(compaction, doneChannel)
	}()
}

// Schedule implements DBForCompaction (it is called by the
// CompactionScheduler).
func (d *DB) Schedule(grantHandle CompactionGrantHandle) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()
	isWaiting := d.mu.versions.pickedCompactionCache.isWaiting()
	if !isWaiting {
		return false
	}
	pc := d.mu.versions.pickedCompactionCache.getForRunning()
	if pc == nil {
		env := d.makeCompactionEnvLocked()
		if env != nil {
			pc = d.pickAnyCompaction(*env)
		}
		if pc == nil {
			d.mu.versions.pickedCompactionCache.setNotWaiting()
			return false
		}
	}
	// INVARIANT: pc != nil and is not in the cache. isWaiting is true, since
	// there may be more compactions to run.
	d.runPickedCompaction(pc, grantHandle)
	return true
}

// GetWaitingCompaction implements DBForCompaction (it is called by the
// CompactionScheduler).
func (d *DB) GetWaitingCompaction() (bool, WaitingCompaction) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.versions.logLock()
	defer d.mu.versions.logUnlock()
	isWaiting := d.mu.versions.pickedCompactionCache.isWaiting()
	if !isWaiting {
		return false, WaitingCompaction{}
	}
	pc := d.mu.versions.pickedCompactionCache.peek()
	if pc == nil {
		// Need to pick a compaction.
		env := d.makeCompactionEnvLocked()
		if env != nil {
			pc = d.pickAnyCompaction(*env)
		}
		if pc == nil {
			// Call setNotWaiting so that next call to GetWaitingCompaction can
			// return early.
			d.mu.versions.pickedCompactionCache.setNotWaiting()
			return false, WaitingCompaction{}
		} else {
			d.mu.versions.pickedCompactionCache.add(pc)
		}
	}
	// INVARIANT: pc != nil and is in the cache.
	return true, makeWaitingCompaction(pc.manualID > 0, pc.kind, pc.score)
}

// GetAllowedWithoutPermission implements DBForCompaction (it is called by the
// CompactionScheduler).
func (d *DB) GetAllowedWithoutPermission() int {
	allowedBasedOnBacklog := int(d.mu.versions.curCompactionConcurrency.Load())
	allowedBasedOnManual := 0
	manualBacklog := int(d.mu.compact.manualLen.Load())
	if manualBacklog > 0 {
		maxAllowed := d.opts.MaxConcurrentCompactions()
		allowedBasedOnManual = min(maxAllowed, manualBacklog+allowedBasedOnBacklog)
	}
	return max(allowedBasedOnBacklog, allowedBasedOnManual)
}

// tryScheduleDownloadCompactions tries to start download compactions.
//
// Requires d.mu to be held. Updates d.mu.compact.downloads.
//
// Returns true iff at least one compaction was started.
func (d *DB) tryScheduleDownloadCompactions(env compactionEnv, maxConcurrentDownloads int) bool {
	started := false
	vers := d.mu.versions.currentVersion()
	for i := 0; i < len(d.mu.compact.downloads); {
		if d.mu.compact.downloadingCount >= maxConcurrentDownloads {
			break
		}
		download := d.mu.compact.downloads[i]
		switch d.tryLaunchDownloadCompaction(download, vers, d.mu.versions.l0Organizer, env, maxConcurrentDownloads) {
		case launchedCompaction:
			started = true
			continue
		case didNotLaunchCompaction:
			// See if we can launch a compaction for another download task.
			i++
		case downloadTaskCompleted:
			// Task is completed and must be removed.
			d.mu.compact.downloads = slices.Delete(d.mu.compact.downloads, i, i+1)
		}
	}
	return started
}

func (d *DB) pickManualCompaction(env compactionEnv) (pc *pickedCompaction) {
	v := d.mu.versions.currentVersion()
	for len(d.mu.compact.manual) > 0 {
		manual := d.mu.compact.manual[0]
		pc, retryLater := newPickedManualCompaction(v, d.mu.versions.l0Organizer, d.opts, env, d.mu.versions.picker.getBaseLevel(), manual)
		if pc != nil {
			return pc
		}
		if retryLater {
			// We are not able to run this manual compaction at this time.
			// Inability to run the head blocks later manual compactions.
			manual.retries++
			return nil
		}
		// Manual compaction is a no-op. Signal that it's complete.
		manual.done <- nil
		d.mu.compact.manual = d.mu.compact.manual[1:]
		d.mu.compact.manualLen.Store(int32(len(d.mu.compact.manual)))
	}
	return nil
}

// tryScheduleDeleteOnlyCompaction tries to kick off a delete-only compaction
// for all files that can be deleted as suggested by deletionHints.
//
// Requires d.mu to be held. Updates d.mu.compact.deletionHints.
//
// Returns true iff a compaction was started.
func (d *DB) tryScheduleDeleteOnlyCompaction() bool {
	if d.opts.private.disableDeleteOnlyCompactions || d.opts.DisableAutomaticCompactions ||
		d.mu.compact.compactingCount >= d.opts.MaxConcurrentCompactions() ||
		len(d.mu.compact.deletionHints) == 0 {
		return false
	}
	v := d.mu.versions.currentVersion()
	snapshots := d.mu.snapshots.toSlice()
	// We need to save the value of exciseEnabled in the compaction itself, as
	// it can change dynamically between now and when the compaction runs.
	exciseEnabled := d.FormatMajorVersion() >= FormatVirtualSSTables &&
		d.opts.Experimental.EnableDeleteOnlyCompactionExcises != nil && d.opts.Experimental.EnableDeleteOnlyCompactionExcises()
	inputs, resolvedHints, unresolvedHints := checkDeleteCompactionHints(d.cmp, v, d.mu.compact.deletionHints, snapshots, exciseEnabled)
	d.mu.compact.deletionHints = unresolvedHints

	if len(inputs) > 0 {
		c := newDeleteOnlyCompaction(d.opts, v, inputs, d.timeNow(), resolvedHints, exciseEnabled)
		d.mu.compact.compactingCount++
		d.addInProgressCompaction(c)
		go d.compact(c, nil)
		return true
	}
	return false
}

// deleteCompactionHintType indicates whether the deleteCompactionHint was
// generated from a span containing a range del (point key only), a range key
// delete (range key only), or both a point and range key.
type deleteCompactionHintType uint8

const (
	// NOTE: While these are primarily used as enumeration types, they are also
	// used for some bitwise operations. Care should be taken when updating.
	deleteCompactionHintTypeUnknown deleteCompactionHintType = iota
	deleteCompactionHintTypePointKeyOnly
	deleteCompactionHintTypeRangeKeyOnly
	deleteCompactionHintTypePointAndRangeKey
)

// String implements fmt.Stringer.
func (h deleteCompactionHintType) String() string {
	switch h {
	case deleteCompactionHintTypeUnknown:
		return "unknown"
	case deleteCompactionHintTypePointKeyOnly:
		return "point-key-only"
	case deleteCompactionHintTypeRangeKeyOnly:
		return "range-key-only"
	case deleteCompactionHintTypePointAndRangeKey:
		return "point-and-range-key"
	default:
		panic(fmt.Sprintf("unknown hint type: %d", h))
	}
}

// compactionHintFromKeys returns a deleteCompactionHintType given a slice of
// keyspan.Keys.
func compactionHintFromKeys(keys []keyspan.Key) deleteCompactionHintType {
	var hintType deleteCompactionHintType
	for _, k := range keys {
		switch k.Kind() {
		case base.InternalKeyKindRangeDelete:
			hintType |= deleteCompactionHintTypePointKeyOnly
		case base.InternalKeyKindRangeKeyDelete:
			hintType |= deleteCompactionHintTypeRangeKeyOnly
		default:
			panic(fmt.Sprintf("unsupported key kind: %s", k.Kind()))
		}
	}
	return hintType
}

// A deleteCompactionHint records a user key and sequence number span that has been
// deleted by a range tombstone. A hint is recorded if at least one sstable
// falls completely within both the user key and sequence number spans.
// Once the tombstones and the observed completely-contained sstables fall
// into the same snapshot stripe, a delete-only compaction may delete any
// sstables within the range.
type deleteCompactionHint struct {
	// The type of key span that generated this hint (point key, range key, or
	// both).
	hintType deleteCompactionHintType
	// start and end are user keys specifying a key range [start, end) of
	// deleted keys.
	start []byte
	end   []byte
	// The level of the file containing the range tombstone(s) when the hint
	// was created. Only lower levels need to be searched for files that may
	// be deleted.
	tombstoneLevel int
	// The file containing the range tombstone(s) that created the hint.
	tombstoneFile *tableMetadata
	// The smallest and largest sequence numbers of the abutting tombstones
	// merged to form this hint. All of a tables' keys must be less than the
	// tombstone smallest sequence number to be deleted. All of a tables'
	// sequence numbers must fall into the same snapshot stripe as the
	// tombstone largest sequence number to be deleted.
	tombstoneLargestSeqNum  base.SeqNum
	tombstoneSmallestSeqNum base.SeqNum
	// The smallest sequence number of a sstable that was found to be covered
	// by this hint. The hint cannot be resolved until this sequence number is
	// in the same snapshot stripe as the largest tombstone sequence number.
	// This is set when a hint is created, so the LSM may look different and
	// notably no longer contain the sstable that contained the key at this
	// sequence number.
	fileSmallestSeqNum base.SeqNum
}

type deletionHintOverlap int8

const (
	// hintDoesNotApply indicates that the hint does not apply to the file.
	hintDoesNotApply deletionHintOverlap = iota
	// hintExcisesFile indicates that the hint excises a portion of the file,
	// and the format major version of the DB supports excises.
	hintExcisesFile
	// hintDeletesFile indicates that the hint deletes the entirety of the file.
	hintDeletesFile
)

func (h deleteCompactionHint) String() string {
	return fmt.Sprintf(
		"L%d.%s %s-%s seqnums(tombstone=%d-%d, file-smallest=%d, type=%s)",
		h.tombstoneLevel, h.tombstoneFile.FileNum, h.start, h.end,
		h.tombstoneSmallestSeqNum, h.tombstoneLargestSeqNum, h.fileSmallestSeqNum,
		h.hintType,
	)
}

func (h *deleteCompactionHint) canDeleteOrExcise(
	cmp Compare, m *tableMetadata, snapshots compact.Snapshots, exciseEnabled bool,
) deletionHintOverlap {
	// The file can only be deleted if all of its keys are older than the
	// earliest tombstone aggregated into the hint. Note that we use
	// m.LargestSeqNumAbsolute, not m.LargestSeqNum. Consider a compaction that
	// zeroes sequence numbers. A compaction may zero the sequence number of a
	// key with a sequence number > h.tombstoneSmallestSeqNum and set it to
	// zero. If we looked at m.LargestSeqNum, the resulting output file would
	// appear to not contain any keys more recent than the oldest tombstone. To
	// avoid this error, the largest pre-zeroing sequence number is maintained
	// in LargestSeqNumAbsolute and used here to make the determination whether
	// the file's keys are older than all of the hint's tombstones.
	if m.LargestSeqNumAbsolute >= h.tombstoneSmallestSeqNum || m.SmallestSeqNum < h.fileSmallestSeqNum {
		return hintDoesNotApply
	}

	// The file's oldest key must  be in the same snapshot stripe as the
	// newest tombstone. NB: We already checked the hint's sequence numbers,
	// but this file's oldest sequence number might be lower than the hint's
	// smallest sequence number despite the file falling within the key range
	// if this file was constructed after the hint by a compaction.
	if snapshots.Index(h.tombstoneLargestSeqNum) != snapshots.Index(m.SmallestSeqNum) {
		return hintDoesNotApply
	}

	switch h.hintType {
	case deleteCompactionHintTypePointKeyOnly:
		// A hint generated by a range del span cannot delete tables that contain
		// range keys.
		if m.HasRangeKeys {
			return hintDoesNotApply
		}
	case deleteCompactionHintTypeRangeKeyOnly:
		// A hint generated by a range key del span cannot delete tables that
		// contain point keys.
		if m.HasPointKeys {
			return hintDoesNotApply
		}
	case deleteCompactionHintTypePointAndRangeKey:
		// A hint from a span that contains both range dels *and* range keys can
		// only be deleted if both bounds fall within the hint. The next check takes
		// care of this.
	default:
		panic(fmt.Sprintf("pebble: unknown delete compaction hint type: %d", h.hintType))
	}
	if cmp(h.start, m.Smallest().UserKey) <= 0 &&
		base.UserKeyExclusive(h.end).CompareUpperBounds(cmp, m.UserKeyBounds().End) >= 0 {
		return hintDeletesFile
	}
	if !exciseEnabled {
		// The file's keys must be completely contained within the hint range; excises
		// aren't allowed.
		return hintDoesNotApply
	}
	// Check for any overlap. In cases of partial overlap, we can excise the part of the file
	// that overlaps with the deletion hint.
	if cmp(h.end, m.Smallest().UserKey) > 0 &&
		(m.UserKeyBounds().End.CompareUpperBounds(cmp, base.UserKeyInclusive(h.start)) >= 0) {
		return hintExcisesFile
	}
	return hintDoesNotApply
}

// checkDeleteCompactionHints checks the passed-in deleteCompactionHints for those that
// can be resolved and those that cannot. A hint is considered resolved when its largest
// tombstone sequence number and the smallest sequence number of covered files fall in
// the same snapshot stripe. No more than maxHintsPerDeleteOnlyCompaction will be resolved
// per method call. Resolved and unresolved hints are returned in separate return values.
// The files that the resolved hints apply to, are returned as compactionLevels.
func checkDeleteCompactionHints(
	cmp Compare,
	v *version,
	hints []deleteCompactionHint,
	snapshots compact.Snapshots,
	exciseEnabled bool,
) (levels []compactionLevel, resolved, unresolved []deleteCompactionHint) {
	var files map[*tableMetadata]bool
	var byLevel [numLevels][]*tableMetadata

	// Delete-only compactions can be quadratic (O(mn)) in terms of runtime
	// where m = number of files in the delete-only compaction and n = number
	// of resolved hints. To prevent these from growing unbounded, we cap
	// the number of hints we resolve for one delete-only compaction. This
	// cap only applies if exciseEnabled == true.
	const maxHintsPerDeleteOnlyCompaction = 10

	unresolvedHints := hints[:0]
	// Lazily populate resolvedHints, similar to files above.
	resolvedHints := make([]deleteCompactionHint, 0)
	for _, h := range hints {
		// Check each compaction hint to see if it's resolvable. Resolvable
		// hints are removed and trigger a delete-only compaction if any files
		// in the current LSM still meet their criteria. Unresolvable hints
		// are saved and don't trigger a delete-only compaction.
		//
		// When a compaction hint is created, the sequence numbers of the
		// range tombstones and the covered file with the oldest key are
		// recorded. The largest tombstone sequence number and the smallest
		// file sequence number must be in the same snapshot stripe for the
		// hint to be resolved. The below graphic models a compaction hint
		// covering the keyspace [b, r). The hint completely contains two
		// files, 000002 and 000003. The file 000003 contains the lowest
		// covered sequence number at #90. The tombstone b.RANGEDEL.230:h has
		// the highest tombstone sequence number incorporated into the hint.
		// The hint may be resolved only once the snapshots at #100, #180 and
		// #210 are all closed. File 000001 is not included within the hint
		// because it extends beyond the range tombstones in user key space.
		//
		// 250
		//
		//       |-b...230:h-|
		// _____________________________________________________ snapshot #210
		// 200               |--h.RANGEDEL.200:r--|
		//
		// _____________________________________________________ snapshot #180
		//
		// 150                     +--------+
		//           +---------+   | 000003 |
		//           | 000002  |   |        |
		//           +_________+   |        |
		// 100_____________________|________|___________________ snapshot #100
		//                         +--------+
		// _____________________________________________________ snapshot #70
		//                             +---------------+
		//  50                         | 000001        |
		//                             |               |
		//                             +---------------+
		// ______________________________________________________________
		//     a b c d e f g h i j k l m n o p q r s t u v w x y z

		if snapshots.Index(h.tombstoneLargestSeqNum) != snapshots.Index(h.fileSmallestSeqNum) ||
			(len(resolvedHints) >= maxHintsPerDeleteOnlyCompaction && exciseEnabled) {
			// Cannot resolve yet.
			unresolvedHints = append(unresolvedHints, h)
			continue
		}

		// The hint h will be resolved and dropped, if it either affects no files at all
		// or if the number of files it creates (eg. through excision) is less than or
		// equal to the number of files it deletes. First, determine how many files are
		// affected by this hint.
		filesDeletedByCurrentHint := 0
		var filesDeletedByLevel [7][]*tableMetadata
		for l := h.tombstoneLevel + 1; l < numLevels; l++ {
			for m := range v.Overlaps(l, base.UserKeyBoundsEndExclusive(h.start, h.end)).All() {
				doesHintApply := h.canDeleteOrExcise(cmp, m, snapshots, exciseEnabled)
				if m.IsCompacting() || doesHintApply == hintDoesNotApply || files[m] {
					continue
				}
				switch doesHintApply {
				case hintDeletesFile:
					filesDeletedByCurrentHint++
				case hintExcisesFile:
					// Account for the original file being deleted.
					filesDeletedByCurrentHint++
					// An excise could produce up to 2 new files. If the hint
					// leaves a fragment of the file on the left, decrement
					// the counter once. If the hint leaves a fragment of the
					// file on the right, decrement the counter once.
					if cmp(h.start, m.Smallest().UserKey) > 0 {
						filesDeletedByCurrentHint--
					}
					if m.UserKeyBounds().End.IsUpperBoundFor(cmp, h.end) {
						filesDeletedByCurrentHint--
					}
				}
				filesDeletedByLevel[l] = append(filesDeletedByLevel[l], m)
			}
		}
		if filesDeletedByCurrentHint < 0 {
			// This hint does not delete a sufficient number of files to warrant
			// a delete-only compaction at this stage. Drop it (ie. don't add it
			// to either resolved or unresolved hints) so it doesn't stick around
			// forever.
			continue
		}
		// This hint will be resolved and dropped.
		for l := h.tombstoneLevel + 1; l < numLevels; l++ {
			byLevel[l] = append(byLevel[l], filesDeletedByLevel[l]...)
			for _, m := range filesDeletedByLevel[l] {
				if files == nil {
					// Construct files lazily, assuming most calls will not
					// produce delete-only compactions.
					files = make(map[*tableMetadata]bool)
				}
				files[m] = true
			}
		}
		resolvedHints = append(resolvedHints, h)
	}

	var compactLevels []compactionLevel
	for l, files := range byLevel {
		if len(files) == 0 {
			continue
		}
		compactLevels = append(compactLevels, compactionLevel{
			level: l,
			files: manifest.NewLevelSliceKeySorted(cmp, files),
		})
	}
	return compactLevels, resolvedHints, unresolvedHints
}

func (d *DB) compactionPprofLabels(c *compaction) pprof.LabelSet {
	activity := "compact"
	if len(c.flushing) != 0 {
		activity = "flush"
	}
	level := "L?"
	// Delete-only compactions don't have an output level.
	if c.outputLevel != nil {
		level = fmt.Sprintf("L%d", c.outputLevel.level)
	}
	if kc := d.opts.Experimental.UserKeyCategories; kc.Len() > 0 {
		cat := kc.CategorizeKeyRange(c.smallest.UserKey, c.largest.UserKey)
		return pprof.Labels("pebble", activity, "output-level", level, "key-type", cat)
	}
	return pprof.Labels("pebble", activity, "output-level", level)
}

// compact runs one compaction and maybe schedules another call to compact.
func (d *DB) compact(c *compaction, errChannel chan error) {
	pprof.Do(context.Background(), d.compactionPprofLabels(c), func(context.Context) {
		d.mu.Lock()
		c.grantHandle.Started()
		if err := d.compact1(c, errChannel); err != nil {
			d.handleCompactFailure(c, err)
		}
		if c.isDownload {
			d.mu.compact.downloadingCount--
		} else {
			d.mu.compact.compactingCount--
		}
		delete(d.mu.compact.inProgress, c)
		// Add this compaction's duration to the cumulative duration. NB: This
		// must be atomic with the above removal of c from
		// d.mu.compact.InProgress to ensure Metrics.Compact.Duration does not
		// miss or double count a completing compaction's duration.
		d.mu.compact.duration += d.timeNow().Sub(c.beganAt)
		d.mu.Unlock()
		// Done must not be called while holding any lock that needs to be
		// acquired by Schedule. Also, it must be called after new Version has
		// been installed, and metadata related to compactingCount and inProgress
		// compactions has been updated. This is because when we are running at
		// the limit of permitted compactions, Done can cause the
		// CompactionScheduler to schedule another compaction. Note that the only
		// compactions that may be scheduled by Done are those integrated with the
		// CompactionScheduler.
		c.grantHandle.Done()
		c.grantHandle = nil
		// The previous compaction may have produced too many files in a level, so
		// reschedule another compaction if needed.
		//
		// The preceding Done call will not necessarily cause a compaction to be
		// scheduled, so we also need to call maybeScheduleCompaction. And
		// maybeScheduleCompaction encompasses all compactions, and not only those
		// scheduled via the CompactionScheduler.
		d.mu.Lock()
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
		d.mu.Unlock()
	})
}

func (d *DB) handleCompactFailure(c *compaction, err error) {
	if errors.Is(err, ErrCancelledCompaction) {
		// ErrCancelledCompaction is expected during normal operation, so we don't
		// want to report it as a background error.
		d.opts.Logger.Infof("%v", err)
		return
	}

	// Record problem spans for a short duration, unless the error is a corruption.
	expiration := 30 * time.Second
	if IsCorruptionError(err) {
		// TODO(radu): ideally, we should be using the corruption reporting
		// mechanism which has a tighter span for the corruption. We would need to
		// somehow plumb the level of the file.
		expiration = 5 * time.Minute
	}
	for i := range c.inputs {
		level := c.inputs[i].level
		if level == 0 {
			// We do not set problem spans on L0, as they could block flushes.
			continue
		}
		it := c.inputs[i].files.Iter()
		for f := it.First(); f != nil; f = it.Next() {
			d.problemSpans.Add(level, f.UserKeyBounds(), expiration)
		}
	}

	// TODO(peter): count consecutive compaction errors and backoff.
	d.opts.EventListener.BackgroundError(err)
}

// cleanupVersionEdit cleans up any on-disk artifacts that were created
// for the application of a versionEdit that is no longer going to be applied.
//
// d.mu must be held when calling this method.
func (d *DB) cleanupVersionEdit(ve *versionEdit) {
	obsoleteFiles := manifest.ObsoleteFiles{
		FileBackings: make([]*fileBacking, 0, len(ve.NewTables)),
		BlobFiles:    make([]*manifest.BlobFileMetadata, 0, len(ve.NewBlobFiles)),
	}
	deletedFiles := make(map[base.FileNum]struct{})
	for key := range ve.DeletedTables {
		deletedFiles[key.FileNum] = struct{}{}
	}
	for i := range ve.NewBlobFiles {
		obsoleteFiles.AddBlob(ve.NewBlobFiles[i])
		d.mu.versions.zombieBlobs.Add(objectInfo{
			fileInfo: fileInfo{
				FileNum:  ve.NewBlobFiles[i].FileNum,
				FileSize: ve.NewBlobFiles[i].Size,
			},
			isLocal: objstorage.IsLocalBlobFile(d.objProvider, ve.NewBlobFiles[i].FileNum),
		})
	}
	for i := range ve.NewTables {
		if ve.NewTables[i].Meta.Virtual {
			// We handle backing files separately.
			continue
		}
		if _, ok := deletedFiles[ve.NewTables[i].Meta.FileNum]; ok {
			// This file is being moved in this ve to a different level.
			// Don't mark it as obsolete.
			continue
		}
		obsoleteFiles.AddBacking(ve.NewTables[i].Meta.PhysicalMeta().FileBacking)
	}
	for i := range ve.CreatedBackingTables {
		if ve.CreatedBackingTables[i].IsUnused() {
			obsoleteFiles.AddBacking(ve.CreatedBackingTables[i])
		}
	}
	for _, of := range obsoleteFiles.FileBackings {
		// Add this file to zombie tables as well, as the versionSet
		// asserts on whether every obsolete file was at one point
		// marked zombie.
		d.mu.versions.zombieTables.Add(objectInfo{
			fileInfo: fileInfo{
				FileNum:  of.DiskFileNum,
				FileSize: of.Size,
			},
			isLocal: objstorage.IsLocalTable(d.objProvider, of.DiskFileNum),
		})
	}
	d.mu.versions.addObsoleteLocked(obsoleteFiles)
}

// compact1 runs one compaction.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compact1(c *compaction, errChannel chan error) (err error) {
	if errChannel != nil {
		defer func() {
			errChannel <- err
		}()
	}

	jobID := d.newJobIDLocked()
	info := c.makeInfo(jobID)
	d.opts.EventListener.CompactionBegin(info)
	startTime := d.timeNow()

	ve, stats, err := d.runCompaction(jobID, c)

	info.Duration = d.timeNow().Sub(startTime)
	if err == nil {
		validateVersionEdit(ve, d.opts.Comparer.ValidateKey, d.opts.Comparer.FormatKey, d.opts.Logger)
		err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
			// Check if this compaction had a conflicting operation (eg. a d.excise())
			// that necessitates it restarting from scratch. Note that since we hold
			// the manifest lock, we don't expect this bool to change its value
			// as only the holder of the manifest lock will ever write to it.
			if c.cancel.Load() {
				d.mu.versions.metrics.Compact.CancelledCount++
				d.mu.versions.metrics.Compact.CancelledBytes += c.bytesWritten

				err = firstError(err, ErrCancelledCompaction)
				// This is the first time we've seen a cancellation during the
				// life of this compaction (or the original condition on err == nil
				// would not have been true). We should delete any tables already
				// created, as d.runCompaction did not do that.
				d.cleanupVersionEdit(ve)
				// Note that UpdateVersionLocked invalidates the pickedCompactionCache
				// when we return, which is relevant because this failed compaction
				// may be the highest priority to run next.
				return versionUpdate{}, err
			}
			return versionUpdate{
				VE:                      ve,
				JobID:                   jobID,
				Metrics:                 c.metrics,
				InProgressCompactionsFn: func() []compactionInfo { return d.getInProgressCompactionInfoLocked(c) },
			}, nil
		})
	}

	info.Done = true
	info.Err = err
	if err == nil {
		for i := range ve.NewTables {
			e := &ve.NewTables[i]
			info.Output.Tables = append(info.Output.Tables, e.Meta.TableInfo())
		}
		d.mu.snapshots.cumulativePinnedCount += stats.CumulativePinnedKeys
		d.mu.snapshots.cumulativePinnedSize += stats.CumulativePinnedSize
		d.mu.versions.metrics.Keys.MissizedTombstonesCount += stats.CountMissizedDels
	}

	// NB: clearing compacting state must occur before updating the read state;
	// L0Sublevels initialization depends on it.
	d.clearCompactingState(c, err != nil)
	if err != nil && errors.Is(err, ErrCancelledCompaction) {
		d.mu.versions.metrics.Compact.CancelledCount++
		d.mu.versions.metrics.Compact.CancelledBytes += c.bytesWritten
	}
	d.mu.versions.incrementCompactions(c.kind, c.extraLevels, c.pickerMetrics)
	d.mu.versions.incrementCompactionBytes(-c.bytesWritten)

	info.TotalDuration = d.timeNow().Sub(c.beganAt)
	d.opts.EventListener.CompactionEnd(info)

	// Update the read state before deleting obsolete files because the
	// read-state update will cause the previous version to be unref'd and if
	// there are no references obsolete tables will be added to the obsolete
	// table list.
	if err == nil {
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewTables)
	}
	d.deleteObsoleteFiles(jobID)

	return err
}

// runCopyCompaction runs a copy compaction where a new FileNum is created that
// is a byte-for-byte copy of the input file or span thereof in some cases. This
// is used in lieu of a move compaction when a file is being moved across the
// local/remote storage boundary. It could also be used in lieu of a rewrite
// compaction as part of a Download() call, which allows copying only a span of
// the external file, provided the file does not contain range keys or value
// blocks (see sstable.CopySpan).
//
// d.mu must be held when calling this method. The mutex will be released when
// doing IO.
func (d *DB) runCopyCompaction(
	jobID JobID, c *compaction,
) (ve *versionEdit, stats compact.Stats, _ error) {
	iter := c.startLevel.files.Iter()
	inputMeta := iter.First()
	if iter.Next() != nil {
		return nil, compact.Stats{}, base.AssertionFailedf("got more than one file for a move compaction")
	}
	if c.cancel.Load() {
		return nil, compact.Stats{}, ErrCancelledCompaction
	}
	ve = &versionEdit{
		DeletedTables: map[deletedFileEntry]*tableMetadata{
			{Level: c.startLevel.level, FileNum: inputMeta.FileNum}: inputMeta,
		},
	}

	objMeta, err := d.objProvider.Lookup(base.FileTypeTable, inputMeta.FileBacking.DiskFileNum)
	if err != nil {
		return nil, compact.Stats{}, err
	}
	if !objMeta.IsExternal() {
		if objMeta.IsRemote() || !remote.ShouldCreateShared(d.opts.Experimental.CreateOnShared, c.outputLevel.level) {
			panic("pebble: scheduled a copy compaction that is not actually moving files to shared storage")
		}
		// Note that based on logic in the compaction picker, we're guaranteed
		// inputMeta.Virtual is nil.
		if inputMeta.Virtual {
			panic(errors.AssertionFailedf("cannot do a copy compaction of a virtual sstable across local/remote storage"))
		}
	}

	// We are in the relatively more complex case where we need to copy this
	// file to remote storage. Drop the db mutex while we do the copy
	//
	// To ease up cleanup of the local file and tracking of refs, we create
	// a new FileNum. This has the potential of making the block cache less
	// effective, however.
	newMeta := &tableMetadata{
		Size:                     inputMeta.Size,
		CreationTime:             inputMeta.CreationTime,
		SmallestSeqNum:           inputMeta.SmallestSeqNum,
		LargestSeqNum:            inputMeta.LargestSeqNum,
		LargestSeqNumAbsolute:    inputMeta.LargestSeqNumAbsolute,
		Stats:                    inputMeta.Stats,
		Virtual:                  inputMeta.Virtual,
		SyntheticPrefixAndSuffix: inputMeta.SyntheticPrefixAndSuffix,
	}
	if inputMeta.HasPointKeys {
		newMeta.ExtendPointKeyBounds(c.cmp, inputMeta.SmallestPointKey, inputMeta.LargestPointKey)
	}
	if inputMeta.HasRangeKeys {
		newMeta.ExtendRangeKeyBounds(c.cmp, inputMeta.SmallestRangeKey, inputMeta.LargestRangeKey)
	}
	newMeta.FileNum = d.mu.versions.getNextFileNum()
	if objMeta.IsExternal() {
		// external -> local/shared copy. File must be virtual.
		// We will update this size later after we produce the new backing file.
		newMeta.InitVirtualBacking(base.DiskFileNum(newMeta.FileNum), inputMeta.FileBacking.Size)
	} else {
		// local -> shared copy. New file is guaranteed to not be virtual.
		newMeta.InitPhysicalBacking()
	}

	// Before dropping the db mutex, grab a ref to the current version. This
	// prevents any concurrent excises from deleting files that this compaction
	// needs to read/maintain a reference to.
	vers := d.mu.versions.currentVersion()
	vers.Ref()
	defer vers.UnrefLocked()

	// NB: The order here is reversed, lock after unlock. This is similar to
	// runCompaction.
	d.mu.Unlock()
	defer d.mu.Lock()

	deleteOnExit := false
	defer func() {
		if deleteOnExit {
			_ = d.objProvider.Remove(base.FileTypeTable, newMeta.FileBacking.DiskFileNum)
		}
	}()

	// If the src obj is external, we're doing an external to local/shared copy.
	if objMeta.IsExternal() {
		ctx := context.TODO()
		src, err := d.objProvider.OpenForReading(
			ctx, base.FileTypeTable, inputMeta.FileBacking.DiskFileNum, objstorage.OpenOptions{},
		)
		if err != nil {
			return nil, compact.Stats{}, err
		}
		defer func() {
			if src != nil {
				_ = src.Close()
			}
		}()

		w, _, err := d.objProvider.Create(
			ctx, base.FileTypeTable, newMeta.FileBacking.DiskFileNum,
			objstorage.CreateOptions{
				PreferSharedStorage: remote.ShouldCreateShared(d.opts.Experimental.CreateOnShared, c.outputLevel.level),
			},
		)
		if err != nil {
			return nil, compact.Stats{}, err
		}
		deleteOnExit = true

		start, end := newMeta.Smallest(), newMeta.Largest()
		if newMeta.SyntheticPrefixAndSuffix.HasPrefix() {
			syntheticPrefix := newMeta.SyntheticPrefixAndSuffix.Prefix()
			start.UserKey = syntheticPrefix.Invert(start.UserKey)
			end.UserKey = syntheticPrefix.Invert(end.UserKey)
		}
		if newMeta.SyntheticPrefixAndSuffix.HasSuffix() {
			// Extend the bounds as necessary so that the keys don't include suffixes.
			start.UserKey = start.UserKey[:c.comparer.Split(start.UserKey)]
			if n := c.comparer.Split(end.UserKey); n < len(end.UserKey) {
				end = base.MakeRangeDeleteSentinelKey(c.comparer.ImmediateSuccessor(nil, end.UserKey[:n]))
			}
		}

		// NB: external files are always virtual.
		var wrote uint64
		err = d.fileCache.withReader(ctx, block.NoReadEnv, inputMeta.VirtualMeta(), func(r *sstable.Reader, env sstable.ReadEnv) error {
			var err error
			// TODO(radu): plumb a ReadEnv to CopySpan (it could use the buffer pool
			// or update category stats).
			wrote, err = sstable.CopySpan(ctx,
				src, r, d.opts.MakeReaderOptions(),
				w, d.opts.MakeWriterOptions(c.outputLevel.level, d.TableFormat()),
				start, end,
			)
			return err
		})

		src = nil // We passed src to CopySpan; it's responsible for closing it.
		if err != nil {
			if errors.Is(err, sstable.ErrEmptySpan) {
				// The virtual table was empty. Just remove the backing file.
				// Note that deleteOnExit is true so we will delete the created object.
				c.metrics[c.outputLevel.level] = &LevelMetrics{
					BytesIn: inputMeta.Size,
				}

				return ve, compact.Stats{}, nil
			}
			return nil, compact.Stats{}, err
		}
		newMeta.FileBacking.Size = wrote
		newMeta.Size = wrote
	} else {
		_, err := d.objProvider.LinkOrCopyFromLocal(context.TODO(), d.opts.FS,
			d.objProvider.Path(objMeta), base.FileTypeTable, newMeta.FileBacking.DiskFileNum,
			objstorage.CreateOptions{PreferSharedStorage: true})
		if err != nil {
			return nil, compact.Stats{}, err
		}
		deleteOnExit = true
	}
	ve.NewTables = []newTableEntry{{
		Level: c.outputLevel.level,
		Meta:  newMeta,
	}}
	if newMeta.Virtual {
		ve.CreatedBackingTables = []*fileBacking{newMeta.FileBacking}
	}
	c.metrics[c.outputLevel.level] = &LevelMetrics{
		BytesIn:         inputMeta.Size,
		BytesCompacted:  newMeta.Size,
		TablesCompacted: 1,
	}

	if err := d.objProvider.Sync(); err != nil {
		return nil, compact.Stats{}, err
	}
	deleteOnExit = false
	return ve, compact.Stats{}, nil
}

// applyHintOnFile applies a deleteCompactionHint to a file, and updates the
// versionEdit accordingly. It returns a list of new files that were created
// if the hint was applied partially to a file (eg. through an exciseTable as opposed
// to an outright deletion). levelMetrics is kept up-to-date with the number
// of tables deleted or excised.
func (d *DB) applyHintOnFile(
	h deleteCompactionHint,
	f *tableMetadata,
	level int,
	levelMetrics *LevelMetrics,
	ve *versionEdit,
	hintOverlap deletionHintOverlap,
) (newFiles []manifest.NewTableEntry, err error) {
	if hintOverlap == hintDoesNotApply {
		return nil, nil
	}

	// The hint overlaps with at least part of the file.
	if hintOverlap == hintDeletesFile {
		// The hint deletes the entirety of this file.
		ve.DeletedTables[deletedFileEntry{
			Level:   level,
			FileNum: f.FileNum,
		}] = f
		levelMetrics.TablesDeleted++
		return nil, nil
	}
	// The hint overlaps with only a part of the file, not the entirety of it. We need
	// to use d.exciseTable. (hintOverlap == hintExcisesFile)
	if d.FormatMajorVersion() < FormatVirtualSSTables {
		panic("pebble: delete-only compaction hint excising a file is not supported in this version")
	}

	levelMetrics.TablesExcised++
	exciseBounds := base.UserKeyBoundsEndExclusive(h.start, h.end)
	leftTable, rightTable, err := d.exciseTable(context.TODO(), exciseBounds, f, level, tightExciseBounds)
	if err != nil {
		return nil, errors.Wrap(err, "error when running excise for delete-only compaction")
	}
	newFiles = applyExciseToVersionEdit(ve, f, leftTable, rightTable, level)
	return newFiles, nil
}

func (d *DB) runDeleteOnlyCompactionForLevel(
	cl compactionLevel,
	levelMetrics *LevelMetrics,
	ve *versionEdit,
	snapshots compact.Snapshots,
	fragments []deleteCompactionHintFragment,
	exciseEnabled bool,
) error {
	if cl.level == 0 {
		panic("cannot run delete-only compaction for L0")
	}
	curFragment := 0

	// Outer loop loops on files. Middle loop loops on fragments. Inner loop
	// loops on raw fragments of hints. Number of fragments are bounded by
	// the number of hints this compaction was created with, which is capped
	// in the compaction picker to avoid very CPU-hot loops here.
	for f := range cl.files.All() {
		// curFile usually matches f, except if f got excised in which case
		// it maps to a virtual file that replaces f, or nil if f got removed
		// in its entirety.
		curFile := f
		for curFragment < len(fragments) && d.cmp(fragments[curFragment].start, f.Smallest().UserKey) <= 0 {
			curFragment++
		}
		if curFragment > 0 {
			curFragment--
		}

		for ; curFragment < len(fragments); curFragment++ {
			if f.UserKeyBounds().End.CompareUpperBounds(d.cmp, base.UserKeyInclusive(fragments[curFragment].start)) < 0 {
				break
			}
			// Process all overlapping hints with this file. Note that applying
			// a hint twice is idempotent; curFile should have already been excised
			// the first time, resulting in no change the second time.
			for _, h := range fragments[curFragment].hints {
				if h.tombstoneLevel >= cl.level {
					// We cannot excise out the deletion tombstone itself, or anything
					// above it.
					continue
				}
				hintOverlap := h.canDeleteOrExcise(d.cmp, curFile, snapshots, exciseEnabled)
				if hintOverlap == hintDoesNotApply {
					continue
				}
				newFiles, err := d.applyHintOnFile(h, curFile, cl.level, levelMetrics, ve, hintOverlap)
				if err != nil {
					return err
				}
				if _, ok := ve.DeletedTables[manifest.DeletedTableEntry{Level: cl.level, FileNum: curFile.FileNum}]; ok {
					curFile = nil
				}
				if len(newFiles) > 0 {
					curFile = newFiles[len(newFiles)-1].Meta
				} else if curFile == nil {
					// Nothing remains of the file.
					break
				}
			}
			if curFile == nil {
				// Nothing remains of the file.
				break
			}
		}
		if _, ok := ve.DeletedTables[deletedFileEntry{
			Level:   cl.level,
			FileNum: f.FileNum,
		}]; !ok {
			panic("pebble: delete-only compaction scheduled with hints that did not delete or excise a file")
		}
	}
	return nil
}

// deleteCompactionHintFragment represents a fragment of the key space and
// contains a set of deleteCompactionHints that apply to that fragment; a
// fragment starts at the start field and ends where the next fragment starts.
type deleteCompactionHintFragment struct {
	start []byte
	hints []deleteCompactionHint
}

// Delete compaction hints can overlap with each other, and multiple fragments
// can apply to a single file. This function takes a list of hints and fragments
// them, to make it easier to apply them to non-overlapping files occupying a level;
// that way, files and hint fragments can be iterated on in lockstep, while efficiently
// being able to apply all hints overlapping with a given file.
func fragmentDeleteCompactionHints(
	cmp Compare, hints []deleteCompactionHint,
) []deleteCompactionHintFragment {
	fragments := make([]deleteCompactionHintFragment, 0, len(hints)*2)
	for i := range hints {
		fragments = append(fragments, deleteCompactionHintFragment{start: hints[i].start},
			deleteCompactionHintFragment{start: hints[i].end})
	}
	slices.SortFunc(fragments, func(i, j deleteCompactionHintFragment) int {
		return cmp(i.start, j.start)
	})
	fragments = slices.CompactFunc(fragments, func(i, j deleteCompactionHintFragment) bool {
		return bytes.Equal(i.start, j.start)
	})
	for _, h := range hints {
		startIdx := sort.Search(len(fragments), func(i int) bool {
			return cmp(fragments[i].start, h.start) >= 0
		})
		endIdx := sort.Search(len(fragments), func(i int) bool {
			return cmp(fragments[i].start, h.end) >= 0
		})
		for i := startIdx; i < endIdx; i++ {
			fragments[i].hints = append(fragments[i].hints, h)
		}
	}
	return fragments
}

// Runs a delete-only compaction.
//
// d.mu must *not* be held when calling this.
func (d *DB) runDeleteOnlyCompaction(
	jobID JobID, c *compaction, snapshots compact.Snapshots,
) (ve *versionEdit, stats compact.Stats, retErr error) {
	fragments := fragmentDeleteCompactionHints(d.cmp, c.deletionHints)
	ve = &versionEdit{
		DeletedTables: map[deletedFileEntry]*tableMetadata{},
	}
	for _, cl := range c.inputs {
		levelMetrics := &LevelMetrics{}
		if err := d.runDeleteOnlyCompactionForLevel(cl, levelMetrics, ve, snapshots, fragments, c.exciseEnabled); err != nil {
			return nil, stats, err
		}
		c.metrics[cl.level] = levelMetrics
	}
	// Remove any files that were added and deleted in the same versionEdit.
	ve.NewTables = slices.DeleteFunc(ve.NewTables, func(e manifest.NewTableEntry) bool {
		deletedFileEntry := manifest.DeletedTableEntry{Level: e.Level, FileNum: e.Meta.FileNum}
		if _, deleted := ve.DeletedTables[deletedFileEntry]; deleted {
			delete(ve.DeletedTables, deletedFileEntry)
			return true
		}
		return false
	})
	// Remove any entries from CreatedBackingTables that are not used in any
	// NewFiles.
	usedBackingFiles := make(map[base.DiskFileNum]struct{})
	for _, e := range ve.NewTables {
		if e.Meta.Virtual {
			usedBackingFiles[e.Meta.FileBacking.DiskFileNum] = struct{}{}
		}
	}
	ve.CreatedBackingTables = slices.DeleteFunc(ve.CreatedBackingTables, func(b *fileBacking) bool {
		_, used := usedBackingFiles[b.DiskFileNum]
		return !used
	})
	// Refresh the disk available statistic whenever a compaction/flush
	// completes, before re-acquiring the mutex.
	d.calculateDiskAvailableBytes()
	return ve, stats, nil
}

func (d *DB) runMoveCompaction(
	jobID JobID, c *compaction,
) (ve *versionEdit, stats compact.Stats, _ error) {
	iter := c.startLevel.files.Iter()
	meta := iter.First()
	if iter.Next() != nil {
		return nil, stats, base.AssertionFailedf("got more than one file for a move compaction")
	}
	if c.cancel.Load() {
		return ve, stats, ErrCancelledCompaction
	}
	c.metrics[c.outputLevel.level] = &LevelMetrics{
		BytesMoved:  meta.Size,
		TablesMoved: 1,
	}
	ve = &versionEdit{
		DeletedTables: map[deletedFileEntry]*tableMetadata{
			{Level: c.startLevel.level, FileNum: meta.FileNum}: meta,
		},
		NewTables: []newTableEntry{
			{Level: c.outputLevel.level, Meta: meta},
		},
	}

	return ve, stats, nil
}

// runCompaction runs a compaction that produces new on-disk tables from
// memtables or old on-disk tables.
//
// runCompaction cannot be used for compactionKindIngestedFlushable.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) runCompaction(
	jobID JobID, c *compaction,
) (ve *versionEdit, stats compact.Stats, retErr error) {
	if c.cancel.Load() {
		return ve, stats, ErrCancelledCompaction
	}
	switch c.kind {
	case compactionKindDeleteOnly:
		// Before dropping the db mutex, grab a ref to the current version. This
		// prevents any concurrent excises from deleting files that this compaction
		// needs to read/maintain a reference to.
		//
		// Note that delete-only compactions can call excise(), which needs to be able
		// to read these files.
		vers := d.mu.versions.currentVersion()
		vers.Ref()
		defer vers.UnrefLocked()
		// Release the d.mu lock while doing I/O.
		// Note the unusual order: Unlock and then Lock.
		snapshots := d.mu.snapshots.toSlice()
		d.mu.Unlock()
		defer d.mu.Lock()
		return d.runDeleteOnlyCompaction(jobID, c, snapshots)
	case compactionKindMove:
		return d.runMoveCompaction(jobID, c)
	case compactionKindCopy:
		return d.runCopyCompaction(jobID, c)
	case compactionKindIngestedFlushable:
		panic("pebble: runCompaction cannot handle compactionKindIngestedFlushable.")
	}

	snapshots := d.mu.snapshots.toSlice()

	if c.flushing == nil {
		// Before dropping the db mutex, grab a ref to the current version. This
		// prevents any concurrent excises from deleting files that this compaction
		// needs to read/maintain a reference to.
		//
		// Note that unlike user iterators, compactionIter does not maintain a ref
		// of the version or read state.
		vers := d.mu.versions.currentVersion()
		vers.Ref()
		defer vers.UnrefLocked()
	}

	// The table is typically written at the maximum allowable format implied by
	// the current format major version of the DB, but Options may define
	// additional constraints.
	tableFormat := d.TableFormat()

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	// Determine whether we should separate values into blob files.
	//
	// TODO(jackson): Currently we never separate values in non-tests. Choose
	// and initialize the appropriate ValueSeparation implementation based on
	// Options and the compaction inputs.
	valueSeparation := c.getValueSeparation(jobID, c, tableFormat)

	result := d.compactAndWrite(jobID, c, snapshots, tableFormat, valueSeparation)
	if result.Err == nil {
		ve, result.Err = c.makeVersionEdit(result)
	}
	if result.Err != nil {
		// Delete any created tables or blob files.
		obsoleteFiles := manifest.ObsoleteFiles{
			FileBackings: make([]*fileBacking, 0, len(result.Tables)),
			BlobFiles:    make([]*manifest.BlobFileMetadata, 0, len(result.Blobs)),
		}
		d.mu.Lock()
		for i := range result.Tables {
			backing := &fileBacking{
				DiskFileNum: result.Tables[i].ObjMeta.DiskFileNum,
				Size:        result.Tables[i].WriterMeta.Size,
			}
			obsoleteFiles.AddBacking(backing)
			// Add this file to zombie tables as well, as the versionSet
			// asserts on whether every obsolete file was at one point
			// marked zombie.
			d.mu.versions.zombieTables.AddMetadata(&result.Tables[i].ObjMeta, backing.Size)
		}
		for i := range result.Blobs {
			obsoleteFiles.AddBlob(result.Blobs[i].Metadata)
			// Add this file to zombie blobs as well, as the versionSet
			// asserts on whether every obsolete file was at one point
			// marked zombie.
			d.mu.versions.zombieBlobs.AddMetadata(&result.Blobs[i].ObjMeta, result.Blobs[i].Metadata.Size)
		}
		d.mu.versions.addObsoleteLocked(obsoleteFiles)
		d.mu.Unlock()
	}
	// Refresh the disk available statistic whenever a compaction/flush
	// completes, before re-acquiring the mutex.
	d.calculateDiskAvailableBytes()
	return ve, result.Stats, result.Err
}

// compactAndWrite runs the data part of a compaction, where we set up a
// compaction iterator and use it to write output tables.
func (d *DB) compactAndWrite(
	jobID JobID,
	c *compaction,
	snapshots compact.Snapshots,
	tableFormat sstable.TableFormat,
	valueSeparation compact.ValueSeparation,
) (result compact.Result) {
	// Compactions use a pool of buffers to read blocks, avoiding polluting the
	// block cache with blocks that will not be read again. We initialize the
	// buffer pool with a size 12. This initial size does not need to be
	// accurate, because the pool will grow to accommodate the maximum number of
	// blocks allocated at a given time over the course of the compaction. But
	// choosing a size larger than that working set avoids any additional
	// allocations to grow the size of the pool over the course of iteration.
	//
	// Justification for initial size 12: In a two-level compaction, at any
	// given moment we'll have 2 index blocks in-use and 2 data blocks in-use.
	// Additionally, when decoding a compressed block, we'll temporarily
	// allocate 1 additional block to hold the compressed buffer. In the worst
	// case that all input sstables have two-level index blocks (+2), value
	// blocks (+2), range deletion blocks (+n) and range key blocks (+n), we'll
	// additionally require 2n+4 blocks where n is the number of input sstables.
	// Range deletion and range key blocks are relatively rare, and the cost of
	// an additional allocation or two over the course of the compaction is
	// considered to be okay. A larger initial size would cause the pool to hold
	// on to more memory, even when it's not in-use because the pool will
	// recycle buffers up to the current capacity of the pool. The memory use of
	// a 12-buffer pool is expected to be within reason, even if all the buffers
	// grow to the typical size of an index block (256 KiB) which would
	// translate to 3 MiB per compaction.
	c.bufferPool.Init(12)
	defer c.bufferPool.Release()
	blockReadEnv := block.ReadEnv{
		BufferPool: &c.bufferPool,
		Stats:      &c.stats,
		IterStats: d.fileCache.SSTStatsCollector().Accumulator(
			uint64(uintptr(unsafe.Pointer(c))),
			categoryCompaction,
		),
	}
	c.valueFetcher.Init(d.fileCache, blockReadEnv)
	iiopts := internalIterOpts{
		compaction:       true,
		readEnv:          sstable.ReadEnv{Block: blockReadEnv},
		blobValueFetcher: &c.valueFetcher,
	}
	defer func() { _ = c.valueFetcher.Close() }()

	pointIter, rangeDelIter, rangeKeyIter, err := c.newInputIters(d.newIters, d.tableNewRangeKeyIter, iiopts)
	defer func() {
		for _, closer := range c.closers {
			closer.FragmentIterator.Close()
		}
	}()
	if err != nil {
		return compact.Result{Err: err}
	}
	c.allowedZeroSeqNum = c.allowZeroSeqNum()
	cfg := compact.IterConfig{
		Comparer:         c.comparer,
		Merge:            d.merge,
		TombstoneElision: c.delElision,
		RangeKeyElision:  c.rangeKeyElision,
		Snapshots:        snapshots,
		AllowZeroSeqNum:  c.allowedZeroSeqNum,
		IneffectualSingleDeleteCallback: func(userKey []byte) {
			d.opts.EventListener.PossibleAPIMisuse(PossibleAPIMisuseInfo{
				Kind:    IneffectualSingleDelete,
				UserKey: slices.Clone(userKey),
			})
		},
		NondeterministicSingleDeleteCallback: func(userKey []byte) {
			d.opts.EventListener.PossibleAPIMisuse(PossibleAPIMisuseInfo{
				Kind:    NondeterministicSingleDelete,
				UserKey: slices.Clone(userKey),
			})
		},
	}
	iter := compact.NewIter(cfg, pointIter, rangeDelIter, rangeKeyIter)

	runnerCfg := compact.RunnerConfig{
		CompactionBounds:           base.UserKeyBoundsFromInternal(c.smallest, c.largest),
		L0SplitKeys:                c.l0Limits,
		Grandparents:               c.grandparents,
		MaxGrandparentOverlapBytes: c.maxOverlapBytes,
		TargetOutputFileSize:       c.maxOutputFileSize,
		GrantHandle:                c.grantHandle,
		ValueSeparation:            valueSeparation,
	}
	runner := compact.NewRunner(runnerCfg, iter)
	for runner.MoreDataToWrite() {
		if c.cancel.Load() {
			return runner.Finish().WithError(ErrCancelledCompaction)
		}
		// Create a new table.
		writerOpts := d.opts.MakeWriterOptions(c.outputLevel.level, tableFormat)
		objMeta, tw, err := d.newCompactionOutputTable(jobID, c, writerOpts)
		if err != nil {
			return runner.Finish().WithError(err)
		}
		runner.WriteTable(objMeta, tw)
	}
	result = runner.Finish()
	if result.Err == nil {
		result.Err = d.objProvider.Sync()
	}
	return result
}

// makeVersionEdit creates the version edit for a compaction, based on the
// tables in compact.Result.
func (c *compaction) makeVersionEdit(result compact.Result) (*versionEdit, error) {
	ve := &versionEdit{
		DeletedTables: map[deletedFileEntry]*tableMetadata{},
	}
	for _, cl := range c.inputs {
		for f := range cl.files.All() {
			ve.DeletedTables[deletedFileEntry{
				Level:   cl.level,
				FileNum: f.FileNum,
			}] = f
		}
	}

	startLevelBytes := c.startLevel.files.SizeSum()
	outputMetrics := &LevelMetrics{
		BytesIn:   startLevelBytes,
		BytesRead: c.outputLevel.files.SizeSum(),
	}
	if len(c.extraLevels) > 0 {
		outputMetrics.BytesIn += c.extraLevels[0].files.SizeSum()
	}
	outputMetrics.BytesRead += outputMetrics.BytesIn

	c.metrics[c.outputLevel.level] = outputMetrics
	if len(c.flushing) == 0 && c.metrics[c.startLevel.level] == nil {
		c.metrics[c.startLevel.level] = &LevelMetrics{}
	}
	if len(c.extraLevels) > 0 {
		c.metrics[c.extraLevels[0].level] = &LevelMetrics{}
		outputMetrics.MultiLevel.BytesInTop = startLevelBytes
		outputMetrics.MultiLevel.BytesIn = outputMetrics.BytesIn
		outputMetrics.MultiLevel.BytesRead = outputMetrics.BytesRead
	}

	// Add any newly constructed blob files to the version edit.
	ve.NewBlobFiles = make([]*manifest.BlobFileMetadata, len(result.Blobs))
	for i := range result.Blobs {
		ve.NewBlobFiles[i] = result.Blobs[i].Metadata
	}

	inputLargestSeqNumAbsolute := c.inputLargestSeqNumAbsolute()
	ve.NewTables = make([]newTableEntry, len(result.Tables))
	for i := range result.Tables {
		t := &result.Tables[i]

		fileMeta := &tableMetadata{
			FileNum:            base.PhysicalTableFileNum(t.ObjMeta.DiskFileNum),
			CreationTime:       t.CreationTime.Unix(),
			Size:               t.WriterMeta.Size,
			SmallestSeqNum:     t.WriterMeta.SmallestSeqNum,
			LargestSeqNum:      t.WriterMeta.LargestSeqNum,
			BlobReferences:     t.BlobReferences,
			BlobReferenceDepth: t.BlobReferenceDepth,
		}
		if c.flushing == nil {
			// Set the file's LargestSeqNumAbsolute to be the maximum value of any
			// of the compaction's input sstables.
			// TODO(jackson): This could be narrowed to be the maximum of input
			// sstables that overlap the output sstable's key range.
			fileMeta.LargestSeqNumAbsolute = inputLargestSeqNumAbsolute
		} else {
			fileMeta.LargestSeqNumAbsolute = t.WriterMeta.LargestSeqNum
		}
		fileMeta.InitPhysicalBacking()

		// If the file didn't contain any range deletions, we can fill its
		// table stats now, avoiding unnecessarily loading the table later.
		maybeSetStatsFromProperties(
			fileMeta.PhysicalMeta(), &t.WriterMeta.Properties.CommonProperties, c.logger,
		)

		if t.WriterMeta.HasPointKeys {
			fileMeta.ExtendPointKeyBounds(c.cmp, t.WriterMeta.SmallestPoint, t.WriterMeta.LargestPoint)
		}
		if t.WriterMeta.HasRangeDelKeys {
			fileMeta.ExtendPointKeyBounds(c.cmp, t.WriterMeta.SmallestRangeDel, t.WriterMeta.LargestRangeDel)
		}
		if t.WriterMeta.HasRangeKeys {
			fileMeta.ExtendRangeKeyBounds(c.cmp, t.WriterMeta.SmallestRangeKey, t.WriterMeta.LargestRangeKey)
		}

		ve.NewTables[i] = newTableEntry{
			Level: c.outputLevel.level,
			Meta:  fileMeta,
		}

		// Update metrics.
		if c.flushing == nil {
			outputMetrics.TablesCompacted++
			outputMetrics.BytesCompacted += fileMeta.Size
		} else {
			outputMetrics.TablesFlushed++
			outputMetrics.BytesFlushed += fileMeta.Size
		}
		outputMetrics.TablesSize += int64(fileMeta.Size)
		outputMetrics.TablesCount++
		outputMetrics.Additional.BytesWrittenDataBlocks += t.WriterMeta.Properties.DataSize
		outputMetrics.Additional.BytesWrittenValueBlocks += t.WriterMeta.Properties.ValueBlocksSize
	}

	// Sanity check that the tables are ordered and don't overlap.
	for i := 1; i < len(ve.NewTables); i++ {
		if ve.NewTables[i-1].Meta.UserKeyBounds().End.IsUpperBoundFor(c.cmp, ve.NewTables[i].Meta.Smallest().UserKey) {
			return nil, base.AssertionFailedf("pebble: compaction output tables overlap: %s and %s",
				ve.NewTables[i-1].Meta.DebugString(c.formatKey, true),
				ve.NewTables[i].Meta.DebugString(c.formatKey, true),
			)
		}
	}

	return ve, nil
}

// newCompactionOutputTable creates an object for a new table produced by a
// compaction or flush.
func (d *DB) newCompactionOutputTable(
	jobID JobID, c *compaction, writerOpts sstable.WriterOptions,
) (objstorage.ObjectMetadata, sstable.RawWriter, error) {
	writable, objMeta, err := d.newCompactionOutputObj(c, base.FileTypeTable)
	if err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}
	d.opts.EventListener.TableCreated(TableCreateInfo{
		JobID:   int(jobID),
		Reason:  c.kind.compactingOrFlushing(),
		Path:    d.objProvider.Path(objMeta),
		FileNum: objMeta.DiskFileNum,
	})
	writerOpts.SetInternal(sstableinternal.WriterOptions{
		CacheOpts: sstableinternal.CacheOptions{
			CacheHandle: d.cacheHandle,
			FileNum:     objMeta.DiskFileNum,
		},
	})
	tw := sstable.NewRawWriterWithCPUMeasurer(writable, writerOpts, c.grantHandle)
	return objMeta, tw, nil
}

// newCompactionOutputBlob creates an object for a new blob produced by a
// compaction or flush.
func (d *DB) newCompactionOutputBlob(
	jobID JobID, c *compaction,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	writable, objMeta, err := d.newCompactionOutputObj(c, base.FileTypeBlob)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}
	d.opts.EventListener.BlobFileCreated(BlobFileCreateInfo{
		JobID:   int(jobID),
		Reason:  c.kind.compactingOrFlushing(),
		Path:    d.objProvider.Path(objMeta),
		FileNum: objMeta.DiskFileNum,
	})
	return writable, objMeta, nil
}

// newCompactionOutputObj creates an object produced by a compaction or flush.
func (d *DB) newCompactionOutputObj(
	c *compaction, typ base.FileType,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	diskFileNum := d.mu.versions.getNextDiskFileNum()

	var writeCategory vfs.DiskWriteCategory
	if d.opts.EnableSQLRowSpillMetrics {
		// In the scenario that the Pebble engine is used for SQL row spills the
		// data written to the memtable will correspond to spills to disk and
		// should be categorized as such.
		writeCategory = "sql-row-spill"
	} else if c.kind == compactionKindFlush {
		writeCategory = "pebble-memtable-flush"
	} else {
		writeCategory = "pebble-compaction"
	}

	ctx := context.TODO()
	if objiotracing.Enabled {
		ctx = objiotracing.WithLevel(ctx, c.outputLevel.level)
		if c.kind == compactionKindFlush {
			ctx = objiotracing.WithReason(ctx, objiotracing.ForFlush)
		} else {
			ctx = objiotracing.WithReason(ctx, objiotracing.ForCompaction)
		}
	}

	// Prefer shared storage if present.
	createOpts := objstorage.CreateOptions{
		PreferSharedStorage: remote.ShouldCreateShared(d.opts.Experimental.CreateOnShared, c.outputLevel.level),
		WriteCategory:       writeCategory,
	}
	writable, objMeta, err := d.objProvider.Create(ctx, typ, diskFileNum, createOpts)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}

	if c.kind != compactionKindFlush {
		writable = &compactionWritable{
			Writable: writable,
			versions: d.mu.versions,
			written:  &c.bytesWritten,
		}
	}
	return writable, objMeta, nil
}

// validateVersionEdit validates that start and end keys across new and deleted
// files in a versionEdit pass the given validation function.
func validateVersionEdit(
	ve *versionEdit, vk base.ValidateKey, format base.FormatKey, logger Logger,
) {
	validateKey := func(f *manifest.TableMetadata, key []byte) {
		if err := vk.Validate(key); err != nil {
			logger.Fatalf("pebble: version edit validation failed (key=%s file=%s): %v", format(key), f, err)
		}
	}

	// Validate both new and deleted files.
	for _, f := range ve.NewTables {
		validateKey(f.Meta, f.Meta.Smallest().UserKey)
		validateKey(f.Meta, f.Meta.Largest().UserKey)
	}
	for _, m := range ve.DeletedTables {
		validateKey(m, m.Smallest().UserKey)
		validateKey(m, m.Largest().UserKey)
	}
}

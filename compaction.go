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
	"github.com/cockroachdb/pebble/internal/problemspans"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/tombspan"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/cockroachdb/pebble/valsep"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
)

var errEmptyTable = errors.New("pebble: empty table")

// ErrCancelledCompaction is returned if a compaction is cancelled by a
// concurrent excise or ingest-split operation.
var ErrCancelledCompaction = errors.New("pebble: compaction cancelled by a concurrent operation, will retry compaction")

var flushLabels = pprof.Labels("pebble", "flush", "output-level", "L0")

// expandedCompactionByteSizeLimit is the maximum number of bytes in all
// compacted files. We avoid expanding the lower level file set of a compaction
// if it would make the total compaction cover more than this many bytes.
func expandedCompactionByteSizeLimit(
	opts *Options, targetFileSize int64, availBytes uint64,
) uint64 {
	v := uint64(25 * targetFileSize)

	// Never expand a compaction beyond half the available capacity, divided
	// by the maximum number of concurrent compactions. Each of the concurrent
	// compactions may expand up to this limit, so this attempts to limit
	// compactions to half of available disk space. Note that this will not
	// prevent compaction picking from pursuing compactions that are larger
	// than this threshold before expansion.
	//
	// NB: this heuristic is an approximation since we may run more compactions
	// than the upper concurrency limit.
	_, maxConcurrency := opts.CompactionConcurrencyRange()
	diskMax := (availBytes / 2) / uint64(maxConcurrency)
	if v > diskMax {
		v = diskMax
	}
	return v
}

// maxGrandparentOverlapBytes is the maximum bytes of overlap with level+1
// before we stop building a single file in a level-1 to level compaction.
func maxGrandparentOverlapBytes(targetFileSize int64) uint64 {
	return uint64(10 * targetFileSize)
}

// maxReadCompactionBytes is used to prevent read compactions which
// are too wide.
func maxReadCompactionBytes(targetFileSize int64) uint64 {
	return uint64(10 * targetFileSize)
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
// far. It also increments a per-compaction `written` atomic int.
type compactionWritable struct {
	objstorage.Writable

	versions *versionSet
	written  *atomic.Int64
}

// Write is part of the objstorage.Writable interface.
func (c *compactionWritable) Write(p []byte) error {
	if err := c.Writable.Write(p); err != nil {
		return err
	}

	c.written.Add(int64(len(p)))
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
	// copied byte-by-byte into a new file with a new TableNum in the output level.
	compactionKindCopy
	// compactionKindDeleteOnly denotes a compaction that only deletes input
	// files. It can occur when wide range tombstones completely contain sstables.
	compactionKindDeleteOnly
	compactionKindElisionOnly
	compactionKindRead
	compactionKindTombstoneDensity
	compactionKindRewrite
	compactionKindIngestedFlushable
	compactionKindBlobFileRewrite
	// compactionKindVirtualRewrite must be the last compactionKind.
	// If a new kind has to be added after VirtualRewrite,
	// update AllCompactionKindStrings() accordingly.
	compactionKindVirtualRewrite
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
	case compactionKindBlobFileRewrite:
		return "blob-file-rewrite"
	case compactionKindVirtualRewrite:
		return "virtual-sst-rewrite"
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

// AllCompactionKindStrings returns all compaction kind string representations
// for testing purposes. Used by tool/logs/compaction_test.go to verify the
// compaction summary tool stays in sync with new compaction types.
//
// NOTE: This function iterates up to compactionKindVirtualRewrite. If a new
// compactionKind is added after VirtualRewrite, update this function accordingly.
func AllCompactionKindStrings() map[string]bool {
	kinds := make(map[string]bool)
	for k := compactionKindDefault; k <= compactionKindVirtualRewrite; k++ {
		kinds[k.String()] = true
	}
	return kinds
}

type compaction interface {
	AddInProgressLocked(*DB)
	BeganAt() time.Time
	Bounds() *base.UserKeyBounds
	Cancel()
	Execute(JobID, *DB) error
	GrantHandle() CompactionGrantHandle
	Info() compactionInfo
	IsDownload() bool
	IsFlush() bool
	PprofLabels(UserKeyCategories) pprof.LabelSet
	RecordError(*problemspans.ByLevel, error)
	Tables() iter.Seq2[int, *manifest.TableMetadata]
	UsesBurstConcurrency() bool
	VersionEditApplied() bool
}

// tableCompaction is a table compaction from one level to the next, starting
// from a given version. It implements the compaction interface.
type tableCompaction struct {
	// cancel is a bool that can be used by other goroutines to signal a compaction
	// to cancel, such as if a conflicting excise operation raced it to manifest
	// application. Only holders of the manifest lock will write to this atomic.
	cancel atomic.Bool
	// kind indicates the kind of compaction. Different compaction kinds have
	// different semantics and mechanics. Some may have additional fields.
	kind compactionKind
	// isDownload is true if this compaction was started as part of a Download
	// operation. In this case kind is compactionKindCopy or
	// compactionKindRewrite.
	isDownload bool

	comparer *base.Comparer
	logger   Logger
	version  *manifest.Version
	// versionEditApplied is set to true when a compaction has completed and the
	// resulting version has been installed (if successful), but the compaction
	// goroutine is still cleaning up (eg, deleting obsolete files).
	versionEditApplied bool
	// getValueSeparation constructs a valsep.ValueSeparation for use in a
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
	getValueSeparation func(JobID, *tableCompaction) valsep.ValueSeparation

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

	// eventualOutputLevel is normally outputLevel.level, unless
	// outputLevel.level+1 has no overlap with the compaction bounds (in which
	// case it is the bottom-most consecutive level with no such overlap).
	//
	// Because of move compactions, we know that any sstables produced by this
	// compaction will be later moved to eventualOutputLevel. So we use
	// eventualOutputLevel when determining the target file size, compression
	// options, etc.
	eventualOutputLevel int
	// maxOutputFileSize is the maximum size of an individual table created
	// during compaction.
	maxOutputFileSize uint64
	// maxOverlapBytes is the maximum number of bytes of overlap allowed for a
	// single output table with the tables in the grandparent level.
	maxOverlapBytes uint64

	// The boundaries of the input data.
	bounds base.UserKeyBounds

	// grandparents are the tables in eventualOutputLevel+2 that overlap with the
	// files being compacted. Used to determine output table boundaries. Do not
	// assume that the actual files in the grandparent when this compaction
	// finishes will be the same.
	grandparents manifest.LevelSlice

	delElision      compact.TombstoneElision
	rangeKeyElision compact.TombstoneElision

	// deleteOnly contains information specific to compactions with kind
	// compactionKindDeleteOnly. A delete-only compaction is a special
	// compaction that does not merge or write sstables. Instead, it only
	// performs deletions either through removing whole sstables from the LSM or
	// virtualizing them into virtual sstables.
	deleteOnly tombspan.DeleteOnlyCompaction
	// flush contains information specific to flushes (compactionKindFlush and
	// compactionKindIngestedFlushable). A flush is modeled by a compaction
	// because it has similar mechanics to a default compaction.
	flush struct {
		// flushables contains the flushables (aka memtables, large batches,
		// flushable ingestions, etc) that are being flushed.
		flushables flushableList
		// Boundaries at which sstables flushed to L0 should be split.
		// Determined by L0Sublevels. If nil, ignored.
		l0Limits [][]byte
	}
	// iterationState contains state used during compaction iteration.
	iterationState struct {
		// bufferPool is a pool of buffers used when reading blocks. Compactions
		// do not populate the block cache under the assumption that the blocks
		// we read will soon be irrelevant when their containing sstables are
		// removed from the LSM.
		bufferPool sstable.BufferPool
		// keyspanIterClosers is a list of fragment iterators to close when the
		// compaction finishes. As iteration opens new keyspan iterators,
		// elements are appended. Keyspan iterators must remain open for the
		// lifetime of the compaction, so they're accumulated here. When the
		// compaction finishes, all the underlying keyspan iterators are closed.
		keyspanIterClosers []*noCloseIter
		// valueFetcher is used to fetch values from blob files. It's propagated
		// down the iterator tree through the internal iterator options.
		valueFetcher blob.ValueFetcher
	}
	// metrics encapsulates various metrics collected during a compaction.
	metrics compactionMetrics

	grantHandle CompactionGrantHandle

	objCreateOpts objstorage.CreateOptions

	annotations []string
}

// Assert that tableCompaction implements the compaction interface.
var _ compaction = (*tableCompaction)(nil)

func (c *tableCompaction) AddInProgressLocked(d *DB) {
	d.mu.compact.inProgress[c] = struct{}{}
	if c.UsesBurstConcurrency() {
		d.mu.compact.burstConcurrency.Add(1)
	}
	var isBase, isIntraL0 bool
	for _, cl := range c.inputs {
		for f := range cl.files.All() {
			if f.IsCompacting() {
				d.opts.Logger.Fatalf("L%d->L%d: %s already being compacted", c.startLevel.level, c.outputLevel.level, f.TableNum)
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
		if err := d.mu.versions.latest.l0Organizer.UpdateStateForStartedCompaction(l0Inputs, isBase); err != nil {
			d.opts.Logger.Fatalf("could not update state for compaction: %s", err)
		}
	}
}

func (c *tableCompaction) BeganAt() time.Time          { return c.metrics.beganAt }
func (c *tableCompaction) Bounds() *base.UserKeyBounds { return &c.bounds }
func (c *tableCompaction) Cancel()                     { c.cancel.Store(true) }

func (c *tableCompaction) Execute(jobID JobID, d *DB) error {
	c.grantHandle.Started()
	err := d.compact1(jobID, c)
	// The version stored in the compaction is ref'd when the compaction is
	// created. We're responsible for un-refing it when the compaction is
	// complete.
	if c.version != nil {
		c.version.UnrefLocked()
	}
	return err
}

func (c *tableCompaction) RecordError(problemSpans *problemspans.ByLevel, err error) {
	// Record problem spans for a short duration, unless the error is a
	// corruption.
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
			problemSpans.Add(level, f.UserKeyBounds(), expiration)
		}
	}
}

func (c *tableCompaction) GrantHandle() CompactionGrantHandle { return c.grantHandle }
func (c *tableCompaction) IsDownload() bool                   { return c.isDownload }
func (c *tableCompaction) IsFlush() bool                      { return len(c.flush.flushables) > 0 }
func (c *tableCompaction) Info() compactionInfo {
	info := compactionInfo{
		versionEditApplied: c.versionEditApplied,
		kind:               c.kind,
		inputs:             c.inputs,
		bounds:             &c.bounds,
		outputLevel:        -1,
	}
	if c.outputLevel != nil {
		info.outputLevel = c.outputLevel.level
	}
	return info
}
func (c *tableCompaction) PprofLabels(kc UserKeyCategories) pprof.LabelSet {
	activity := "compact"
	if len(c.flush.flushables) != 0 {
		activity = "flush"
	}
	level := "L?"
	// Delete-only compactions don't have an output level.
	if c.outputLevel != nil {
		level = fmt.Sprintf("L%d", c.outputLevel.level)
	}
	if kc.Len() > 0 {
		cat := kc.CategorizeKeyRange(c.bounds.Start, c.bounds.End.Key)
		return pprof.Labels("pebble", activity, "output-level", level, "key-type", cat)
	}
	return pprof.Labels("pebble", activity, "output-level", level)
}

func (c *tableCompaction) Tables() iter.Seq2[int, *manifest.TableMetadata] {
	return func(yield func(int, *manifest.TableMetadata) bool) {
		for _, cl := range c.inputs {
			for f := range cl.files.All() {
				if !yield(cl.level, f) {
					return
				}
			}
		}
	}
}

func (c *tableCompaction) UsesBurstConcurrency() bool { return false }

func (c *tableCompaction) VersionEditApplied() bool { return c.versionEditApplied }

// compactionMetrics contians metrics surrounding a compaction.
type compactionMetrics struct {
	// beganAt is the time when the compaction began.
	beganAt time.Time
	// bytesWritten contains the number of bytes that have been written to
	// outputs. It's updated whenever the compaction outputs'
	// objstorage.Writables receive new writes. See newCompactionOutputObj.
	bytesWritten atomic.Int64
	// internalIterStats contains statistics from the internal iterators used by
	// the compaction.
	//
	// TODO(jackson): Use these to power the compaction BytesRead metric.
	internalIterStats base.InternalIteratorStats
	// perLevel contains metrics for each level involved in the compaction.
	perLevel levelMetricsDelta
	// picker contains metrics from the compaction picker when the compaction
	// was picked.
	picker pickedCompactionMetrics
}

// inputLargestSeqNumAbsolute returns the maximum LargestSeqNumAbsolute of any
// input sstables.
func (c *tableCompaction) inputLargestSeqNumAbsolute() base.SeqNum {
	var seqNum base.SeqNum
	for _, cl := range c.inputs {
		for m := range cl.files.All() {
			seqNum = max(seqNum, m.LargestSeqNumAbsolute)
		}
	}
	return seqNum
}

func (c *tableCompaction) makeInfo(jobID JobID) CompactionInfo {
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

	for i, score := range c.metrics.picker.scores {
		info.Input[i].Score = score
	}
	info.SingleLevelOverlappingRatio = c.metrics.picker.singleLevelOverlappingRatio
	info.MultiLevelOverlappingRatio = c.metrics.picker.multiLevelOverlappingRatio
	if len(info.Input) > 2 {
		info.Annotations = append(info.Annotations, "multilevel")
	}
	return info
}

type getValueSeparation func(JobID, *tableCompaction) valsep.ValueSeparation

// newCompaction constructs a compaction from the provided picked compaction.
//
// The compaction is created with a reference to its version that must be
// released when the compaction is complete.
func newCompaction(
	pc *pickedTableCompaction,
	opts *Options,
	beganAt time.Time,
	provider objstorage.Provider,
	grantHandle CompactionGrantHandle,
	preferSharedStorage bool,
	getValueSeparation getValueSeparation,
) *tableCompaction {
	c := &tableCompaction{
		kind:               compactionKindDefault,
		comparer:           opts.Comparer,
		inputs:             pc.inputs,
		bounds:             pc.bounds,
		logger:             opts.Logger,
		version:            pc.version,
		getValueSeparation: getValueSeparation,
		metrics: compactionMetrics{
			beganAt: beganAt,
			picker:  pc.pickerMetrics,
		},
		grantHandle: grantHandle,
	}

	// Determine eventual output level.
	c.eventualOutputLevel = pc.outputLevel.level
	// TODO(radu): for intra-L0 compactions, we could check if the compaction
	// includes all L0 files within the bounds.
	if pc.outputLevel.level != 0 {
		for c.eventualOutputLevel < manifest.NumLevels-1 && !c.version.HasOverlap(c.eventualOutputLevel+1, c.bounds) {
			// All output tables are guaranteed to be moved down.
			c.eventualOutputLevel++
		}
	}

	targetFileSize := opts.TargetFileSize(c.eventualOutputLevel, pc.baseLevel)
	c.maxOutputFileSize = uint64(targetFileSize)
	c.maxOverlapBytes = maxGrandparentOverlapBytes(targetFileSize)

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

	c.startLevel = &c.inputs[0]
	if pc.startLevel.l0SublevelInfo != nil {
		c.startLevel.l0SublevelInfo = pc.startLevel.l0SublevelInfo
	}

	c.outputLevel = &c.inputs[len(c.inputs)-1]

	if len(pc.inputs) > 2 {
		// TODO(xinhaoz): Look into removing extraLevels on the compaction struct.
		c.extraLevels = make([]*compactionLevel, 0, len(pc.inputs)-2)
		for i := 1; i < len(pc.inputs)-1; i++ {
			c.extraLevels = append(c.extraLevels, &c.inputs[i])
		}
	}
	// Compute the set of outputLevel+1 files that overlap this compaction (these
	// are the grandparent sstables).
	if c.eventualOutputLevel < manifest.NumLevels-1 {
		c.grandparents = c.version.Overlaps(max(c.eventualOutputLevel+1, pc.baseLevel), c.bounds)
	}
	c.delElision, c.rangeKeyElision = compact.SetupTombstoneElision(
		c.comparer.Compare, c.version, pc.l0Organizer, c.outputLevel.level, c.bounds,
	)
	c.kind = pc.kind

	c.maybeSwitchToMoveOrCopy(preferSharedStorage, provider)
	c.objCreateOpts = objstorage.CreateOptions{
		PreferSharedStorage: preferSharedStorage,
		WriteCategory:       getDiskWriteCategoryForCompaction(opts, c.kind),
	}
	if preferSharedStorage {
		c.getValueSeparation = neverSeparateValues
	}

	return c
}

// maybeSwitchToMoveOrCopy decides if the compaction can be changed into a move
// or copy compaction, in which case c.kind is updated.
func (c *tableCompaction) maybeSwitchToMoveOrCopy(
	preferSharedStorage bool, provider objstorage.Provider,
) {
	// Only non-multi-level compactions with a single input file can be
	// considered.
	if c.startLevel.files.Len() != 1 || !c.outputLevel.files.Empty() || c.hasExtraLevelData() {
		return
	}

	// In addition to the default compaction, we also check whether a tombstone
	// density compaction can be optimized into a move compaction. However, we
	// want to avoid performing a move compaction into the lowest level, since the
	// goal there is to actually remove the tombstones.
	//
	// Tombstone density compaction is meant to address cases where tombstones
	// don't reclaim much space but are still expensive to scan over. We can only
	// remove the tombstones once there's nothing at all underneath them.
	switch c.kind {
	case compactionKindDefault:
		// Proceed.
	case compactionKindTombstoneDensity:
		// Tombstone density compaction can be optimized into a move compaction.
		// However, we want to avoid performing a move compaction into the lowest
		// level, since the goal there is to actually remove the tombstones; even if
		// they don't prevent a lot of space from being reclaimed, tombstones can
		// still be expensive to scan over.
		if c.outputLevel.level == numLevels-1 {
			return
		}
	default:
		// Other compaction kinds not supported.
		return
	}

	// We avoid a move or copy if there is lots of overlapping grandparent data.
	// Otherwise, the move could create a parent file that will require a very
	// expensive merge later on.
	//
	// Note that if eventualOutputLevel != outputLevel, there are no
	// "grandparents" on the output level.
	if c.eventualOutputLevel == c.outputLevel.level && c.grandparents.AggregateSizeSum() > c.maxOverlapBytes {
		return
	}

	iter := c.startLevel.files.Iter()
	meta := iter.First()

	// We should always be passed a provider, except in some unit tests.
	isRemote := provider != nil && !objstorage.IsLocalTable(provider, meta.TableBacking.DiskFileNum)

	// Shared and external tables can always be moved. We can also move a local
	// table unless we need the result to be on shared storage.
	if isRemote || !preferSharedStorage {
		c.kind = compactionKindMove
		return
	}

	// We can rewrite the table (regular compaction) or we can use a copy compaction.
	switch {
	case meta.Virtual:
		// We want to avoid a copy compaction if the table is virtual, as we may end
		// up copying a lot more data than necessary.
	case meta.BlobReferenceDepth != 0:
		// We also want to avoid copy compactions for tables with blob references,
		// as we currently lack a mechanism to propagate blob references along with
		// the sstable.
	default:
		c.kind = compactionKindCopy
	}
}

func adjustGrandparentOverlapBytesForFlush(c *tableCompaction, flushingBytes uint64) {
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
	grandparentFileBytes := c.grandparents.AggregateSizeSum()
	fileCountUpperBoundDueToGrandparents :=
		float64(grandparentFileBytes) / float64(c.maxOverlapBytes)
	if fileCountUpperBoundDueToGrandparents > acceptableFileCount {
		c.maxOverlapBytes = uint64(
			float64(c.maxOverlapBytes) *
				(fileCountUpperBoundDueToGrandparents / acceptableFileCount))
	}
}

// newFlush creates the state necessary for a flush (modeled with the compaction
// struct).
//
// newFlush takes the current Version in order to populate grandparent flushing
// limits, but it does not reference the version.
//
// TODO(jackson): Consider maintaining a reference to the version anyways since
// in the future in-memory Version state may only be available while a Version
// is referenced (eg, if we start recycling B-Tree nodes once they're no longer
// referenced). There's subtlety around unref'ing the version at the right
// moment, so we defer it for now.
func newFlush(
	opts *Options,
	cur *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	baseLevel int,
	flushing flushableList,
	beganAt time.Time,
	preferSharedStorage bool,
	getValueSeparation getValueSeparation,
) (*tableCompaction, error) {
	c := &tableCompaction{
		kind:               compactionKindFlush,
		comparer:           opts.Comparer,
		logger:             opts.Logger,
		inputs:             []compactionLevel{{level: -1}, {level: 0}},
		getValueSeparation: getValueSeparation,
		// TODO(radu): consider calculating the eventual output level for flushes.
		// We expect the bounds to be very wide in practice, but perhaps we can do a
		// finer-grained overlap analysis.
		eventualOutputLevel: 0,
		maxOutputFileSize:   math.MaxUint64,
		maxOverlapBytes:     math.MaxUint64,
		grantHandle:         noopGrantHandle{},
		metrics: compactionMetrics{
			beganAt: beganAt,
		},
	}

	c.flush.flushables = flushing
	c.flush.l0Limits = l0Organizer.FlushSplitKeys()
	c.startLevel = &c.inputs[0]
	c.outputLevel = &c.inputs[1]
	if len(flushing) > 0 {
		if _, ok := flushing[0].flushable.(*ingestedFlushable); ok {
			if len(flushing) != 1 {
				panic("pebble: ingestedFlushable must be flushed one at a time.")
			}
			c.kind = compactionKindIngestedFlushable
			return c, nil
		} else {
			// Make sure there's no ingestedFlushable after the first flushable
			// in the list.
			for _, f := range c.flush.flushables[1:] {
				if _, ok := f.flushable.(*ingestedFlushable); ok {
					panic("pebble: flushables shouldn't contain ingestedFlushable")
				}
			}
		}
	}

	c.objCreateOpts = objstorage.CreateOptions{
		PreferSharedStorage: preferSharedStorage,
		WriteCategory:       getDiskWriteCategoryForCompaction(opts, c.kind),
	}
	if preferSharedStorage {
		c.getValueSeparation = neverSeparateValues
	}

	cmp := c.comparer.Compare
	updatePointBounds := func(iter internalIterator) {
		if kv := iter.First(); kv != nil {
			if c.bounds.Start == nil || cmp(c.bounds.Start, kv.K.UserKey) > 0 {
				c.bounds.Start = slices.Clone(kv.K.UserKey)
			}
		}
		if kv := iter.Last(); kv != nil {
			if c.bounds.End.Key == nil || !c.bounds.End.IsUpperBoundForInternalKey(cmp, kv.K) {
				c.bounds.End = base.UserKeyExclusiveIf(slices.Clone(kv.K.UserKey), kv.K.IsExclusiveSentinel())
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
			c.bounds = c.bounds.Union(cmp, s.Bounds().Clone())
		}
		if s, err := iter.Last(); err != nil {
			return err
		} else if s != nil {
			c.bounds = c.bounds.Union(cmp, s.Bounds().Clone())
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
		c.maxOutputFileSize = uint64(opts.TargetFileSizes[0])
		c.maxOverlapBytes = maxGrandparentOverlapBytes(opts.TargetFileSizes[0])
		c.grandparents = cur.Overlaps(baseLevel, c.bounds)
		adjustGrandparentOverlapBytesForFlush(c, flushingBytes)
	}

	// We don't elide tombstones for flushes.
	c.delElision, c.rangeKeyElision = compact.NoTombstoneElision(), compact.NoTombstoneElision()
	return c, nil
}

func (c *tableCompaction) hasExtraLevelData() bool {
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
func (c *tableCompaction) errorOnUserKeyOverlap(ve *manifest.VersionEdit) error {
	if n := len(ve.NewTables); n > 1 {
		meta := ve.NewTables[n-1].Meta
		prevMeta := ve.NewTables[n-2].Meta
		if !prevMeta.Largest().IsExclusiveSentinel() &&
			c.comparer.Compare(prevMeta.Largest().UserKey, meta.Smallest().UserKey) >= 0 {
			return errors.Errorf("pebble: compaction split user key across two sstables: %s in %s and %s",
				prevMeta.Largest().Pretty(c.comparer.FormatKey),
				prevMeta.TableNum,
				meta.TableNum)
		}
	}
	return nil
}

// isBottommostDataLayer returns true if the compaction's inputs are known to be
// the bottommost layer of data for the compaction's key range. If true, this
// allows the compaction iterator to perform transformations to keys such as
// setting a key's sequence number to zero.
//
// This function performs this determination by looking at the TombstoneElision
// values which are set up based on sstables which overlap the bounds of the
// compaction at a lower level in the LSM. This function always returns false
// for flushes.
func (c *tableCompaction) isBottommostDataLayer() bool {
	// TODO(peter): we disable zeroing of seqnums during flushing to match
	// RocksDB behavior and to avoid generating overlapping sstables during
	// DB.replayWAL. When replaying WAL files at startup, we flush after each
	// WAL is replayed building up a single version edit that is
	// applied. Because we don't apply the version edit after each flush, this
	// code doesn't know that L0 contains files and zeroing of seqnums should
	// be disabled. That is fixable, but it seems safer to just match the
	// RocksDB behavior for now.
	return len(c.flush.flushables) == 0 && c.delElision.ElidesEverything() && c.rangeKeyElision.ElidesEverything()
}

// newInputIters returns an iterator over all the input tables in a compaction.
func (c *tableCompaction) newInputIters(
	newIters tableNewIters, iiopts internalIterOpts,
) (
	pointIter internalIterator,
	rangeDelIter, rangeKeyIter keyspan.FragmentIterator,
	retErr error,
) {
	ctx := context.TODO()
	cmp := c.comparer.Compare

	// Validate the ordering of compaction input files for defense in depth.
	if len(c.flush.flushables) == 0 {
		if c.startLevel.level >= 0 {
			err := manifest.CheckOrdering(c.comparer, manifest.Level(c.startLevel.level),
				c.startLevel.files.Iter())
			if err != nil {
				return nil, nil, nil, err
			}
		}
		err := manifest.CheckOrdering(c.comparer, manifest.Level(c.outputLevel.level),
			c.outputLevel.files.Iter())
		if err != nil {
			return nil, nil, nil, err
		}
		if c.startLevel.level == 0 {
			if c.startLevel.l0SublevelInfo == nil {
				panic("l0SublevelInfo not created for compaction out of L0")
			}
			for _, info := range c.startLevel.l0SublevelInfo {
				err := manifest.CheckOrdering(c.comparer, info.sublevel, info.Iter())
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
			err := manifest.CheckOrdering(c.comparer, manifest.Level(interLevel.level),
				interLevel.files.Iter())
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
	numInputLevels := max(len(c.flush.flushables), len(c.inputs))
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
	if len(c.flush.flushables) != 0 {
		// If flushing, we need to build the input iterators over the memtables
		// stored in c.flush.flushables.
		for _, f := range c.flush.flushables {
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
			iters = append(iters, newLevelIter(ctx, iterOpts, c.comparer,
				newIters, level.files.Iter(), l, iiopts))
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
				rangeDelIter, err := c.newRangeDelIter(ctx, newIters, iter.Take(), iterOpts, iiopts, l)
				if err != nil {
					// The error will already be annotated with the BackingFileNum, so
					// we annotate it with the FileNum.
					return errors.Wrapf(err, "pebble: could not open table %s", errors.Safe(f.TableNum))
				}
				if rangeDelIter == nil {
					continue
				}
				rangeDelIters = append(rangeDelIters, rangeDelIter)
				c.iterationState.keyspanIterClosers = append(c.iterationState.keyspanIterClosers, rangeDelIter)
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
					iters, err := newIters(ctx, file, &iterOpts, iiopts, iterRangeKeys)
					if err != nil {
						return nil, err
					} else if iters.rangeKey == nil {
						return emptyKeyspanIter, nil
					}
					// Ensure that the range key iter is not closed until the compaction is
					// finished. This is necessary because range key processing
					// requires the range keys to be held in memory for up to the
					// lifetime of the compaction.
					noCloseIter := &noCloseIter{iters.rangeKey}
					c.iterationState.keyspanIterClosers = append(c.iterationState.keyspanIterClosers, noCloseIter)

					// We do not need to truncate range keys to sstable boundaries, or
					// only read within the file's atomic compaction units, unlike with
					// range tombstones. This is because range keys were added after we
					// stopped splitting user keys across sstables, so all the range keys
					// in this sstable must wholly lie within the file's bounds.
					return noCloseIter, err
				}
				li := keyspanimpl.NewLevelIter(ctx, keyspan.SpanIterOptions{}, cmp,
					newRangeKeyIterWrapper, level.files.Iter(), l, manifest.KeyTypeRange)
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
		pointIter = newMergingIter(c.logger, &c.metrics.internalIterStats, cmp, nil, iters...)
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

func (c *tableCompaction) newRangeDelIter(
	ctx context.Context,
	newIters tableNewIters,
	f manifest.LevelTable,
	opts IterOptions,
	iiopts internalIterOpts,
	l manifest.Layer,
) (*noCloseIter, error) {
	opts.layer = l
	iterSet, err := newIters(ctx, f.TableMetadata, &opts, iiopts, iterRangeDeletions)
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

func (c *tableCompaction) String() string {
	if len(c.flush.flushables) != 0 {
		return "flush\n"
	}

	var buf bytes.Buffer
	for _, l := range c.inputs {
		fmt.Fprintf(&buf, "%d:", l.level)
		for f := range l.files.All() {
			fmt.Fprintf(&buf, " %s:%s-%s", f.TableNum, f.Smallest(), f.Largest())
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
	tableNum base.TableNum
}

// Removes compaction markers from files in a compaction. The rollback parameter
// indicates whether the compaction state should be rolled back to its original
// state in the case of an unsuccessful compaction.
//
// DB.mu must be held when calling this method, however this method can drop and
// re-acquire that mutex. All writes to the manifest for this compaction should
// have completed by this point.
func (d *DB) clearCompactingState(c *tableCompaction, rollback bool) {
	c.versionEditApplied = true
	for _, cl := range c.inputs {
		for f := range cl.files.All() {
			if !f.IsCompacting() {
				d.opts.Logger.Fatalf("L%d->L%d: %s not being compacted", c.startLevel.level, c.outputLevel.level, f.TableNum)
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
		d.mu.versions.latest.l0Organizer.InitCompactingFileInfo(l0InProgress)
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
	deadline := d.opts.private.timeNow().Add(dur)
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
		d.compactionScheduler.UpdateGetAllowedWithoutPermission()
		// The flush may have produced too many files in a level, so schedule a
		// compaction if needed.
		d.maybeScheduleCompaction()
		d.mu.compact.cond.Broadcast()
	})
}

// runIngestFlush is used to generate a flush version edit for sstables which
// were ingested as flushables. Both DB.mu and the manifest lock must be held
// while runIngestFlush is called.
func (d *DB) runIngestFlush(c *tableCompaction) (*manifest.VersionEdit, error) {
	if len(c.flush.flushables) != 1 {
		panic("pebble: ingestedFlushable must be flushed one at a time.")
	}

	// Finding the target level for ingestion must use the latest version
	// after the logLock has been acquired.
	version := d.mu.versions.currentVersion()

	baseLevel := d.mu.versions.picker.getBaseLevel()
	ve := &manifest.VersionEdit{}
	var ingestSplitFiles []ingestSplitFile
	ingestFlushable := c.flush.flushables[0].flushable.(*ingestedFlushable)

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
		v: version,
	}
	replacedTables := make(map[base.TableNum][]manifest.NewTableEntry)
	for _, file := range ingestFlushable.files {
		var fileToSplit *manifest.TableMetadata
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
		ve.NewTables = append(ve.NewTables, manifest.NewTableEntry{Level: level, Meta: file})
		if fileToSplit != nil {
			ingestSplitFiles = append(ingestSplitFiles, ingestSplitFile{
				ingestFile: file,
				splitFile:  fileToSplit,
				level:      level,
			})
		}
		levelMetrics := c.metrics.perLevel.level(level)
		levelMetrics.TablesIngested.Inc(file.Size)
	}
	if ingestFlushable.exciseSpan.Valid() {
		exciseBounds := ingestFlushable.exciseSpan.UserKeyBounds()
		if d.FormatMajorVersion() >= FormatExciseBoundsRecord {
			ve.ExciseBoundsRecord = append(ve.ExciseBoundsRecord, manifest.ExciseOpEntry{
				Bounds: exciseBounds,
				SeqNum: ingestFlushable.exciseSeqNum,
			})
			d.mu.versions.metrics.Ingest.ExciseIngestCount++
		}
		// Iterate through all levels and find files that intersect with exciseSpan.
		for layer, ls := range version.AllLevelsAndSublevels() {
			for m := range ls.Overlaps(d.cmp, ingestFlushable.exciseSpan.UserKeyBounds()).All() {
				leftTable, rightTable, err := d.exciseTable(context.TODO(), exciseBounds, m, layer.Level(), tightExciseBounds)
				if err != nil {
					return nil, err
				}
				newFiles := applyExciseToVersionEdit(ve, m, leftTable, rightTable, layer.Level())
				replacedTables[m.TableNum] = newFiles
			}
		}
	}

	if len(ingestSplitFiles) > 0 {
		if err := d.ingestSplit(context.TODO(), ve, ingestSplitFiles, replacedTables); err != nil {
			return nil, err
		}
	}

	// Add any blob files referenced by the ingested sstables to the version edit.
	for id, bf := range ingestFlushable.blobFileMap {
		ve.NewBlobFiles = append(ve.NewBlobFiles, manifest.BlobFileMetadata{
			FileID:   id,
			Physical: bf,
		})
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

	c, err := newFlush(
		d.opts,
		d.mu.versions.currentVersion(),
		d.mu.versions.latest.l0Organizer,
		d.mu.versions.picker.getBaseLevel(),
		d.mu.mem.queue[:n],
		d.opts.private.timeNow(),
		d.shouldCreateShared(0),
		d.determineCompactionValueSeparation,
	)
	if err != nil {
		return 0, err
	}
	c.AddInProgressLocked(d)

	jobID := d.newJobIDLocked()
	info := FlushInfo{
		JobID:      int(jobID),
		Input:      inputs,
		InputBytes: inputBytes,
		Ingest:     ingest,
	}
	d.opts.EventListener.FlushBegin(info)

	startTime := d.opts.private.timeNow()

	var ve *manifest.VersionEdit
	var stats compact.Stats
	var outputBlobs []compact.OutputBlob
	// To determine the target level of the files in the ingestedFlushable, we
	// need to acquire the logLock, and not release it for that duration. Since
	// UpdateVersionLocked acquires it anyway, we create the VersionEdit for
	// ingestedFlushable outside runCompaction. For all other flush cases, we
	// construct the VersionEdit inside runCompaction.
	var compactionErr error
	if c.kind != compactionKindIngestedFlushable {
		ve, stats, outputBlobs, compactionErr = d.runCompaction(jobID, c)
	}

	_, err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
		err := compactionErr
		if c.kind == compactionKindIngestedFlushable {
			ve, err = d.runIngestFlush(c)
		}
		info.Duration = d.opts.private.timeNow().Sub(startTime)
		if err != nil {
			return versionUpdate{}, err
		}

		validateVersionEdit(ve, d.opts.Comparer.ValidateKey, d.opts.Comparer.FormatKey, d.opts.Logger)
		for i := range ve.NewTables {
			e := &ve.NewTables[i]
			info.OutputTables = append(info.OutputTables, e.Meta.TableInfo())
			// Ingested tables are not necessarily flushed to L0. Record the level of
			// each ingested file explicitly.
			if ingest {
				info.IngestLevels = append(info.IngestLevels, e.Level)
			}
		}
		for i := range outputBlobs {
			b := &outputBlobs[i]
			info.OutputBlobs = append(info.OutputBlobs, BlobFileInfo{
				BlobFileID:      base.BlobFileID(b.Metadata.FileNum),
				DiskFileNum:     b.ObjMeta.DiskFileNum,
				Size:            b.Metadata.Size,
				ValueSize:       b.Stats.UncompressedValueBytes,
				MVCCGarbageSize: b.Stats.MVCCGarbageBytes,
			})

		}

		// The flush succeeded or it produced an empty sstable. In either case we
		// want to bump the minimum unflushed log number to the log number of the
		// oldest unflushed memtable.
		ve.MinUnflushedLogNum = minUnflushedLogNum
		if c.kind != compactionKindIngestedFlushable {
			l0Metrics := c.metrics.perLevel.level(0)
			if d.opts.DisableWAL {
				// If the WAL is disabled, every flushable has a zero [logSize],
				// resulting in zero bytes in. Instead, use the number of bytes we
				// flushed as the BytesIn. This ensures we get a reasonable w-amp
				// calculation even when the WAL is disabled.
				l0Metrics.TableBytesIn = l0Metrics.TablesFlushed.Bytes + l0Metrics.BlobBytesFlushed
			} else {
				for i := 0; i < n; i++ {
					l0Metrics.TableBytesIn += d.mu.mem.queue[i].logSize
				}
			}
		} else {
			// c.kind == compactionKindIngestedFlushable && we could have deleted files due
			// to ingest-time splits or excises.
			ingestFlushable := c.flush.flushables[0].flushable.(*ingestedFlushable)
			exciseBounds := ingestFlushable.exciseSpan.UserKeyBounds()
			for c2 := range d.mu.compact.inProgress {
				// Check if this compaction overlaps with the excise span. Note that just
				// checking if the inputs individually overlap with the excise span
				// isn't sufficient; for instance, a compaction could have [a,b] and [e,f]
				// as inputs and write it all out as [a,b,e,f] in one sstable. If we're
				// doing a [c,d) excise at the same time as this compaction, we will have
				// to error out the whole compaction as we can't guarantee it hasn't/won't
				// write a file overlapping with the excise span.
				bounds := c2.Bounds()
				if bounds != nil && bounds.Overlaps(d.cmp, exciseBounds) {
					c2.Cancel()
				}
			}

			if len(ve.DeletedTables) > 0 {
				// Iterate through all other compactions, and check if their inputs have
				// been replaced due to an ingest-time split or excise. In that case,
				// cancel the compaction.
				for c2 := range d.mu.compact.inProgress {
					for level, table := range c2.Tables() {
						if _, ok := ve.DeletedTables[manifest.DeletedTableEntry{FileNum: table.TableNum, Level: level}]; ok {
							c2.Cancel()
							break
						}
					}
				}
			}
		}
		return versionUpdate{
			VE:                      ve,
			JobID:                   jobID,
			Metrics:                 c.metrics.perLevel,
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
	if c.UsesBurstConcurrency() {
		if v := d.mu.compact.burstConcurrency.Add(-1); v < 0 {
			panic(errors.AssertionFailedf("burst concurrency underflow: %d", v))
		}
	}
	delete(d.mu.compact.inProgress, c)
	d.mu.versions.incrementCompactions(c.kind, c.extraLevels, c.metrics.bytesWritten.Load(), err)

	var flushed flushableList
	if err == nil {
		flushed = d.mu.mem.queue[:n]
		d.mu.mem.queue = d.mu.mem.queue[n:]
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewTables)
		if ingest {
			d.mu.versions.metrics.Flush.AsIngestCount++
			for _, l := range c.metrics.perLevel {
				if l != nil {
					d.mu.versions.metrics.Flush.AsIngestBytes += l.TablesIngested.Bytes
					d.mu.versions.metrics.Flush.AsIngestTableCount += l.TablesIngested.Count
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
	info.TotalDuration = d.opts.private.timeNow().Sub(startTime)
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
	for d.tryScheduleDeleteOnlyCompaction() {
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
		success, grantHandle := d.compactionScheduler.TrySchedule()
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
func (d *DB) pickAnyCompaction(env compactionEnv) (pc pickedCompaction) {
	if !d.opts.DisableAutomaticCompactions {
		// Pick a score-based compaction first, since a misshapen LSM is bad.
		// We allow an exception for a high-priority disk-space reclamation
		// compaction. Future work will explore balancing the various competing
		// compaction priorities more judiciously. For now, we're relying on the
		// configured heuristic to be set carefully so that we don't starve
		// score-based compactions.
		if pc := d.mu.versions.picker.pickHighPrioritySpaceCompaction(env); pc != nil {
			return pc
		}
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
func (d *DB) runPickedCompaction(pc pickedCompaction, grantHandle CompactionGrantHandle) {
	var doneChannel chan error
	if pc.ManualID() > 0 {
		for i := range d.mu.compact.manual {
			if d.mu.compact.manual[i].id == pc.ManualID() {
				doneChannel = d.mu.compact.manual[i].done
				d.mu.compact.manual = slices.Delete(d.mu.compact.manual, i, i+1)
				d.mu.compact.manualLen.Store(int32(len(d.mu.compact.manual)))
				break
			}
		}
		if doneChannel == nil {
			panic(errors.AssertionFailedf("did not find manual compaction with id %d", pc.ManualID()))
		}
	}

	d.mu.compact.compactingCount++
	d.mu.compact.compactProcesses++
	c := pc.ConstructCompaction(d, grantHandle)
	c.AddInProgressLocked(d)
	go func() {
		d.compact(c, doneChannel)
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
	return true, pc.WaitingCompaction()
}

// GetAllowedWithoutPermission implements DBForCompaction (it is called by the
// CompactionScheduler).
func (d *DB) GetAllowedWithoutPermission() int {
	allowedBasedOnBacklog := int(d.mu.versions.curCompactionConcurrency.Load())
	allowedBasedOnManual := int(d.mu.compact.manualLen.Load())
	allowedBasedOnSpaceHeuristic := int(d.mu.compact.burstConcurrency.Load())

	v := allowedBasedOnBacklog + allowedBasedOnManual + allowedBasedOnSpaceHeuristic
	if v == allowedBasedOnBacklog {
		return v
	}
	_, maxAllowed := d.opts.CompactionConcurrencyRange()
	return min(v, maxAllowed)
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
		switch d.tryLaunchDownloadCompaction(download, vers, d.mu.versions.latest.l0Organizer, env, maxConcurrentDownloads) {
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

func (d *DB) pickManualCompaction(env compactionEnv) (pc pickedCompaction) {
	v := d.mu.versions.currentVersion()
	for len(d.mu.compact.manual) > 0 {
		manual := d.mu.compact.manual[0]
		pc, retryLater := newPickedManualCompaction(v, d.mu.versions.latest.l0Organizer,
			d.opts, env, d.mu.versions.picker.getBaseLevel(), manual)
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

// compact runs one compaction and maybe schedules another call to compact.
func (d *DB) compact(c compaction, errChannel chan error) {
	pprof.Do(context.Background(), c.PprofLabels(d.opts.Experimental.UserKeyCategories), func(context.Context) {
		func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			jobID := d.newJobIDLocked()

			compactErr := c.Execute(jobID, d)

			d.deleteObsoleteFiles(jobID)
			// We send on the error channel only after we've deleted
			// obsolete files so that tests performing manual compactions
			// block until the obsolete files are deleted, and the test
			// observes the deletion.
			if errChannel != nil {
				errChannel <- compactErr
			}
			if compactErr != nil {
				d.handleCompactFailure(c, compactErr)
			}
			if c.IsDownload() {
				d.mu.compact.downloadingCount--
			} else {
				d.mu.compact.compactingCount--
			}
			if c.UsesBurstConcurrency() {
				if v := d.mu.compact.burstConcurrency.Add(-1); v < 0 {
					panic(errors.AssertionFailedf("burst concurrency underflow: %d", v))
				}
			}
			delete(d.mu.compact.inProgress, c)
			// Add this compaction's duration to the cumulative duration. NB: This
			// must be atomic with the above removal of c from
			// d.mu.compact.InProgress to ensure Metrics.Compact.Duration does not
			// miss or double count a completing compaction's duration.
			d.mu.compact.duration += d.opts.private.timeNow().Sub(c.BeganAt())
		}()
		// Done must not be called while holding any lock that needs to be
		// acquired by Schedule. Also, it must be called after new Version has
		// been installed, and metadata related to compactingCount and inProgress
		// compactions has been updated. This is because when we are running at
		// the limit of permitted compactions, Done can cause the
		// CompactionScheduler to schedule another compaction. Note that the only
		// compactions that may be scheduled by Done are those integrated with the
		// CompactionScheduler.
		c.GrantHandle().Done()
		// The previous compaction may have produced too many files in a level, so
		// reschedule another compaction if needed.
		//
		// The preceding Done call will not necessarily cause a compaction to be
		// scheduled, so we also need to call maybeScheduleCompaction. And
		// maybeScheduleCompaction encompasses all compactions, and not only those
		// scheduled via the CompactionScheduler.
		func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			d.maybeScheduleCompaction()
			d.mu.compact.compactProcesses--
			d.mu.compact.cond.Broadcast()
		}()
	})
}

func (d *DB) handleCompactFailure(c compaction, err error) {
	if errors.Is(err, ErrCancelledCompaction) {
		// ErrCancelledCompaction is expected during normal operation, so we don't
		// want to report it as a background error.
		d.opts.Logger.Infof("%v", err)
		return
	}
	c.RecordError(&d.problemSpans, err)
	// TODO(peter): count consecutive compaction errors and backoff.
	d.opts.EventListener.BackgroundError(err)
}

// cleanupVersionEdit cleans up any on-disk artifacts that were created
// for the application of a versionEdit that is no longer going to be applied.
//
// d.mu must be held when calling this method.
func (d *DB) cleanupVersionEdit(ve *manifest.VersionEdit) {
	obsoleteFiles := manifest.ObsoleteFiles{
		TableBackings: make([]*manifest.TableBacking, 0, len(ve.NewTables)),
		BlobFiles:     make([]*manifest.PhysicalBlobFile, 0, len(ve.NewBlobFiles)),
	}
	deletedTables := make(map[base.TableNum]struct{})
	for key := range ve.DeletedTables {
		deletedTables[key.FileNum] = struct{}{}
	}
	for i := range ve.NewBlobFiles {
		obsoleteFiles.AddBlob(ve.NewBlobFiles[i].Physical)
		d.mu.versions.zombieBlobs.Add(objectInfo{
			fileInfo: fileInfo{
				FileNum:  ve.NewBlobFiles[i].Physical.FileNum,
				FileSize: ve.NewBlobFiles[i].Physical.Size,
			},
			placement: objstorage.Placement(d.objProvider, base.FileTypeBlob, ve.NewBlobFiles[i].Physical.FileNum),
		})
	}
	for i := range ve.NewTables {
		if ve.NewTables[i].Meta.Virtual {
			// We handle backing files separately.
			continue
		}
		if _, ok := deletedTables[ve.NewTables[i].Meta.TableNum]; ok {
			// This file is being moved in this ve to a different level.
			// Don't mark it as obsolete.
			continue
		}
		obsoleteFiles.AddBacking(ve.NewTables[i].Meta.PhysicalMeta().TableBacking)
	}
	for i := range ve.CreatedBackingTables {
		if ve.CreatedBackingTables[i].IsUnused() {
			obsoleteFiles.AddBacking(ve.CreatedBackingTables[i])
		}
	}
	for _, of := range obsoleteFiles.TableBackings {
		// Add this file to zombie tables as well, as the versionSet
		// asserts on whether every obsolete file was at one point
		// marked zombie.
		d.mu.versions.zombieTables.Add(objectInfo{
			fileInfo: fileInfo{
				FileNum:  of.DiskFileNum,
				FileSize: of.Size,
			},
			placement: objstorage.Placement(d.objProvider, base.FileTypeTable, of.DiskFileNum),
		})
	}
	d.mu.versions.addObsoleteLocked(obsoleteFiles)
}

// compact1 runs one compaction.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) compact1(jobID JobID, c *tableCompaction) (err error) {
	info := c.makeInfo(jobID)
	d.opts.EventListener.CompactionBegin(info)
	startTime := d.opts.private.timeNow()

	ve, stats, outputBlobs, err := d.runCompaction(jobID, c)

	info.Annotations = append(info.Annotations, c.annotations...)
	info.Duration = d.opts.private.timeNow().Sub(startTime)
	if err == nil {
		validateVersionEdit(ve, d.opts.Comparer.ValidateKey, d.opts.Comparer.FormatKey, d.opts.Logger)
		_, err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
			// Check if this compaction had a conflicting operation (eg. a d.excise())
			// that necessitates it restarting from scratch. Note that since we hold
			// the manifest lock, we don't expect this bool to change its value
			// as only the holder of the manifest lock will ever write to it.
			if c.cancel.Load() {
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
				Metrics:                 c.metrics.perLevel,
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
		for i := range outputBlobs {
			b := &outputBlobs[i]
			info.Output.Blobs = append(info.Output.Blobs, BlobFileInfo{
				BlobFileID:      base.BlobFileID(b.Metadata.FileNum),
				DiskFileNum:     b.ObjMeta.DiskFileNum,
				Size:            b.Metadata.Size,
				ValueSize:       b.Stats.UncompressedValueBytes,
				MVCCGarbageSize: b.Stats.MVCCGarbageBytes,
			})
		}
		d.mu.snapshots.cumulativePinnedCount += stats.CumulativePinnedKeys
		d.mu.snapshots.cumulativePinnedSize += stats.CumulativePinnedSize
		d.mu.versions.metrics.Keys.MissizedTombstonesCount += stats.CountMissizedDels
	}

	// NB: clearing compacting state must occur before updating the read state;
	// L0Sublevels initialization depends on it.
	d.clearCompactingState(c, err != nil)
	d.mu.versions.incrementCompactions(c.kind, c.extraLevels, c.metrics.bytesWritten.Load(), err)
	d.mu.versions.incrementCompactionBytes(-c.metrics.bytesWritten.Load())

	info.TotalDuration = d.opts.private.timeNow().Sub(c.metrics.beganAt)
	d.opts.EventListener.CompactionEnd(info)

	// Update the read state before deleting obsolete files because the
	// read-state update will cause the previous version to be unref'd and if
	// there are no references obsolete tables will be added to the obsolete
	// table list.
	if err == nil {
		d.updateReadStateLocked(d.opts.DebugCheck)
		d.updateTableStatsLocked(ve.NewTables)
	}

	return err
}

// runCopyCompaction runs a copy compaction where a new TableNum is created that
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
	jobID JobID, c *tableCompaction,
) (ve *manifest.VersionEdit, stats compact.Stats, blobs []compact.OutputBlob, _ error) {
	if c.cancel.Load() {
		return nil, compact.Stats{}, blobs, ErrCancelledCompaction
	}
	iter := c.startLevel.files.Iter()
	inputMeta := iter.First()
	if iter.Next() != nil {
		return nil, compact.Stats{}, []compact.OutputBlob{}, base.AssertionFailedf("got more than one file for a move compaction")
	}
	if inputMeta.BlobReferenceDepth > 0 || len(inputMeta.BlobReferences) > 0 {
		return nil, compact.Stats{}, []compact.OutputBlob{}, base.AssertionFailedf(
			"copy compaction for %s with blob references (depth=%d, refs=%d)",
			inputMeta.TableNum, inputMeta.BlobReferenceDepth, len(inputMeta.BlobReferences),
		)
	}
	ve = &manifest.VersionEdit{
		DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{
			{Level: c.startLevel.level, FileNum: inputMeta.TableNum}: inputMeta,
		},
	}

	objMeta, err := d.objProvider.Lookup(base.FileTypeTable, inputMeta.TableBacking.DiskFileNum)
	if err != nil {
		return nil, compact.Stats{}, []compact.OutputBlob{}, err
	}
	// This code does not support copying a shared table (which should never be necessary).
	if objMeta.IsShared() {
		return nil, compact.Stats{}, []compact.OutputBlob{}, base.AssertionFailedf("copy compaction of shared table")
	}

	// We are in the relatively more complex case where we need to copy this
	// file to remote storage. Drop the db mutex while we do the copy
	//
	// To ease up cleanup of the local file and tracking of refs, we create
	// a new FileNum. This has the potential of making the block cache less
	// effective, however.
	newMeta := &manifest.TableMetadata{
		Size:                     inputMeta.Size,
		CreationTime:             inputMeta.CreationTime,
		SeqNums:                  inputMeta.SeqNums,
		LargestSeqNumAbsolute:    inputMeta.LargestSeqNumAbsolute,
		Virtual:                  inputMeta.Virtual,
		SyntheticPrefixAndSuffix: inputMeta.SyntheticPrefixAndSuffix,
	}
	if inputStats, ok := inputMeta.Stats(); ok {
		newMeta.PopulateStats(inputStats)
	}
	if inputMeta.HasPointKeys {
		newMeta.ExtendPointKeyBounds(c.comparer.Compare,
			inputMeta.PointKeyBounds.Smallest(),
			inputMeta.PointKeyBounds.Largest())
	}
	if inputMeta.HasRangeKeys {
		newMeta.ExtendRangeKeyBounds(c.comparer.Compare,
			inputMeta.RangeKeyBounds.Smallest(),
			inputMeta.RangeKeyBounds.Largest())
	}
	newMeta.TableNum = d.mu.versions.getNextTableNum()
	if objMeta.IsExternal() {
		// external -> local/shared copy. File must be virtual.
		// We will update this size later after we produce the new backing file.
		newMeta.InitVirtualBacking(base.DiskFileNum(newMeta.TableNum), inputMeta.TableBacking.Size)
	} else {
		// local -> shared copy. New file is guaranteed to not be virtual.
		newMeta.InitPhysicalBacking()
	}

	// NB: The order here is reversed, lock after unlock. This is similar to
	// runCompaction.
	d.mu.Unlock()
	defer d.mu.Lock()

	deleteOnExit := false
	defer func() {
		if deleteOnExit {
			_ = d.objProvider.Remove(base.FileTypeTable, newMeta.TableBacking.DiskFileNum)
		}
	}()

	// If the src obj is external, we're doing an external to local/shared copy.
	if objMeta.IsExternal() {
		ctx := context.TODO()
		src, err := d.objProvider.OpenForReading(
			ctx, base.FileTypeTable, inputMeta.TableBacking.DiskFileNum, objstorage.OpenOptions{},
		)
		if err != nil {
			return nil, compact.Stats{}, []compact.OutputBlob{}, err
		}
		defer func() {
			if src != nil {
				_ = src.Close()
			}
		}()

		w, _, err := d.objProvider.Create(ctx, base.FileTypeTable, newMeta.TableBacking.DiskFileNum, c.objCreateOpts)
		if err != nil {
			return nil, compact.Stats{}, []compact.OutputBlob{}, err
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
			writerOpts := d.opts.MakeWriterOptions(c.outputLevel.level, d.TableFormat())
			writerOpts.CompressionCounters = d.compressionCounters.Compressed.ForLevel(base.MakeLevel(c.outputLevel.level))
			// TODO(radu): plumb a ReadEnv to CopySpan (it could use the buffer pool
			// or update category stats).
			wrote, err = sstable.CopySpan(ctx,
				src, r, c.startLevel.level,
				w, d.makeWriterOptions(c.outputLevel.level),
				start, end,
			)
			return err
		})

		src = nil // We passed src to CopySpan; it's responsible for closing it.
		if err != nil {
			if errors.Is(err, sstable.ErrEmptySpan) {
				// The virtual table was empty. Just remove the backing file.
				// Note that deleteOnExit is true so we will delete the created object.
				outputMetrics := c.metrics.perLevel.level(c.outputLevel.level)
				outputMetrics.TableBytesIn = inputMeta.Size

				return ve, compact.Stats{}, []compact.OutputBlob{}, nil
			}
			return nil, compact.Stats{}, []compact.OutputBlob{}, err
		}
		newMeta.TableBacking.Size = wrote
		newMeta.Size = wrote
	} else {
		_, err := d.objProvider.LinkOrCopyFromLocal(context.TODO(), d.opts.FS,
			d.objProvider.Path(objMeta), base.FileTypeTable, newMeta.TableBacking.DiskFileNum,
			objstorage.CreateOptions{PreferSharedStorage: true})
		if err != nil {
			return nil, compact.Stats{}, []compact.OutputBlob{}, err
		}
		deleteOnExit = true
	}
	ve.NewTables = []manifest.NewTableEntry{{
		Level: c.outputLevel.level,
		Meta:  newMeta,
	}}
	if newMeta.Virtual {
		ve.CreatedBackingTables = []*manifest.TableBacking{newMeta.TableBacking}
	}
	outputMetrics := c.metrics.perLevel.level(c.outputLevel.level)
	outputMetrics.TableBytesIn = inputMeta.Size
	outputMetrics.TablesCompacted.Inc(newMeta.Size)

	if err := d.objProvider.Sync(); err != nil {
		return nil, compact.Stats{}, []compact.OutputBlob{}, err
	}
	deleteOnExit = false
	return ve, compact.Stats{}, []compact.OutputBlob{}, nil
}

func (d *DB) runMoveCompaction(
	jobID JobID, c *tableCompaction,
) (ve *manifest.VersionEdit, stats compact.Stats, blobs []compact.OutputBlob, _ error) {
	iter := c.startLevel.files.Iter()
	meta := iter.First()
	if iter.Next() != nil {
		return nil, stats, blobs, base.AssertionFailedf("got more than one file for a move compaction")
	}
	if c.cancel.Load() {
		return ve, stats, blobs, ErrCancelledCompaction
	}
	outputMetrics := c.metrics.perLevel.level(c.outputLevel.level)
	outputMetrics.TablesMoved.Inc(meta.Size)
	ve = &manifest.VersionEdit{
		DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{
			{Level: c.startLevel.level, FileNum: meta.TableNum}: meta,
		},
		NewTables: []manifest.NewTableEntry{
			{Level: c.outputLevel.level, Meta: meta},
		},
	}

	return ve, stats, blobs, nil
}

// runCompaction runs a compaction that produces new on-disk tables from
// memtables or old on-disk tables.
//
// runCompaction cannot be used for compactionKindIngestedFlushable.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) runCompaction(
	jobID JobID, c *tableCompaction,
) (
	ve *manifest.VersionEdit,
	stats compact.Stats,
	outputBlobs []compact.OutputBlob,
	retErr error,
) {
	if c.cancel.Load() {
		return ve, stats, outputBlobs, ErrCancelledCompaction
	}
	switch c.kind {
	case compactionKindDeleteOnly:
		return d.runDeleteOnlyCompaction(c)
	case compactionKindMove:
		return d.runMoveCompaction(jobID, c)
	case compactionKindCopy:
		return d.runCopyCompaction(jobID, c)
	case compactionKindIngestedFlushable:
		panic("pebble: runCompaction cannot handle compactionKindIngestedFlushable.")
	}
	return d.runDefaultTableCompaction(jobID, c)
}

func (d *DB) runDefaultTableCompaction(
	jobID JobID, c *tableCompaction,
) (
	ve *manifest.VersionEdit,
	stats compact.Stats,
	outputBlobs []compact.OutputBlob,
	retErr error,
) {
	snapshots := d.mu.snapshots.toSlice()

	// Release the d.mu lock while doing I/O.
	// Note the unusual order: Unlock and then Lock.
	d.mu.Unlock()
	defer d.mu.Lock()

	result := d.compactAndWrite(jobID, c, snapshots)
	if result.Err == nil {
		ve, result.Err = c.makeVersionEdit(result)
	}
	if result.Err != nil {
		// Delete any created tables or blob files.
		obsoleteFiles := manifest.ObsoleteFiles{
			TableBackings: make([]*manifest.TableBacking, 0, len(result.Tables)),
			BlobFiles:     make([]*manifest.PhysicalBlobFile, 0, len(result.Blobs)),
		}
		d.mu.Lock()
		for i := range result.Tables {
			backing := &manifest.TableBacking{
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
	return ve, result.Stats, result.Blobs, result.Err
}

// compactAndWrite runs the data part of a compaction, where we set up a
// compaction iterator and use it to write output tables.
func (d *DB) compactAndWrite(
	jobID JobID, c *tableCompaction, snapshots compact.Snapshots,
) (result compact.Result) {
	suggestedCacheReaders := blob.SuggestedCachedReaders(len(c.inputs))
	// Compactions use a pool of buffers to read blocks, avoiding polluting the
	// block cache with blocks that will not be read again. We initialize the
	// buffer pool with a size 12. This initial size does not need to be
	// accurate, because the pool will grow to accommodate the maximum number of
	// blocks allocated at a given time over the course of the compaction. But
	// choosing a size larger than that working set avoids any additional
	// allocations to grow the size of the pool over the course of iteration.
	//
	// Justification for initial size 18: In a compaction with up to 3 levels,
	// at any given moment we'll have 3 index blocks in-use and 3 data blocks in-use.
	// Additionally, when decoding a compressed block, we'll temporarily
	// allocate 1 additional block to hold the compressed buffer. In the worst
	// case that all input sstables have two-level index blocks (+3), value
	// blocks (+3), range deletion blocks (+n) and range key blocks (+n), we'll
	// additionally require 2n+6 blocks where n is the number of input sstables.
	// Range deletion and range key blocks are relatively rare, and the cost of
	// an additional allocation or two over the course of the compaction is
	// considered to be okay. A larger initial size would cause the pool to hold
	// on to more memory, even when it's not in-use because the pool will
	// recycle buffers up to the current capacity of the pool. The memory use of
	// a 18-buffer pool is expected to be within reason, even if all the buffers
	// grow to the typical size of an index block (256 KiB) which would
	// translate to 4.5 MiB per compaction.
	c.iterationState.bufferPool.Init(18+suggestedCacheReaders*2, block.ForCompaction)
	defer c.iterationState.bufferPool.Release()
	blockReadEnv := block.ReadEnv{
		BufferPool: &c.iterationState.bufferPool,
		Stats:      &c.metrics.internalIterStats,
		IterStats: d.fileCache.SSTStatsCollector().Accumulator(
			uint64(uintptr(unsafe.Pointer(c))),
			categoryCompaction,
		),
		ValueRetrievalProfile: d.valueRetrievalProfile.Load(),
	}
	if c.version != nil {
		c.iterationState.valueFetcher.Init(&c.version.BlobFiles, d.fileCache, blockReadEnv, suggestedCacheReaders)
	}
	iiopts := internalIterOpts{
		compaction:       true,
		readEnv:          sstable.ReadEnv{Block: blockReadEnv},
		blobValueFetcher: &c.iterationState.valueFetcher,
	}
	defer func() { _ = c.iterationState.valueFetcher.Close() }()

	pointIter, rangeDelIter, rangeKeyIter, err := c.newInputIters(d.newIters, iiopts)
	defer func() {
		for _, closer := range c.iterationState.keyspanIterClosers {
			closer.FragmentIterator.Close()
		}
	}()
	if err != nil {
		return compact.Result{Err: err}
	}
	cfg := compact.IterConfig{
		Comparer:              c.comparer,
		Merge:                 d.merge,
		TombstoneElision:      c.delElision,
		RangeKeyElision:       c.rangeKeyElision,
		Snapshots:             snapshots,
		IsBottommostDataLayer: c.isBottommostDataLayer(),
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
		MissizedDeleteCallback: func(userKey []byte, elidedSize, expectedSize uint64) {
			d.opts.EventListener.PossibleAPIMisuse(PossibleAPIMisuseInfo{
				Kind:    MissizedDelete,
				UserKey: slices.Clone(userKey),
				ExtraInfo: redact.Sprintf("elidedSize=%d,expectedSize=%d",
					redact.SafeUint(elidedSize), redact.SafeUint(expectedSize)),
			})
		},
	}
	iter := compact.NewIter(cfg, pointIter, rangeDelIter, rangeKeyIter)

	runnerCfg := compact.RunnerConfig{
		CompactionBounds:           c.bounds,
		L0SplitKeys:                c.flush.l0Limits,
		Grandparents:               c.grandparents,
		MaxGrandparentOverlapBytes: c.maxOverlapBytes,
		TargetOutputFileSize:       c.maxOutputFileSize,
		GrantHandle:                c.grantHandle,
	}
	runner := compact.NewRunner(runnerCfg, iter)

	var spanPolicyValid bool
	var spanPolicy base.SpanPolicy
	// If spanPolicyValid is true and spanPolicyEndKey is empty, then spanPolicy
	// applies for the rest of the keyspace.
	var spanPolicyEndKey []byte

	valueSeparation := c.getValueSeparation(jobID, c)
	for runner.MoreDataToWrite() {
		if c.cancel.Load() {
			return runner.Finish().WithError(ErrCancelledCompaction)
		}
		// Create a new table.
		firstKey := runner.FirstKey()
		if !spanPolicyValid || (len(spanPolicyEndKey) > 0 && d.cmp(firstKey, spanPolicyEndKey) >= 0) {
			var err error
			spanPolicy, spanPolicyEndKey, err = d.opts.Experimental.SpanPolicyFunc(firstKey)
			if err != nil {
				return runner.Finish().WithError(err)
			}
			spanPolicyValid = true
		}
		writerOpts := d.makeWriterOptions(c.eventualOutputLevel)
		if spanPolicy.ValueStoragePolicy.DisableSeparationBySuffix {
			writerOpts.DisableValueBlocks = true
		}
		if spanPolicy.PreferFastCompression && writerOpts.Compression != block.NoCompression {
			writerOpts.Compression = block.FastestCompression
		}
		vSep := valueSeparation
		if spanPolicy.ValueStoragePolicy.DisableBlobSeparation {
			vSep = valsep.NeverSeparateValues{}
		} else if spanPolicy.ValueStoragePolicy.ContainsOverrides() {
			vSep.SetNextOutputConfig(valsep.ValueSeparationOutputConfig{
				MinimumSize:                    spanPolicy.ValueStoragePolicy.OverrideBlobSeparationMinimumSize,
				DisableValueSeparationBySuffix: spanPolicy.ValueStoragePolicy.DisableSeparationBySuffix,
				MinimumMVCCGarbageSize:         spanPolicy.ValueStoragePolicy.MinimumMVCCGarbageSize,
			})
		}
		objMeta, tw, err := d.newCompactionOutputTable(jobID, c, writerOpts)
		if err != nil {
			return runner.Finish().WithError(err)
		}
		runner.WriteTable(objMeta, tw, spanPolicyEndKey, vSep)
	}
	result = runner.Finish()
	if result.Err == nil {
		result.Err = d.objProvider.Sync()
	}
	return result
}

// makeVersionEdit creates the version edit for a compaction, based on the
// tables in compact.Result.
func (c *tableCompaction) makeVersionEdit(result compact.Result) (*manifest.VersionEdit, error) {
	ve := &manifest.VersionEdit{
		DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{},
	}
	for _, cl := range c.inputs {
		for f := range cl.files.All() {
			ve.DeletedTables[manifest.DeletedTableEntry{
				Level:   cl.level,
				FileNum: f.TableNum,
			}] = f
		}
	}
	// Add any newly constructed blob files to the version edit.
	ve.NewBlobFiles = make([]manifest.BlobFileMetadata, len(result.Blobs))
	for i := range result.Blobs {
		ve.NewBlobFiles[i] = manifest.BlobFileMetadata{
			FileID:   base.BlobFileID(result.Blobs[i].Metadata.FileNum),
			Physical: result.Blobs[i].Metadata,
		}
	}

	startLevelBytes := c.startLevel.files.TableSizeSum()

	outputMetrics := c.metrics.perLevel.level(c.outputLevel.level)
	outputMetrics.TableBytesIn = startLevelBytes
	for i := range c.metrics.internalIterStats.BlockReads {
		switch blockkind.Kind(i) {
		case blockkind.BlobValue:
			outputMetrics.BlobBytesRead += c.metrics.internalIterStats.BlockReads[i].BlockBytes
		default:
			outputMetrics.TableBytesRead += c.metrics.internalIterStats.BlockReads[i].BlockBytes
		}
	}
	if len(c.extraLevels) > 0 {
		outputMetrics.TableBytesIn += c.extraLevels[0].files.TableSizeSum()
	}

	if len(c.flush.flushables) == 0 {
		c.metrics.perLevel.level(c.startLevel.level)
		outputMetrics.BlobBytesCompacted = result.Stats.CumulativeBlobFileSize
	} else {
		outputMetrics.BlobBytesFlushed = result.Stats.CumulativeBlobFileSize
	}
	if len(c.extraLevels) > 0 {
		c.metrics.perLevel.level(c.extraLevels[0].level)
		outputMetrics.MultiLevel.TableBytesInTop = startLevelBytes
		outputMetrics.MultiLevel.TableBytesIn = outputMetrics.TableBytesIn
		outputMetrics.MultiLevel.TableBytesRead = outputMetrics.TableBytesRead
	}

	inputLargestSeqNumAbsolute := c.inputLargestSeqNumAbsolute()
	ve.NewTables = make([]manifest.NewTableEntry, len(result.Tables))
	for i := range result.Tables {
		t := &result.Tables[i]

		if t.WriterMeta.Properties.NumValuesInBlobFiles > 0 {
			if len(t.BlobReferences) == 0 {
				return nil, base.AssertionFailedf("num values in blob files %d but no blob references",
					t.WriterMeta.Properties.NumValuesInBlobFiles)
			}
		}

		fileMeta := &manifest.TableMetadata{
			TableNum:           base.PhysicalTableFileNum(t.ObjMeta.DiskFileNum),
			CreationTime:       t.CreationTime.Unix(),
			Size:               t.WriterMeta.Size,
			SeqNums:            t.WriterMeta.SeqNums,
			BlobReferences:     t.BlobReferences,
			BlobReferenceDepth: t.BlobReferenceDepth,
		}
		if c.flush.flushables == nil {
			// Set the file's LargestSeqNumAbsolute to be the maximum value of any
			// of the compaction's input sstables.
			// TODO(jackson): This could be narrowed to be the maximum of input
			// sstables that overlap the output sstable's key range.
			fileMeta.LargestSeqNumAbsolute = inputLargestSeqNumAbsolute
		} else {
			fileMeta.LargestSeqNumAbsolute = t.WriterMeta.SeqNums.High
		}
		fileMeta.InitPhysicalBacking()

		// If the file didn't contain any range deletions, we can fill its
		// table stats now, avoiding unnecessarily loading the table later.
		maybeSetStatsFromProperties(fileMeta.PhysicalMeta(), &t.WriterMeta.Properties)

		if t.WriterMeta.HasPointKeys {
			fileMeta.ExtendPointKeyBounds(c.comparer.Compare,
				t.WriterMeta.SmallestPoint,
				t.WriterMeta.LargestPoint)
		}
		if t.WriterMeta.HasRangeDelKeys {
			fileMeta.ExtendPointKeyBounds(c.comparer.Compare,
				t.WriterMeta.SmallestRangeDel,
				t.WriterMeta.LargestRangeDel)
		}
		if t.WriterMeta.HasRangeKeys {
			fileMeta.ExtendRangeKeyBounds(c.comparer.Compare,
				t.WriterMeta.SmallestRangeKey,
				t.WriterMeta.LargestRangeKey)
		}

		ve.NewTables[i] = manifest.NewTableEntry{
			Level: c.outputLevel.level,
			Meta:  fileMeta,
		}
		// Update metrics.
		if c.flush.flushables == nil {
			outputMetrics.TablesCompacted.Inc(fileMeta.Size)
		} else {
			outputMetrics.TablesFlushed.Inc(fileMeta.Size)
		}
		outputMetrics.Additional.BytesWrittenDataBlocks += t.WriterMeta.Properties.DataSize
		outputMetrics.Additional.BytesWrittenValueBlocks += t.WriterMeta.Properties.ValueBlocksSize
	}

	// Sanity check that the tables are ordered and don't overlap.
	for i := 1; i < len(ve.NewTables); i++ {
		if ve.NewTables[i-1].Meta.Largest().IsUpperBoundFor(c.comparer.Compare, ve.NewTables[i].Meta.Smallest().UserKey) {
			return nil, base.AssertionFailedf("pebble: compaction output tables overlap: %s and %s",
				ve.NewTables[i-1].Meta.DebugString(c.comparer.FormatKey, true),
				ve.NewTables[i].Meta.DebugString(c.comparer.FormatKey, true),
			)
		}
	}

	return ve, nil
}

// newCompactionOutputTable creates an object for a new table produced by a
// compaction or flush.
func (d *DB) newCompactionOutputTable(
	jobID JobID, c *tableCompaction, writerOpts sstable.WriterOptions,
) (objstorage.ObjectMetadata, sstable.RawWriter, error) {
	writable, objMeta, err := d.newCompactionOutputObj(
		base.FileTypeTable, c.kind, c.outputLevel.level, &c.metrics.bytesWritten, c.objCreateOpts)
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
	jobID JobID,
	kind compactionKind,
	outputLevel int,
	bytesWritten *atomic.Int64,
	opts objstorage.CreateOptions,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	writable, objMeta, err := d.newCompactionOutputObj(base.FileTypeBlob, kind, outputLevel, bytesWritten, opts)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}
	d.opts.EventListener.BlobFileCreated(BlobFileCreateInfo{
		JobID:   int(jobID),
		Reason:  kind.compactingOrFlushing(),
		Path:    d.objProvider.Path(objMeta),
		FileNum: objMeta.DiskFileNum,
	})
	return writable, objMeta, nil
}

// newCompactionOutputObj creates an object produced by a compaction or flush.
func (d *DB) newCompactionOutputObj(
	typ base.FileType,
	kind compactionKind,
	outputLevel int,
	bytesWritten *atomic.Int64,
	opts objstorage.CreateOptions,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	diskFileNum := d.mu.versions.getNextDiskFileNum()
	ctx := context.TODO()

	if objiotracing.Enabled {
		ctx = objiotracing.WithLevel(ctx, outputLevel)
		if kind == compactionKindFlush {
			ctx = objiotracing.WithReason(ctx, objiotracing.ForFlush)
		} else {
			ctx = objiotracing.WithReason(ctx, objiotracing.ForCompaction)
		}
	}

	writable, objMeta, err := d.objProvider.Create(ctx, typ, diskFileNum, opts)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}

	if kind != compactionKindFlush {
		writable = &compactionWritable{
			Writable: writable,
			versions: d.mu.versions,
			written:  bytesWritten,
		}
	}
	return writable, objMeta, nil
}

// validateVersionEdit validates that start and end keys across new and deleted
// files in a versionEdit pass the given validation function.
func validateVersionEdit(
	ve *manifest.VersionEdit, vk base.ValidateKey, format base.FormatKey, logger Logger,
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

func getDiskWriteCategoryForCompaction(opts *Options, kind compactionKind) vfs.DiskWriteCategory {
	if opts.EnableSQLRowSpillMetrics {
		// In the scenario that the Pebble engine is used for SQL row spills the
		// data written to the memtable will correspond to spills to disk and
		// should be categorized as such.
		return "sql-row-spill"
	} else if kind == compactionKindFlush {
		return "pebble-memtable-flush"
	} else if kind == compactionKindBlobFileRewrite {
		return "pebble-blob-file-rewrite"
	} else {
		return "pebble-compaction"
	}
}

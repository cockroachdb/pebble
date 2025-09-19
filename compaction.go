// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	stdcmp "cmp"
	"context"
	"fmt"
	"iter"
	"maps"
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
	"github.com/cockroachdb/pebble/internal/problemspans"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
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
	getValueSeparation func(JobID, *tableCompaction, sstable.TableFormat) compact.ValueSeparation

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

	// The boundaries of the input data.
	bounds base.UserKeyBounds

	// grandparents are the tables in level+2 that overlap with the files being
	// compacted. Used to determine output table boundaries. Do not assume that the actual files
	// in the grandparent when this compaction finishes will be the same.
	grandparents manifest.LevelSlice

	delElision      compact.TombstoneElision
	rangeKeyElision compact.TombstoneElision

	// deleteOnly contains information specific to compactions with kind
	// compactionKindDeleteOnly. A delete-only compaction is a special
	// compaction that does not merge or write sstables. Instead, it only
	// performs deletions either through removing whole sstables from the LSM or
	// virtualizing them into virtual sstables.
	deleteOnly struct {
		// hints are collected by the table stats collector and describe range
		// deletions and the files containing keys deleted by them.
		hints []deleteCompactionHint
		// exciseEnabled is set to true if this compaction is allowed to excise
		// files. If false, the compaction will only remove whole sstables that
		// are wholly contained within the bounds of range deletions.
		exciseEnabled bool
	}
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

	tableFormat   sstable.TableFormat
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

type getValueSeparation func(JobID, *tableCompaction, sstable.TableFormat) compact.ValueSeparation

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
	tableFormat sstable.TableFormat,
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
		maxOutputFileSize:  pc.maxOutputFileSize,
		maxOverlapBytes:    pc.maxOverlapBytes,
		metrics: compactionMetrics{
			beganAt: beganAt,
			picker:  pc.pickerMetrics,
		},
		grantHandle: grantHandle,
		tableFormat: tableFormat,
	}
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
	if c.outputLevel.level+1 < numLevels {
		c.grandparents = c.version.Overlaps(max(c.outputLevel.level+1, pc.baseLevel), c.bounds)
	}
	c.delElision, c.rangeKeyElision = compact.SetupTombstoneElision(
		c.comparer.Compare, c.version, pc.l0Organizer, c.outputLevel.level, c.bounds,
	)
	c.kind = pc.kind

	preferSharedStorage := tableFormat >= FormatMinForSharedObjects.MaxTableFormat() &&
		remote.ShouldCreateShared(opts.Experimental.CreateOnShared, c.outputLevel.level)
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
	if c.grandparents.AggregateSizeSum() > c.maxOverlapBytes {
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

// newDeleteOnlyCompaction constructs a delete-only compaction from the provided
// inputs.
//
// The compaction is created with a reference to its version that must be
// released when the compaction is complete.
func newDeleteOnlyCompaction(
	opts *Options,
	cur *manifest.Version,
	inputs []compactionLevel,
	beganAt time.Time,
	hints []deleteCompactionHint,
	exciseEnabled bool,
) *tableCompaction {
	c := &tableCompaction{
		kind:        compactionKindDeleteOnly,
		comparer:    opts.Comparer,
		logger:      opts.Logger,
		version:     cur,
		inputs:      inputs,
		grantHandle: noopGrantHandle{},
		metrics: compactionMetrics{
			beganAt: beganAt,
		},
	}
	c.deleteOnly.hints = hints
	c.deleteOnly.exciseEnabled = exciseEnabled
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

	// Set c.smallest, c.largest.
	cmp := opts.Comparer.Compare
	for _, in := range inputs {
		c.bounds = manifest.ExtendKeyRange(cmp, c.bounds, in.files.All())
	}
	return c
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
	tableFormat sstable.TableFormat,
	getValueSeparation getValueSeparation,
) (*tableCompaction, error) {
	c := &tableCompaction{
		kind:               compactionKindFlush,
		comparer:           opts.Comparer,
		logger:             opts.Logger,
		inputs:             []compactionLevel{{level: -1}, {level: 0}},
		getValueSeparation: getValueSeparation,
		maxOutputFileSize:  math.MaxUint64,
		maxOverlapBytes:    math.MaxUint64,
		grantHandle:        noopGrantHandle{},
		tableFormat:        tableFormat,
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

	preferSharedStorage := tableFormat >= FormatMinForSharedObjects.MaxTableFormat() &&
		remote.ShouldCreateShared(opts.Experimental.CreateOnShared, c.outputLevel.level)
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
	f manifest.LevelFile,
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

	updateLevelMetricsOnExcise := func(m *manifest.TableMetadata, level int, added []manifest.NewTableEntry) {
		levelMetrics := c.metrics.perLevel[level]
		if levelMetrics == nil {
			levelMetrics = &LevelMetrics{}
			c.metrics.perLevel[level] = levelMetrics
		}
		levelMetrics.TablesCount--
		levelMetrics.TablesSize -= int64(m.Size)
		levelMetrics.EstimatedReferencesSize -= m.EstimatedReferenceSize()
		for i := range added {
			levelMetrics.TablesCount++
			levelMetrics.TablesSize += int64(added[i].Meta.Size)
			levelMetrics.EstimatedReferencesSize += added[i].Meta.EstimatedReferenceSize()
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
		levelMetrics.TableBytesIngested += file.Size
		levelMetrics.TablesIngested++
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
				updateLevelMetricsOnExcise(m, layer.Level(), newFiles)
			}
		}
	}

	if len(ingestSplitFiles) > 0 {
		if err := d.ingestSplit(context.TODO(), ve, updateLevelMetricsOnExcise, ingestSplitFiles, replacedTables); err != nil {
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

	c, err := newFlush(d.opts, d.mu.versions.currentVersion(), d.mu.versions.latest.l0Organizer,
		d.mu.versions.picker.getBaseLevel(), d.mu.mem.queue[:n], d.timeNow(), d.TableFormat(), d.determineCompactionValueSeparation)
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

	startTime := d.timeNow()

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
		info.Duration = d.timeNow().Sub(startTime)
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
				l0Metrics.TableBytesIn = l0Metrics.TableBytesFlushed + l0Metrics.BlobBytesFlushed
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
				if bounds != nil && bounds.Overlaps(d.cmp, &exciseBounds) {
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
					d.mu.versions.metrics.Flush.AsIngestBytes += l.TableBytesIngested
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

// tryScheduleDeleteOnlyCompaction tries to kick off a delete-only compaction
// for all files that can be deleted as suggested by deletionHints.
//
// Requires d.mu to be held. Updates d.mu.compact.deletionHints.
//
// Returns true iff a compaction was started.
func (d *DB) tryScheduleDeleteOnlyCompaction() bool {
	if d.opts.private.disableDeleteOnlyCompactions || d.opts.DisableAutomaticCompactions ||
		len(d.mu.compact.deletionHints) == 0 {
		return false
	}
	if _, maxConcurrency := d.opts.CompactionConcurrencyRange(); d.mu.compact.compactingCount >= maxConcurrency {
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
		c.AddInProgressLocked(d)
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
	tombstoneFile *manifest.TableMetadata
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
		h.tombstoneLevel, h.tombstoneFile.TableNum, h.start, h.end,
		h.tombstoneSmallestSeqNum, h.tombstoneLargestSeqNum, h.fileSmallestSeqNum,
		h.hintType,
	)
}

func (h *deleteCompactionHint) canDeleteOrExcise(
	cmp Compare, m *manifest.TableMetadata, snapshots compact.Snapshots, exciseEnabled bool,
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
	v *manifest.Version,
	hints []deleteCompactionHint,
	snapshots compact.Snapshots,
	exciseEnabled bool,
) (levels []compactionLevel, resolved, unresolved []deleteCompactionHint) {
	var files map[*manifest.TableMetadata]bool
	var byLevel [numLevels][]*manifest.TableMetadata

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
		var filesDeletedByLevel [7][]*manifest.TableMetadata
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
					files = make(map[*manifest.TableMetadata]bool)
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
			d.mu.compact.duration += d.timeNow().Sub(c.BeganAt())
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
			isLocal: objstorage.IsLocalBlobFile(d.objProvider, ve.NewBlobFiles[i].Physical.FileNum),
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
			isLocal: objstorage.IsLocalTable(d.objProvider, of.DiskFileNum),
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
	startTime := d.timeNow()

	ve, stats, outputBlobs, err := d.runCompaction(jobID, c)

	info.Annotations = append(info.Annotations, c.annotations...)
	info.Duration = d.timeNow().Sub(startTime)
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

	info.TotalDuration = d.timeNow().Sub(c.metrics.beganAt)
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
		SmallestSeqNum:           inputMeta.SmallestSeqNum,
		LargestSeqNum:            inputMeta.LargestSeqNum,
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

		w, _, err := d.objProvider.Create(
			ctx, base.FileTypeTable, newMeta.TableBacking.DiskFileNum,
			objstorage.CreateOptions{
				PreferSharedStorage: d.shouldCreateShared(c.outputLevel.level),
			},
		)
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
			// TODO(radu): plumb a ReadEnv to CopySpan (it could use the buffer pool
			// or update category stats).
			wrote, err = sstable.CopySpan(ctx,
				src, r, c.startLevel.level,
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
	outputMetrics.TableBytesCompacted = newMeta.Size
	outputMetrics.TablesCompacted = 1

	if err := d.objProvider.Sync(); err != nil {
		return nil, compact.Stats{}, []compact.OutputBlob{}, err
	}
	deleteOnExit = false
	return ve, compact.Stats{}, []compact.OutputBlob{}, nil
}

// applyHintOnFile applies a deleteCompactionHint to a file, and updates the
// versionEdit accordingly. It returns a list of new files that were created
// if the hint was applied partially to a file (eg. through an exciseTable as opposed
// to an outright deletion). levelMetrics is kept up-to-date with the number
// of tables deleted or excised.
func (d *DB) applyHintOnFile(
	h deleteCompactionHint,
	f *manifest.TableMetadata,
	level int,
	levelMetrics *LevelMetrics,
	ve *manifest.VersionEdit,
	hintOverlap deletionHintOverlap,
) (newFiles []manifest.NewTableEntry, err error) {
	if hintOverlap == hintDoesNotApply {
		return nil, nil
	}

	// The hint overlaps with at least part of the file.
	if hintOverlap == hintDeletesFile {
		// The hint deletes the entirety of this file.
		ve.DeletedTables[manifest.DeletedTableEntry{
			Level:   level,
			FileNum: f.TableNum,
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
	ve *manifest.VersionEdit,
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
				if _, ok := ve.DeletedTables[manifest.DeletedTableEntry{Level: cl.level, FileNum: curFile.TableNum}]; ok {
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
		if _, ok := ve.DeletedTables[manifest.DeletedTableEntry{
			Level:   cl.level,
			FileNum: f.TableNum,
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
	jobID JobID, c *tableCompaction, snapshots compact.Snapshots,
) (ve *manifest.VersionEdit, stats compact.Stats, blobs []compact.OutputBlob, retErr error) {
	fragments := fragmentDeleteCompactionHints(d.cmp, c.deleteOnly.hints)
	ve = &manifest.VersionEdit{
		DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{},
	}
	for _, cl := range c.inputs {
		levelMetrics := c.metrics.perLevel.level(cl.level)
		err := d.runDeleteOnlyCompactionForLevel(cl, levelMetrics, ve, snapshots, fragments, c.deleteOnly.exciseEnabled)
		if err != nil {
			return nil, stats, blobs, err
		}
	}
	// Remove any files that were added and deleted in the same versionEdit.
	ve.NewTables = slices.DeleteFunc(ve.NewTables, func(e manifest.NewTableEntry) bool {
		entry := manifest.DeletedTableEntry{Level: e.Level, FileNum: e.Meta.TableNum}
		if _, deleted := ve.DeletedTables[entry]; deleted {
			delete(ve.DeletedTables, entry)
			return true
		}
		return false
	})
	sort.Slice(ve.NewTables, func(i, j int) bool {
		return ve.NewTables[i].Meta.TableNum < ve.NewTables[j].Meta.TableNum
	})
	deletedTableEntries := slices.Collect(maps.Keys(ve.DeletedTables))
	slices.SortFunc(deletedTableEntries, func(a, b manifest.DeletedTableEntry) int {
		return stdcmp.Compare(a.FileNum, b.FileNum)
	})
	// Remove any entries from CreatedBackingTables that are not used in any
	// NewFiles.
	usedBackingFiles := make(map[base.DiskFileNum]struct{})
	for _, e := range ve.NewTables {
		if e.Meta.Virtual {
			usedBackingFiles[e.Meta.TableBacking.DiskFileNum] = struct{}{}
		}
	}
	ve.CreatedBackingTables = slices.DeleteFunc(ve.CreatedBackingTables, func(b *manifest.TableBacking) bool {
		_, used := usedBackingFiles[b.DiskFileNum]
		return !used
	})

	// Iterate through the deleted tables and new tables to annotate excised tables.
	// If a new table is virtual and the base.DiskFileNum is the same as a deleted table, then
	// our deleted table was excised.
	for _, table := range deletedTableEntries {
		for _, newEntry := range ve.NewTables {
			if newEntry.Meta.Virtual &&
				newEntry.Meta.TableBacking.DiskFileNum == ve.DeletedTables[table].TableBacking.DiskFileNum {
				c.annotations = append(c.annotations,
					fmt.Sprintf("(excised: %s)", ve.DeletedTables[table].TableNum))
				break
			}
		}

	}

	// Refresh the disk available statistic whenever a compaction/flush
	// completes, before re-acquiring the mutex.
	d.calculateDiskAvailableBytes()
	return ve, stats, blobs, nil
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
	outputMetrics.TableBytesMoved = meta.Size
	outputMetrics.TablesMoved = 1
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

	// Determine whether we should separate values into blob files.
	valueSeparation := c.getValueSeparation(jobID, c, c.tableFormat)

	result := d.compactAndWrite(jobID, c, snapshots, c.tableFormat, valueSeparation)
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
	jobID JobID,
	c *tableCompaction,
	snapshots compact.Snapshots,
	tableFormat sstable.TableFormat,
	valueSeparation compact.ValueSeparation,
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
	c.iterationState.bufferPool.Init(18 + suggestedCacheReaders*2)
	defer c.iterationState.bufferPool.Release()
	blockReadEnv := block.ReadEnv{
		BufferPool: &c.iterationState.bufferPool,
		Stats:      &c.metrics.internalIterStats,
		IterStats: d.fileCache.SSTStatsCollector().Accumulator(
			uint64(uintptr(unsafe.Pointer(c))),
			categoryCompaction,
		),
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
	var spanPolicy SpanPolicy
	// If spanPolicyValid is true and spanPolicyEndKey is empty, then spanPolicy
	// applies for the rest of the keyspace.
	var spanPolicyEndKey []byte

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

		writerOpts := d.opts.MakeWriterOptions(c.outputLevel.level, tableFormat)
		if spanPolicy.DisableValueSeparationBySuffix {
			writerOpts.DisableValueBlocks = true
		}
		if spanPolicy.PreferFastCompression && writerOpts.Compression != block.NoCompression {
			writerOpts.Compression = block.FastestCompression
		}
		vSep := valueSeparation
		switch spanPolicy.ValueStoragePolicy {
		case ValueStorageLowReadLatency:
			vSep = compact.NeverSeparateValues{}
		case ValueStorageLatencyTolerant:
			// This span of keyspace is more tolerant of latency, so set a more
			// aggressive value separation policy for this output.
			vSep.SetNextOutputConfig(compact.ValueSeparationOutputConfig{
				MinimumSize: latencyTolerantMinimumSize,
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
	outputMetrics.BlobBytesCompacted = result.Stats.CumulativeBlobFileSize
	if c.flush.flushables != nil {
		outputMetrics.BlobBytesFlushed = result.Stats.CumulativeBlobFileSize
	}
	if len(c.extraLevels) > 0 {
		outputMetrics.TableBytesIn += c.extraLevels[0].files.TableSizeSum()
	}

	if len(c.flush.flushables) == 0 {
		c.metrics.perLevel.level(c.startLevel.level)
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
			SmallestSeqNum:     t.WriterMeta.SmallestSeqNum,
			LargestSeqNum:      t.WriterMeta.LargestSeqNum,
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
			fileMeta.LargestSeqNumAbsolute = t.WriterMeta.LargestSeqNum
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
			outputMetrics.TablesCompacted++
			outputMetrics.TableBytesCompacted += fileMeta.Size
		} else {
			outputMetrics.TablesFlushed++
			outputMetrics.TableBytesFlushed += fileMeta.Size
		}
		outputMetrics.EstimatedReferencesSize += fileMeta.EstimatedReferenceSize()
		outputMetrics.TablesSize += int64(fileMeta.Size)
		outputMetrics.TablesCount++
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

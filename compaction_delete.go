// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	stdcmp "cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
)

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
		c := newDeleteOnlyCompaction(d.opts, v, inputs, d.opts.private.timeNow(), resolvedHints, exciseEnabled)
		d.mu.compact.compactingCount++
		d.mu.compact.compactProcesses++
		c.AddInProgressLocked(d)
		go d.compact(c, nil)
		return true
	}
	return false
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

// A deleteCompactionHint records a user key and sequence number span that has
// been deleted by tombstones. A hint is recorded if at least one sstable
// containing keys older than the tombstones and either (a) completely falls
// within both the user key, or (b) has a boundary that overlaps with the span
// [which could be shorted with an excise]. Once the tombstones fall into the
// last snapshot stripe, a delete-only compaction may delete or excise any
// applicable sstables within the range.
type deleteCompactionHint struct {
	// The type of key span that generated this hint (point key, range key, or
	// both).
	keyType manifest.KeyType
	// bounds are the bounds of the tombstone(s).
	bounds base.UserKeyBounds
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
		"L%d.%s %s seqnums(tombstone=%d-%d, type=%s)",
		h.tombstoneLevel, h.tombstoneFile.TableNum, h.bounds,
		h.tombstoneSmallestSeqNum, h.tombstoneLargestSeqNum, h.keyType)
}

func (h *deleteCompactionHint) canDeleteOrExcise(
	cmp Compare, m *manifest.TableMetadata, exciseEnabled bool,
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
	if m.LargestSeqNumAbsolute >= h.tombstoneSmallestSeqNum {
		return hintDoesNotApply
	}

	switch h.keyType {
	case manifest.KeyTypePoint:
		// A hint generated by a range del span cannot delete tables that contain
		// range keys.
		if m.HasRangeKeys {
			return hintDoesNotApply
		}
	case manifest.KeyTypeRange:
		// A hint generated by a range key del span cannot delete tables that
		// contain point keys.
		if m.HasPointKeys {
			return hintDoesNotApply
		}
	case manifest.KeyTypePointAndRange:
		// A hint from a span that contains both range dels *and* range keys can
		// only be deleted if both bounds fall within the hint. The next check takes
		// care of this.
	default:
		panic(errors.AssertionFailedf("pebble: unknown delete compaction key type: %s", h.keyType))
	}
	tableBounds := m.UserKeyBounds()
	if h.bounds.ContainsBounds(cmp, tableBounds) {
		return hintDeletesFile
	}
	if !exciseEnabled {
		// The file's keys must be completely contained within the hint range; excises
		// aren't allowed.
		return hintDoesNotApply
	} else if tableBounds.ContainsBounds(cmp, h.bounds) {
		// If the table's bounds completely contain the hint, applying the hint
		// would cut the table into two virtual sstables, increasing the number
		// of tables in the level. We only pursue excises if they shorten an
		// existing table's bounds.
		return hintDoesNotApply
	}
	// Check for any overlap. In cases of partial overlap, we can excise the part of the file
	// that overlaps with the deletion hint.
	if h.bounds.Overlaps(cmp, m.UserKeyBounds()) {
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
		// When a compaction hint is created, the sequence numbers of the range
		// tombstones are recorded. The largest tombstone sequence number must
		// be in the last snapshot stripe for the hint to be resolved. Note that
		// technically the largest tombstone sequence number only needs to be in
		// the same snapshot stripe as an affected table's lowest sequence
		// number. The difference is unlikely to delay hint resolution
		// significantly, and we don't know candidate tables' lowest sequence
		// numbers until we examine the LSM.
		//
		// The below graphic models a compaction hint covering the keyspace [b,
		// r). The hint completely contains two files, 000002 and 000003. The
		// file 000003 contains the lowest covered sequence number at #90. The
		// tombstone b.RANGEDEL.230:h has the highest tombstone sequence number
		// incorporated into the hint. The hint may be resolved only once all
		// the snapshots are closed. File 000001 is not included within the hint
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

		if snapshots.Index(h.tombstoneLargestSeqNum) != 0 ||
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
			for m := range v.Overlaps(l, h.bounds).All() {
				doesHintApply := h.canDeleteOrExcise(cmp, m, exciseEnabled)
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
					if cmp(h.bounds.Start, m.Smallest().UserKey) > 0 {
						filesDeletedByCurrentHint--
					}
					if m.UserKeyBounds().End.IsUpperBoundFor(cmp, h.bounds.End.Key) {
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

// Runs a delete-only compaction.
//
// d.mu must *not* be held when calling this.
func (d *DB) runDeleteOnlyCompaction(
	jobID JobID, c *tableCompaction, snapshots compact.Snapshots,
) (ve *manifest.VersionEdit, stats compact.Stats, blobs []compact.OutputBlob, retErr error) {
	// If any snapshots exist beneath the largest tombstone sequence number,
	// then deleting data beneath the tombstone violates the snapshot's
	// isolation. Validate that all hints' tombstones' sequence numbers fall
	// within the last snapshot stripe.
	for _, h := range c.deleteOnly.hints {
		if snapshots.Index(h.tombstoneLargestSeqNum) != 0 {
			return nil, stats, blobs, errors.AssertionFailedf(
				"tombstone largest sequence number is not in the last snapshot stripe")
		}
	}

	fragments := fragmentDeleteCompactionHints(d.cmp, c.deleteOnly.hints)
	ve = &manifest.VersionEdit{
		DeletedTables: map[manifest.DeletedTableEntry]*manifest.TableMetadata{},
	}
	for _, cl := range c.inputs {
		levelMetrics := c.metrics.perLevel.level(cl.level)
		err := d.runDeleteOnlyCompactionForLevel(cl, levelMetrics, ve, fragments, c.deleteOnly.exciseEnabled)
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
		fragments = append(fragments, deleteCompactionHintFragment{start: hints[i].bounds.Start},
			deleteCompactionHintFragment{start: hints[i].bounds.End.Key})
	}
	slices.SortFunc(fragments, func(i, j deleteCompactionHintFragment) int {
		return cmp(i.start, j.start)
	})
	fragments = slices.CompactFunc(fragments, func(i, j deleteCompactionHintFragment) bool {
		return bytes.Equal(i.start, j.start)
	})
	for _, h := range hints {
		startIdx := sort.Search(len(fragments), func(i int) bool {
			return cmp(fragments[i].start, h.bounds.Start) >= 0
		})
		endIdx := sort.Search(len(fragments), func(i int) bool {
			return cmp(fragments[i].start, h.bounds.End.Key) >= 0
		})
		for i := startIdx; i < endIdx; i++ {
			fragments[i].hints = append(fragments[i].hints, h)
		}
	}
	return fragments
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
	leftTable, rightTable, err := d.exciseTable(context.TODO(), h.bounds, f, level, tightExciseBounds)
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
				hintOverlap := h.canDeleteOrExcise(d.cmp, curFile, exciseEnabled)
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

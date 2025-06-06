// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/strparse"
)

// Compare exports the base.Compare type.
type Compare = base.Compare

// InternalKey exports the base.InternalKey type.
type InternalKey = base.InternalKey

// KeyRange returns the minimum smallest and maximum largest internalKey for
// all the TableMetadata in iters.
func KeyRange(ucmp Compare, iters ...iter.Seq[*TableMetadata]) (smallest, largest InternalKey) {
	first := true
	for _, iter := range iters {
		for meta := range iter {
			if first {
				first = false
				smallest, largest = meta.Smallest(), meta.Largest()
				continue
			}
			if base.InternalCompare(ucmp, smallest, meta.Smallest()) >= 0 {
				smallest = meta.Smallest()
			}
			if base.InternalCompare(ucmp, largest, meta.Largest()) <= 0 {
				largest = meta.Largest()
			}
		}
	}
	return smallest, largest
}

type bySeqNum []*TableMetadata

func (b bySeqNum) Len() int { return len(b) }
func (b bySeqNum) Less(i, j int) bool {
	return b[i].lessSeqNum(b[j])
}
func (b bySeqNum) Swap(i, j int) { b[i], b[j] = b[j], b[i] }

// SortBySeqNum sorts the specified files by increasing sequence number.
func SortBySeqNum(files []*TableMetadata) {
	sort.Sort(bySeqNum(files))
}

type bySmallest struct {
	files []*TableMetadata
	cmp   Compare
}

func (b bySmallest) Len() int { return len(b.files) }
func (b bySmallest) Less(i, j int) bool {
	return b.files[i].cmpSmallestKey(b.files[j], b.cmp) < 0
}
func (b bySmallest) Swap(i, j int) { b.files[i], b.files[j] = b.files[j], b.files[i] }

// SortBySmallest sorts the specified files by smallest key using the supplied
// comparison function to order user keys.
func SortBySmallest(files []*TableMetadata, cmp Compare) {
	sort.Sort(bySmallest{files, cmp})
}

// NumLevels is the number of levels a Version contains.
const NumLevels = 7

// NewInitialVersion creates a version with no files. The L0Organizer should be freshly created.
func NewInitialVersion(comparer *base.Comparer) *Version {
	v := &Version{
		cmp:       comparer,
		BlobFiles: MakeBlobFileSet(nil),
	}
	for level := range v.Levels {
		v.Levels[level] = MakeLevelMetadata(comparer.Compare, level, nil /* files */)
		v.RangeKeyLevels[level] = MakeLevelMetadata(comparer.Compare, level, nil /* files */)
	}
	return v
}

// NewVersionForTesting constructs a new Version with the provided files. It
// requires the provided files are already well-ordered. The L0Organizer should
// be freshly created.
func NewVersionForTesting(
	comparer *base.Comparer, l0Organizer *L0Organizer, files [7][]*TableMetadata,
) *Version {
	v := &Version{
		cmp: comparer,
	}
	for l := range files {
		// NB: We specifically insert `files` into the B-Tree in the order
		// they appear within `files`. Some tests depend on this behavior in
		// order to test consistency checking, etc. Once we've constructed the
		// initial B-Tree, we swap out the btreeCmp for the correct one.
		// TODO(jackson): Adjust or remove the tests and remove this.
		v.Levels[l].tree = makeBTree(btreeCmpSpecificOrder(files[l]), files[l])
		v.Levels[l].level = l
		if l == 0 {
			v.Levels[l].tree.bcmp = btreeCmpSeqNum
		} else {
			v.Levels[l].tree.bcmp = btreeCmpSmallestKey(comparer.Compare)
		}
		for _, f := range files[l] {
			v.Levels[l].totalTableSize += f.Size
		}
	}
	l0Organizer.ResetForTesting(v)
	return v
}

// Version is a collection of table metadata for on-disk tables at various
// levels. In-memory DBs are written to level-0 tables, and compactions
// migrate data from level N to level N+1. The tables map internal keys (which
// are a user key, a delete or set bit, and a sequence number) to user values.
//
// The tables at level 0 are sorted by largest sequence number. Due to file
// ingestion, there may be overlap in the ranges of sequence numbers contain in
// level 0 sstables. In particular, it is valid for one level 0 sstable to have
// the seqnum range [1,100] while an adjacent sstable has the seqnum range
// [50,50]. This occurs when the [50,50] table was ingested and given a global
// seqnum. The ingestion code will have ensured that the [50,50] sstable will
// not have any keys that overlap with the [1,100] in the seqnum range
// [1,49]. The range of internal keys [fileMetadata.smallest,
// fileMetadata.largest] in each level 0 table may overlap.
//
// The tables at any non-0 level are sorted by their internal key range and any
// two tables at the same non-0 level do not overlap.
//
// The internal key ranges of two tables at different levels X and Y may
// overlap, for any X != Y.
//
// Finally, for every internal key in a table at level X, there is no internal
// key in a higher level table that has both the same user key and a higher
// sequence number.
type Version struct {
	refs atomic.Int32

	// L0SublevelFiles contains the L0 sublevels.
	L0SublevelFiles []LevelSlice

	Levels [NumLevels]LevelMetadata

	// RangeKeyLevels holds a subset of the same files as Levels that contain range
	// keys (i.e. fileMeta.HasRangeKeys == true). The memory amplification of this
	// duplication should be minimal, as range keys are expected to be rare.
	RangeKeyLevels [NumLevels]LevelMetadata

	// BlobFiles holds the set of physical blob files that are referenced by the
	// version. The BlobFileSet is responsible for maintaining reference counts
	// on physical blob files so that they remain on storage until they're no
	// longer referenced by any version.
	BlobFiles BlobFileSet

	// The callback to invoke when the last reference to a version is
	// removed. Will be called with list.mu held.
	Deleted func(obsolete ObsoleteFiles)

	// Stats holds aggregated stats about the version maintained from
	// version to version.
	Stats struct {
		// MarkedForCompaction records the count of files marked for
		// compaction within the version.
		MarkedForCompaction int
	}

	cmp *base.Comparer

	// The list the version is linked into.
	list *VersionList

	// The next/prev link for the versionList doubly-linked list of versions.
	prev, next *Version
}

// String implements fmt.Stringer, printing the TableMetadata for each level in
// the Version.
func (v *Version) String() string {
	return v.string(v.cmp.FormatKey, false)
}

// DebugString returns an alternative format to String() which includes sequence
// number and kind information for the sstable boundaries.
func (v *Version) DebugString() string {
	return v.string(v.cmp.FormatKey, true)
}

// DebugStringFormatKey is like DebugString but allows overriding key formatting
// with the provided FormatKey.
func (v *Version) DebugStringFormatKey(fmtKey base.FormatKey) string {
	return v.string(fmtKey, true)
}

func describeSublevels(format base.FormatKey, verbose bool, sublevels []LevelSlice) string {
	var buf bytes.Buffer
	for sublevel := len(sublevels) - 1; sublevel >= 0; sublevel-- {
		fmt.Fprintf(&buf, "L0.%d:\n", sublevel)
		for f := range sublevels[sublevel].All() {
			fmt.Fprintf(&buf, "  %s\n", f.DebugString(format, verbose))
		}
	}
	return buf.String()
}

func (v *Version) string(fmtKey base.FormatKey, verbose bool) string {
	var buf bytes.Buffer
	if len(v.L0SublevelFiles) > 0 {
		fmt.Fprintf(&buf, "%s", describeSublevels(fmtKey, verbose, v.L0SublevelFiles))
	}
	for level := 1; level < NumLevels; level++ {
		if v.Levels[level].Empty() {
			continue
		}
		fmt.Fprintf(&buf, "L%d:\n", level)
		for f := range v.Levels[level].All() {
			fmt.Fprintf(&buf, "  %s\n", f.DebugString(fmtKey, verbose))
		}
	}
	if v.BlobFiles.Count() > 0 {
		fmt.Fprintf(&buf, "Blob files:\n")
		for f := range v.BlobFiles.All() {
			fmt.Fprintf(&buf, "  %s\n", f.String())
		}
	}
	return buf.String()
}

// ParseVersionDebug parses a Version from its DebugString output.
func ParseVersionDebug(
	comparer *base.Comparer, l0Organizer *L0Organizer, s string,
) (*Version, error) {
	var files [NumLevels][]*TableMetadata
	level := -1
	for _, l := range strings.Split(s, "\n") {
		if l == "" {
			continue
		}
		p := strparse.MakeParser(debugParserSeparators, l)
		if l, ok := p.TryLevel(); ok {
			level = l
			continue
		}

		if level == -1 {
			return nil, errors.Errorf("version string must start with a level")
		}
		m, err := ParseTableMetadataDebug(l)
		if err != nil {
			return nil, err
		}
		files[level] = append(files[level], m)
	}
	// L0 files are printed from higher sublevel to lower, which means in a
	// partial order that represents newest to oldest. Reverse the order of L0
	// files to ensure we construct the same sublevels.
	slices.Reverse(files[0])
	v := NewVersionForTesting(comparer, l0Organizer, files)
	if err := v.CheckOrdering(); err != nil {
		return nil, err
	}
	return v, nil
}

// Refs returns the number of references to the version.
func (v *Version) Refs() int32 {
	return v.refs.Load()
}

// Ref increments the version refcount.
func (v *Version) Ref() {
	v.refs.Add(1)
}

// Unref decrements the version refcount. If the last reference to the version
// was removed, the version is removed from the list of versions and the
// Deleted callback is invoked. Requires that the VersionList mutex is NOT
// locked.
func (v *Version) Unref() {
	if v.refs.Add(-1) == 0 {
		l := v.list
		l.mu.Lock()
		l.Remove(v)
		v.Deleted(v.unrefFiles())
		l.mu.Unlock()
	}
}

// UnrefLocked decrements the version refcount. If the last reference to the
// version was removed, the version is removed from the list of versions and
// the Deleted callback is invoked. Requires that the VersionList mutex is
// already locked.
func (v *Version) UnrefLocked() {
	if v.refs.Add(-1) == 0 {
		v.list.Remove(v)
		v.Deleted(v.unrefFiles())
	}
}

func (v *Version) unrefFiles() ObsoleteFiles {
	var obsoleteFiles ObsoleteFiles
	for _, lm := range v.Levels {
		lm.release(&obsoleteFiles)
	}
	for _, lm := range v.RangeKeyLevels {
		lm.release(&obsoleteFiles)
	}
	v.BlobFiles.release(&obsoleteFiles)
	return obsoleteFiles
}

// ObsoleteFiles holds a set of files that are no longer referenced by any
// referenced Version.
type ObsoleteFiles struct {
	TableBackings []*TableBacking
	BlobFiles     []*PhysicalBlobFile
}

// AddBacking appends the provided TableBacking to the list of obsolete files.
func (of *ObsoleteFiles) AddBacking(fb *TableBacking) {
	of.TableBackings = append(of.TableBackings, fb)
}

// AddBlob appends the provided BlobFileMetadata to the list of obsolete files.
func (of *ObsoleteFiles) AddBlob(bm *PhysicalBlobFile) {
	of.BlobFiles = append(of.BlobFiles, bm)
}

// Count returns the number of files in the ObsoleteFiles.
func (of *ObsoleteFiles) Count() int {
	return len(of.TableBackings) + len(of.BlobFiles)
}

// Assert that ObsoleteFiles implements the obsoleteFiles interface.
var _ ObsoleteFilesSet = (*ObsoleteFiles)(nil)

// Next returns the next version in the list of versions.
func (v *Version) Next() *Version {
	return v.next
}

// CalculateInuseKeyRanges examines table metadata in levels [level, maxLevel]
// within bounds [smallest,largest], returning an ordered slice of key ranges
// that include all keys that exist within levels [level, maxLevel] and within
// [smallest,largest].
func (v *Version) CalculateInuseKeyRanges(
	l0Organizer *L0Organizer, level, maxLevel int, smallest, largest []byte,
) []base.UserKeyBounds {
	// Use two slices, alternating which one is input and which one is output
	// as we descend the LSM.
	var input, output []base.UserKeyBounds

	// L0 requires special treatment, since sstables within L0 may overlap.
	// We use the L0 Sublevels structure to efficiently calculate the merged
	// in-use key ranges.
	if level == 0 {
		output = l0Organizer.InUseKeyRanges(smallest, largest)
		level++
	}

	// NB: We always treat `largest` as inclusive for simplicity, because
	// there's little consequence to calculating slightly broader in-use key
	// ranges.
	bounds := base.UserKeyBoundsInclusive(smallest, largest)
	for ; level <= maxLevel; level++ {
		overlaps := v.Overlaps(level, bounds)
		iter := overlaps.Iter()

		// We may already have in-use key ranges from higher levels. Iterate
		// through both our accumulated in-use key ranges and this level's
		// files, merging the two.
		//
		// Tables higher within the LSM have broader key spaces. We use this
		// when possible to seek past a level's files that are contained by
		// our current accumulated in-use key ranges. This helps avoid
		// per-sstable work during flushes or compactions in high levels which
		// overlap the majority of the LSM's sstables.
		input, output = output, input
		output = output[:0]

		cmp := v.cmp.Compare
		inputIdx := 0
		var currFile *TableMetadata
		// If we have an accumulated key range and its start is ≤ smallest,
		// we can seek to the accumulated range's end. Otherwise, we need to
		// start at the first overlapping file within the level.
		if len(input) > 0 && cmp(input[0].Start, smallest) <= 0 {
			currFile = seekGT(&iter, cmp, input[0].End)
		} else {
			currFile = iter.First()
		}

		for currFile != nil && inputIdx < len(input) {
			// Invariant: Neither currFile nor input[inputIdx] overlaps any earlier
			// ranges.
			switch {
			case cmp(currFile.Largest().UserKey, input[inputIdx].Start) < 0:
				// File is completely before input range.
				output = append(output, currFile.UserKeyBounds())
				currFile = iter.Next()

			case cmp(input[inputIdx].End.Key, currFile.Smallest().UserKey) < 0:
				// Input range is completely before the next file.
				output = append(output, input[inputIdx])
				inputIdx++

			default:
				// Input range and file range overlap or touch. We will maximally extend
				// the range with more overlapping inputs and files.
				currAccum := currFile.UserKeyBounds()
				if cmp(input[inputIdx].Start, currAccum.Start) < 0 {
					currAccum.Start = input[inputIdx].Start
				}
				currFile = iter.Next()

				// Extend curAccum with any overlapping (or touching) input intervals or
				// files. Note that we will always consume at least input[inputIdx].
				for {
					if inputIdx < len(input) && cmp(input[inputIdx].Start, currAccum.End.Key) <= 0 {
						if currAccum.End.CompareUpperBounds(cmp, input[inputIdx].End) < 0 {
							currAccum.End = input[inputIdx].End
							// Skip over files that are entirely inside this newly extended
							// accumulated range; we expect ranges to be wider in levels that
							// are higher up so this might skip over a non-trivial number of
							// files.
							currFile = seekGT(&iter, cmp, currAccum.End)
						}
						inputIdx++
					} else if currFile != nil && cmp(currFile.Smallest().UserKey, currAccum.End.Key) <= 0 {
						if b := currFile.UserKeyBounds(); currAccum.End.CompareUpperBounds(cmp, b.End) < 0 {
							currAccum.End = b.End
						}
						currFile = iter.Next()
					} else {
						// No overlaps remaining.
						break
					}
				}
				output = append(output, currAccum)
			}
		}
		// If we have either files or input ranges left over, add them to the
		// output.
		output = append(output, input[inputIdx:]...)
		for ; currFile != nil; currFile = iter.Next() {
			output = append(output, currFile.UserKeyBounds())
		}
	}
	return output
}

// seekGT seeks to the first file that ends with a boundary that is after the
// given boundary. Specifically:
//   - if boundary.End is inclusive, the returned file ending boundary is strictly
//     greater than boundary.End.Key
//   - if boundary.End is exclusive, the returned file ending boundary is either
//     greater than boundary.End.Key, or it's inclusive at boundary.End.Key.
func seekGT(iter *LevelIterator, cmp base.Compare, boundary base.UserKeyBoundary) *TableMetadata {
	f := iter.SeekGE(cmp, boundary.Key)
	if f == nil {
		return nil
	}
	// If boundary is inclusive or the file boundary is exclusive we do not
	// tolerate an equal largest key.
	// Note: we know f.Largest.UserKey >= boundary.End.Key so this condition is
	// equivalent to boundary.End.IsUpperBoundForInternalKey(cmp, f.Largest).
	if (boundary.Kind == base.Inclusive || f.Largest().IsExclusiveSentinel()) && cmp(boundary.Key, f.Largest().UserKey) == 0 {
		return iter.Next()
	}
	return f
}

// Contains returns a boolean indicating whether the provided file exists in
// the version at the given level. If level is non-zero then Contains binary
// searches among the files. If level is zero, Contains scans the entire
// level.
func (v *Version) Contains(level int, m *TableMetadata) bool {
	if level == 0 {
		for f := range v.Levels[0].All() {
			if f == m {
				return true
			}
		}
		return false
	}
	for f := range v.Overlaps(level, m.UserKeyBounds()).All() {
		if f == m {
			return true
		}
	}
	return false
}

// Overlaps returns all elements of v.files[level] whose user key range
// intersects the given bounds. If level is non-zero then the user key bounds of
// v.files[level] are assumed to not overlap (although they may touch). If level
// is zero then that assumption cannot be made, and the given bounds are
// expanded to the union of those matching bounds so far and the computation is
// repeated until the bounds stabilize.
// The returned files are a subsequence of the input files, i.e., the ordering
// is not changed.
func (v *Version) Overlaps(level int, bounds base.UserKeyBounds) LevelSlice {
	if level == 0 {
		// Indices that have been selected as overlapping.
		l0 := v.Levels[level]
		l0Iter := l0.Iter()
		selectedIndices := make([]bool, l0.Len())
		numSelected := 0
		var slice LevelSlice
		for {
			restart := false
			for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
				selected := selectedIndices[i]
				if selected {
					continue
				}
				if !meta.Overlaps(v.cmp.Compare, &bounds) {
					// meta is completely outside the specified range; skip it.
					continue
				}
				// Overlaps.
				selectedIndices[i] = true
				numSelected++

				// Since this is L0, check if the newly added fileMetadata has expanded
				// the range. We expand the range immediately for files we have
				// remaining to check in this loop. All already checked and unselected
				// files will need to be rechecked via the restart below.
				if v.cmp.Compare(meta.Smallest().UserKey, bounds.Start) < 0 {
					bounds.Start = meta.Smallest().UserKey
					restart = true
				}
				if !bounds.End.IsUpperBoundForInternalKey(v.cmp.Compare, meta.Largest()) {
					bounds.End = base.UserKeyExclusiveIf(meta.Largest().UserKey, meta.Largest().IsExclusiveSentinel())
					restart = true
				}
			}

			if !restart {
				// Construct a B-Tree containing only the matching items.
				var tr btree[*TableMetadata]
				tr.bcmp = v.Levels[level].tree.bcmp
				for i, meta := 0, l0Iter.First(); meta != nil; i, meta = i+1, l0Iter.Next() {
					if selectedIndices[i] {
						err := tr.Insert(meta)
						if err != nil {
							panic(err)
						}
					}
				}
				slice = newLevelSlice(tableMetadataIter(&tr))
				// TODO(jackson): Avoid the oddity of constructing and
				// immediately releasing a B-Tree. Make LevelSlice an
				// interface?
				tr.Release(assertNoObsoleteFiles{})
				break
			}
			// Continue looping to retry the files that were not selected.
		}
		return slice
	}

	return v.Levels[level].Slice().Overlaps(v.cmp.Compare, bounds)
}

// AllLevelsAndSublevels returns an iterator that produces a Layer, LevelSlice
// pair for each L0 sublevel (from top to bottom) and each level below L0.
func (v *Version) AllLevelsAndSublevels() iter.Seq2[Layer, LevelSlice] {
	return func(yield func(Layer, LevelSlice) bool) {
		for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
			if !yield(L0Sublevel(sublevel), v.L0SublevelFiles[sublevel]) {
				return
			}
		}
		for level := 1; level < NumLevels; level++ {
			if !yield(Level(level), v.Levels[level].Slice()) {
				return
			}
		}
	}
}

// CheckOrdering checks that the files are consistent with respect to
// increasing file numbers (for level 0 files) and increasing and non-
// overlapping internal key ranges (for level non-0 files).
func (v *Version) CheckOrdering() error {
	for sublevel := len(v.L0SublevelFiles) - 1; sublevel >= 0; sublevel-- {
		sublevelIter := v.L0SublevelFiles[sublevel].Iter()
		if err := CheckOrdering(v.cmp.Compare, v.cmp.FormatKey, L0Sublevel(sublevel), sublevelIter); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString())
		}
	}

	for level, lm := range v.Levels {
		if err := CheckOrdering(v.cmp.Compare, v.cmp.FormatKey, Level(level), lm.Iter()); err != nil {
			return base.CorruptionErrorf("%s\n%s", err, v.DebugString())
		}
	}
	return nil
}

// validateBlobFileInvariants validates invariants around blob files. Currently
// it validates that the set of BlobFileIDs referenced by the Version's tables'
// blob references is exactly the same as the set of BlobFileIDs present in the
// Version's blob files B-Tree.
func (v *Version) validateBlobFileInvariants() error {
	// Collect all the blob file IDs that are referenced by the Version's
	// tables' blob references.
	var referencedFileIDs []base.BlobFileID
	{
		referencedFileIDsMap := make(map[base.BlobFileID]struct{}, v.BlobFiles.tree.Count())
		for i := 0; i < len(v.Levels); i++ {
			for table := range v.Levels[i].All() {
				for _, br := range table.BlobReferences {
					referencedFileIDsMap[br.FileID] = struct{}{}
				}
			}
		}
		referencedFileIDs = slices.Collect(maps.Keys(referencedFileIDsMap))
		slices.Sort(referencedFileIDs)
	}

	// Collect all the blob file IDs that are present in the Version's blob
	// files B-Tree.
	var versionBlobFileIDs []base.BlobFileID
	{
		versionBlobFileIDsMap := make(map[base.BlobFileID]struct{}, v.BlobFiles.tree.Count())
		for bf := range v.BlobFiles.All() {
			versionBlobFileIDsMap[bf.FileID] = struct{}{}
		}
		versionBlobFileIDs = slices.Collect(maps.Keys(versionBlobFileIDsMap))
		slices.Sort(versionBlobFileIDs)
	}

	if !slices.Equal(referencedFileIDs, versionBlobFileIDs) {
		return base.AssertionFailedf("divergence between referenced BlobFileIDs and Version's BlobFiles B-Tree: %v vs %v",
			referencedFileIDs, versionBlobFileIDs)
	}
	return nil
}

// VersionList holds a list of versions. The versions are ordered from oldest
// to newest.
type VersionList struct {
	mu   *sync.Mutex
	root Version
}

// Init initializes the version list.
func (l *VersionList) Init(mu *sync.Mutex) {
	l.mu = mu
	l.root.next = &l.root
	l.root.prev = &l.root
}

// Empty returns true if the list is empty, and false otherwise.
func (l *VersionList) Empty() bool {
	return l.root.next == &l.root
}

// Front returns the oldest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Front() *Version {
	return l.root.next
}

// Back returns the newest version in the list. Note that this version is only
// valid if Empty() returns true.
func (l *VersionList) Back() *Version {
	return l.root.prev
}

// PushBack adds a new version to the back of the list. This new version
// becomes the "newest" version in the list.
func (l *VersionList) PushBack(v *Version) {
	if v.list != nil || v.prev != nil || v.next != nil {
		panic("pebble: version list is inconsistent")
	}
	v.prev = l.root.prev
	v.prev.next = v
	v.next = &l.root
	v.next.prev = v
	v.list = l
}

// Remove removes the specified version from the list.
func (l *VersionList) Remove(v *Version) {
	if v == &l.root {
		panic("pebble: cannot remove version list root node")
	}
	if v.list != l {
		panic("pebble: version list is inconsistent")
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	v.list = nil // avoid memory leaks
}

// CheckOrdering checks that the files are consistent with respect to
// seqnums (for level 0 files -- see detailed comment below) and increasing and non-
// overlapping internal key ranges (for non-level 0 files).
func CheckOrdering(cmp Compare, format base.FormatKey, level Layer, files LevelIterator) error {
	// The invariants to check for L0 sublevels are the same as the ones to
	// check for all other levels. However, if L0 is not organized into
	// sublevels, or if all L0 files are being passed in, we do the legacy L0
	// checks, defined in the detailed comment below.
	if level == Level(0) {
		// We have 2 kinds of files:
		// - Files with exactly one sequence number: these could be either ingested files
		//   or flushed files. We cannot tell the difference between them based on TableMetadata,
		//   so our consistency checking here uses the weaker checks assuming it is a narrow
		//   flushed file. We cannot error on ingested files having sequence numbers coincident
		//   with flushed files as the seemingly ingested file could just be a flushed file
		//   with just one key in it which is a truncated range tombstone sharing sequence numbers
		//   with other files in the same flush.
		// - Files with multiple sequence numbers: these are necessarily flushed files.
		//
		// Three cases of overlapping sequence numbers:
		// Case 1:
		// An ingested file contained in the sequence numbers of the flushed file -- it must be
		// fully contained (not coincident with either end of the flushed file) since the memtable
		// must have been at [a, b-1] (where b > a) when the ingested file was assigned sequence
		// num b, and the memtable got a subsequent update that was given sequence num b+1, before
		// being flushed.
		//
		// So a sequence [1000, 1000] [1002, 1002] [1000, 2000] is invalid since the first and
		// third file are inconsistent with each other. So comparing adjacent files is insufficient
		// for consistency checking.
		//
		// Visually we have something like
		// x------y x-----------yx-------------y (flushed files where x, y are the endpoints)
		//     y       y  y        y             (y's represent ingested files)
		// And these are ordered in increasing order of y. Note that y's must be unique.
		//
		// Case 2:
		// A flushed file that did not overlap in keys with any file in any level, but does overlap
		// in the file key intervals. This file is placed in L0 since it overlaps in the file
		// key intervals but since it has no overlapping data, it is assigned a sequence number
		// of 0 in RocksDB. We handle this case for compatibility with RocksDB.
		//
		// Case 3:
		// A sequence of flushed files that overlap in sequence numbers with one another,
		// but do not overlap in keys inside the sstables. These files correspond to
		// partitioned flushes or the results of intra-L0 compactions of partitioned
		// flushes.
		//
		// Since these types of SSTables violate most other sequence number
		// overlap invariants, and handling this case is important for compatibility
		// with future versions of pebble, this method relaxes most L0 invariant
		// checks.

		var prev *TableMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if prev == nil {
				continue
			}
			// Validate that the sorting is sane.
			if prev.LargestSeqNum == 0 && f.LargestSeqNum == prev.LargestSeqNum {
				// Multiple files satisfying case 2 mentioned above.
			} else if !prev.lessSeqNum(f) {
				return base.CorruptionErrorf("L0 files %s and %s are not properly ordered: <#%d-#%d> vs <#%d-#%d>",
					errors.Safe(prev.TableNum), errors.Safe(f.TableNum),
					errors.Safe(prev.SmallestSeqNum), errors.Safe(prev.LargestSeqNum),
					errors.Safe(f.SmallestSeqNum), errors.Safe(f.LargestSeqNum))
			}
		}
	} else {
		var prev *TableMetadata
		for f := files.First(); f != nil; f, prev = files.Next(), f {
			if err := f.Validate(cmp, format); err != nil {
				return errors.Wrapf(err, "%s ", level)
			}
			if prev != nil {
				if prev.cmpSmallestKey(f, cmp) >= 0 {
					return base.CorruptionErrorf("%s files %s and %s are not properly ordered: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.TableNum), errors.Safe(f.TableNum),
						prev.Smallest().Pretty(format), prev.Largest().Pretty(format),
						f.Smallest().Pretty(format), f.Largest().Pretty(format))
				}

				// In all supported format major version, split user keys are
				// prohibited, so both files cannot contain keys with the same user
				// keys. If the bounds have the same user key, the previous file's
				// boundary must have a InternalKeyTrailer indicating that it's exclusive.
				if v := cmp(prev.Largest().UserKey, f.Smallest().UserKey); v > 0 || (v == 0 && !prev.Largest().IsExclusiveSentinel()) {
					return base.CorruptionErrorf("%s files %s and %s have overlapping ranges: [%s-%s] vs [%s-%s]",
						errors.Safe(level), errors.Safe(prev.TableNum), errors.Safe(f.TableNum),
						prev.Smallest().Pretty(format), prev.Largest().Pretty(format),
						f.Smallest().Pretty(format), f.Largest().Pretty(format))
				}
			}
		}
	}
	return nil
}

// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(bilal): work items:
// - Integration with Pebble

// Intervals are of the form [start, end) with no gap between intervals. Each
// file overlaps perfectly with a sequence of intervals. This perfect overlap
// occurs because the union of file boundary keys is used to pick intervals.
// However the largest key in a file is inclusive, so when it is used as
// an interval, the actual key is ImmediateSuccessor(key). We don't have the
// ImmediateSuccessor function to do this computation, so we instead keep an
// isLargest bool to remind the code about this fact. This is used for
// comparisons in the following manner:
// - intervalKey{k, false} < intervalKey{k, true}
// - k1 < k2 => intervalKey{k1, _} < intervalKey{k2, _}.
//
// For example, consider three files with bounds [a,e], [b,g], and [e,j]. The
// interval keys produced would be intervalKey{a, false}, intervalKey{b, false},
// intervalKey{e, false}, intervalKey{e, true}, intervalKey{g, true} and
// intervalKey{j, true}, resulting in intervals
// [a, b), [b, (e, false)), [(e,false), (e, true)), [(e, true), (g, true)) and
// [(g, true), (j, true)). The first file overlaps with the first three
// perfectly, the second file overlaps with the second through to fourth
// intervals, and the third file overlaps with the last three.
//
// The intervals are indexed starting from 0, with the index of the interval
// being the index of the start key of the interval.
//
// In addition to helping with compaction picking, we use interval indices
// to assign each file an interval range once. Subsequent operations, say
// picking overlapping files for a compaction, only need to use the index
// numbers and so avoid expensive byte slice comparisons.
type intervalKey struct {
	key       []byte
	isLargest bool
}

func intervalKeyCompare(cmp Compare, a, b intervalKey) int {
	rv := cmp(a.key, b.key)
	if rv == 0 {
		if a.isLargest && !b.isLargest {
			return +1
		}
		if !a.isLargest && b.isLargest {
			return -1
		}
	}
	return rv
}

type intervalKeySorter struct {
	keys []intervalKey
	cmp  Compare
}

func (s intervalKeySorter) Len() int { return len(s.keys) }
func (s intervalKeySorter) Less(i, j int) bool {
	return intervalKeyCompare(s.cmp, s.keys[i], s.keys[j]) < 0
}
func (s intervalKeySorter) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

func sortAndDedup(keys []intervalKey, cmp Compare) []intervalKey {
	if len(keys) == 0 {
		return nil
	}
	sorter := intervalKeySorter{keys: keys, cmp: cmp}
	sort.Sort(sorter)
	j := 0
	for i := 1; i < len(keys); i++ {
		cmp := intervalKeyCompare(cmp, keys[i], keys[j])
		if cmp != 0 {
			j++
			keys[j] = keys[i]
		}
	}
	return keys[:j+1]
}

type subLevelAndFile struct {
	subLevel  int
	fileIndex int
}

// A key interval of the form [start, end). The end is not represented here
// since it is implicit in the start of the next interval. The last interval is
// an exception but we don't need to ever lookup the end of that interval; the
// last fileInterval will only act as an end key marker. The set of intervals
// is const after initialization.
type fileInterval struct {
	index    int
	startKey intervalKey

	// True iff some file in this interval is compacting to base. Such intervals
	// cannot have any files participate in L0 => Lbase compactions.
	isBaseCompacting bool

	// The min and max intervals index across all the files that overlap with this
	// interval. Inclusive on both sides.
	filesMinIntervalIndex int
	filesMaxIntervalIndex int

	// True if another interval that has a file extending into this interval is
	// undergoing a compaction into Lbase. In other words, this bool is true
	// if any interval in [filesMinIntervalIndex,
	// filesMaxIntervalIndex] has isBaseCompacting set to true. This
	// lets the compaction picker de-prioritize this interval for picking
	// compactions, since there's a high chance that a base compaction with a
	// sufficient height of sublevels rooted at this interval could not be
	// chosen due to the ongoing base compaction in the
	// other interval. If the file straddling the two intervals is at a
	// sufficiently high sublevel (with enough compactible files below it to
	// satisfy minCompactionDepth), this is not an issue, but to optimize for
	// quickly picking base compactions far away from other base compactions,
	// this bool is used as a heuristic (but not as a complete disqualifier).
	intervalRangeIsBaseCompacting bool

	// fileCount - compactingFileCount is the stack depth that requires
	// starting new compactions. This metric is not precise since the
	// compactingFileCount can include files that are part of N (where N > 1)
	// intra-L0 compactions, so the stack depth after those complete will be
	// fileCount - compactingFileCount + N. We ignore this imprecision since
	// we don't want to track which files are part of which intra-L0
	// compaction.
	fileCount           int
	compactingFileCount int

	// The number of consecutive files starting from the the top of the stack
	// in this range that are not compacting. Note that any intra-L0
	// compaction can only choose from these files. Additionally after some
	// subset of files starting from the top are disqualified because of being
	// too young (earliestUnflushedSeqNum), any files picked are the next
	// ones.
	topOfStackNonCompactingFileCount int
	// In increasing sublevel order.
	subLevelAndFileList []subLevelAndFile

	// Interpolated from files in this interval. For files spanning multiple
	// intervals, we assume an equal distribution of bytes across all those
	// intervals.
	estimatedBytes uint64
}

// L0SubLevels represents a sublevel view of SSTables in L0. Tables in one
// sublevel are non-overlapping in key ranges, and keys in higher-indexed
// sublevels shadow older versions in lower-indexed sublevels. These invariants
// are similar to the regular level invariants, except with higher indexed
// sublevels having newer keys as opposed to lower indexed levels.
//
// There is no limit to the number of sublevels that can exist in L0 at any
// time, however read and compaction performance is best when there are as few
// sublevels as possible.
type L0SubLevels struct {
	// Files are ordered from oldest sublevel to youngest sublevel in the
	// outer slice, and the inner slice contains non-overlapping files for
	// that sublevel in increasing key order.
	Files [][]*FileMetadata

	cmp       Compare
	formatKey base.FormatKey

	fileBytes uint64
	// Contains all files in one slice, ordered from oldest to youngest.
	filesByAge []*FileMetadata

	// The file intervals in increasing key order.
	orderedIntervals []fileInterval

	// Keys to break flushes at.
	flushSplitUserKeys [][]byte
}

func insertIntoSubLevel(files []*FileMetadata, f *FileMetadata) []*FileMetadata {
	index := sort.Search(len(files), func(i int) bool {
		return f.minIntervalIndex < files[i].minIntervalIndex
	})
	if index == len(files) {
		files = append(files, f)
		return files
	}
	files = append(files, nil)
	copy(files[index+1:], files[index:])
	files[index] = f
	return files
}

// NewL0SubLevels creates an L0SubLevels instance for a given set of L0 files.
// These files must all be in L0 and must be sorted by seqnum (see
// SortBySeqNum). During interval iteration, when flushSplitMaxBytes bytes are
// exceeded in the range of intervals since the last flush split key, a flush
// split key is added.
func NewL0SubLevels(
	files []*FileMetadata,
	cmp Compare,
	formatKey base.FormatKey,
	flushSplitMaxBytes uint64,
) (*L0SubLevels, error) {
	s := &L0SubLevels{cmp: cmp, formatKey: formatKey}
	s.filesByAge = files
	keys := make([]intervalKey, 0, 2*len(files))
	for i := range s.filesByAge {
		files[i].l0Index = i
		keys = append(keys, intervalKey{key: files[i].Smallest.UserKey})
		keys = append(keys, intervalKey{key: files[i].Largest.UserKey, isLargest: true})
	}
	keys = sortAndDedup(keys, cmp)
	// All interval indices reference s.orderedIntervals.
	s.orderedIntervals = make([]fileInterval, len(keys))
	for i := range keys {
		s.orderedIntervals[i] = fileInterval{
			index:                 i,
			startKey:              keys[i],
			filesMinIntervalIndex: i,
			filesMaxIntervalIndex: i,
		}
	}
	// Initialize minIntervalIndex and maxIntervalIndex for each file, and use that
	// to update intervals.
	intervalRangeIsBaseCompacting := make([]bool, len(keys))
	for fileIndex, f := range s.filesByAge {
		// Set f.minIntervalIndex and f.maxIntervalIndex.
		f.minIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(cmp, intervalKey{key: f.Smallest.UserKey}, keys[index]) <= 0
		})
		if f.minIntervalIndex == len(keys) {
			return nil, errors.Errorf("expected sstable bound to be in interval keys: %s", f.Smallest.UserKey)
		}
		f.maxIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(
				cmp, intervalKey{key: f.Largest.UserKey, isLargest: true}, keys[index]) <= 0
		})
		if f.maxIntervalIndex == len(keys) {
			return nil, errors.Errorf("expected sstable bound to be in interval keys: %s", f.Largest.UserKey)
		}
		f.maxIntervalIndex--
		// This is a simple and not very accurate estimate of the number of
		// bytes this SSTable contributes to the intervals it is a part of.
		//
		// TODO(bilal): Call EstimateDiskUsage in sstable.Reader with interval
		// bounds to get a better estimate for each interval.
		interpolatedBytes := f.Size / uint64(f.maxIntervalIndex-f.minIntervalIndex+1)
		s.fileBytes += f.Size
		subLevel := 0
		// Update state in every fileInterval for this file.
		for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			if len(interval.subLevelAndFileList) > 0 &&
				subLevel <= interval.subLevelAndFileList[len(interval.subLevelAndFileList)-1].subLevel {
				subLevel = interval.subLevelAndFileList[len(interval.subLevelAndFileList)-1].subLevel + 1
			}
			s.orderedIntervals[i].fileCount++
			if f.Compacting {
				interval.compactingFileCount++
				interval.topOfStackNonCompactingFileCount = 0
				if !f.IsIntraL0Compacting {
					// If f.Compacting && !f.IsIntraL0Compacting, this file is
					// being compacted to Lbase.
					interval.isBaseCompacting = true
					intervalRangeIsBaseCompacting[i] = true
				}
			} else if f.IsIntraL0Compacting {
				return nil, errors.Errorf("file %s not marked as compacting but marked as intra-L0 compacting", f.FileNum)
			} else {
				interval.topOfStackNonCompactingFileCount++
			}
			interval.estimatedBytes += interpolatedBytes
			if f.minIntervalIndex < interval.filesMinIntervalIndex {
				interval.filesMinIntervalIndex = f.minIntervalIndex
			}
			if f.maxIntervalIndex > interval.filesMaxIntervalIndex {
				interval.filesMaxIntervalIndex = f.maxIntervalIndex
			}
		}
		for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			interval.subLevelAndFileList = append(interval.subLevelAndFileList,
				subLevelAndFile{subLevel: subLevel, fileIndex: fileIndex})
		}
		f.subLevel = subLevel
		if subLevel > len(s.Files) {
			return nil, errors.Errorf("chose a sublevel beyond allowed range of sublevels: %d vs 0-%d", subLevel, len(s.Files))
		}
		if subLevel == len(s.Files) {
			s.Files = append(s.Files, []*FileMetadata{f})
		} else {
			s.Files[subLevel] = insertIntoSubLevel(s.Files[subLevel], f)
		}
	}
	min := 0
	var cumulativeBytes uint64
	for i := 0; i < len(s.orderedIntervals); i++ {
		interval := &s.orderedIntervals[i]
		if interval.isBaseCompacting {
			minIndex := interval.filesMinIntervalIndex
			if minIndex < min {
				minIndex = min
			}
			for j := minIndex; j <= interval.filesMaxIntervalIndex; j++ {
				min = j
				s.orderedIntervals[j].intervalRangeIsBaseCompacting = true
			}
		}
		if cumulativeBytes > flushSplitMaxBytes && (len(s.flushSplitUserKeys) == 0 ||
			!bytes.Equal(interval.startKey.key, s.flushSplitUserKeys[len(s.flushSplitUserKeys)-1])) {
			s.flushSplitUserKeys = append(s.flushSplitUserKeys, interval.startKey.key)
			cumulativeBytes = 0
		}
		cumulativeBytes += s.orderedIntervals[i].estimatedBytes
	}
	return s, nil
}

// String produces a string containing useful debug information. Useful in test
// code and debugging.
func (s *L0SubLevels) String() string {
	return s.describe(false)
}

func (s *L0SubLevels) describe(verbose bool) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "file count: %d, sublevels: %d, intervals: %d\nflush split keys(%d): [",
		len(s.filesByAge), len(s.Files), len(s.orderedIntervals), len(s.flushSplitUserKeys))
	for i := range s.flushSplitUserKeys {
		fmt.Fprintf(&buf, "%s", s.formatKey(s.flushSplitUserKeys[i]))
		if i < len(s.flushSplitUserKeys) - 1 {
			fmt.Fprintf(&buf, ", ")
		}
	}
	fmt.Fprintln(&buf, "]")
	numCompactingFiles := 0
	for i := len(s.Files) - 1; i >= 0; i-- {
		maxIntervals := 0
		sumIntervals := 0
		var totalBytes uint64
		for _, f := range s.Files[i] {
			intervals := f.maxIntervalIndex - f.minIntervalIndex + 1
			if intervals > maxIntervals {
				maxIntervals = intervals
			}
			sumIntervals += intervals
			totalBytes += f.Size
			if f.Compacting {
				numCompactingFiles++
			}
		}
		fmt.Fprintf(&buf, "0.%d: file count: %d, bytes: %d, width (mean, max): %0.1f, %d, interval range: [%d, %d]\n",
			i, len(s.Files[i]), totalBytes, float64(sumIntervals)/float64(len(s.Files[i])), maxIntervals, s.Files[i][0].minIntervalIndex,
			s.Files[i][len(s.Files[i])-1].maxIntervalIndex)
		if verbose {
			for _, f := range s.Files[i] {
				intervals := f.maxIntervalIndex - f.minIntervalIndex + 1
				fmt.Fprintf(&buf, "\t%s\n", f)
				if len(s.filesByAge) > 50 && intervals*3 > len(s.orderedIntervals) {
					var intervalsBytes uint64
					for k := f.minIntervalIndex; k <= f.maxIntervalIndex; k++ {
						intervalsBytes += s.orderedIntervals[k].estimatedBytes
					}
					fmt.Fprintf(&buf, "wide file: %d, [%d, %d], byte fraction: %f\n",
						f.FileNum, f.minIntervalIndex, f.maxIntervalIndex,
						float64(intervalsBytes)/float64(s.fileBytes))
				}
			}
		}
	}
	lastCompactingIntervalStart := -1
	fmt.Fprintf(&buf, "compacting file count: %d, base compacting intervals: ", numCompactingFiles)
	i := 0
	for ; i < len(s.orderedIntervals); i++ {
		interval := &s.orderedIntervals[i]
		if interval.fileCount == 0 {
			continue
		}
		if !interval.isBaseCompacting {
			if lastCompactingIntervalStart != -1 {
				fmt.Fprintf(&buf, "[%d, %d], ", lastCompactingIntervalStart, i-1)
			}
			lastCompactingIntervalStart = -1
		} else {
			if lastCompactingIntervalStart == -1 {
				lastCompactingIntervalStart = i
			}
		}
	}
	if lastCompactingIntervalStart != -1 {
		fmt.Fprintf(&buf, "[%d, %d], ", lastCompactingIntervalStart, i-1)
	}
	fmt.Fprintln(&buf, "")
	return buf.String()
}

// ReadAmplification returns the contribution of L0Sublevels to the read
// amplification for any particular point key. It is the maximum height of any
// tracked fileInterval. This is always less than or equal to the number of
// sublevels.
func (s *L0SubLevels) ReadAmplification() int {
	amp := 0
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		if amp < interval.fileCount {
			amp = interval.fileCount
		}
	}
	return amp
}

// FlushSplitKeys returns a slice of user keys to split flushes at.
// Used by flushes to avoid writing sstables that straddle these split keys.
// These should be interpreted as the keys to start the next sstable (not the
// last key to include in the prev sstable). These are user keys so that
// range tombstones can be properly truncated (untruncated range tombstones
// are not permitted for L0 files).
func (s *L0SubLevels) FlushSplitKeys() [][]byte {
	return s.flushSplitUserKeys
}

// MaxDepthAfterOngoingCompactions returns an estimate of maximum depth of
// sublevels after all ongoing compactions run to completion. Used by compaction
// picker to decide compaction score for L0. There is no scoring for intra-L0
// compactions -- they only run if L0 score is high but we're unable to pick an
// L0 => Lbase compaction.
func (s *L0SubLevels) MaxDepthAfterOngoingCompactions() int {
	depth := 0
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		intervalDepth := interval.fileCount - interval.compactingFileCount
		if depth < intervalDepth {
			depth = intervalDepth
		}
	}
	return depth
}

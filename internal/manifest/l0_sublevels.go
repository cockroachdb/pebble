package manifest

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(sbhola): work items:
// - Integration with Pebble (probably as a followup to #546)
// - TPCC import experiments

// Intervals are of the form [start, end) with no gap between intervals. Each
// file overlaps perfectly with a sequence of intervals. This perfect overlap
// occurs because the union of file boundary keys is used to pick intervals.
// However the largest key in a file is not inclusive, so when it is used as
// an interval, the actual key is ImmediateSuccessor(key). We don't have the
// ImmediateSuccessor function to do this computation, so we instead keep an
// isLargest bool to remind the code about this fact. This is used for
// comparisons in the following manner:
// - intervalKey{k, false} < intervalKey{k, true}
// - k1 < k2 => intervalKey{k1, _} < intervalKey{k2, _}.
//
// The intervals are indexed starting from 0, with the index of the interval
// being the index of the start key of the interval.
//
// In addition to helping with compaction choosing, we use interval indices
// to assign each file an interval range once. Subsequent operations, say picking
// overlapping files for a compaction, only need to use the index numbers and
// so avoid expensive string comparisons.
type intervalKey struct {
	key       InternalKey
	isLargest bool
}

func intervalKeyCompare(cmp Compare, a, b intervalKey) int {
	rv := base.InternalCompare(cmp, a.key, b.key)
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
	return keys[0 : j+1]
}

// A wrapper around FileMetadata.
type fileMeta struct {
	index int
	meta  *FileMetadata

	// Const after initialization.
	subLevel int
	// Interval is inclusive on both sides.
	minIntervalIndex int
	maxIntervalIndex int
}

type subLevelAndFile struct {
	subLevel  int
	fileIndex int
}

// A key interval of the form [start, end). The end is not represented here since it
// is implicit in the start of the next interval. The last interval is an exception
// but we don't need to ever lookup the end of that interval. The set of intervals
// is const after initialization.
type fileInterval struct {
	index    int
	startKey intervalKey

	// True iff some file in this interval is compacting to base. Such intervals
	// cannot have any files participate in L0 => Lbase compactions.
	// (there can be rare cases where seqnum 0 files get added and occupy lower
	// sublevels in this interval than files undergoing compaction, which may
	// allow for this interval to participate in another compaction, but for now
	// we eschew such complexity).
	isBaseCompacting bool

	// The min and max intervals index across all the files that overlap with this
	// interval. Inclusive on both sides.
	filesMinIntervalIndex int
	filesMaxIntervalIndex int

	// One could consider an interval for an L0 => Lbase compaction if
	// !isBaseCompacting but if it is near another interval that is undergoing
	// such a compaction it may have a file that extends into that interval
	// and prevents compaction. To reduce the number of failed candidate
	// intervals, the following bit encodes whether any interval in
	// [internalFilesMinIntervalIndex, internalFilesMaxIntervalIndex] has isBaseCompacting set
	// to true. Note that this is a pessimistic filter: the file that widened
	// [internalFilesMinIntervalIndex, internalFilesMaxIntervalIndex] may be at a high
	// sub-level and may not need to be included in the compaction.
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
	fileBytes uint64
}

// Logger is the same as pebble.Logger, to break import cycle.
type Logger interface {
	Infof(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}
type defaultLogger struct{}

// DefaultLogger logs to the Go stdlib logs.
var DefaultLogger defaultLogger

// Infof implements the Logger.Infof interface.
func (defaultLogger) Infof(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
}

// Fatalf implements the Logger.Fatalf interface.
func (defaultLogger) Fatalf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// L0SubLevels represents a sublevel view of SSTables in L0. Tables in one
// sublevel are non-overlapping in key ranges, and keys in higher-indexed
// sublevels shadow older versions in lower-indexed sublevels. These invariants
// are similar to the regular level invariants, except with higher indexed
// sublevels having newer keys as opposed to lower indexed levels.
//
// There is no limit to the number of sublevels that can exist in L0 at any time
// however read and compaction performance is best when there are as few
// sublevels as possible.
type L0SubLevels struct {
	cmp    Compare
	logger Logger
	format base.Formatter

	fileBytes uint64
	// Oldest to youngest.
	filesByAge []*fileMeta

	// Files in each sub-level ordered by increasing key order. Sub-levels
	// are ordered from oldest to youngest.
	subLevels [][]*fileMeta

	// The file intervals in increasing key order.
	orderedIntervals []fileInterval

	// Keys to break flushes at.
	flushSplitUserKeys      [][]byte
	flushSplitFraction      []float64
	flushSplitIntervalIndex []int

	// for debugging
	CompactingFilesAtCreation []*FileMetadata
}

func insertIntoSubLevel(files []*fileMeta, f *fileMeta) []*fileMeta {
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
	logger Logger,
	formatter base.Formatter,
	flushSplitMaxBytes uint64,
) *L0SubLevels {
	s := &L0SubLevels{cmp: cmp, logger: logger, format: formatter}
	s.filesByAge = make([]*fileMeta, len(files))
	keys := make([]intervalKey, 0, 2*len(files))
	for i := range files {
		s.filesByAge[i] = &fileMeta{index: i, meta: files[i]}
		keys = append(keys, intervalKey{key: files[i].Smallest})
		keys = append(keys, intervalKey{key: files[i].Largest, isLargest: true})
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
			return intervalKeyCompare(cmp, intervalKey{key: f.meta.Smallest}, keys[index]) <= 0
		})
		if f.minIntervalIndex == len(keys) {
			s.logger.Fatalf("expected sstable bound to be in interval keys: %s", f.meta.Smallest)
		}
		f.maxIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(
				cmp, intervalKey{key: f.meta.Largest, isLargest: true}, keys[index]) <= 0
		})
		if f.maxIntervalIndex == len(keys) {
			s.logger.Fatalf("expected sstable bound to be in interval keys: %s", f.meta.Largest)
		}
		f.maxIntervalIndex--
		interpolatedBytes := f.meta.Size / uint64(f.maxIntervalIndex-f.minIntervalIndex+1)
		s.fileBytes += f.meta.Size
		subLevel := 0
		if f.meta.Compacting {
			if !f.meta.IsBaseCompacting && !f.meta.IsIntraL0Compacting {
				s.logger.Fatalf("file %d is compacting but not marked as base or intra-l0", f.meta.FileNum)
			}
			s.CompactingFilesAtCreation = append(s.CompactingFilesAtCreation, f.meta)
		}
		// Update state in every fileInterval for this file.
		for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			if len(interval.subLevelAndFileList) > 0 &&
				subLevel <= interval.subLevelAndFileList[len(interval.subLevelAndFileList)-1].subLevel {
				subLevel = interval.subLevelAndFileList[len(interval.subLevelAndFileList)-1].subLevel + 1
			}
			s.orderedIntervals[i].fileCount++
			if f.meta.IsBaseCompacting {
				interval.isBaseCompacting = true
				interval.compactingFileCount++
				interval.topOfStackNonCompactingFileCount = 0
				intervalRangeIsBaseCompacting[i] = true
			} else if f.meta.IsIntraL0Compacting {
				interval.compactingFileCount++
				interval.topOfStackNonCompactingFileCount = 0
			} else {
				if f.meta.Compacting {
					s.logger.Fatalf("Compacting, but not intra-L0 or base: %d", f.meta.FileNum)
				}
				interval.topOfStackNonCompactingFileCount++
			}
			interval.fileBytes += interpolatedBytes
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
		if subLevel > len(s.subLevels) {
			s.logger.Fatalf("chose a sublevel beyond allowed range of sublevels: %d vs 0-%d", subLevel, len(s.subLevels))
		}
		if subLevel == len(s.subLevels) {
			s.subLevels = append(s.subLevels, []*fileMeta{f})
		} else {
			s.subLevels[subLevel] = insertIntoSubLevel(s.subLevels[subLevel], f)
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
			!bytes.Equal(interval.startKey.key.UserKey, s.flushSplitUserKeys[len(s.flushSplitUserKeys)-1])) {
			s.flushSplitUserKeys = append(s.flushSplitUserKeys, interval.startKey.key.UserKey)
			fractionOfTotal := float64(cumulativeBytes) / float64(s.fileBytes)
			s.flushSplitFraction = append(s.flushSplitFraction, fractionOfTotal)
			s.flushSplitIntervalIndex = append(s.flushSplitIntervalIndex, i-1)
			cumulativeBytes = 0
		}
		cumulativeBytes += s.orderedIntervals[i].fileBytes
	}
	return s
}

// String produces a string containing useful debug information. Useful in test
// code and debugging.
func (s *L0SubLevels) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "file count: %d, sublevels: %d, intervals: %d, flush keys: %d\nsplit:",
		len(s.filesByAge), len(s.subLevels), len(s.orderedIntervals), len(s.flushSplitUserKeys))
	for i := range s.flushSplitUserKeys {
		fmt.Fprintf(&buf, "(%d,%d):%.2f,", i, s.flushSplitIntervalIndex[i], s.flushSplitFraction[i])
	}
	fmt.Fprintln(&buf, "")
	numCompactingFiles := 0
	for i := len(s.subLevels) - 1; i >= 0; i-- {
		maxIntervals := 0
		sumIntervals := 0
		var totalBytes uint64
		for _, f := range s.subLevels[i] {
			intervals := f.maxIntervalIndex - f.minIntervalIndex + 1
			if intervals > maxIntervals {
				maxIntervals = intervals
			}
			sumIntervals += intervals
			totalBytes += f.meta.Size
			if f.meta.Compacting || f.meta.IsBaseCompacting || f.meta.IsIntraL0Compacting {
				numCompactingFiles++
			}
			if len(s.filesByAge) > 50 && intervals*3 > len(s.orderedIntervals) {
				var intervalsBytes uint64
				for k := f.minIntervalIndex; k <= f.maxIntervalIndex; k++ {
					intervalsBytes += s.orderedIntervals[k].fileBytes
				}
				fmt.Fprintf(&buf, "wide file: %d, [%d, %d], byte fraction: %f\n",
					f.meta.FileNum, f.minIntervalIndex, f.maxIntervalIndex,
					float64(intervalsBytes)/float64(s.fileBytes))
			}
		}
		fmt.Fprintf(&buf, "0.%d: file count: %d, bytes: %d, width (mean, max): %d, %d, interval range: [%d, %d]\n",
			i, len(s.subLevels[i]), totalBytes, sumIntervals/len(s.subLevels[i]), maxIntervals, s.subLevels[i][0].minIntervalIndex,
			s.subLevels[i][len(s.subLevels[i])-1].maxIntervalIndex)
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

package manifest

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
)

// TODO(sbhola): work items:
// - Prior to integration:
//   - basic correctness test.
//   - benchmark with ~1000 files to ensure that initialization and
//     picking is fast enough.
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
	// In increasing sublevel order
	subLevelAndFileList []subLevelAndFile

	fileBytes uint64 // interpolated
}

// Same as pebble.Logger, to break cycle.
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

type L0SubLevels struct {
	cmp    Compare
	logger Logger

	// Oldest to youngest.
	filesByAge []*fileMeta

	// Files in each sub-level ordered by increasing key order. Sub-levels
	// are ordered from oldest to youngest.
	subLevels [][]*fileMeta

	// The file intervals in increasing key order.
	orderedIntervals []fileInterval

	// Keys to break flushes at.
	flushSplitUserKeys [][]byte

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

func NewL0SubLevels(
	files []*FileMetadata, cmp Compare, logger Logger, flushSplitMaxBytes uint64,
) *L0SubLevels {
	s := &L0SubLevels{cmp: cmp, logger: logger}
	s.filesByAge = make([]*fileMeta, len(files))
	keys := make([]intervalKey, 0, 2*len(files))
	for i := range files {
		s.filesByAge[i] = &fileMeta{index: i, meta: files[i]}
		keys = append(keys, intervalKey{key: files[i].Smallest})
		keys = append(keys, intervalKey{key: files[i].Largest, isLargest: true})
	}
	keys = sortAndDedup(keys, cmp)
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
		f.minIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(cmp, intervalKey{key: f.meta.Smallest}, keys[index]) <= 0
		})
		if f.minIntervalIndex == len(keys) {
			s.logger.Fatalf("bug")
		}
		f.maxIntervalIndex = sort.Search(len(keys), func(index int) bool {
			return intervalKeyCompare(
				cmp, intervalKey{key: f.meta.Largest, isLargest: true}, keys[index]) <= 0
		})
		if f.maxIntervalIndex == len(keys) {
			s.logger.Fatalf("bug")
		}
		f.maxIntervalIndex--
		interpolatedBytes := f.meta.Size / uint64(f.maxIntervalIndex-f.minIntervalIndex+1)
		subLevel := 0
		if f.meta.Compacting {
			if !f.meta.IsBaseCompacting && !f.meta.IsIntraL0Compacting {
				s.logger.Fatalf("file %d is compacting but not marked as base or intra-l0", f.meta.FileNum)
			}
			s.CompactingFilesAtCreation = append(s.CompactingFilesAtCreation, f.meta)
		}
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
			s.logger.Fatalf("bug")
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
			cumulativeBytes = 0
		}
		cumulativeBytes += s.orderedIntervals[i].fileBytes
	}
	// fmt.Printf("Sublevels: %s\n", s)
	return s
}

func (s *L0SubLevels) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "file count: %d, sublevels: %d, intervals: %d, flush keys: %d\n",
		len(s.filesByAge), len(s.subLevels), len(s.orderedIntervals), len(s.flushSplitUserKeys))
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
				fmt.Fprintf(&buf, "wide file: %d, [%d, %d]\n", f.meta.FileNum, f.minIntervalIndex, f.maxIntervalIndex)
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

// For stats etc.
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

// Used by flushes to avoid writing sstables that straddle these split keys.
// These should be interpreted as the keys to start the next sstable (not the
// last key to include in the prev sstable). These are user keys so that
// range tombstones can be properly truncated (untruncated range tombstones
// are not permitted for L0 files).
func (s *L0SubLevels) FlushSplitKeys() [][]byte {
	return s.flushSplitUserKeys
}

// Used by compaction picker to decide compaction score for L0. There is no scoring for
// intra-L0 compaction -- they only run if L0 score is high but unable to pick an
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

// Only for temporary debugging in the absence of proper tests.
func (s *L0SubLevels) checkCompaction(c *Level0CompactionFiles, isBase bool) {
	includedFiles := make([]bool, len(s.filesByAge))
	fileIntervalsByLevel := make([]struct {
		min int
		max int
	}, len(s.subLevels))
	for i := range fileIntervalsByLevel {
		fileIntervalsByLevel[i].min = math.MaxInt32
		fileIntervalsByLevel[i].max = 0
	}
	var topLevel int
	var increment int
	var limitReached func(int) bool
	if isBase {
		topLevel = 0
		increment = -1
		limitReached = func(level int) bool {
			return level < 0
		}
	} else {
		topLevel = len(s.subLevels) - 1
		increment = +1
		limitReached = func(level int) bool {
			return level == len(s.subLevels)
		}
	}
	for _, f := range c.Files {
		if f.minIntervalIndex < fileIntervalsByLevel[f.subLevel].min {
			fileIntervalsByLevel[f.subLevel].min = f.minIntervalIndex
		}
		if f.maxIntervalIndex > fileIntervalsByLevel[f.subLevel].max {
			fileIntervalsByLevel[f.subLevel].max = f.maxIntervalIndex
		}
		includedFiles[f.index] = true
		if isBase {
			if topLevel < f.subLevel {
				topLevel = f.subLevel
			}
		} else {
			if topLevel > f.subLevel {
				topLevel = f.subLevel
			}
		}
	}
	// fmt.Printf("topLevel: %d\n", topLevel)
	min := fileIntervalsByLevel[topLevel].min
	max := fileIntervalsByLevel[topLevel].max
	for level := topLevel; !limitReached(level); level += increment {
		if fileIntervalsByLevel[level].min < min {
			min = fileIntervalsByLevel[level].min
		}
		if fileIntervalsByLevel[level].max > max {
			max = fileIntervalsByLevel[level].max
		}
		index := sort.Search(len(s.subLevels[level]), func(i int) bool {
			return s.subLevels[level][i].maxIntervalIndex >= min
		})
		// start := index
		for ; index < len(s.subLevels[level]); index++ {
			f := s.subLevels[level][index]
			if f.minIntervalIndex > max {
				break
			}
			if c.isIntraL0 && f.meta.LargestSeqNum >= c.earliestUnflushedSeqNum {
				continue
			}
			if !includedFiles[f.index] {
				str := fmt.Sprintf("bug %t: level %d, sl index %d, f.index %d, min %d, max %d, f.min %d, f.max %d, filenum: %d, isCompacting: %t\n%s",
					c.isIntraL0, level, index, f.index, min, max, f.minIntervalIndex, f.maxIntervalIndex, f.meta.FileNum, f.meta.Compacting, s)

				s.logger.Fatalf(str)
			}
		}
		// fmt.Printf("checked level: %d, [%d, %d], files [%d, %d)\n", level, min, max,
		//	start, index)
	}
}

func (s *L0SubLevels) UpdateStateForManualCompaction(files []*FileMetadata) {
	i := 0
	j := 0
	c := Level0CompactionFiles{
		Files:         make([]*fileMeta, 0, len(files)),
		FilesIncluded: make([]bool, len(s.filesByAge)),
	}
	for ; i < len(files) && j < len(s.filesByAge); j++ {
		f1 := files[i]
		f2 := s.filesByAge[j].meta
		if f1 == f2 {
			c.FilesIncluded[j] = true
			c.Files = append(c.Files, s.filesByAge[j])
			i++
		}
	}
	if i != len(files) {
		s.logger.Fatalf("bug")
	}
	s.UpdateStateForStartedCompaction(&c, true)
}

func (s *L0SubLevels) UpdateStateForStartedCompaction(c *Level0CompactionFiles, isBase bool) {
	s.checkCompaction(c, isBase)
	for _, f := range c.Files {
		if f.meta.Compacting {
			s.logger.Fatalf("L9: %06d already being compacted", f.meta.FileNum)
		}
		if isBase {
			f.meta.IsBaseCompacting = true
		} else {
			f.meta.IsIntraL0Compacting = true
		}
		f.meta.Compacting = true
		for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			if isBase {
				interval.isBaseCompacting = true
			}
			interval.compactingFileCount++
		}
	}
	if isBase {
		for i := c.minIntervalIndex; i <= c.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			for j := interval.filesMinIntervalIndex; j <= interval.filesMaxIntervalIndex; j++ {
				s.orderedIntervals[j].intervalRangeIsBaseCompacting = true
			}
			// If there is no intra-L0 for this interval, the compacting files
			// may have encroached on the topOfStackNonCompactingFileCount.
			if interval.compactingFileCount+interval.topOfStackNonCompactingFileCount > interval.fileCount {
				interval.topOfStackNonCompactingFileCount = interval.fileCount - interval.compactingFileCount
			}
		}
	} else {
		for i := c.minIntervalIndex; i <= c.maxIntervalIndex; i++ {
			interval := &s.orderedIntervals[i]
			interval.topOfStackNonCompactingFileCount = 0
			for j := len(interval.subLevelAndFileList) - 1; j >= 0; j-- {
				fileIndex := interval.subLevelAndFileList[j].fileIndex
				if s.filesByAge[fileIndex].meta.IsIntraL0Compacting {
					break
				}
				interval.topOfStackNonCompactingFileCount++
			}
		}
	}
	s.logger.Infof("started L0 compaction (is-base: %t). Sublevels:\n%s\n", isBase, s)
}

type Level0CompactionFiles struct {
	Files                           []*fileMeta
	FilesIncluded                   []bool
	seedIntervalStackDepthReduction int
	seedIntervalExtremeLevel        int
	fileBytes                       uint64
	minIntervalIndex                int
	maxIntervalIndex                int

	// Only for intra-L0 compaction
	isIntraL0               bool
	earliestUnflushedSeqNum uint64

	// For internal use.
	internalFilesMinIntervalIndex int
	internalFilesMaxIntervalIndex int
}

// Helper to order intervals being considered for compaction.
type intervalAndScore struct {
	interval *fileInterval
	score    int
}
type intervalSorterByDecreasingScore []intervalAndScore

func (is intervalSorterByDecreasingScore) Len() int { return len(is) }
func (is intervalSorterByDecreasingScore) Less(i, j int) bool {
	return is[i].score > is[j].score
}
func (is intervalSorterByDecreasingScore) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
}

// Compactions:
//
// The sub-levels and intervals can be visualized in 2 dimensions as the X
// axis containing intervals in increasing order and the Y axis containing
// sub-levels (older to younger). The intervals can be sparse wrt sub-levels.
// We observe that the system is typically under severe pressure in L0 during
// large imports where most files added to L0 are narrow and non-overlapping.
// In that case we expect the rectangle represented in the above visualization
// to be wide and short, and not too sparse (most intervals will have
// fileCount close to the sub-level count), which would make it amenable to
// concurrent L0 => Lbase compactions.
//
// L0 => Lbase: The high-level goal of a L0 => Lbase compaction is to reduce
// stack depth, by compacting files in the intervals with the highest
// (fileCount - compactingCount). Additionally, we would like compactions to
// not involve a huge number of files, so that they finish quickly, and to
// allow for concurrent L0 => Lbase compactions when needed. In order to
// achieve these goals we would like compactions to visualize as capturing
// thin and tall rectangles. The approach below is to consider intervals in
// some order and then try to construct a compaction using the interval. The
// first interval we can construct a compaction for is the compaction that is
// started. There can be multiple heuristics in choosing the ordering of the
// intervals -- the code uses one heuristic, but experimentation is probably
// needed to pick a good one. Additionally, the compaction that gets picked
// may be not as desirable as one that could be constructed later in terms of
// reducing stack depth (since adding more files to the compaction can get
// blocked by needing to encompass files that are already being compacted). So
// an alternative would be to try to construct more than one compaction and
// pick the best one.
//
// Intra-L0: If the L0 score is high, but PickBaseCompaction() is unable to
// pick a compaction, PickIntraL0Compaction will be used to pick an intra-L0
// compaction. Similar to L0 => Lbase compactions, we want to allow for
// multiple intra-L0 compactions and not generate wide output files that
// hinder later concurrency of L0 => Lbase compactions. Also compactions
// that produce wide files don't reduce stack depth -- they represent wide
// rectangles in our visualization, which means many intervals have their
// depth reduced by a small amount. Typically, L0 files have non-overlapping
// sequence numbers, and sticking to that invariant would require us to
// consider intra-L0 compactions that proceed from youngest to oldest files,
// which could result in the aforementioned undesirable wide rectangle
// shape. But this non-overlapping sequence number is already relaxed in
// RocksDB -- sstables are primarily ordered by their largest sequence
// number. So we can arrange for intra-L0 compactions to capture thin and
// tall rectangles starting with the top of the stack (youngest files).
// Like the L0 => Lbase case we order the intervals using a heuristic and
// consider each in turn. The same comment about better heuristics and not
// being greedy applies here.
//
// TODO(sbhola): after experimenting and settling on the right heuristics
// we can probably generalize the code below for more code sharing between
// the two kinds of compactions.

func (s *L0SubLevels) PickBaseCompaction(
	minCompactionDepth int, baseFiles []*FileMetadata,
) *Level0CompactionFiles {
	// We consider intervals in a greedy manner in the following order:
	// - pool1: Contains intervals that are unlikely to be blocked due
	//   to ongoing L0 => Lbase compactions. These are the ones with
	//   !isBaseCompacting && !intervalRangeIsBaseCompacting.
	// - pool2: Contains intervals that are !isBaseCompacting && intervalRangeIsBaseCompacting.
	//
	// The ordering heuristic exists just to avoid wasted work. Ideally,
	// we would consider all intervals with isBaseCompacting = false and
	// construct a compaction for it and compare the constructed compactions
	// and pick the best one. If microbenchmarks show that we can afford
	// this cost we can eliminate this heuristic.
	var pool1, pool2 []intervalAndScore
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		depth := interval.fileCount - interval.compactingFileCount
		if interval.isBaseCompacting || minCompactionDepth > depth {
			continue
		}
		if interval.intervalRangeIsBaseCompacting {
			pool2 = append(pool2, intervalAndScore{interval: interval, score: depth})
		} else {
			pool1 = append(pool1, intervalAndScore{interval: interval, score: depth})
		}
	}
	sort.Sort(intervalSorterByDecreasingScore(pool1))
	sort.Sort(intervalSorterByDecreasingScore(pool2))

	// Optimization to avoid considering different intervals that
	// are likely to choose the same seed file. Again this is just
	// to reduce wasted work.
	consideredIntervals := make([]bool, len(s.orderedIntervals))
	countFailedDueToOngoingBase := 0
	for _, pool := range [2][]intervalAndScore{pool1, pool2} {
		for _, interval := range pool {
			if consideredIntervals[interval.interval.index] {
				continue
			}
			// Pick the seed file for the interval as the file
			// in the lowest sub-level.
			slf := interval.interval.subLevelAndFileList[0]
			f := s.filesByAge[slf.fileIndex]
			for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
				// Don't bother considering the intervals that are
				// covered by the seed file since they are likely
				// nearby. Note that it is possible that those intervals
				// have seed files at lower sub-levels so could be
				// viable for compaction.
				consideredIntervals[i] = true
			}
			if f.meta.IsBaseCompacting {
				s.logger.Fatalf("")
			}
			if f.meta.IsIntraL0Compacting {
				continue
			}
			c := s.baseCompactionUsingSeed(f, interval.interval.index, minCompactionDepth)
			if c != nil {
				firstBaseIndex := sort.Search(len(baseFiles), func(i int) bool {
					// An interval starting at ImmediateSuccessor(key) can never be the
					// first interval of a compaction since no file can start at that
					// interval.
					return base.InternalCompare(
						s.cmp, baseFiles[i].Largest, s.orderedIntervals[c.minIntervalIndex].startKey.key) >= 0
				})
				// Exclusive
				lastBaseIndex := sort.Search(len(baseFiles), func(i int) bool {
					cmp := base.InternalCompare(
						s.cmp, baseFiles[i].Smallest, s.orderedIntervals[c.maxIntervalIndex+1].startKey.key)
					// Compaction is ending at exclusive bound of c.maxIntervalIndex+1
					if cmp > 0 || (cmp == 0 && !s.orderedIntervals[c.maxIntervalIndex+1].startKey.isLargest) {
						return true
					}
					return false
				})
				baseCompacting := false
				for j := firstBaseIndex; j < lastBaseIndex; j++ {
					if baseFiles[j].Compacting {
						baseCompacting = true
						break
					}
				}
				if baseCompacting {
					countFailedDueToOngoingBase++
					continue
				}
				var buf strings.Builder
				for _, slf := range interval.interval.subLevelAndFileList {
					fmt.Fprintf(&buf, "%d, ", slf.subLevel)
				}
				s.logger.Infof("Seed interval for base compaction: %d, sublevels: %s, ongoing-base: %d",
					interval.interval.index, buf.String(), countFailedDueToOngoingBase)
				return c
			}
		}
	}
	return nil
}

func (s *L0SubLevels) baseCompactionUsingSeed(
	f *fileMeta, intervalIndex int, minCompactionDepth int,
) *Level0CompactionFiles {
	cFiles := &Level0CompactionFiles{
		Files:                           []*fileMeta{f},
		seedIntervalStackDepthReduction: 1,
		seedIntervalExtremeLevel:        f.subLevel,
		minIntervalIndex:                f.minIntervalIndex,
		maxIntervalIndex:                f.maxIntervalIndex,
		internalFilesMinIntervalIndex:   f.minIntervalIndex,
		internalFilesMaxIntervalIndex:   f.maxIntervalIndex,
		fileBytes:                       f.meta.Size,
	}
	sl := f.subLevel
	fileIncluded := make([]bool, len(s.filesByAge))
	fileIncluded[f.index] = true
	// The seed file captures all files in the next level that fall
	// in the range of intervals. That may extend the range of
	// intervals so for correctness we need to capture all files
	// in the next level that fall in this extended interval and
	// so on. This can result in a triangular shape like the following
	// where again the X axis is the key intervals and the Y axis
	// is oldest to youngest. Note that it is not necessary for
	// correctness to fill out the shape at the higher sub-levels
	// to make it more rectangular since the invariant only requires
	// that younger versions of a key not be moved to Lbase while
	// leaving behind older versions.
	//                     -
	//                    ---
	//                   -----
	// It may be better for performance to have a more rectangular
	// shape since the files being left behind will induce touch the
	// same Lbase key range as that of this compaction. But there is
	// also the danger that in trying to construct a more rectangular
	// shape we will be forced to pull in a file that is already
	// compacting. We assume that the performance concern is not a
	// practical issue.
	for currLevel := sl - 1; currLevel >= 0; currLevel-- {
		if !s.extendFiles(currLevel, math.MaxUint64, cFiles, fileIncluded) {
			// Failed due to ongoing compaction.
			return nil
		}
	}

	// Now that we have a candidate group of files we can optionally add to it
	// by stacking more files from intervalIndex and repeating. This is an
	// optional activity so when it fails we can fallback to the last
	// successful candidate. Currently the code keeps adding until it can't
	// add more, but we could optionally stop based on
	// levelOCompactionFiles.fileBytes being too large.
	lastCandidate := &Level0CompactionFiles{}
	*lastCandidate = *cFiles
	slfList := s.orderedIntervals[intervalIndex].subLevelAndFileList
	slIndex := 1
	for ; slIndex < len(slfList); slIndex++ {
		sl := slfList[slIndex].subLevel
		f2 := s.filesByAge[slfList[slIndex].fileIndex]
		fileIncluded[f2.index] = true
		cFiles.seedIntervalStackDepthReduction++
		cFiles.seedIntervalExtremeLevel = sl
		cFiles.fileBytes += f2.meta.Size
		cFiles.Files = append(cFiles.Files, f2)
		// Reset the min and max to that of this file. See the triangular
		// shape comment above on why this is correct.
		cFiles.internalFilesMinIntervalIndex = f2.minIntervalIndex
		cFiles.internalFilesMaxIntervalIndex = f2.maxIntervalIndex
		if f2.minIntervalIndex < cFiles.minIntervalIndex {
			cFiles.minIntervalIndex = f2.minIntervalIndex
		}
		if f2.maxIntervalIndex > cFiles.maxIntervalIndex {
			cFiles.maxIntervalIndex = f2.maxIntervalIndex
		}
		done := false
		for currLevel := sl - 1; currLevel >= 0; currLevel-- {
			if !s.extendFiles(currLevel, math.MaxUint64, cFiles, fileIncluded) {
				// Failed to extend due to ongoing compaction.
				done = true
				break
			}
		}
		if done {
			break
		}
		if lastCandidate.seedIntervalStackDepthReduction >= minCompactionDepth &&
			cFiles.fileBytes > 100<<20 && (float64(cFiles.fileBytes)/float64(lastCandidate.fileBytes) > 1.5) {
			break
		}
		*lastCandidate = *cFiles
	}
	if lastCandidate.seedIntervalStackDepthReduction >= minCompactionDepth {
		// s.updateStateForStartedCompaction(lastCandidate, true)
		for i := range fileIncluded {
			fileIncluded[i] = false
		}
		for _, f := range lastCandidate.Files {
			fileIncluded[f.index] = true
		}
		lastCandidate.FilesIncluded = fileIncluded
		return lastCandidate
	}
	return nil
}

func (s *L0SubLevels) extendFiles(
	sl int, earliestUnflushdSeqNum uint64, cFiles *Level0CompactionFiles, fileIncluded []bool,
) bool {
	index := sort.Search(len(s.subLevels[sl]), func(i int) bool {
		return s.subLevels[sl][i].maxIntervalIndex >= cFiles.internalFilesMinIntervalIndex
	})
	for ; index < len(s.subLevels[sl]); index++ {
		f := s.subLevels[sl][index]
		if f.minIntervalIndex > cFiles.internalFilesMaxIntervalIndex {
			break
		}
		if fileIncluded[f.index] || f.meta.LargestSeqNum >= earliestUnflushdSeqNum {
			continue
		}
		if f.meta.IsBaseCompacting || f.meta.IsIntraL0Compacting {
			return false
		}
		fileIncluded[f.index] = true
		cFiles.Files = append(cFiles.Files, f)
		cFiles.fileBytes += f.meta.Size
		if f.minIntervalIndex < cFiles.internalFilesMinIntervalIndex {
			cFiles.internalFilesMinIntervalIndex = f.minIntervalIndex
		}
		if f.maxIntervalIndex > cFiles.internalFilesMaxIntervalIndex {
			cFiles.internalFilesMaxIntervalIndex = f.maxIntervalIndex
		}
		if f.minIntervalIndex < cFiles.minIntervalIndex {
			cFiles.minIntervalIndex = f.minIntervalIndex
		}
		if f.maxIntervalIndex > cFiles.maxIntervalIndex {
			cFiles.maxIntervalIndex = f.maxIntervalIndex
		}
	}
	return true
}

func (s *L0SubLevels) PickIntraL0Compaction(
	earliestUnflushedSeqNum uint64, minCompactionDepth int,
) *Level0CompactionFiles {
	var pool []intervalAndScore
	for i := range s.orderedIntervals {
		interval := &s.orderedIntervals[i]
		depth := interval.fileCount - interval.compactingFileCount
		if minCompactionDepth > depth || minCompactionDepth > interval.topOfStackNonCompactingFileCount {
			continue
		}
		// Is there a way to incorporate topOfStackNonCompactingFileCount into the score?
		pool = append(pool, intervalAndScore{interval: interval, score: depth})
	}
	sort.Sort(intervalSorterByDecreasingScore(pool))

	// Optimization to avoid considering different intervals that
	// are likely to choose the same seed file. This is just
	// to reduce wasted work.
	consideredIntervals := make([]bool, len(s.orderedIntervals))
	for _, interval := range pool {
		if consideredIntervals[interval.interval.index] {
			continue
		}
		// Pick the seed file for the interval as the file
		// in the highest sub-level.
		slIndex := len(interval.interval.subLevelAndFileList) - 1
		adjustedNonCompactingFileCount := interval.interval.topOfStackNonCompactingFileCount
		var f *fileMeta
		for ; slIndex >= 0; slIndex-- {
			slf := interval.interval.subLevelAndFileList[slIndex]
			f = s.filesByAge[slf.fileIndex]
			if f.meta.Compacting {
				s.logger.Fatalf("file %d being considered for intra-L0 should not be compacting %d", f.meta.FileNum)
			}
			for i := f.minIntervalIndex; i <= f.maxIntervalIndex; i++ {
				consideredIntervals[i] = true
			}
			// Can this be the seed file?
			if f.meta.LargestSeqNum >= earliestUnflushedSeqNum {
				adjustedNonCompactingFileCount--
				if adjustedNonCompactingFileCount == 0 {
					break
				}
			} else {
				break
			}
		}
		if adjustedNonCompactingFileCount < minCompactionDepth {
			// Can't use this interval.
			continue
		}
		if f.meta.IsBaseCompacting || f.meta.IsIntraL0Compacting {
			// Since adjustedNonCompactingFileCount > 0 this file must not
			// be compacting.
			s.logger.Fatalf("file %d being considered for intra-L0 should not be compacting %d", f.meta.FileNum)
		}
		// We have a seed file.
		c := s.intraL0CompactionUsingSeed(
			f, interval.interval.index, earliestUnflushedSeqNum, minCompactionDepth)
		if c != nil {
			var buf strings.Builder
			for _, slf := range interval.interval.subLevelAndFileList {
				fmt.Fprintf(&buf, "%d, ", slf.subLevel)
			}
			s.logger.Infof("Seed interval for intra-L0 compaction: %d, sublevels: %s",
				interval.interval.index, buf.String())
			return c
		}
	}
	return nil
}

func (s *L0SubLevels) intraL0CompactionUsingSeed(
	f *fileMeta, intervalIndex int, earliestUnflushedSeqNum uint64, minCompactionDepth int,
) *Level0CompactionFiles {
	// We know that all the files that overlap with intervalIndex have
	// LargestSeqNum < earliestUnflushedSeqNum, but for other intervals
	// we need to exclude files >= earliestUnflushedSeqNum

	cFiles := &Level0CompactionFiles{
		Files:                           []*fileMeta{f},
		seedIntervalStackDepthReduction: 1,
		seedIntervalExtremeLevel:        f.subLevel,
		minIntervalIndex:                f.minIntervalIndex,
		maxIntervalIndex:                f.maxIntervalIndex,
		internalFilesMinIntervalIndex:   f.minIntervalIndex,
		internalFilesMaxIntervalIndex:   f.maxIntervalIndex,
		fileBytes:                       f.meta.Size,
		isIntraL0:                       true,
		earliestUnflushedSeqNum:         earliestUnflushedSeqNum,
	}
	sl := f.subLevel
	fileIncluded := make([]bool, len(s.filesByAge))
	fileIncluded[f.index] = true

	// The seed file captures all files in the higher level that fall in the
	// range of intervals. That may extend the range of intervals so for
	// correctness we need to capture all files in the next higher level that
	// fall in this extended interval and so on. This can result in an
	// inverted triangular shape like the following where again the X axis is the
	// key intervals and the Y axis is oldest to youngest. Note that it is not
	// necessary for correctness to fill out the shape at lower sub-levels to
	// make it more rectangular since the invariant only requires that if we
	// move an older seqnum for key k into a file that has a higher seqnum, we
	// also move all younger seqnums for that key k into that file.
	//                  -----
	//                   ---
	//                    -
	//
	// It may be better for performance to have a more rectangular shape since
	// it will reduce the stack depth for more intervals. But there is also
	// the danger that in explicitly trying to construct a more rectangular
	// shape we will be forced to pull in a file that is already compacting.
	// We assume that the performance concern is not a practical issue.
	for currLevel := sl + 1; currLevel < len(s.subLevels); currLevel++ {
		if !s.extendFiles(currLevel, earliestUnflushedSeqNum, cFiles, fileIncluded) {
			// Failed due to ongoing compaction.
			return nil
		}
	}

	// Now that we have a candidate group of files we can optionally add to it
	// by stacking more files from intervalIndex and repeating. This is an
	// optional activity so when it fails we can fallback to the last
	// successful candidate. Currently the code keeps adding until it can't
	// add more, but we could optionally stop based on
	// levelOCompactionFiles.fileBytes being too large.
	lastCandidate := &Level0CompactionFiles{}
	*lastCandidate = *cFiles
	slfList := s.orderedIntervals[intervalIndex].subLevelAndFileList
	slIndex := len(slfList) - 1
	for {
		if slfList[slIndex].fileIndex == f.index {
			break
		}
		slIndex--
	}
	slIndex--
	for ; slIndex >= 0; slIndex-- {
		sl := slfList[slIndex].subLevel
		f2 := s.filesByAge[slfList[slIndex].fileIndex]
		if f2.meta.IsBaseCompacting || f2.meta.IsIntraL0Compacting {
			break
		}
		fileIncluded[f2.index] = true
		cFiles.seedIntervalStackDepthReduction++
		cFiles.seedIntervalExtremeLevel = sl
		cFiles.fileBytes += f2.meta.Size
		cFiles.Files = append(cFiles.Files, f2)
		// Reset the min and max to that of this file. See the triangular
		// shape comment above on why this is correct.
		cFiles.internalFilesMinIntervalIndex = f2.minIntervalIndex
		cFiles.internalFilesMaxIntervalIndex = f2.maxIntervalIndex
		if f2.minIntervalIndex < cFiles.minIntervalIndex {
			cFiles.minIntervalIndex = f2.minIntervalIndex
		}
		if f2.maxIntervalIndex > cFiles.maxIntervalIndex {
			cFiles.maxIntervalIndex = f2.maxIntervalIndex
		}
		done := false
		for currLevel := sl + 1; currLevel < len(s.subLevels); currLevel++ {
			if !s.extendFiles(currLevel, earliestUnflushedSeqNum, cFiles, fileIncluded) {
				// Failed to extend due to ongoing compaction.
				done = true
				break
			}
		}
		if done {
			break
		}
		if cFiles.seedIntervalStackDepthReduction >= minCompactionDepth &&
			cFiles.fileBytes > 100<<20 {
			break
		}
		*lastCandidate = *cFiles
	}
	if lastCandidate.seedIntervalStackDepthReduction >= minCompactionDepth {
		// s.updateStateForStartedCompaction(lastCandidate, false)
		for i := range fileIncluded {
			fileIncluded[i] = false
		}
		for _, f := range lastCandidate.Files {
			fileIncluded[f.index] = true
		}
		lastCandidate.FilesIncluded = fileIncluded
		s.logger.Infof("ExtendL0ForIntraL0Compaction: [%d, %d]\n",
			lastCandidate.minIntervalIndex, lastCandidate.maxIntervalIndex)
		s.extendCandidateToRectangle(
			lastCandidate.minIntervalIndex, lastCandidate.maxIntervalIndex, lastCandidate, false)
		return lastCandidate
	}
	return nil
}

func (s *L0SubLevels) ExtendL0ForBaseCompactionTo(
	smallest []byte, largest []byte, candidate *Level0CompactionFiles,
) bool {
	firstIntervalIndex := sort.Search(len(s.orderedIntervals), func(i int) bool {
		return s.cmp(smallest, s.orderedIntervals[i].startKey.key.UserKey) <= 0
	})
	// Even if smallest is equal to the start UserKey, the previous interval may
	// have keys equal to this UserKey
	if firstIntervalIndex > 0 && s.orderedIntervals[firstIntervalIndex-1].fileCount > 0 {
		firstIntervalIndex--
	}
	lastIntervalIndex := sort.Search(len(s.orderedIntervals), func(i int) bool {
		return s.cmp(largest, s.orderedIntervals[i].startKey.key.UserKey) < 0
	})
	if lastIntervalIndex > 0 {
		lastIntervalIndex--
	}
	s.logger.Infof("ExtendL0ForBaseCompaction: [%d, %d], original: [%d, %d]\n",
		firstIntervalIndex, lastIntervalIndex, candidate.minIntervalIndex, candidate.maxIntervalIndex)
	return s.extendCandidateToRectangle(firstIntervalIndex, lastIntervalIndex, candidate, true)
}

// Best-effort attempt to make the compaction include more files in the rectangle
// defined by [minIntervalIndex, maxIntervalIndex] on the X axis and bounded on
// one side of the Y axis by candidate.seeIntervalExtremeLevel (the other side is 0
// for L0 => Lbase compactions and len(s.subLevels)-1 for intra-L0 compactions).
// REQUIRES: minIntervalIndex <= candidate.minIntervalIndex
//           maxIntervalIndex >= candidate.maxIntervalIndex
func (s *L0SubLevels) extendCandidateToRectangle(
	minIntervalIndex int, maxIntervalIndex int, candidate *Level0CompactionFiles, isBase bool,
) bool {
	var bottomLevel int
	var increment int
	var limitReached func(int) bool
	if isBase {
		bottomLevel = 0
		increment = +1
		limitReached = func(sl int) bool {
			return sl > candidate.seedIntervalExtremeLevel
		}
	} else {
		bottomLevel = len(s.subLevels) - 1
		increment = -1
		limitReached = func(sl int) bool {
			return sl < candidate.seedIntervalExtremeLevel
		}
	}
	// Stats for files.
	addedCount := 0
	extendingOutCount := 0
	compactingCount := 0
	// Iterate from the oldest sub-level for L0 => Lbase and youngest
	// sub-level for intra-L0. The idea here is that anything that can't
	// be included from that level constrains what can be included from
	// the next level. This change in constraint is directly incorporated
	// into minIntervalIndex, maxIntervalIndex.
	for sl := bottomLevel; !limitReached(sl); sl += increment {
		files := s.subLevels[sl]
		// Find the first file that overlaps with minIntervalIndex.
		index := sort.Search(len(files), func(i int) bool {
			return minIntervalIndex <= files[i].maxIntervalIndex
		})
		// Track the files that are fully within the current constraint
		// of [minIntervalIndex, maxIntervalIndex].
		firstIndex := -1
		lastIndex := -1
		for ; index < len(files); index++ {
			f := files[index]
			if f.minIntervalIndex > maxIntervalIndex {
				break
			}
			include := true
			// Extends out on the left so can't be included. This narrows
			// what we can included in the next level.
			if f.minIntervalIndex < minIntervalIndex {
				include = false
				extendingOutCount++
				minIntervalIndex = f.maxIntervalIndex + 1
			}
			// Extends out on the right so can't be included.
			if f.maxIntervalIndex > maxIntervalIndex {
				if include {
					extendingOutCount++
				}
				include = false
				maxIntervalIndex = f.minIntervalIndex - 1
			}
			if !include {
				continue
			}
			if firstIndex == -1 {
				firstIndex = index
			}
			lastIndex = index
		}
		if minIntervalIndex > maxIntervalIndex {
			// We excluded files that prevent continuation.
			break
		}
		if firstIndex < 0 {
			// No files to add in this sub-level.
			continue
		}
		// We have the files in [firstIndex, lastIndex] as potential for
		// inclusion. Some of these may already have been picked. Some
		// of them may be already compacting. The latter is tricky since
		// we have to decide whether to contract minIntervalIndex or
		// maxIntervalIndex when we encounter an already compacting file.
		// We pick the longest sequence between firstIndex
		// and lastIndex of non-compacting files -- this is represented by
		// [candidateNonCompactingFirst, candidateNonCompactingLast].
		nonCompactingFirst := -1
		candidateNonCompactingFirst := -1
		candidateNonCompactingLast := -1
		for index = firstIndex; index <= lastIndex; index++ {
			f := files[index]
			if f.meta.Compacting {
				compactingCount++
				if nonCompactingFirst != -1 {
					last := index - 1
					if candidateNonCompactingFirst == -1 ||
						(last-nonCompactingFirst) > (candidateNonCompactingLast-candidateNonCompactingFirst) {
						candidateNonCompactingFirst = nonCompactingFirst
						candidateNonCompactingLast = last
					}
				}
				nonCompactingFirst = -1
				continue
			}
			if nonCompactingFirst == -1 {
				nonCompactingFirst = index
			}
		}
		if nonCompactingFirst != -1 {
			last := index - 1
			if candidateNonCompactingFirst == -1 ||
				(last-nonCompactingFirst) > (candidateNonCompactingLast-candidateNonCompactingFirst) {
				candidateNonCompactingFirst = nonCompactingFirst
				candidateNonCompactingLast = last
			}
		}
		if candidateNonCompactingFirst == -1 {
			// All files are compacting. There will be gaps that we could exploit
			// to continue, but don't bother.
			break
		}
		// May need to shrink [minIntervalIndex, maxIntervalIndex] for the next level.
		if candidateNonCompactingFirst > firstIndex {
			minIntervalIndex = files[candidateNonCompactingFirst-1].maxIntervalIndex + 1
		}
		if candidateNonCompactingLast < lastIndex {
			maxIntervalIndex = files[candidateNonCompactingLast+1].minIntervalIndex - 1
		}
		for index := candidateNonCompactingFirst; index <= candidateNonCompactingLast; index++ {
			f := files[index]
			if f.meta.Compacting {
				s.logger.Fatalf("bug")
			}
			if candidate.isIntraL0 && f.meta.LargestSeqNum >= candidate.earliestUnflushedSeqNum {
				continue
			}
			if !candidate.FilesIncluded[f.index] {
				candidate.FilesIncluded[f.index] = true
				addedCount++
				candidate.Files = append(candidate.Files, f)
				candidate.fileBytes += f.meta.Size
				if f.minIntervalIndex < candidate.minIntervalIndex {
					candidate.minIntervalIndex = f.minIntervalIndex
				}
				if f.maxIntervalIndex > candidate.maxIntervalIndex {
					candidate.maxIntervalIndex = f.maxIntervalIndex
				}
			}
		}
	}
	s.logger.Infof("adjustCandidateToRectangle: added: %d, compacting: %d, extending-out: %d\n",
		addedCount, compactingCount, extendingOutCount)
	return addedCount > 0
}

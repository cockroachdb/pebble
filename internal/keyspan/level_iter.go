// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"runtime/debug"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
)

// Logger defines an interface for writing log messages.
type Logger interface {
	Infof(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

// LevelIter provides a merged view of the range keys from sstables in a level.
// It takes advantage of level invaraints to only have one sstable range
// key block open at once time, opened using the newIter function passed in.
type LevelIter struct {
	logger Logger
	cmp    base.Compare
	// The lower/upper bounds for iteration as specified at creation or the most
	// recent call to SetBounds. These slices could point to the same underlying
	// memory as {Lower,Upper}Bounds in the passed-in RangeIterOptions, and also
	// returned as part of Spans when the iterator is at bounds. While the
	// bounds in tableOpts could be set to nil if they don't apply to the current
	// file, the bounds here will remain unmodified until reset or updated by the
	// caller using SetBounds.
	lower []byte
	upper []byte
	// The LSM level this LevelIter is initialized for. Used in logging.
	level manifest.Level
	// The below fields are used to fill in gaps between adjacent files' range
	// key spaces. This is an optimization to avoid unnecessarily loading files
	// in cases where range keys are sparse and rare. dir is set by every
	// positioning operation, straddleDir is set to dir whenever a straddling
	// Span is synthesized, and straddle is Valid() whenever the last positioning
	// operation returned a synthesized straddle span.
	//
	// Note that when a straddle span is initialized, iterFile is modified to
	// point to the next file in the straddleDir direction. A change of direction
	// on a straddle key therefore necessitates the value of iterFile to be
	// reverted.
	dir         int
	straddle    Span
	straddleDir int
	// The iter for the current file. It is nil under any of the following conditions:
	// - files.Current() == nil
	// - err != nil
	// - straddle.Valid(), in which case iterFile is not nil and points to the
	//   next file (in the straddleDir direction).
	// - some other constraint, like the bounds in opts, caused the file at index to not
	//   be relevant to the iteration.
	iter     FragmentIterator
	iterFile *manifest.FileMetadata
	newIter  TableNewRangeKeyIter
	files    manifest.LevelIterator
	err      error

	// The options that were passed in. If bounds do not need to be checked for
	// the current file, tableOpts.{Lower,Upper}Bound could be set to nil.
	tableOpts RangeIterOptions

	// Set to true if the last positioning operation put us beyond the bounds.
	// The underlying iterator is left valid to allow relative positioning
	// operations that could make the iterator valid again.
	exhaustedBounds bool

	// TODO(bilal): Add InternalIteratorStats.
}

// LevelIter implements the keyspan.FragmentIterator interface.
var _ FragmentIterator = (*LevelIter)(nil)

// newLevelIter returns a LevelIter.
func newLevelIter(
	opts RangeIterOptions,
	cmp base.Compare,
	newIter TableNewRangeKeyIter,
	files manifest.LevelIterator,
	level manifest.Level,
	logger Logger,
) *LevelIter {
	l := &LevelIter{}
	l.Init(opts, cmp, newIter, files, level, logger)
	return l
}

// Init initializes a LevelIter.
func (l *LevelIter) Init(
	opts RangeIterOptions,
	cmp base.Compare,
	newIter TableNewRangeKeyIter,
	files manifest.LevelIterator,
	level manifest.Level,
	logger Logger,
) {
	l.err = nil
	l.level = level
	l.logger = logger
	l.lower = opts.LowerBound
	l.upper = opts.UpperBound
	l.tableOpts.Filters = opts.Filters
	l.cmp = cmp
	l.iterFile = nil
	l.newIter = newIter
	l.files = files.Filter(manifest.KeyTypeRange)
}

func (l *LevelIter) findFileGE(key []byte) *manifest.FileMetadata {
	// Find the earliest file whose largest key is >= key.
	//
	// If the earliest file has its largest key == key and that largest key is a
	// range deletion sentinel, we know that we manufactured this sentinel to convert
	// the exclusive range deletion end key into an inclusive key (reminder: [start, end)#seqnum
	// is the form of a range deletion sentinel which can contribute a largest key = end#sentinel).
	// In this case we don't return this as the earliest file since there is nothing actually
	// equal to key in it.

	m := l.files.SeekGE(l.cmp, key)
	for m != nil && m.LargestRangeKey.IsExclusiveSentinel() &&
		l.cmp(m.LargestRangeKey.UserKey, key) == 0 {
		m = l.files.Next()
	}
	return m
}

func (l *LevelIter) findFileLT(key []byte) *manifest.FileMetadata {
	// Find the last file whose smallest key is < key.
	return l.files.SeekLT(l.cmp, key)
}

// Init the iteration bounds for the current table. Returns -1 if the table
// lies fully before the lower bound, +1 if the table lies fully after the
// upper bound, and 0 if the table overlaps the iteration bounds.
func (l *LevelIter) initTableBounds(f *manifest.FileMetadata) int {
	l.tableOpts.LowerBound = l.lower
	l.tableOpts.UpperBound = l.upper
	if l.tableOpts.LowerBound != nil {
		if cmp := l.cmp(f.LargestRangeKey.UserKey, l.tableOpts.LowerBound); cmp < 0 ||
			(cmp == 0 && f.LargestRangeKey.IsExclusiveSentinel()) {
			// The largest key in the sstable is smaller than the lower bound.
			return -1
		}
		if l.cmp(l.tableOpts.LowerBound, f.SmallestRangeKey.UserKey) <= 0 {
			// The lower bound is smaller or equal to the smallest key in the
			// table. Iteration within the table does not need to check the lower
			// bound.
			l.tableOpts.LowerBound = nil
		}
	}
	if l.tableOpts.UpperBound != nil {
		if l.cmp(f.SmallestRangeKey.UserKey, l.tableOpts.UpperBound) >= 0 {
			// The smallest key in the sstable is greater than or equal to the upper
			// bound.
			return 1
		}
		if l.cmp(l.tableOpts.UpperBound, f.LargestRangeKey.UserKey) > 0 {
			// The upper bound is greater than the largest key in the
			// table. Iteration within the table does not need to check the upper
			// bound. NB: tableOpts.UpperBound is exclusive and f.Largest is inclusive.
			l.tableOpts.UpperBound = nil
		}
	}
	return 0
}

type loadFileReturnIndicator int8

const (
	noFileLoaded loadFileReturnIndicator = iota
	fileAlreadyLoaded
	newFileLoaded
)

func (l *LevelIter) loadFile(file *manifest.FileMetadata, dir int) loadFileReturnIndicator {
	indicator := noFileLoaded
	if l.iterFile == file {
		if l.err != nil {
			return noFileLoaded
		}
		if l.iter != nil {
			// We are already at the file, but we would need to check for bounds.
			// Set indicator accordingly.
			indicator = fileAlreadyLoaded
		}
		// We were already at file, but don't have an iterator, probably because the file was
		// beyond the iteration bounds. It may still be, but it is also possible that the bounds
		// have changed. We handle that below.
	}

	// Note that LevelIter.Close() can be called multiple times.
	if indicator != fileAlreadyLoaded {
		if err := l.Close(); err != nil {
			return noFileLoaded
		}
	}

	for {
		l.iterFile = file
		if file == nil {
			return noFileLoaded
		}

		switch l.initTableBounds(file) {
		case -1:
			// The largest key in the sstable is smaller than the lower bound.
			if dir < 0 {
				l.exhaustedBounds = true
				return noFileLoaded
			}
			file = l.files.Next()
			indicator = noFileLoaded
			continue
		case +1:
			// The smallest key in the sstable is greater than or equal to the upper
			// bound.
			if dir > 0 {
				l.exhaustedBounds = true
				return noFileLoaded
			}
			file = l.files.Prev()
			indicator = noFileLoaded
			continue
		}
		// case 0: The current file overlaps with the iteration bounds. We could
		// still have to check returned Spans based on bounds.

		if indicator != fileAlreadyLoaded {
			l.iter, l.err = l.newIter(l.files.Current(), &l.tableOpts)
			indicator = newFileLoaded
		}
		if l.err != nil {
			return noFileLoaded
		}
		return indicator
	}
}

// In race builds we verify that the keys returned by LevelIter lie within
// [lower,upper).
func (l *LevelIter) verify(span Span) Span {
	if invariants.Enabled && span.Valid() {
		// Confirm that bounds checking is working.
		if l.lower != nil && l.cmp(span.End, l.lower) <= 0 {
			l.logger.Fatalf("LevelIter %s: lower bound violation: %s <= %s\n%s", l.level, span.End, l.lower, debug.Stack())
		}
		// Note that we are checking span.End here, and not key. We expect that
		// the corresponding end key for this range key has already been populated
		// into span.
		if l.upper != nil && l.cmp(span.Start, l.upper) >= 0 {
			l.logger.Fatalf("LevelIter %s: upper bound violation: %s >= %s\n%s", l.level, span.Start, l.upper, debug.Stack())
		}
	}
	return span
}

// checkLowerBound checks the span according to the lower bound. Could also
// invalidate the entire iterator if we are beyond bounds.
func (l *LevelIter) checkLowerBound(span Span, lowerBound []byte) Span {
	if l.cmp(lowerBound, span.End) >= 0 {
		// Completely past bound.
		l.exhaustedBounds = true
		return Span{}
	}
	return span
}

// checkUpperBound checks the span according to the table upper bound. Could also
// invalidate the entire iterator if we are beyond bounds.
func (l *LevelIter) checkUpperBound(span Span, upperBound []byte) Span {
	if l.cmp(span.Start, upperBound) >= 0 {
		// Past bound.
		l.exhaustedBounds = true
		return Span{}
	}
	return span
}

// SeekGE implements keyspan.FragmentIterator.
func (l *LevelIter) SeekGE(key []byte) Span {
	l.dir = +1
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	f := l.findFileGE(key)
	if f != nil && l.cmp(key, f.SmallestRangeKey.UserKey) < 0 {
		// Return a straddling key instead of loading the file.
		l.iterFile = f
		l.iter = nil
		l.straddleDir = +1
		// The synthetic span that we are creating starts at the seeked key. This
		// is an optimization as it prevents us from loading the adjacent file's
		// bounds, at the expense of this iterator appearing "inconsistent" to its
		// callers i.e.:
		//
		// SeekGE(bb) -> {bb-c, empty}
		// Next()     -> {c-d, RANGEKEYSET}
		// Prev()     -> {a-c, empty}
		//
		// Seeing as the inconsistency will only be around empty spans, which are
		// expected to be elided by one of the higher-level iterators (either
		// top-level Iterator or the defragmenting iter), the entire iterator should
		// still appear consistent to the user.
		l.straddle = Span{
			Start: key,
			End:   f.SmallestRangeKey.UserKey,
			Keys:  nil,
		}
		if l.upper != nil {
			l.straddle = l.checkUpperBound(l.straddle, l.upper)
		}
		return l.verify(l.straddle)
	}
	loadFileIndicator := l.loadFile(f, +1)
	if loadFileIndicator == noFileLoaded {
		return Span{}
	}
	if span := l.iter.SeekGE(key); span.Valid() {
		if l.tableOpts.LowerBound != nil {
			span = l.checkLowerBound(span, l.tableOpts.LowerBound)
		}
		if l.tableOpts.UpperBound != nil {
			span = l.checkUpperBound(span, l.tableOpts.UpperBound)
		}
		return l.verify(span)
	}
	return l.verify(l.skipEmptyFileForward())
}

// SeekLT implements keyspan.FragmentIterator.
func (l *LevelIter) SeekLT(key []byte) Span {
	l.dir = -1
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.UpperBound.
	f := l.findFileLT(key)
	if f != nil && l.cmp(f.LargestRangeKey.UserKey, key) < 0 {
		// Return a straddling key instead of loading the file.
		l.iterFile = f
		l.iter = nil
		l.straddleDir = -1
		// The synthetic span that we are creating ends at the seeked key. This
		// is an optimization as it prevents us from loading the adjacent file's
		// bounds, at the expense of this iterator appearing "inconsistent" to its
		// callers i.e.:
		//
		// SeekLT(dd) -> {d-dd, empty}
		// Prev()     -> {c-d, RANGEKEYSET}
		// Next()     -> {d-e, empty}
		//
		// Seeing as the inconsistency will only be around empty spans, which are
		// expected to be elided by one of the higher-level iterators (either
		// top-level Iterator or the defragmenting iter), the entire iterator should
		// still appear consistent to the user.
		l.straddle = Span{
			Start: f.LargestRangeKey.UserKey,
			End:   key,
			Keys:  nil,
		}
		if l.lower != nil {
			l.straddle = l.checkLowerBound(l.straddle, l.lower)
		}
		return l.verify(l.straddle)
	}
	if l.loadFile(l.findFileLT(key), -1) == noFileLoaded {
		return Span{}
	}
	if span := l.iter.SeekLT(key); span.Valid() {
		if l.tableOpts.LowerBound != nil {
			span = l.checkLowerBound(span, l.tableOpts.LowerBound)
		}
		if l.tableOpts.UpperBound != nil {
			span = l.checkUpperBound(span, l.tableOpts.UpperBound)
		}
		return l.verify(span)
	}
	return l.verify(l.skipEmptyFileBackward())
}

// First implements keyspan.FragmentIterator.
func (l *LevelIter) First() Span {
	l.dir = +1
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	if l.loadFile(l.files.First(), +1) == noFileLoaded {
		return Span{}
	}
	if span := l.iter.First(); span.Valid() {
		// We only need to check for the upper bound here, as the toplevel Iter
		// should have turned a First call into a SeekGE if we had a lower bound
		if l.tableOpts.UpperBound != nil {
			span = l.checkUpperBound(span, l.tableOpts.UpperBound)
		}
		return l.verify(span)
	}
	return l.verify(l.skipEmptyFileForward())
}

// Last implements keyspan.FragmentIterator.
func (l *LevelIter) Last() Span {
	l.dir = -1
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	if l.loadFile(l.files.Last(), -1) == noFileLoaded {
		return Span{}
	}
	if span := l.iter.Last(); span.Valid() {
		if l.tableOpts.LowerBound != nil {
			span = l.checkLowerBound(span, l.tableOpts.LowerBound)
		}
		if l.tableOpts.UpperBound != nil {
			span = l.checkUpperBound(span, l.tableOpts.UpperBound)
		}
		return l.verify(span)
	}
	return l.verify(l.skipEmptyFileBackward())
}

// Next implements keyspan.FragmentIterator.
func (l *LevelIter) Next() Span {
	l.dir = +1
	if l.err != nil || (l.iter == nil && l.iterFile == nil) {
		return Span{}
	}
	l.exhaustedBounds = false

	if l.iter != nil {
		if span := l.iter.Next(); span.Valid() {
			if l.tableOpts.LowerBound != nil {
				span = l.checkLowerBound(span, l.tableOpts.LowerBound)
			}
			if l.tableOpts.UpperBound != nil {
				span = l.checkUpperBound(span, l.tableOpts.UpperBound)
			}
			return l.verify(span)
		}
	}
	return l.verify(l.skipEmptyFileForward())
}

// Prev implements keyspan.FragmentIterator.
func (l *LevelIter) Prev() Span {
	l.dir = -1
	if l.err != nil || (l.iter == nil && l.iterFile == nil) {
		return Span{}
	}
	l.exhaustedBounds = false

	if l.iter != nil {
		if span := l.iter.Prev(); span.Valid() {
			if l.tableOpts.LowerBound != nil {
				span = l.checkLowerBound(span, l.tableOpts.LowerBound)
			}
			if l.tableOpts.UpperBound != nil {
				span = l.checkUpperBound(span, l.tableOpts.UpperBound)
			}
			return l.verify(span)
		}
	}
	return l.verify(l.skipEmptyFileBackward())
}

func (l *LevelIter) skipEmptyFileForward() Span {
	if !l.straddle.Valid() && l.iterFile != nil && l.iter != nil {
		// We were at a file that had range keys. Check if the next file that has
		// range keys is not directly adjacent to the current file i.e. there is a
		// gap in the range keyspace between the two files. In that case, synthesize
		// a "straddle span" in l.straddle and return that.
		if err := l.Close(); err != nil {
			l.err = err
			return Span{}
		}
		startKey := l.iterFile.LargestRangeKey.UserKey
		l.iterFile = l.files.Next()
		if l.iterFile == nil {
			return Span{}
		}
		endKey := l.iterFile.SmallestRangeKey.UserKey
		if l.cmp(startKey, endKey) < 0 {
			// There is a gap between the two files. Synthesize a straddling span
			// to avoid unnecessarily loading the next file.
			l.straddle = Span{
				Start: startKey,
				End:   endKey,
			}
			l.straddleDir = l.dir
			if l.upper != nil {
				l.straddle = l.checkUpperBound(l.straddle, l.upper)
			}
			return l.straddle
		}
	} else if l.straddle.Valid() && l.straddleDir < 0 {
		// We were at a straddle key, but are now changing directions. l.iterFile
		// was already moved backward by skipEmptyFileBackward, so advance it
		// forward.
		l.iterFile = l.files.Next()
	}
	l.straddle = Span{}
	var span Span
	if l.loadFile(l.iterFile, +1) == noFileLoaded {
		return Span{}
	}
	span = l.iter.First()
	if l.tableOpts.UpperBound != nil {
		span = l.checkUpperBound(span, l.tableOpts.UpperBound)
	}
	return span
}

func (l *LevelIter) skipEmptyFileBackward() Span {
	// We were at a file that had range keys. Check if the previous file that has
	// range keys is not directly adjacent to the current file i.e. there is a
	// gap in the range keyspace between the two files. In that case, synthesize
	// a "straddle span" in l.straddle and return that.
	if !l.straddle.Valid() && l.iterFile != nil && l.iter != nil {
		if err := l.Close(); err != nil {
			l.err = err
			return Span{}
		}
		endKey := l.iterFile.SmallestRangeKey.UserKey
		l.iterFile = l.files.Prev()
		if l.iterFile == nil {
			return Span{}
		}
		startKey := l.iterFile.LargestRangeKey.UserKey
		if l.cmp(startKey, endKey) < 0 {
			// There is a gap between the two files. Synthesize a straddling span
			// to avoid unnecessarily loading the next file.
			l.straddle = Span{
				Start: startKey,
				End:   endKey,
			}
			l.straddleDir = l.dir
			if l.lower != nil {
				l.straddle = l.checkLowerBound(l.straddle, l.lower)
			}
			return l.straddle
		}
	} else if l.straddle.Valid() && l.straddleDir > 0 {
		// We were at a straddle key, but are now changing directions. l.iterFile
		// was already advanced forward by skipEmptyFileForward, so move it
		// backward.
		l.iterFile = l.files.Prev()
	}
	l.straddle = Span{}
	var span Span
	if l.loadFile(l.iterFile, -1) == noFileLoaded {
		return Span{}
	}
	span = l.iter.Last()
	if l.tableOpts.LowerBound != nil {
		span = l.checkLowerBound(span, l.tableOpts.LowerBound)
	}
	return span
}

// Error implements keyspan.FragmentIterator.
func (l *LevelIter) Error() error {
	if l.err != nil || l.iter == nil {
		return l.err
	}
	return l.iter.Error()
}

// Close implements keyspan.FragmentIterator.
func (l *LevelIter) Close() error {
	if l.iter != nil {
		l.err = l.iter.Close()
		l.iter = nil
	}
	return l.err
}

// SetBounds implements keyspan.FragmentIterator.
func (l *LevelIter) SetBounds(lower, upper []byte) {
	l.lower = lower
	l.upper = upper

	if l.iter == nil {
		return
	}

	// Update tableOpts.{Lower,Upper}Bound in case the new boundaries fall within
	// the boundaries of the current table.
	if l.initTableBounds(l.iterFile) != 0 {
		// The table does not overlap the bounds. Close() will set LevelIter.err if
		// an error occurs.
		_ = l.Close()
		return
	}

	// We do not call SetBounds on l.iter, as range key block iterators do not
	// support SetBounds.
	//
	// TODO(bilal): Update fragmentBlockIter to do bounds checking as well.
	l.exhaustedBounds = true
}

// String implements keyspan.FragmentIterator.
func (l *LevelIter) String() string {
	if l.iterFile != nil {
		return fmt.Sprintf("%s: fileNum=%s", l.level, l.iterFile.FileNum)
	}
	return fmt.Sprintf("%s: fileNum=<nil>", l.level)
}

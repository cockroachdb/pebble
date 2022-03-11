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
	// returned as part of Spans when the iterator is at bounds.
	lower []byte
	upper []byte
	// The LSM level this LevelIter is initialized for. Used in logging.
	level manifest.Level
	// The iter for the current file. It is nil under any of the following conditions:
	// - files.Current() == nil
	// - err != nil
	// - some other constraint, like the bounds in opts, caused the file at index to not
	//   be relevant to the iteration.
	iter     FragmentIterator
	iterFile *manifest.FileMetadata
	span     Span
	newIter  TableNewRangeKeyIter
	files    manifest.LevelIterator
	err      error

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
	l.tableOpts.TableFilter = opts.TableFilter
	l.tableOpts.Filters = opts.Filters
	l.cmp = cmp
	l.iterFile = nil
	l.newIter = newIter
	l.files = files.Filter(manifest.KeyTypeRange)
}

// Valid implements the keyspan.FragmentIterator interface.
func (l *LevelIter) Valid() bool {
	return l.iter != nil && l.iter.Valid() && !l.exhaustedBounds
}

// End implements the keyspan.FragmentIterator interface.
func (l *LevelIter) End() []byte {
	if l.iter == nil {
		return nil
	}
	return l.span.End
}

// Current implements the keyspan.FragmentIterator interface
func (l *LevelIter) Current() Span {
	if !l.Valid() {
		return Span{}
	}
	return l.span
}

// Clone implements the keyspan.FragmentIterator interface
func (l *LevelIter) Clone() FragmentIterator {
	l2 := &LevelIter{
		logger:    l.logger,
		cmp:       l.cmp,
		lower:     append([]byte(nil), l.lower...),
		upper:     append([]byte(nil), l.upper...),
		level:     l.level,
		iter:      l.iter.Clone(),
		iterFile:  l.iterFile,
		newIter:   l.newIter,
		files:     l.files.Clone(),
		err:       l.err,
		span:      l.span,
		tableOpts: l.tableOpts,
	}
	return l2
}

func (l *LevelIter) findFileGE(key []byte) *manifest.FileMetadata {
	// Find the earliest file whose largest key is >= ikey.
	//
	// If the earliest file has its largest key == ikey and that largest key is a
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
	// Find the last file whose smallest key is < ikey.
	return l.files.SeekLT(l.cmp, key)
}

// Init the iteration bounds for the current table. Returns -1 if the table
// lies fully before the lower bound, +1 if the table lies fully after the
// upper bound, and 0 if the table overlaps the iteration bounds.
func (l *LevelIter) initTableBounds(f *manifest.FileMetadata) int {
	l.tableOpts.LowerBound = l.lower
	l.tableOpts.UpperBound = l.upper
	if l.tableOpts.LowerBound != nil {
		if l.cmp(f.Largest.UserKey, l.tableOpts.LowerBound) < 0 {
			// The largest key in the sstable is smaller than the lower bound.
			return -1
		}
		if l.cmp(l.tableOpts.LowerBound, f.Smallest.UserKey) <= 0 {
			// The lower bound is smaller or equal to the smallest key in the
			// table. Iteration within the table does not need to check the lower
			// bound.
			l.tableOpts.LowerBound = nil
		}
	}
	if l.tableOpts.UpperBound != nil {
		if l.cmp(f.Smallest.UserKey, l.tableOpts.UpperBound) >= 0 {
			// The smallest key in the sstable is greater than or equal to the upper
			// bound.
			return 1
		}
		if l.cmp(l.tableOpts.UpperBound, f.Largest.UserKey) > 0 {
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
	if l.iterFile == file {
		if l.err != nil {
			return noFileLoaded
		}
		if l.iter != nil {
			// We don't bother comparing the file bounds with the iteration bounds when we have
			// an already open iterator. It is possible that the iter may not be relevant given the
			// current iteration bounds, but it knows those bounds, so it will enforce them.
			return fileAlreadyLoaded
		}
		// We were already at file, but don't have an iterator, probably because the file was
		// beyond the iteration bounds. It may still be, but it is also possible that the bounds
		// have changed. We handle that below.
	}

	// Note that LevelIter.Close() can be called multiple times.
	if err := l.Close(); err != nil {
		return noFileLoaded
	}

	for {
		l.iterFile = file
		if file == nil {
			return noFileLoaded
		}
		if !file.HasRangeKeys {
			// Skip file with no range keys.
			switch {
			case dir > 0:
				file = l.files.Next()
			case dir < 0:
				file = l.files.Prev()
			}
		}

		switch l.initTableBounds(file) {
		case -1:
			// The largest key in the sstable is smaller than the lower bound.
			if dir < 0 {
				return noFileLoaded
			}
			file = l.files.Next()
			continue
		case +1:
			// The smallest key in the sstable is greater than or equal to the upper
			// bound.
			if dir > 0 {
				return noFileLoaded
			}
			file = l.files.Prev()
			continue
		}
		// case 0: The current file overlaps with the iteration bounds. We could
		// still have to truncate returned Spans based on bounds.

		var iter FragmentIterator
		iter, l.err = l.newIter(l.files.Current(), &l.tableOpts)
		if l.err != nil {
			return noFileLoaded
		}
		l.iter = iter
		return newFileLoaded
	}
}

// In race builds we verify that the keys returned by LevelIter lie within
// [lower,upper).
func (l *LevelIter) verify(key *base.InternalKey, val []byte) (*base.InternalKey, []byte) {
	if invariants.Enabled && key != nil {
		// Confirm that bounds checking is working.
		if l.lower != nil && l.cmp(key.UserKey, l.lower) < 0 {
			l.logger.Fatalf("LevelIter %s: lower bound violation: %s < %s\n%s", l.level, key, l.lower, debug.Stack())
		}
		// Note that we are checking l.span.End here, and not key. We expect that
		// the corresponding end key for this range key has already been populated
		// into l.span.
		if l.upper != nil && l.cmp(l.span.End, l.upper) > 0 {
			l.logger.Fatalf("LevelIter %s: upper bound violation: %s > %s\n%s", l.level, key, l.upper, debug.Stack())
		}
	}
	return key, val
}

// truncateLowerBound truncates l.span's start key according to the lower bound.
// Expects that l.span has already been populated with the current key.
func (l *LevelIter) truncateLowerBound(
	ikey *base.InternalKey, val []byte,
) (*base.InternalKey, []byte) {
	if l.cmp(l.tableOpts.LowerBound, l.span.End) >= 0 {
		// Completely past bound.
		//
		// TODO(bilal): Return a synthesized boundary key here, once this iter
		// is a bound iter.
		l.exhaustedBounds = true
		l.span = Span{}
		return nil, nil
	}
	if l.cmp(ikey.UserKey, l.tableOpts.LowerBound) < 0 {
		// Truncate the start key at the lower bound.
		ikey.UserKey = l.tableOpts.LowerBound
		l.span.Start = *ikey
	}
	return ikey, val
}

// truncateUpperBound truncates l.span's end key according to the lower bound.
// Expects that l.span has already been populated with the current key.
func (l *LevelIter) truncateUpperBound(
	ikey *base.InternalKey, val []byte,
) (*base.InternalKey, []byte) {
	if l.cmp(ikey.UserKey, l.tableOpts.UpperBound) >= 0 {
		// Past bound.
		//
		// TODO(bilal): Return a synthesized boundary key here.
		l.exhaustedBounds = true
		l.span = Span{}
		return nil, nil
	}
	if l.cmp(l.span.End, l.tableOpts.UpperBound) > 0 {
		// Truncate the end key at the upper bound.
		l.span.End = l.tableOpts.UpperBound
		// We do not update the returned val with the new end key. This is okay as
		// all range key uses of FragmentIterator ignore the returned value, and
		// instead call into End() or Current() to get the desired end key.
	}
	return ikey, val
}

// SeekGE implements keyspan.FragmentIterator.
func (l *LevelIter) SeekGE(key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	loadFileIndicator := l.loadFile(l.findFileGE(key), +1)
	if loadFileIndicator == noFileLoaded {
		return nil, nil
	}
	if key, val := l.iter.SeekGE(key, false /* trySeekUsingNext */); key != nil {
		l.span = l.iter.Current()
		if l.tableOpts.LowerBound != nil {
			key, val = l.truncateLowerBound(key, val)
		}
		if l.tableOpts.UpperBound != nil {
			key, val = l.truncateUpperBound(key, val)
		}
		return l.verify(key, val)
	}
	return l.verify(l.skipEmptyFileForward())
}

// SeekPrefixGE implements keyspan.FragmentIterator.
func (l *LevelIter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	// This should never be called as prefix iteration is only done for point
	// records.
	panic("pebble: SeekPrefixGE unimplemented")
}

// SeekLT implements keyspan.FragmentIterator.
func (l *LevelIter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.UpperBound.
	if l.loadFile(l.findFileLT(key), -1) == noFileLoaded {
		return nil, nil
	}
	if key, val := l.iter.SeekLT(key); key != nil {
		l.span = l.iter.Current()
		if l.tableOpts.LowerBound != nil {
			key, val = l.truncateLowerBound(key, val)
		}
		if l.tableOpts.UpperBound != nil {
			key, val = l.truncateUpperBound(key, val)
		}
		return l.verify(key, val)
	}
	return l.verify(l.skipEmptyFileBackward())
}

// First implements keyspan.FragmentIterator.
func (l *LevelIter) First() (*base.InternalKey, []byte) {
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	if l.loadFile(l.files.First(), +1) == noFileLoaded {
		return nil, nil
	}
	if key, val := l.iter.First(); key != nil {
		l.span = l.iter.Current()
		// We only need to check for the upper bound here, as the toplevel Iter
		// should have turned a First call into a SeekGE if we had a lower bound
		if l.tableOpts.UpperBound != nil {
			key, val = l.truncateUpperBound(key, val)
		}
		return l.verify(key, val)
	}
	return l.verify(l.skipEmptyFileForward())
}

// Last implements keyspan.FragmentIterator.
func (l *LevelIter) Last() (*base.InternalKey, []byte) {
	l.err = nil // clear cached iteration error
	l.exhaustedBounds = false

	if l.loadFile(l.files.Last(), -1) == noFileLoaded {
		return nil, nil
	}
	if key, val := l.iter.Last(); key != nil {
		l.span = l.iter.Current()
		if l.tableOpts.LowerBound != nil {
			key, val = l.truncateLowerBound(key, val)
		}
		if l.tableOpts.UpperBound != nil {
			key, val = l.truncateUpperBound(key, val)
		}
		return l.verify(key, val)
	}
	return l.verify(l.skipEmptyFileBackward())
}

// Next implements keyspan.FragmentIterator.
func (l *LevelIter) Next() (*base.InternalKey, []byte) {
	if l.err != nil || l.iter == nil {
		return nil, nil
	}
	l.exhaustedBounds = false

	if key, val := l.iter.Next(); key != nil {
		l.span = l.iter.Current()
		if l.tableOpts.LowerBound != nil {
			key, val = l.truncateLowerBound(key, val)
		}
		if l.tableOpts.UpperBound != nil {
			key, val = l.truncateUpperBound(key, val)
		}
		return l.verify(key, val)
	}
	return l.verify(l.skipEmptyFileForward())
}

// Prev implements keyspan.FragmentIterator.
func (l *LevelIter) Prev() (*base.InternalKey, []byte) {
	if l.err != nil || l.iter == nil {
		return nil, nil
	}
	l.exhaustedBounds = false

	if key, val := l.iter.Prev(); key != nil {
		l.span = l.iter.Current()
		if l.tableOpts.LowerBound != nil {
			key, val = l.truncateLowerBound(key, val)
		}
		if l.tableOpts.UpperBound != nil {
			key, val = l.truncateUpperBound(key, val)
		}
		return l.verify(key, val)
	}
	return l.verify(l.skipEmptyFileBackward())
}

func (l *LevelIter) skipEmptyFileForward() (*base.InternalKey, []byte) {
	var key *base.InternalKey
	var val []byte
	for ; key == nil; key, val = l.iter.First() {
		// Current file was exhausted. Move to the next file.
		if l.loadFile(l.files.Next(), +1) == noFileLoaded {
			return nil, nil
		}
	}
	l.span = l.iter.Current()
	if l.tableOpts.UpperBound != nil {
		key, val = l.truncateUpperBound(key, val)
	}
	return key, val
}

func (l *LevelIter) skipEmptyFileBackward() (*base.InternalKey, []byte) {
	var key *base.InternalKey
	var val []byte
	for ; key == nil; key, val = l.iter.Last() {
		// Current file was exhausted. Move to the previous file.
		if l.loadFile(l.files.Prev(), -1) == noFileLoaded {
			return nil, nil
		}
	}
	l.span = l.iter.Current()
	if l.tableOpts.LowerBound != nil {
		key, val = l.truncateLowerBound(key, val)
	}
	return key, val
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
	// TODO(bilal): Once fragmentBlockIter has been refactored to only deal with
	// range keys and not rangedels, update it to do span truncation itself
	// instead of doing it in LevelIter.
	l.span = Span{}
}

// String implements keyspan.FragmentIterator.
func (l *LevelIter) String() string {
	if l.iterFile != nil {
		return fmt.Sprintf("%s: fileNum=%s", l.level, l.iter.String())
	}
	return fmt.Sprintf("%s: fileNum=<nil>", l.level)
}

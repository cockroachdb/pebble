// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
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

	// The options that were passed in.
	tableOpts RangeIterOptions

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

	l.iterFile = file
	if file == nil {
		return noFileLoaded
	}
	if indicator != fileAlreadyLoaded {
		l.iter, l.err = l.newIter(l.files.Current(), &l.tableOpts)
		indicator = newFileLoaded
	}
	if l.err != nil {
		return noFileLoaded
	}
	return indicator
}

// SeekGE implements keyspan.FragmentIterator.
func (l *LevelIter) SeekGE(key []byte) Span {
	l.dir = +1
	l.err = nil // clear cached iteration error

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
		return l.straddle
	}
	loadFileIndicator := l.loadFile(f, +1)
	if loadFileIndicator == noFileLoaded {
		return Span{}
	}
	if span := l.iter.SeekGE(key); span.Valid() {
		return span
	}
	return l.skipEmptyFileForward()
}

// SeekLT implements keyspan.FragmentIterator.
func (l *LevelIter) SeekLT(key []byte) Span {
	l.dir = -1
	l.err = nil // clear cached iteration error

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
		return l.straddle
	}
	if l.loadFile(l.findFileLT(key), -1) == noFileLoaded {
		return Span{}
	}
	if span := l.iter.SeekLT(key); span.Valid() {
		return span
	}
	return l.skipEmptyFileBackward()
}

// First implements keyspan.FragmentIterator.
func (l *LevelIter) First() Span {
	l.dir = +1
	l.err = nil // clear cached iteration error

	if l.loadFile(l.files.First(), +1) == noFileLoaded {
		return Span{}
	}
	if span := l.iter.First(); span.Valid() {
		return span
	}
	return l.skipEmptyFileForward()
}

// Last implements keyspan.FragmentIterator.
func (l *LevelIter) Last() Span {
	l.dir = -1
	l.err = nil // clear cached iteration error

	if l.loadFile(l.files.Last(), -1) == noFileLoaded {
		return Span{}
	}
	if span := l.iter.Last(); span.Valid() {
		return span
	}
	return l.skipEmptyFileBackward()
}

// Next implements keyspan.FragmentIterator.
func (l *LevelIter) Next() Span {
	l.dir = +1
	if l.err != nil || (l.iter == nil && l.iterFile == nil) {
		return Span{}
	}

	if l.iter != nil {
		if span := l.iter.Next(); span.Valid() {
			return span
		}
	}
	return l.skipEmptyFileForward()
}

// Prev implements keyspan.FragmentIterator.
func (l *LevelIter) Prev() Span {
	l.dir = -1
	if l.err != nil || (l.iter == nil && l.iterFile == nil) {
		return Span{}
	}

	if l.iter != nil {
		if span := l.iter.Prev(); span.Valid() {
			return span
		}
	}
	return l.skipEmptyFileBackward()
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
			return l.straddle
		}
	} else if l.straddle.Valid() && l.straddleDir < 0 {
		// We were at a straddle key, but are now changing directions. l.iterFile
		// was already moved backward by skipEmptyFileBackward, so advance it
		// forward.
		l.iterFile = l.files.Next()
	}
	l.straddle = Span{}
	if l.loadFile(l.iterFile, +1) == noFileLoaded {
		return Span{}
	}
	return l.iter.First()
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
			return l.straddle
		}
	} else if l.straddle.Valid() && l.straddleDir > 0 {
		// We were at a straddle key, but are now changing directions. l.iterFile
		// was already advanced forward by skipEmptyFileForward, so move it
		// backward.
		l.iterFile = l.files.Prev()
	}
	l.straddle = Span{}
	if l.loadFile(l.iterFile, -1) == noFileLoaded {
		return Span{}
	}
	return l.iter.Last()
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

// String implements keyspan.FragmentIterator.
func (l *LevelIter) String() string {
	if l.iterFile != nil {
		return fmt.Sprintf("%s: fileNum=%s", l.level, l.iterFile.FileNum)
	}
	return fmt.Sprintf("%s: fileNum=<nil>", l.level)
}

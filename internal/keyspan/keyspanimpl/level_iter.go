// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspanimpl

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/invariants"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/manifest"
	"github.com/cockroachdb/pebble/v2/internal/treeprinter"
)

// TableNewSpanIter creates a new iterator for range key spans for the given
// file.
type TableNewSpanIter func(
	ctx context.Context, file *manifest.TableMetadata, iterOptions keyspan.SpanIterOptions,
) (keyspan.FragmentIterator, error)

// LevelIter provides a merged view of spans from sstables in an L1+ level or an
// L0 sublevel.
//
// LevelIter takes advantage of level invariants to only have one sstable span
// block open at one time, opened using the newIter function passed in.
//
// A LevelIter is configured with a key type that is either KeyTypePoint
// (corresponding to range dels) or KeyTypeRange (corresponding to range keys).
// The key type decides which bounds we use for the files (and which files we
// filter out).
//
// LevelIter supports emitting "straddling spans": these are empty spans that
// cover the gaps between the keyspaces of adjacent files. This is an
// optimization to avoid unnecessarily loading files in cases where spans are
// very sparse (in the context of merging spans from multiple levels). We
// currently produce straddling spans only in range key mode.
//
// TODO(radu): investigate enabling straddling spans for rangedel mode.
type LevelIter struct {
	cmp     base.Compare
	keyType manifest.KeyType
	// The LSM level this LevelIter is initialized for.
	level manifest.Layer
	// newIter creates a range del iterator if keyType is KeyTypePoint or a range
	// key iterator if keyType is KeyTypeRange.
	newIter TableNewSpanIter
	// ctx is passed to TableNewSpanIter.
	ctx context.Context

	// The options that were passed in.
	tableOpts keyspan.SpanIterOptions

	files manifest.LevelIterator

	// file always corresponds to the current position of LevelIter.files.
	file *manifest.TableMetadata
	pos  levelIterPos
	// fileIter is the iterator for LevelIter.file when pos is atFile; it is nil
	// otherwise.
	fileIter keyspan.FragmentIterator

	// lastIter retains the last opened iterator, in case that the next time we
	// need an iterator it is for the same file. When fileIter is not nil,
	// fileIter is the same with lastIter and file is the same with lastIterFile.
	lastIter     keyspan.FragmentIterator
	lastIterFile *manifest.TableMetadata

	wrapFn       keyspan.WrapFn
	straddleSpan keyspan.Span

	// TODO(bilal): Add InternalIteratorStats.
}

// LevelIter implements the keyspan.FragmentIterator interface.
var _ keyspan.FragmentIterator = (*LevelIter)(nil)

// NewLevelIter returns a LevelIter.
//
// newIter must create a range del iterator for the given file if keyType is
// KeyTypePoint or a range key iterator if keyType is KeyTypeRange.
func NewLevelIter(
	ctx context.Context,
	opts keyspan.SpanIterOptions,
	cmp base.Compare,
	newIter TableNewSpanIter,
	files manifest.LevelIterator,
	level manifest.Layer,
	keyType manifest.KeyType,
) *LevelIter {
	l := &LevelIter{}
	l.Init(ctx, opts, cmp, newIter, files, level, keyType)
	return l
}

// Init initializes a LevelIter.
//
// newIter must create a range del iterator for the given file if keyType is
// KeyTypePoint or a range key iterator if keyType is KeyTypeRange.
func (l *LevelIter) Init(
	ctx context.Context,
	opts keyspan.SpanIterOptions,
	cmp base.Compare,
	newIter TableNewSpanIter,
	files manifest.LevelIterator,
	level manifest.Layer,
	keyType manifest.KeyType,
) {
	if keyType != manifest.KeyTypePoint && keyType != manifest.KeyTypeRange {
		panic("keyType must be point or range")
	}
	*l = LevelIter{
		cmp:       cmp,
		keyType:   keyType,
		level:     level,
		newIter:   newIter,
		ctx:       ctx,
		tableOpts: opts,
		files:     files.Filter(keyType),
	}
	l.setPosAfterFile(nil)
}

// levelIterPos narrows down the position of the iterator in relation to the file:
//
//   - atFile: the iterator is currently positioned inside LevelIter.file.
//
//   - beforeFile: the iterator is currently positioned right before
//     LevelIter.file. If .file is not the first file, this position corresponds
//     to a straddle span.
//
//   - afterFile: the iterator is currently positioned right after
//     LevelIter.file. If .file is not the last file, this position corresponds
//     to a straddle span.
//
// Example:
//
//	              beforeFile    atFile   afterFile
//	                       |      |      |
//	                       v      v      v
//	..--- .files.Prev() ------- .file ------- .files.Next() ---...
//
// Note that each straddle position can be represented in two different ways
// (either after one file, or before the other file). We use the one which makes
// it easier to keep l.file in sync with the l.files iterator (which depends on
// the iteration direction).
//
// When file is nil, it should be considered a sentinel either before or after
// all the files.  When file is nil and pos is afterFile, we are positioned
// after the imaginary start sentinel, i.e. before the first file:
//
//		         afterFile
//		              |
//		              v
//		.file=nil ------- .files.First() ---...
//
//	 When file is nil and pos is beforeFile, we are positioned after the
//	 imaginary end sentinel, i.e. after the last file:
//
//
//		             beforeFile
//		                 |
//		                 v
//		...--- .files.Last() ------- .file=nil
//
// Note that when straddle spans are not emitted, the position is always
// `atFile` unless the iterator is exhausted.
type levelIterPos uint8

const (
	atFile levelIterPos = iota
	beforeFile
	afterFile
)

// SeekGE implements keyspan.FragmentIterator.
func (l *LevelIter) SeekGE(key []byte) (*keyspan.Span, error) {
	file := l.files.SeekGE(l.cmp, key)
	if file == nil {
		l.setPosBeforeFile(nil)
		return nil, nil
	}
	if l.straddleSpansEnabled() && l.cmp(key, file.RangeKeyBounds.SmallestUserKey()) < 0 {
		// Peek at the previous file.
		if prevFile := l.files.Prev(); prevFile != nil {
			// We could unconditionally return an empty span between the seek
			// key and f.RangeKeyBounds.Smallest(), however if this span is to
			// the left of all range keys on this level, it could lead to
			// inconsistent behaviour in relative positioning operations.
			// Consider this example, with a b-c range key:
			//   SeekGE(a) -> a-b:{}
			//   Next() -> b-c{(#5,RANGEKEYSET,@4,foo)}
			//   Prev() -> nil
			// Iterators higher up in the iterator stack rely on this sort
			// of relative positioning consistency.
			//
			// TODO(bilal): Investigate ways to be able to return straddle spans in
			// cases similar to the above, while still retaining correctness.
			// Return a straddling key instead of loading the file.
			l.setPosAfterFile(prevFile)
			return l.makeStraddleSpan(prevFile, file), nil
		}
		// Return the iterator to file.
		l.files.Next()
	}

	if err := l.setPosAtFile(file); err != nil {
		return nil, err
	}
	if span, err := l.fileIter.SeekGE(key); span != nil || err != nil {
		return span, err
	}
	return l.moveToNextFile()
}

// SeekLT implements keyspan.FragmentIterator.
func (l *LevelIter) SeekLT(key []byte) (*keyspan.Span, error) {
	file := l.files.SeekLT(l.cmp, key)
	if file == nil {
		l.setPosAfterFile(nil)
		return nil, nil
	}
	if l.straddleSpansEnabled() && l.cmp(file.RangeKeyBounds.LargestUserKey(), key) < 0 {
		// Peek at the next file.
		if nextFile := l.files.Next(); nextFile != nil {
			// We could unconditionally return an empty span between f.LargestRangeKey
			// and the seek key, however if this span is to the right of all range keys
			// on this level, it could lead to inconsistent behaviour in relative
			// positioning operations. Consider this example, with a b-c range key:
			//   SeekLT(d) -> c-d:{}
			//   Prev() -> b-c{(#5,RANGEKEYSET,@4,foo)}
			//   Next() -> nil
			// Iterators higher up in the iterator stack rely on this sort of relative
			// positioning consistency.
			//
			// TODO(bilal): Investigate ways to be able to return straddle spans in
			// cases similar to the above, while still retaining correctness.
			// Return a straddling key instead of loading the file.
			l.setPosBeforeFile(nextFile)
			return l.makeStraddleSpan(file, nextFile), nil
		}
		// Return the iterator to file.
		l.files.Prev()
	}
	if err := l.setPosAtFile(file); err != nil {
		return nil, err
	}
	if span, err := l.fileIter.SeekLT(key); span != nil || err != nil {
		return span, err
	}
	return l.moveToPrevFile()
}

// First implements keyspan.FragmentIterator.
func (l *LevelIter) First() (*keyspan.Span, error) {
	file := l.files.First()
	if file == nil {
		l.setPosBeforeFile(nil)
		return nil, nil
	}
	if err := l.setPosAtFile(file); err != nil {
		return nil, err
	}
	if span, err := l.fileIter.First(); span != nil || err != nil {
		return span, err
	}
	return l.moveToNextFile()
}

// Last implements keyspan.FragmentIterator.
func (l *LevelIter) Last() (*keyspan.Span, error) {
	file := l.files.Last()
	if file == nil {
		l.setPosAfterFile(nil)
		return nil, nil
	}
	if err := l.setPosAtFile(file); err != nil {
		return nil, err
	}
	if span, err := l.fileIter.Last(); span != nil || err != nil {
		return span, err
	}
	return l.moveToPrevFile()
}

// Next implements keyspan.FragmentIterator.
func (l *LevelIter) Next() (*keyspan.Span, error) {
	if l.file == nil {
		if l.pos == afterFile {
			return l.First()
		}
		// Iterator is exhausted.
		return nil, nil
	}
	switch l.pos {
	case atFile:
		if span, err := l.fileIter.Next(); span != nil || err != nil {
			return span, err
		}
	case beforeFile:
		// We were positioned on a straddle span before l.file; now we can advance to the file.
		if err := l.setPosAtFile(l.file); err != nil {
			return nil, err
		}
		if span, err := l.fileIter.First(); span != nil || err != nil {
			return span, err
		}
	case afterFile:
		// We were positioned on a straddle span after l.file. Move to the next file.
	}
	return l.moveToNextFile()
}

// Prev implements keyspan.FragmentIterator.
func (l *LevelIter) Prev() (*keyspan.Span, error) {
	if l.file == nil {
		if l.pos == beforeFile {
			return l.Last()
		}
		// Iterator is exhausted.
		return nil, nil
	}
	switch l.pos {
	case atFile:
		if span, err := l.fileIter.Prev(); span != nil || err != nil {
			return span, err
		}
	case afterFile:
		// We were positioned on a straddle span after l.file; now we can advance
		// (backwards) to the file.
		if err := l.setPosAtFile(l.file); err != nil {
			return nil, err
		}
		if span, err := l.fileIter.Last(); span != nil || err != nil {
			return span, err
		}
	case beforeFile:
		// We were positioned on a straddle span before l.file. Move to the previous file.
	}
	return l.moveToPrevFile()
}

func (l *LevelIter) moveToNextFile() (*keyspan.Span, error) {
	if invariants.Enabled && l.pos == beforeFile {
		panic("moveToNextFile with beforeFile pos")
	}
	for {
		nextFile := l.files.Next()
		if nextFile == nil {
			l.setPosBeforeFile(nil)
			return nil, nil
		}
		// Emit a straddle span, if necessary.
		if l.pos == atFile && nextFile != nil && l.needStraddleSpan(l.file, nextFile) {
			span := l.makeStraddleSpan(l.file, nextFile)
			l.setPosBeforeFile(nextFile)
			return span, nil
		}
		if err := l.setPosAtFile(nextFile); err != nil {
			return nil, err
		}
		if span, err := l.fileIter.First(); span != nil || err != nil {
			return span, err
		}
		// The file had no spans; continue.
	}
}

func (l *LevelIter) moveToPrevFile() (*keyspan.Span, error) {
	if invariants.Enabled && l.pos == afterFile {
		panic("eofBackward with afterFile pos")
	}
	for {
		prevFile := l.files.Prev()
		if prevFile == nil {
			l.setPosAfterFile(nil)
			return nil, nil
		}
		// Emit a straddle span, if necessary.
		if l.pos == atFile && l.file != nil && l.needStraddleSpan(prevFile, l.file) {
			span := l.makeStraddleSpan(prevFile, l.file)
			l.setPosAfterFile(prevFile)
			return span, nil
		}
		if err := l.setPosAtFile(prevFile); err != nil {
			return nil, err
		}
		if span, err := l.fileIter.Last(); span != nil || err != nil {
			return span, err
		}
		// The file had no spans; continue.
	}
}

// SetContext is part of the FragmentIterator interface.
func (l *LevelIter) SetContext(ctx context.Context) {
	l.ctx = ctx
	if l.lastIter != nil {
		l.lastIter.SetContext(ctx)
	}
}

// Close implements keyspan.FragmentIterator.
func (l *LevelIter) Close() {
	l.file = nil
	l.fileIter = nil
	if l.lastIter != nil {
		l.lastIter.Close()
		l.lastIter = nil
		l.lastIterFile = nil
	}
}

// String implements keyspan.FragmentIterator.
func (l *LevelIter) String() string {
	if l.file != nil {
		return fmt.Sprintf("%s: fileNum=%s", l.level, l.file.TableNum)
	}
	return fmt.Sprintf("%s: fileNum=<nil>", l.level)
}

// WrapChildren implements FragmentIterator.
func (l *LevelIter) WrapChildren(wrap keyspan.WrapFn) {
	if l.fileIter != nil {
		l.fileIter = wrap(l.fileIter)
	}
	l.wrapFn = wrap
}

// DebugTree is part of the FragmentIterator interface.
func (l *LevelIter) DebugTree(tp treeprinter.Node) {
	n := tp.Childf("%T(%p) %s", l, l, l.level)
	if l.fileIter != nil {
		l.fileIter.DebugTree(n)
	}
}

func (l *LevelIter) setPosBeforeFile(f *manifest.TableMetadata) {
	l.setPosInternal(f, beforeFile)
}

func (l *LevelIter) setPosAfterFile(f *manifest.TableMetadata) {
	l.setPosInternal(f, afterFile)
}

// setPosAtFile sets the current position and opens an iterator for the file (if
// necessary).
func (l *LevelIter) setPosAtFile(f *manifest.TableMetadata) error {
	l.setPosInternal(f, atFile)
	// See if the last iterator was for the same file; if not, close it and open a
	// new one.
	if l.lastIter == nil || l.lastIterFile != f {
		if l.lastIter != nil {
			l.lastIter.Close()
			l.lastIter = nil
			l.lastIterFile = nil
		}
		iter, err := l.newIter(l.ctx, l.file, l.tableOpts)
		if err != nil {
			return err
		}
		iter = keyspan.MaybeAssert(iter, l.cmp)
		if l.wrapFn != nil {
			iter = l.wrapFn(iter)
		}
		l.lastIter = iter
		l.lastIterFile = f
	}
	l.fileIter = l.lastIter
	return nil
}

// setPos sets l.file and l.pos (and closes the iteris for the new file).
func (l *LevelIter) setPosInternal(f *manifest.TableMetadata, pos levelIterPos) {
	l.file = f
	l.fileIter = nil
	l.pos = pos
}

func (l *LevelIter) straddleSpansEnabled() bool {
	return l.keyType == manifest.KeyTypeRange
}

// needStraddleSpan returns true if straddle spans are enabled and there is a
// gap between the bounds of the files. file and nextFile are assumed to be
// consecutive files in the level, in the order they appear in the level.
func (l *LevelIter) needStraddleSpan(file, nextFile *manifest.TableMetadata) bool {
	// We directly use range key bounds because that is the current condition for
	// straddleSpansEnabled.
	return l.straddleSpansEnabled() && l.cmp(file.RangeKeyBounds.LargestUserKey(), nextFile.RangeKeyBounds.SmallestUserKey()) < 0
}

// makeStraddleSpan returns a straddle span that covers the gap between file and
// nextFile.
func (l *LevelIter) makeStraddleSpan(file, nextFile *manifest.TableMetadata) *keyspan.Span {
	l.straddleSpan = keyspan.Span{
		Start: file.RangeKeyBounds.LargestUserKey(),
		End:   nextFile.RangeKeyBounds.SmallestUserKey(),
		Keys:  nil,
	}
	return &l.straddleSpan
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import "sort"

// tableNewIters creates a new point and range-del iterator for the given file
// number. If bytesIterated is specified, it is incremented as the given file is
// iterated through.
type tableNewIters func(
	meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
) (internalIterator, internalIterator, error)

var sentinelUpperBound = make([]byte, 0)

// levelIter provides a merged view of the sstables in a level.
//
// levelIter is used during compaction and as part of the Iterator
// implementation. When used as part of the Iterator implementation, level
// iteration needs to "pause" at sstable boundaries if a range deletion
// tombstone is the source of that boundary. We know if a range tombstone is
// the smallest or largest key in a file because the kind will be
// InternalKeyKindRangeDeletion. If the boundary key is a range deletion
// tombstone, we materialize a fake entry to return from levelIter. This
// prevents mergingIter from advancing past the sstable until the sstable
// contains the smallest (or largest for reverse iteration) key in the merged
// heap. Note that Iterator treat a range deletion tombstone as a no-op and
// processes range deletions via mergingIter.
//
// SeekPrefixGE presents the need for a second type of pausing. If an sstable
// iterator returns "not found" for a SeekPrefixGE operation, we don't want to
// advance to the next sstable as the "not found" does not indicate that all of
// the keys in the sstable are less than the search key. Advancing to the next
// sstable would cause us to skip over range tombstones, violating
// correctness. Instead, SeekPrefixGE creates a synthetic boundary key with the
// kind InternalKeyKindRangeDeletion which will be used to pause the levelIter
// at the sstable until the mergingIter is ready to advance past it.
type levelIter struct {
	opts      *IterOptions
	tableOpts IterOptions
	cmp       Compare
	// The current file wrt the iterator position.
	index int
	// The key to return when iterating past an sstable boundary and that
	// boundary is a range deletion tombstone.
	boundary *InternalKey
	// A synthetic boundary key to return when SeekPrefixGE finds an sstable
	// which doesn't contain the search key, but which does contain range
	// tombstones.
	syntheticBoundary InternalKey
	// The iter for the current index. It is nil under any of the following conditions:
	// - index < 0 or index > len(files)
	// - err != nil
	// - some other constraint, like the bounds in opts, caused the file at index to not
	//   be relevant to the iteration.
	iter         internalIterator
	newIters     tableNewIters
	rangeDelIter *internalIterator
	files        []fileMetadata
	err          error

	// Pointer into this level's entry in `mergingIterLevel::smallestUserKey,largestUserKey`.
	// We populate it with the corresponding bounds for the currently opened file. It is used for
	// two purposes (described for forward iteration. The explanation for backward iteration is
	// similar.)
	// - To limit the optimization that seeks lower-level iterators past keys shadowed by a range
	//   tombstone. Limiting this seek to the file largestUserKey is necessary since
	//   range tombstones are stored untruncated, while they only apply to keys within their
	//   containing file's boundaries. For a detailed example, see comment above `mergingIter`.
	// - To constrain the tombstone to act-within the bounds of the sstable when checking
	//   containment. For forward iteration we need the smallestUserKey.
	//
	// An example is sstable bounds [c#8, g#12] containing a tombstone [b, i)#7.
	// - When doing a SeekGE to user key X, the levelIter is at this sstable because X is either within
	//   the sstable bounds or earlier than the start of the sstable (and there is no sstable in
	//   between at this level). If X >= smallestUserKey, and the tombstone [b, i) contains X,
	//   it is correct to SeekGE the sstables at lower levels to min(g, i) (i.e., min of
	//   largestUserKey, tombstone.End) since any user key preceding min(g, i) must be covered by this
	//   tombstone (since it cannot have a version younger than this tombstone as it is at a lower
	//   level). And even if X = smallestUserKey or equal to the start user key of the tombstone,
	//   if the above conditions are satisfied we know that the internal keys corresponding to X at
	//   lower levels must have a version smaller than that in this file (again because of the level
	//   argument). So we don't need to use sequence numbers for this comparison.
	// - When checking whether this tombstone deletes internal key X we know that the levelIter is at this
	//   sstable so (repeating the above) X.UserKey is either within the sstable bounds or earlier than the
	//   start of the sstable (and there is no sstable in between at this level).
	//   - X is at at a lower level. If X.UserKey >= smallestUserKey, and the tombstone contains
	//     X.UserKey, we know X is deleted. This argument also works when X is a user key (we use
	//     it when seeking to test whether a user key is deleted).
	//   - X is at the same level. X must be within the sstable bounds of the tombstone so the
	//     X.UserKey >= smallestUserKey comparison is trivially true. In addition to the tombstone containing
	//     X we need to compare the sequence number of X and the tombstone (we don't need to look
	//     at how this tombstone is truncated to act-within the file bounds, which are InternalKeys,
	//     since X and the tombstone are from the same file).
	//
	// Iterating backwards has one more complication when checking whether a tombstone deletes
	// internal key X at a lower level (the construction we do here also works for a user key X).
	// Consider sstable bounds [c#8, g#InternalRangeDelSentinel] containing a tombstone [b, i)#7.
	// If we are positioned at key g#10 at a lower sstable, the tombstone we will see is [b, i)#7,
	// since the higher sstable is positioned at a key <= g#10. We should not use this tombstone
	// to delete g#10. This requires knowing that the largestUserKey is a range delete sentinel,
	// which we set in a separate bool below.
	//
	// These fields differs from the `boundary` field in a few ways:
	// - `boundary` is only populated when the iterator is positioned exactly on the sentinel key.
	// - `boundary` can hold either the lower- or upper-bound, depending on the iterator direction.
	// - `boundary` is not exposed to the next higher-level iterator, i.e., `mergingIter`.
	smallestUserKey, largestUserKey  *[]byte
	isLargestUserKeyRangeDelSentinel *bool

	// bytesIterated keeps track of the number of bytes iterated during compaction.
	bytesIterated *uint64
}

// levelIter implements the internalIterator interface.
var _ internalIterator = (*levelIter)(nil)

func newLevelIter(
	opts *IterOptions,
	cmp Compare,
	newIters tableNewIters,
	files []fileMetadata,
	bytesIterated *uint64,
) *levelIter {
	l := &levelIter{}
	l.init(opts, cmp, newIters, files, bytesIterated)
	return l
}

func (l *levelIter) init(
	opts *IterOptions,
	cmp Compare,
	newIters tableNewIters,
	files []fileMetadata,
	bytesIterated *uint64,
) {
	l.opts = opts
	if l.opts != nil {
		l.tableOpts.TableFilter = l.opts.TableFilter
	}
	l.cmp = cmp
	l.index = -1
	l.newIters = newIters
	l.files = files
	l.bytesIterated = bytesIterated
}

func (l *levelIter) initRangeDel(rangeDelIter *internalIterator) {
	l.rangeDelIter = rangeDelIter
}

func (l *levelIter) initSmallestLargestUserKey(
	smallestUserKey, largestUserKey *[]byte, isLargestUserKeyRangeDelSentinel *bool,
) {
	l.smallestUserKey = smallestUserKey
	l.largestUserKey = largestUserKey
	l.isLargestUserKeyRangeDelSentinel = isLargestUserKeyRangeDelSentinel
}

func (l *levelIter) findFileGE(key []byte) int {
	// Find the earliest file whose largest key is >= ikey.
	//
	// If the earliest file has its largest key == ikey and that largest key is a
	// range deletion sentinel, we know that we manufactured this sentinel to convert
	// the exclusive range deletion end key into an inclusive key (reminder: [start, end)#seqnum
	// is the form of a range deletion sentinel which can contribute a largest key = end#sentinel).
	// In this case we don't return this as the earliest file since there is nothing actually
	// equal to key in it.
	//
	// Additionally, this prevents loading untruncated range deletions from a table which can't
	// possibly contain the target key and is required for correctness by mergingIter.SeekGE
	// (see the comment in that function).
	//
	// TODO(peter): inline the binary search.
	return sort.Search(len(l.files), func(i int) bool {
		largest := &l.files[i].Largest
		c := l.cmp(largest.UserKey, key)
		if c > 0 {
			return true
		}
		return c == 0 && largest.Trailer != InternalKeyRangeDeleteSentinel
	})
}

func (l *levelIter) findFileLT(key []byte) int {
	// Find the last file whose smallest key is < ikey.
	index := sort.Search(len(l.files), func(i int) bool {
		return l.cmp(l.files[i].Smallest.UserKey, key) >= 0
	})
	return index - 1
}

func (l *levelIter) loadFile(index, dir int) bool {
	l.boundary = nil
	if l.index == index {
		if l.err != nil {
			return false
		}
		if l.iter != nil {
			// We don't bother comparing the file bounds with the iteration bounds when we have
			// an already open iterator. It is possible that the iter may not be relevant given the
			// current iteration bounds, but it knows those bounds, so it will enforce them.
			return true
		}
		// We were already at index, but don't have an iterator, probably because the file was
		// beyond the iteration bounds. It may still be, but it is also possible that the bounds
		// have changed. We handle that below.
	}
	// Close both iter and rangeDelIter. While mergingIter knows about
	// rangeDelIter, it can't call Close() on it because it does not know when
	// the levelIter will switch it.
	if l.iter != nil {
		l.err = l.iter.Close()
		l.iter = nil
	}
	if l.rangeDelIter != nil && *l.rangeDelIter != nil {
		l.err = firstError(l.err, (*l.rangeDelIter).Close())
		*l.rangeDelIter = nil
	}
	if l.err != nil {
		return false
	}

	for ; ; index += dir {
		l.index = index
		if l.index < 0 || l.index >= len(l.files) {
			return false
		}

		f := &l.files[l.index]
		l.tableOpts.LowerBound = l.opts.GetLowerBound()
		if l.tableOpts.LowerBound != nil {
			if l.cmp(f.Largest.UserKey, l.tableOpts.LowerBound) < 0 {
				// The largest key in the sstable is smaller than the lower bound.
				if dir < 0 {
					return false
				}
				continue
			}
			if l.cmp(l.tableOpts.LowerBound, f.Smallest.UserKey) <= 0 {
				// The lower bound is smaller or equal to the smallest key in the
				// table. Iteration within the table does not need to check the lower
				// bound.
				l.tableOpts.LowerBound = nil
			}
		}
		l.tableOpts.UpperBound = l.opts.GetUpperBound()
		if l.tableOpts.UpperBound != nil {
			if l.cmp(f.Smallest.UserKey, l.tableOpts.UpperBound) >= 0 {
				// The smallest key in the sstable is greater than or equal to the
				// lower bound.
				if dir > 0 {
					return false
				}
				continue
			}
			if l.cmp(l.tableOpts.UpperBound, f.Largest.UserKey) > 0 {
				// The upper bound is greater than the largest key in the
				// table. Iteration within the table does not need to check the upper
				// bound. NB: tableOpts.UpperBound is exclusive and f.Largest is inclusive.
				l.tableOpts.UpperBound = nil
			}
		}

		var rangeDelIter internalIterator
		l.iter, rangeDelIter, l.err = l.newIters(f, &l.tableOpts, l.bytesIterated)
		if l.err != nil || l.iter == nil {
			return false
		}
		if l.rangeDelIter != nil {
			*l.rangeDelIter = rangeDelIter
		}
		if l.smallestUserKey != nil {
			*l.smallestUserKey = f.Smallest.UserKey
		}
		if l.largestUserKey != nil {
			*l.largestUserKey = f.Largest.UserKey
		}
		if l.isLargestUserKeyRangeDelSentinel != nil {
			*l.isLargestUserKeyRangeDelSentinel = f.Largest.Trailer == InternalKeyRangeDeleteSentinel
		}
		return true
	}
}

func (l *levelIter) SeekGE(key []byte) (*InternalKey, []byte) {
	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.LowerBound.
	if !l.loadFile(l.findFileGE(key), 1) {
		return nil, nil
	}
	if key, val := l.iter.SeekGE(key); key != nil {
		return key, val
	}
	return l.skipEmptyFileForward()
}

func (l *levelIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.LowerBound.
	if !l.loadFile(l.findFileGE(key), 1) {
		return nil, nil
	}
	if key, val := l.iter.SeekPrefixGE(prefix, key); key != nil {
		return key, val
	}
	// When SeekPrefixGE returns nil, we have not necessarily reached the end of
	// the sstable. All we know is that a key with prefix does not exist in the
	// current sstable. We do know that the key lies within the bounds of the
	// table as findFileGE found the table where key <= meta.Largest. We treat
	// this case the same as SeekGE where an upper-bound resides within the
	// sstable, and force this to occur by ensuring that tableOpts.UpperBound is
	// non-nil.
	if l.tableOpts.UpperBound == nil {
		l.tableOpts.UpperBound = sentinelUpperBound
	}
	return l.skipEmptyFileForward()
}

func (l *levelIter) SeekLT(key []byte) (*InternalKey, []byte) {
	// NB: the top-level Iterator has already adjusted key based on
	// IterOptions.UpperBound.
	if !l.loadFile(l.findFileLT(key), -1) {
		return nil, nil
	}
	if key, val := l.iter.SeekLT(key); key != nil {
		return key, val
	}
	return l.skipEmptyFileBackward()
}

func (l *levelIter) First() (*InternalKey, []byte) {
	// NB: the top-level Iterator will call SeekGE if IterOptions.LowerBound is
	// set.
	if !l.loadFile(0, 1) {
		return nil, nil
	}
	if key, val := l.iter.First(); key != nil {
		return key, val
	}
	return l.skipEmptyFileForward()
}

func (l *levelIter) Last() (*InternalKey, []byte) {
	// NB: the top-level Iterator will call SeekLT if IterOptions.UpperBound is
	// set.
	if !l.loadFile(len(l.files)-1, -1) {
		return nil, nil
	}
	if key, val := l.iter.Last(); key != nil {
		return key, val
	}
	return l.skipEmptyFileBackward()
}

func (l *levelIter) Next() (*InternalKey, []byte) {
	if l.err != nil {
		return nil, nil
	}

	if l.boundary != nil {
		// We're stepping past the boundary key, so now we can load the next file.
		if l.loadFile(l.index+1, 1) {
			if key, val := l.iter.First(); key != nil {
				return key, val
			}
			return l.skipEmptyFileForward()
		}
		return nil, nil
	}

	if l.iter == nil {
		return nil, nil
	}
	if key, val := l.iter.Next(); key != nil {
		return key, val
	}
	return l.skipEmptyFileForward()
}

func (l *levelIter) Prev() (*InternalKey, []byte) {
	if l.err != nil {
		return nil, nil
	}

	if l.boundary != nil {
		// We're stepping past the boundary key, so now we can load the prev file.
		if l.loadFile(l.index-1, -1) {
			if key, val := l.iter.Last(); key != nil {
				return key, val
			}
			return l.skipEmptyFileBackward()
		}
		return nil, nil
	}

	if l.iter == nil {
		return nil, nil
	}
	if key, val := l.iter.Prev(); key != nil {
		return key, val
	}
	return l.skipEmptyFileBackward()
}

func (l *levelIter) skipEmptyFileForward() (*InternalKey, []byte) {
	var key *InternalKey
	var val []byte
	// The first iteration of this loop starts with an already exhausted
	// l.iter. The reason for the exhaustion is either that we iterated to the
	// end of the sstable, or our iteration was terminated early due to the
	// presence of an upper-bound or the use of SeekPrefixGE. If l.rangeDelIter
	// is non-nil, we may need to pretend the iterator is not exhausted to allow
	// for the merging to finish consuming the l.rangeDelIter before levelIter
	// switches the rangeDelIter from under it. This pretense is done by either
	// generating a synthetic boundary key or returning the largest key of the
	// file, depending on the exhaustion reason.

	// Subsequent iterations will examine consecutive files such that the first
	// file that does not have an exhausted iterator causes the code to return
	// that key, else the behavior described above if there is a corresponding
	// rangeDelIter.
	for ; key == nil; key, val = l.iter.First() {
		if l.rangeDelIter != nil {
			// We're being used as part of a mergingIter and we've exhausted the
			// current sstable. If an upper bound is present and the upper bound lies
			// within the current sstable, then we will have reached the upper bound
			// rather than the end of the sstable. We need to return a synthetic
			// boundary key so that mergingIter can use the range tombstone iterator
			// until the other levels have reached this boundary.
			//
			// It is safe to set the boundary key kind to RANGEDEL because we're
			// never going to look at subsequent sstables (we've reached the upper
			// bound).
			f := &l.files[l.index]
			if l.tableOpts.UpperBound != nil {
				l.syntheticBoundary = f.Largest
				l.syntheticBoundary.SetKind(InternalKeyKindRangeDelete)
				l.boundary = &l.syntheticBoundary
				return l.boundary, nil
			}
			// If the boundary is a range deletion tombstone, return that key.
			if f.Largest.Kind() == InternalKeyKindRangeDelete {
				l.boundary = &f.Largest
				return l.boundary, nil
			}
		}

		// Current file was exhausted. Move to the next file.
		if !l.loadFile(l.index+1, 1) {
			return nil, nil
		}
	}
	return key, val
}

func (l *levelIter) skipEmptyFileBackward() (*InternalKey, []byte) {
	var key *InternalKey
	var val []byte
	// The first iteration of this loop starts with an already exhausted
	// l.iter. The reason for the exhaustion is either that we iterated to the
	// end of the sstable, or our iteration was terminated early due to the
	// presence of a lower-bound. If l.rangeDelIter is non-nil, we may need to
	// pretend the iterator is not exhausted to allow for the merging to finish
	// consuming the l.rangeDelIter before levelIter switches the rangeDelIter
	// from under it. This pretense is done by either generating a synthetic
	// boundary key or returning the largest key of the file, depending on the
	// exhaustion reason.

	// Subsequent iterations will examine consecutive files such that the first
	// file that does not have an exhausted iterator causes the code to return
	// that key, else the behavior described above if there is a corresponding
	// rangeDelIter.
	for ; key == nil; key, val = l.iter.Last() {
		if l.rangeDelIter != nil {
			// We're being used as part of a mergingIter and we've exhausted the
			// current sstable. If a lower bound is present and the lower bound lies
			// within the current sstable, then we will have reached the lower bound
			// rather than the beginning of the sstable. We need to return a
			// synthetic boundary key so that mergingIter can use the range tombstone
			// iterator until the other levels have reached this boundary.
			//
			// It is safe to set the boundary key kind to RANGEDEL because we're
			// never going to look at earlier sstables (we've reached the lower
			// bound).
			f := &l.files[l.index]
			if l.tableOpts.LowerBound != nil && l.rangeDelIter != nil {
				l.syntheticBoundary = f.Smallest
				l.syntheticBoundary.SetKind(InternalKeyKindRangeDelete)
				l.boundary = &l.syntheticBoundary
				return l.boundary, nil
			}
			// If the boundary is a range deletion tombstone, return that key.
			if f.Smallest.Kind() == InternalKeyKindRangeDelete {
				l.boundary = &f.Smallest
				return l.boundary, nil
			}
		}

		// Current file was exhausted. Move to the previous file.
		if !l.loadFile(l.index-1, -1) {
			return nil, nil
		}
	}
	return key, val
}

func (l *levelIter) Error() error {
	if l.err != nil || l.iter == nil {
		return l.err
	}
	return l.iter.Error()
}

func (l *levelIter) Close() error {
	if l.iter != nil {
		l.err = l.iter.Close()
		l.iter = nil
	}
	if l.rangeDelIter != nil {
		if t := *l.rangeDelIter; t != nil {
			if err := t.Close(); err != nil && l.err == nil {
				l.err = err
			}
		}
		*l.rangeDelIter = nil
	}
	return l.err
}

func (l *levelIter) SetBounds(lower, upper []byte) {
	l.opts.LowerBound = lower
	l.opts.UpperBound = upper
	if l.iter != nil {
		l.iter.SetBounds(lower, upper)
	}
}

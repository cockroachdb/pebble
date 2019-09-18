// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sort"
)

// tableNewIters creates a new point and range-del iterator for the given file
// number. If bytesIterated is specified, it is incremented as the given file is
// iterated through.
type tableNewIters func(
	meta *fileMetadata, opts *IterOptions, bytesIterated *uint64,
) (internalIterator, internalIterator, error)

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
type levelIter struct {
	opts      *IterOptions
	tableOpts IterOptions
	cmp       Compare
	index     int
	// The key to return when iterating past an sstable boundary and that
	// boundary is a range deletion tombstone. Note that if boundary != nil, then
	// iter == nil, and if iter != nil, then boundary == nil.
	boundary     *InternalKey
	iter         internalIterator
	newIters     tableNewIters
	rangeDelIter *internalIterator
	files        []fileMetadata
	err          error
	// Pointer into this level's entry in `mergingIter::largestUserKeys`. We populate it
	// with the largest user key for the currently opened file. It is used to limit the optimization
	// that seeks lower-level iterators past keys shadowed by a range tombstone. Limiting this
	// seek to the file upper-bound is necessary since range tombstones are stored untruncated,
	// while they only apply to keys within their containing file's boundaries. For a detailed
	// example, see comment above `mergingIter`.
	//
	// This field differs from the `boundary` field in a few ways:
	// - `boundary` is only populated when the iterator is positioned exactly on the sentinel key.
	// - `boundary` can hold either the lower- or upper-bound, depending on the iterator direction.
	// - `boundary` is not exposed to the next higher-level iterator, i.e., `mergingIter`.
	largestUserKey *[]byte
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

func (l *levelIter) initLargestUserKey(largestUserKey *[]byte) {
	l.largestUserKey = largestUserKey
}

func (l *levelIter) findFileGE(key []byte) int {
	// Find the earliest file whose largest key is >= ikey. Note that the range
	// deletion sentinel key is handled specially and a search for K will not
	// find a table where K<range-del-sentinel> is the largest key. This prevents
	// loading untruncated range deletions from a table which can't possibly
	// contain the target key and is required for correctness by DB.Get.
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
		return l.iter != nil
	}
	if l.iter != nil {
		l.err = l.iter.Close()
		if l.err != nil {
			return false
		}
		l.iter = nil
	}
	if l.rangeDelIter != nil {
		*l.rangeDelIter = nil
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
			if l.cmp(l.tableOpts.LowerBound, f.Smallest.UserKey) < 0 {
				// The lower bound is smaller than the smallest key in the
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
				// bound.
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
		if l.largestUserKey != nil {
			*l.largestUserKey = f.Largest.UserKey
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

	if l.iter == nil {
		if l.boundary != nil {
			if l.loadFile(l.index+1, 1) {
				if key, val := l.iter.First(); key != nil {
					return key, val
				}
				return l.skipEmptyFileForward()
			}
			return nil, nil
		}
		if l.index == -1 && l.loadFile(0, 1) {
			// The iterator was positioned off the beginning of the level. Position
			// at the first entry.
			if key, val := l.iter.First(); key != nil {
				return key, val
			}
			return l.skipEmptyFileForward()
		}
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

	if l.iter == nil {
		if l.boundary != nil {
			if l.loadFile(l.index-1, -1) {
				if key, val := l.iter.Last(); key != nil {
					return key, val
				}
				return l.skipEmptyFileBackward()
			}
			return nil, nil
		}
		if n := len(l.files); l.index == n && l.loadFile(n-1, -1) {
			// The iterator was positioned off the end of the level. Position at the
			// last entry.
			if key, val := l.iter.Last(); key != nil {
				return key, val
			}
			return l.skipEmptyFileBackward()
		}
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
	for ; key == nil; key, val = l.iter.First() {
		if l.err = l.iter.Close(); l.err != nil {
			return nil, nil
		}
		l.iter = nil

		if l.rangeDelIter != nil {
			// We're being used as part of an Iterator and we've reached the end of
			// the sstable. If the boundary is a range deletion tombstone, return
			// that key.
			if f := &l.files[l.index]; f.Largest.Kind() == InternalKeyKindRangeDelete {
				l.boundary = &f.Largest
				return l.boundary, nil
			}
			*l.rangeDelIter = nil
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
	for ; key == nil; key, val = l.iter.Last() {
		if l.err = l.iter.Close(); l.err != nil {
			return nil, nil
		}
		l.iter = nil

		if l.rangeDelIter != nil {
			// We're being used as part of an Iterator and we've reached the end of
			// the sstable. If the boundary is a range deletion tombstone, return
			// that key.
			if f := &l.files[l.index]; f.Smallest.Kind() == InternalKeyKindRangeDelete {
				l.boundary = &f.Smallest
				return l.boundary, nil
			}
			*l.rangeDelIter = nil
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

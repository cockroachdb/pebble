// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/treesteps"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
)

// Get gets the value for the given key. It returns ErrNotFound if the DB does
// not contain the key.
//
// The caller should not modify the contents of the returned slice, but it is
// safe to modify the contents of the argument after Get returns. The returned
// slice will remain valid until the returned Closer is closed. On success, the
// caller MUST call closer.Close() or a memory leak will occur.
func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	return d.getInternal(key, nil /* batch */, nil /* snapshot */)
}

type getIterAlloc struct {
	dbi    Iterator
	keyBuf []byte
	get    getIter
}

// Close closes the contained iterator and recycles the alloc.
//
// During a successful call to DB.Get, *getIterAlloc is returned to the caller
// as the io.Closer.
func (g *getIterAlloc) Close() error {
	err := g.dbi.Close()
	keyBuf := g.keyBuf
	if cap(g.dbi.keyBuf) < maxKeyBufCacheSize && cap(g.dbi.keyBuf) > cap(keyBuf) {
		keyBuf = g.dbi.keyBuf
	}
	*g = getIterAlloc{}
	g.keyBuf = keyBuf
	getIterAllocPool.Put(g)
	return err
}

var getIterAllocPool = sync.Pool{
	New: func() interface{} {
		return &getIterAlloc{}
	},
}

func (d *DB) getInternal(key []byte, b *Batch, s *Snapshot) ([]byte, io.Closer, error) {
	if err := d.closed.Load(); err != nil {
		panic(err)
	}

	// Grab and reference the current readState. This prevents the underlying
	// files in the associated version from being deleted if there is a current
	// compaction. The readState is unref'd by Iterator.Close().
	readState := d.loadReadState()

	// Determine the seqnum to read at after grabbing the read state (current and
	// memtables) above.
	var seqNum base.SeqNum
	if s != nil {
		seqNum = s.seqNum
	} else {
		seqNum = d.mu.versions.visibleSeqNum.Load()
	}

	buf := getIterAllocPool.Get().(*getIterAlloc)

	get := &buf.get
	*get = getIter{
		comparer: d.opts.Comparer,
		newIters: d.newIters,
		snapshot: seqNum,
		iterOpts: IterOptions{
			// TODO(sumeer): replace with a parameter provided by the caller.
			Category:                      categoryGet,
			logger:                        d.opts.Logger,
			snapshotForHideObsoletePoints: seqNum,
		},
		key: key,
		// Compute the key prefix for bloom filtering.
		prefix:  key[:d.opts.Comparer.Split(key)],
		batch:   b,
		mem:     readState.memtables,
		l0:      readState.current.L0SublevelFiles,
		version: readState.current,
	}

	// Strip off memtables which cannot possibly contain the seqNum being read
	// at.
	for len(get.mem) > 0 {
		n := len(get.mem)
		if logSeqNum := get.mem[n-1].logSeqNum; logSeqNum < seqNum {
			break
		}
		get.mem = get.mem[:n-1]
	}

	i := &buf.dbi
	pointIter := get
	*i = Iterator{
		ctx:       context.Background(),
		iter:      pointIter,
		pointIter: pointIter,
		merge:     d.merge,
		comparer:  d.opts.Comparer,
		readState: readState,
		keyBuf:    buf.keyBuf,
	}

	// Set up a blob value fetcher to use for retrieving values from blob files.
	if blobFileMapping := i.combinedBlobMapping.Resolve(
		&readState.current.BlobFiles, get.mem,
	); blobFileMapping != nil {
		i.blobValueFetcher.Init(blobFileMapping, d.fileCache, block.NoReadEnv,
			blob.SuggestedCachedReaders(readState.current.MaxReadAmp()))
	}
	get.iiopts.blobValueFetcher = &i.blobValueFetcher

	if !i.First() {
		err := buf.Close()
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, ErrNotFound
	}
	val, err := i.ValueAndErr()
	if err != nil {
		return nil, nil, errors.CombineErrors(err, buf.Close())
	}
	return val, buf, nil
}

// getIter is an internal iterator used to perform gets. It iterates through
// the values for a particular key, level by level. It is not a general purpose
// internalIterator, but specialized for Get operations so that it loads data
// lazily.
type getIter struct {
	comparer *Comparer
	newIters tableNewIters
	snapshot base.SeqNum
	iterOpts IterOptions
	iiopts   internalIterOpts
	key      []byte
	prefix   []byte
	iter     internalIterator
	level    int
	batch    *Batch
	mem      flushableList
	l0       []manifest.LevelSlice
	version  *manifest.Version
	iterKV   *base.InternalKV
	// tombstoned and tombstonedSeqNum track whether the key has been deleted by
	// a range delete tombstone. The first visible (at getIter.snapshot) range
	// deletion encounterd transitions tombstoned to true. The tombstonedSeqNum
	// field is updated to hold the sequence number of the tombstone.
	tombstoned       bool
	tombstonedSeqNum base.SeqNum
	err              error
}

// TODO(sumeer): CockroachDB code doesn't use getIter, but, for completeness,
// make this implement InternalIteratorWithStats.

// getIter implements the base.InternalIterator interface.
var _ base.InternalIterator = (*getIter)(nil)

func (g *getIter) String() string {
	return fmt.Sprintf("len(l0)=%d, len(mem)=%d, level=%d", len(g.l0), len(g.mem), g.level)
}

func (g *getIter) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
	panic("pebble: SeekGE unimplemented")
}

func (g *getIter) SeekPrefixGE(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	return g.SeekPrefixGEStrict(prefix, key, flags)
}

func (g *getIter) SeekPrefixGEStrict(prefix, key []byte, flags base.SeekGEFlags) *base.InternalKV {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (g *getIter) SeekLT(key []byte, flags base.SeekLTFlags) *base.InternalKV {
	panic("pebble: SeekLT unimplemented")
}

func (g *getIter) First() *base.InternalKV {
	return g.Next()
}

func (g *getIter) Last() *base.InternalKV {
	panic("pebble: Last unimplemented")
}

func (g *getIter) Next() *base.InternalKV {
	// If g.iter != nil, we're already iterating through a level. Next. Note
	// that it's possible the next key within the level is still relevant (eg,
	// MERGE keys written in the presence of an LSM snapshot).
	//
	// NB: We can't perform this Next below, in the for loop, because when we
	// open an iterator into the next level, we need to seek to the key.
	if g.iter != nil {
		g.iterKV = g.iter.Next()
		if err := g.iter.Error(); err != nil {
			g.err = err
			return nil
		}
	}

	// This for loop finds the next internal key in the LSM that is equal to
	// g.key, visible at g.snapshot and not shadowed by a range deletion. If it
	// exhausts a level, it initializes iterators for the next level.
	for {
		if g.iter != nil {
			if g.iterKV != nil {
				// Check if the current KV pair is deleted by a range deletion.
				if g.tombstoned && g.tombstonedSeqNum > g.iterKV.SeqNum() {
					// We have a range tombstone covering this key. Rather than
					// return a point or range deletion here, we return nil and
					// close our internal iterator stopping iteration.
					g.err = g.iter.Close()
					g.iter = nil
					return nil
				}

				// Is this the correct user key?
				if g.comparer.Equal(g.key, g.iterKV.K.UserKey) {
					// If the KV pair is not visible at the get's snapshot,
					// Next. The level may still contain older keys with the
					// same user key that are visible.
					if !g.iterKV.Visible(g.snapshot, base.SeqNumMax) {
						g.iterKV = g.iter.Next()
						continue
					}
					return g.iterKV
				}
			}
			// We've advanced the iterator passed the desired key. Move on to the
			// next memtable / level.
			g.err = g.iter.Close()
			g.iter = nil
			if g.err != nil {
				return nil
			}
		}
		// g.iter == nil; we need to initialize the next iterator.
		if !g.initializeNextIterator() {
			return nil
		}
		g.iterKV = g.iter.SeekPrefixGE(g.prefix, g.key, base.SeekGEFlagsNone)
	}
}

func (g *getIter) Prev() *base.InternalKV {
	panic("pebble: Prev unimplemented")
}

func (g *getIter) NextPrefix([]byte) *base.InternalKV {
	panic("pebble: NextPrefix unimplemented")
}

func (g *getIter) Error() error {
	return g.err
}

func (g *getIter) Close() error {
	if g.iter != nil {
		if err := g.iter.Close(); err != nil && g.err == nil {
			g.err = err
		}
		g.iter = nil
	}
	return g.err
}

func (g *getIter) SetBounds(lower, upper []byte) {
	panic("pebble: SetBounds unimplemented")
}

func (g *getIter) SetContext(_ context.Context) {}

// TreeStepsNode is part of the InternalIterator interface.
func (g *getIter) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(g, "%T(%p)", g, g)
	info.AddChildren(g.iter)
	return info
}

func (g *getIter) initializeNextIterator() (ok bool) {
	// A batch's keys shadow all other keys, so we visit the batch first.
	if g.batch != nil {
		if g.batch.index == nil {
			g.err = ErrNotIndexed
			g.iterKV = nil
			return false
		}
		g.iter = g.batch.newInternalIter(nil)
		if !g.maybeSetTombstone(g.batch.newRangeDelIter(nil,
			// Get always reads the entirety of the batch's history, so no
			// batch keys should be filtered.
			base.SeqNumMax,
		)) {
			return false
		}
		g.batch = nil
		return true
	}

	// If we're trying to initialize the next level of the iterator stack but
	// have a tombstone from a previous level, it is guaranteed to delete keys
	// in lower levels. This key is deleted.
	if g.tombstoned {
		return false
	}

	// Create iterators from memtables from newest to oldest.
	if n := len(g.mem); n > 0 {
		m := g.mem[n-1]
		// For ingested flushables, use newIterInternal to pass the blob
		// value fetcher so that blob values can be read.
		if fi, ok := m.flushable.(*ingestedFlushable); ok {
			g.iter = fi.newIterInternal(nil, g.iiopts)
		} else {
			g.iter = m.newIter(nil)
		}
		if !g.maybeSetTombstone(m.newRangeDelIter(nil)) {
			return false
		}
		g.mem = g.mem[:n-1]
		return true
	}

	// Visit each sublevel of L0 individually, so that we only need to read
	// at most one file per sublevel.
	if g.level == 0 {
		// Create iterators from L0 from newest to oldest.
		if n := len(g.l0); n > 0 {
			files := g.l0[n-1].Iter()
			g.l0 = g.l0[:n-1]

			iter, rangeDelIter, err := g.getSSTableIterators(files, manifest.L0Sublevel(n))
			if err != nil {
				g.err = firstError(g.err, err)
				return false
			}
			if !g.maybeSetTombstone(rangeDelIter) {
				return false
			}
			g.iter = iter
			return true
		}
		// We've exhausted all the sublevels of L0. Progress to L1.
		g.level++
	}
	for g.level < numLevels {
		if g.version.Levels[g.level].Empty() {
			g.level++
			continue
		}
		// Open the next level of the LSM.
		iter, rangeDelIter, err := g.getSSTableIterators(g.version.Levels[g.level].Iter(), manifest.Level(g.level))
		if err != nil {
			g.err = firstError(g.err, err)
			return false
		}
		if !g.maybeSetTombstone(rangeDelIter) {
			return false
		}
		g.level++
		g.iter = iter
		return true
	}
	// We've exhausted all levels of the LSM.
	return false
}

// getSSTableIterators returns a point iterator and a range deletion iterator
// for the sstable in files that overlaps with the key g.key. Pebble does not
// split user keys across adjacent sstables within a level, ensuring that at
// most one sstable overlaps g.key.
func (g *getIter) getSSTableIterators(
	files manifest.LevelIterator, level manifest.Layer,
) (internalIterator, keyspan.FragmentIterator, error) {
	files = files.Filter(manifest.KeyTypePoint)
	m := files.SeekGE(g.comparer.Compare, g.key)
	if m == nil {
		return emptyIter, nil, nil
	}
	// m is now positioned at the file containing the first point key ≥ `g.key`.
	// Does it exist and possibly contain point keys with the user key 'g.key'?
	if m == nil || !m.HasPointKeys || g.comparer.Compare(m.PointKeyBounds.SmallestUserKey(), g.key) > 0 {
		return emptyIter, nil, nil
	}
	// m may possibly contain point (or range deletion) keys relevant to g.key.
	g.iterOpts.layer = level
	iters, err := g.newIters(context.Background(), m, &g.iterOpts, g.iiopts, iterPointKeys|iterRangeDeletions)
	if err != nil {
		return emptyIter, nil, err
	}
	return iters.Point(), iters.RangeDeletion(), nil
}

// maybeSetTombstone updates g.tombstoned[SeqNum] to reflect the presence of a
// range deletion covering g.key, if there are any. It returns true if
// successful, or false if an error occurred and the caller should abort
// iteration.
func (g *getIter) maybeSetTombstone(rangeDelIter keyspan.FragmentIterator) (ok bool) {
	if rangeDelIter == nil {
		// Nothing to do.
		return true
	}
	// Find the range deletion that covers the sought key, if any.
	t, err := keyspan.Get(g.comparer.Compare, rangeDelIter, g.key)
	if err != nil {
		g.err = firstError(g.err, err)
		return false
	}
	// Find the most recent visible range deletion's sequence number. We only
	// care about the most recent range deletion that's visible because it's the
	// "most powerful."
	g.tombstonedSeqNum, g.tombstoned = t.LargestVisibleSeqNum(g.snapshot)
	rangeDelIter.Close()
	return true
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"fmt"
	"sort"

	"github.com/petermattis/pebble/db"
)

type tombstonesByEndKey struct {
	cmp db.Compare
	buf []Tombstone
}

func (v *tombstonesByEndKey) Len() int { return len(v.buf) }
func (v *tombstonesByEndKey) Less(i, j int) bool {
	return v.cmp(v.buf[i].End, v.buf[j].End) < 0
}
func (v *tombstonesByEndKey) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}

type tombstonesBySeqNum []Tombstone

func (v *tombstonesBySeqNum) Len() int { return len(*v) }
func (v *tombstonesBySeqNum) Less(i, j int) bool {
	return (*v)[i].Start.SeqNum() > (*v)[j].Start.SeqNum()
}
func (v *tombstonesBySeqNum) Swap(i, j int) {
	(*v)[i], (*v)[j] = (*v)[j], (*v)[i]
}

// Fragmenter fragments a set of range tombstones such that overlapping
// tombstones are split at their overlap points. The fragmented tombstones are
// output to the supplied Output function.
type Fragmenter struct {
	Cmp db.Compare
	// Emit is called to emit a chunk of tombstone fragments. Every tombstone
	// within the chunk has the same start and end key, and differ only by
	// sequence number.
	Emit func([]Tombstone)
	// pending contains the list of pending range tombstone fragments that have
	// not been flushed to the block writer. Note that the tombstones have not
	// been fragmented on the end keys yet. That happens as the tombstones are
	// flushed.
	pending  []Tombstone
	doneBuf  []Tombstone
	sortBuf  tombstonesByEndKey
	flushBuf tombstonesBySeqNum
	finished bool
}

func (f *Fragmenter) checkSameStart(buf []Tombstone) {
	for i := 1; i < len(buf); i++ {
		if f.Cmp(buf[i-1].Start.UserKey, buf[i].Start.UserKey) != 0 {
			panic(fmt.Sprintf("pebble: pending tombstone invariant violated: %s %s",
				buf[i-1].Start, buf[i].Start))
		}
	}
}

func (f *Fragmenter) checkInvariants() {
	f.checkSameStart(f.pending)
}

// Add adds a tombstone to the fragmenter. Tombstones may overlap and the
// fragmenter will internally split them. The tombstones must be presented in
// increasing start key order. That is, Add must be called with a series of
// tombstones like:
//
//   a---e
//     c---g
//     c-----i
//            j---n
//            j-l
//
// We need to fragment the tombstones at overlap points. In the above
// example, we'd create:
//
//   a-c-e
//     c-e-g
//     c-e-g-i
//            j-l-n
//            j-l
//
// The fragments need to be output sorted by start key, and for equal start
// keys, sorted by descending sequence number. This last part requires a mild
// bit of care as the fragments are not created in descending sequence number
// order.
//
// Once a start key has been seen, we know that we'll never see a smaller
// start key and can thus flush all of the fragments that lie before that
// start key.
//
// Walking through the example above, we start with:
//
//   a---e
//
// Next we add [c,g) resulting in:
//
//   a-c-e
//     c---g
//
// The fragment [a,c) is flushed leaving the pending tombstones as:
//
//   c-e
//   c---g
//
// The next tombstone is [c,i):
//
//   c-e
//   c---g
//   c-----i
//
// No fragments are flushed. The next tombstone is [j,n):
//
//   c-e
//   c---g
//   c-----i
//          j---n
//
// The fragments [c,e), [c,g) and [c,i) are flushed. We sort these fragments
// by their end key, then split the fragments on the end keys:
//
//   c-e
//   c-e-g
//   c-e---i
//
// The [c,e) fragments all get flushed leaving:
//
//   e-g
//   e---i
//
// This process continues until there are no more fragments to flush.
//
// WARNING: the slices backing start.UserKey and end are retained after this
// method returns and should not be modified. This is safe for tombstones that
// are added from a memtable or batch. It is not safe for a tombstone added
// from an sstable where the range-del block has been prefix compressed.
func (f *Fragmenter) Add(start db.InternalKey, end []byte) {
	if f.finished {
		panic("pebble: tombstone fragmenter already finished")
	}
	if raceEnabled {
		f.checkInvariants()
		defer f.checkInvariants()
	}

	if len(f.pending) > 0 {
		// Since all of the pending tombstones have the same start key, we only need
		// to compare against the first one.
		switch c := f.Cmp(f.pending[0].Start.UserKey, start.UserKey); {
		case c > 0:
			panic(fmt.Sprintf("pebble: keys must be added in order: %s > %s",
				f.pending[0].Start, start))
		case c == 0:
			// The new tombstone has the same start key as the existing pending
			// tombstones. Add it to the pending buffer.
			f.pending = append(f.pending, Tombstone{
				Start: start,
				End:   end,
			})
			return
		}

		// At this point we know that the new start key is greater than the pending
		// tombstones start keys.
		f.truncateAndFlush(start.UserKey)
	}

	f.pending = append(f.pending, Tombstone{
		Start: start,
		End:   end,
	})
}

// Deleted returns true if the specified key is covered by one of the pending
// tombstones. The key must be consistent with the ordering of the
// tombstones. That is, it is invalid to specify a key here that is out of
// order with the tombstone start keys passed to Add.
func (f *Fragmenter) Deleted(key db.InternalKey, snapshot uint64) bool {
	if f.finished {
		panic("pebble: tombstone fragmenter already finished")
	}
	if len(f.pending) == 0 {
		return false
	}

	if f.Cmp(f.pending[0].Start.UserKey, key.UserKey) > 0 {
		panic(fmt.Sprintf("pebble: keys must be in order: %s > %s",
			f.pending[0].Start, key))
	}

	seqNum := key.SeqNum()
	flush := true
	for _, t := range f.pending {
		if f.Cmp(key.UserKey, t.End) < 0 {
			// NB: A range deletion tombstone deletes a point operation at the same
			// sequence number.
			if t.Start.Visible(snapshot) && t.Start.SeqNum() > seqNum {
				return true
			}
			flush = false
		}
	}

	if flush {
		// All of the pending tombstones ended before the specified key which means
		// we can flush them without causing fragmentation at key. This is an
		// optimization to allow flushing the pending tombstones as early as
		// possible so that we don't have to continually reconsider them in
		// Deleted.
		f.flush(f.pending, true /* all */)
		f.pending = f.pending[:0]
	}
	return false
}

// FlushTo flushes all of the fragments before key. Used internally by Add to
// flush tombstone fragments, and can be used externally to fragment tombstones
// during compaction when a tombstone straddles an sstable boundary.
func (f *Fragmenter) FlushTo(key []byte) {
	if f.finished {
		panic("pebble: tombstone fragmenter already finished")
	}
	if len(f.pending) == 0 {
		return
	}
	// Since all of the pending tombstones have the same start key, we only need
	// to compare against the first one.
	switch c := f.Cmp(f.pending[0].Start.UserKey, key); {
	case c > 0:
		panic(fmt.Sprintf("pebble: keys must be in order: %s > %s",
			f.pending[0].Start, key))
	}

	// At this point we know that the new start key is greater than the pending
	// tombstones start keys. We flush the pending first set of fragments for the
	// pending tombstones.
	f.flush(f.pending, false /* all */)

	for i := range f.pending {
		f.pending[i].Start.UserKey = key
	}
}

func (f *Fragmenter) truncateAndFlush(key []byte) {
	done := f.doneBuf[:0]
	pending := f.pending
	f.pending = f.pending[:0]

	for _, t := range pending {
		if f.Cmp(key, t.End) < 0 {
			//   t: a--+--e
			// new:    c------
			done = append(done, Tombstone{Start: t.Start, End: key})
			f.pending = append(f.pending, Tombstone{
				Start: db.MakeInternalKey(key, t.Start.SeqNum(), t.Start.Kind()),
				End:   t.End,
			})
		} else {
			//   t: a-----e
			// new:       e----
			done = append(done, t)
		}
	}

	f.doneBuf = done[:0]
	f.flush(done, true /* all */)
}

// flush a group of range tombstones to the block. The tombstones are required
// to all have the same start key.
func (f *Fragmenter) flush(buf []Tombstone, all bool) {
	if raceEnabled {
		f.checkSameStart(buf)
	}

	// Sort the tombstones by end key. This will allow us to walk over the
	// tombstones and easily determine the next split point (the smallest
	// end-key).
	f.sortBuf.cmp = f.Cmp
	f.sortBuf.buf = buf
	sort.Sort(&f.sortBuf)

	// Loop over the range tombstones, splitting by end key.
	for len(buf) > 0 {
		remove := 1
		split := buf[0].End
		f.flushBuf = append(f.flushBuf[:0], buf[0])

		for i := 1; i < len(buf); i++ {
			if f.Cmp(split, buf[i].End) == 0 {
				remove++
			}
			f.flushBuf = append(f.flushBuf, Tombstone{
				Start: buf[i].Start,
				End:   split,
			})
		}

		buf = buf[remove:]

		sort.Sort(&f.flushBuf)
		f.Emit(f.flushBuf)

		if !all {
			break
		}

		// Adjust the start key for every remaining tombstone.
		for i := range buf {
			buf[i].Start.UserKey = split
		}
	}
}

// Finish flushes any remaining fragments to the output. It is an error to call
// this if any other tombstones will be added.
func (f *Fragmenter) Finish() {
	if f.finished {
		panic("pebble: tombstone fragmenter already finished")
	}
	f.flush(f.pending, true /* all */)
	f.finished = true
}

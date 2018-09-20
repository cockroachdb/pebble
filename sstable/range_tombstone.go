// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"fmt"
	"sort"

	"github.com/petermattis/pebble/db"
)

type rangeTombstone struct {
	start db.InternalKey
	end   []byte
}

type rangeTombstonesByEndKey struct {
	cmp db.Compare
	buf []rangeTombstone
}

func (v *rangeTombstonesByEndKey) Len() int { return len(v.buf) }
func (v *rangeTombstonesByEndKey) Less(i, j int) bool {
	return v.cmp(v.buf[i].end, v.buf[j].end) < 0
}
func (v *rangeTombstonesByEndKey) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}

type rangeTombstonesBySeqNum []rangeTombstone

func (v *rangeTombstonesBySeqNum) Len() int { return len(*v) }
func (v *rangeTombstonesBySeqNum) Less(i, j int) bool {
	return (*v)[i].start.SeqNum() > (*v)[j].start.SeqNum()
}
func (v *rangeTombstonesBySeqNum) Swap(i, j int) {
	(*v)[i], (*v)[j] = (*v)[j], (*v)[i]
}

// rangeTombstoneBlockWriter builds range tombstone blocks. A range tombstone
// block has the same format as a normal data block, but the builder takes care
// to fragment range tombstones as they are added.
type rangeTombstoneBlockWriter struct {
	cmp   db.Compare
	block blockWriter
	// pending contains the list of pending range tombstone fragments that have
	// not been flushed to the block writer. Note that the tombstones have not
	// been fragmented on the end keys yet. That happens as the tombstones are
	// flushed.
	pending  []rangeTombstone
	doneBuf  []rangeTombstone
	sortBuf  rangeTombstonesByEndKey
	flushBuf rangeTombstonesBySeqNum
}

func (w *rangeTombstoneBlockWriter) checkSameStart(buf []rangeTombstone) {
	for i := 1; i < len(buf); i++ {
		if w.cmp(buf[i-1].start.UserKey, buf[i].start.UserKey) != 0 {
			panic(fmt.Sprintf("pebble/table: pending tombstone invariant violated: %q, %q",
				buf[i-1].start.UserKey, buf[i].start.UserKey))
		}
	}
}

func (w *rangeTombstoneBlockWriter) checkInvariants() {
	w.checkSameStart(w.pending)
}

// Add adds a tombstone to the block being constructed. Tombstones must be
// added in increasing start key order. Tombstones may overlap causing the
// builder to fragment the tombstones as necessary.
func (w *rangeTombstoneBlockWriter) add(start, end []byte, seqNum uint64) {
	// The tombstones are presented in increasing start key order. That is, we'll
	// receive a series of tombstones like:
	//
	// a---e
	//   c---g
	//   c-----i
	//          j---n
	//          j-l
	//
	// We need to fragment the tombstones at overlap points. In the above
	// example, we'd create:
	//
	// a-c-e
	//   c-e-g
	//   c-e-g-i
	//          j-l-n
	//          j-l
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
	// a---e
	//
	// Next we add [c,g) resulting in:
	//
	// a-c-e
	//   c---g
	//
	// The fragment [a,c) is flushed leaving the pending tombstones as:
	//
	// c-e
	// c---g
	//
	// The next tombstone is [c,i):
	//
	// c-e
	// c---g
	// c-----i
	//
	// No fragments are flushed. The next tombstone is [j,n):
	//
	// c-e
	// c---g
	// c-----i
	//        j---n
	//
	// The fragments [c,e), [c,g) and [c,i) are flushed. We sort these fragments
	// by their end key, then split the fragments on the end keys:
	//
	// c-e
	// c-e-g
	// c-e---i
	//
	// The [c,e) fragments all get flushed leaving:
	//
	// e-g
	// e---i
	//
	// This process continues until there are no more fragments to flush.

	// TODO(peter): remove the invariant checking when the code is stable.
	w.checkInvariants()
	defer w.checkInvariants()

	if len(w.pending) > 0 {
		// Since all of the pending tombstones have the same start key, we only need
		// to compare against the first one.
		switch c := w.cmp(w.pending[0].start.UserKey, start); {
		case c > 0:
			panic(fmt.Sprintf("pebble/table: Add called in non-increasing key order: %q, %q",
				w.pending[0].start.UserKey, start))
		case c == 0:
			// The new tombstone has the same start key as the existing pending
			// tombstones. Add it to the pending buffer.
			w.pending = append(w.pending, rangeTombstone{
				start: db.MakeInternalKey(start, seqNum, db.InternalKeyKindRangeDelete),
				end:   end,
			})
			return
		}

		// At this point we know that the new start key is greater than the pending
		// tombstones start keys.
		done := w.doneBuf[:0]
		pending := w.pending
		w.pending = w.pending[:0]

		for _, t := range pending {
			if w.cmp(start, t.end) < 0 {
				//   t: a--+--e
				// new:    c------
				done = append(done, rangeTombstone{start: t.start, end: start})
				w.pending = append(w.pending, rangeTombstone{
					start: db.MakeInternalKey(start, t.start.SeqNum(), t.start.Kind()),
					end:   t.end,
				})
			} else {
				//   t: a-----e
				// new:       e----
				done = append(done, t)
			}
		}

		w.doneBuf = done[:0]
		w.flush(done)
	}

	w.pending = append(w.pending, rangeTombstone{
		start: db.MakeInternalKey(start, seqNum, db.InternalKeyKindRangeDelete),
		end:   end,
	})
}

// flush a group of range tombstones to the block. The tombstones are required
// to all have the same start key.
func (w *rangeTombstoneBlockWriter) flush(buf []rangeTombstone) {
	// TODO(peter): remove the invariant checking when the code is stable.
	w.checkSameStart(buf)

	// Sort the tombstones by end key. This will allow us to walk over the
	// tombstones and easily determine the next split point (the smallest
	// end-key).
	w.sortBuf.cmp = w.cmp
	w.sortBuf.buf = buf
	sort.Sort(&w.sortBuf)

	// Loop over the range tombstones, splitting by end key.
	for len(buf) > 0 {
		remove := 1
		split := buf[0].end
		w.flushBuf = append(w.flushBuf[:0], buf[0])

		for i := 1; i < len(buf); i++ {
			if w.cmp(split, buf[i].end) == 0 {
				remove++
			}
			w.flushBuf = append(w.flushBuf, rangeTombstone{
				start: buf[i].start,
				end:   split,
			})
		}

		buf = buf[remove:]

		sort.Sort(&w.flushBuf)
		for _, t := range w.flushBuf {
			w.block.add(t.start, t.end)
		}
	}
}

// Finish flushes any remaining fragments and returns the block data.
func (w *rangeTombstoneBlockWriter) finish() []byte {
	w.flush(w.pending)
	return w.block.finish()
}

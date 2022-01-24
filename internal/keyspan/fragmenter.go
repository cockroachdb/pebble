// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
)

type spansByStartKey struct {
	cmp base.Compare
	buf []Span
}

func (v *spansByStartKey) Len() int { return len(v.buf) }
func (v *spansByStartKey) Less(i, j int) bool {
	return base.InternalCompare(v.cmp, v.buf[i].Start, v.buf[j].Start) < 0
}
func (v *spansByStartKey) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}

type spansByEndKey struct {
	cmp base.Compare
	buf []Span
}

func (v *spansByEndKey) Len() int { return len(v.buf) }
func (v *spansByEndKey) Less(i, j int) bool {
	return v.cmp(v.buf[i].End, v.buf[j].End) < 0
}
func (v *spansByEndKey) Swap(i, j int) {
	v.buf[i], v.buf[j] = v.buf[j], v.buf[i]
}

// spansBySeqNumKind sorts spans by the start key's sequence number in
// descending order. If two spans have equal sequence number, they're compared
// by key kind in descending order. This ordering matches the ordering of
// base.InternalCompare among keys with matching user keys.
type spansBySeqNumKind []Span

func (v *spansBySeqNumKind) Len() int { return len(*v) }
func (v *spansBySeqNumKind) Less(i, j int) bool {
	a, b := (*v)[i].Start, (*v)[j].Start
	switch {
	case a.SeqNum() > b.SeqNum():
		return true
	case a.SeqNum() == b.SeqNum():
		return a.Kind() > b.Kind()
	default:
		return false
	}
}
func (v *spansBySeqNumKind) Swap(i, j int) {
	(*v)[i], (*v)[j] = (*v)[j], (*v)[i]
}

// Sort the spans by start key. This is the ordering required by the
// Fragmenter. Usually spans are naturally sorted by their start key,
// but that isn't true for range deletion tombstones in the legacy
// range-del-v1 block format.
func Sort(cmp base.Compare, spans []Span) {
	sorter := spansByStartKey{
		cmp: cmp,
		buf: spans,
	}
	sort.Sort(&sorter)
}

// Fragmenter fragments a set of spans such that overlapping spans are
// split at their overlap points. The fragmented spans are output to the
// supplied Output function.
type Fragmenter struct {
	Cmp    base.Compare
	Format base.FormatKey
	// Emit is called to emit a chunk of span fragments. Every span
	// within the chunk has the same start and end key and are in
	// decreasing order of their sequence numbers.
	Emit func([]Span)
	// pending contains the list of pending fragments that have not been
	// flushed to the block writer. Note that the spans have not been
	// fragmented on the end keys yet. That happens as the spans are
	// flushed. All pending spans have the same Start.UserKey.
	pending []Span
	// doneBuf is used to buffer completed span fragments when flushing to a
	// specific key (e.g. TruncateAndFlushTo). It is cached in the Fragmenter to
	// allow reuse.
	doneBuf []Span
	// sortBuf is used to sort fragments by end key when flushing.
	sortBuf spansByEndKey
	// flushBuf is used to sort fragments by (seqnum,kind) before emitting.
	flushBuf spansBySeqNumKind
	// flushedKey is the key that fragments have been flushed up to. Any
	// additional spans added to the fragmenter must have a start key >=
	// flushedKey. A nil value indicates flushedKey has not been set.
	flushedKey []byte
	finished   bool
}

func (f *Fragmenter) checkInvariants(buf []Span) {
	for i := 1; i < len(buf); i++ {
		if f.Cmp(buf[i].Start.UserKey, buf[i].End) >= 0 {
			panic(fmt.Sprintf("pebble: empty pending span invariant violated: %s", buf[i]))
		}
		if f.Cmp(buf[i-1].Start.UserKey, buf[i].Start.UserKey) != 0 {
			panic(fmt.Sprintf("pebble: pending span invariant violated: %s %s",
				buf[i-1].Start.Pretty(f.Format), buf[i].Start.Pretty(f.Format)))
		}
	}
}

// Add adds a span to the fragmenter. Spans may overlap and the
// fragmenter will internally split them. The spans must be presented in
// increasing start key order. That is, Add must be called with a series
// of spans like:
//
//   a---e
//     c---g
//     c-----i
//            j---n
//            j-l
//
// We need to fragment the spans at overlap points. In the above
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
// The fragment [a,c) is flushed leaving the pending spans as:
//
//   c-e
//   c---g
//
// The next span is [c,i):
//
//   c-e
//   c---g
//   c-----i
//
// No fragments are flushed. The next span is [j,n):
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
// method returns and should not be modified. This is safe for spans that are
// added from a memtable or batch. It is not safe for a range deletion span
// added from an sstable where the range-del block has been prefix compressed.
func (f *Fragmenter) Add(s Span) {
	if f.finished {
		panic("pebble: span fragmenter already finished")
	}
	if f.flushedKey != nil {
		switch c := f.Cmp(s.Start.UserKey, f.flushedKey); {
		case c < 0:
			panic(fmt.Sprintf("pebble: start key (%s) < flushed key (%s)",
				f.Format(s.Start.UserKey), f.Format(f.flushedKey)))
		}
	}
	if f.Cmp(s.Start.UserKey, s.End) >= 0 {
		// An empty span, we can ignore it.
		return
	}
	if invariants.RaceEnabled {
		f.checkInvariants(f.pending)
		defer func() { f.checkInvariants(f.pending) }()
	}

	if len(f.pending) > 0 {
		// Since all of the pending spans have the same start key, we only need
		// to compare against the first one.
		switch c := f.Cmp(f.pending[0].Start.UserKey, s.Start.UserKey); {
		case c > 0:
			panic(fmt.Sprintf("pebble: keys must be added in order: %s > %s",
				f.pending[0].Start.Pretty(f.Format), s.Start.Pretty(f.Format)))
		case c == 0:
			// The new span has the same start key as the existing pending
			// spans. Add it to the pending buffer.
			f.pending = append(f.pending, s)
			return
		}

		// At this point we know that the new start key is greater than the pending
		// spans start keys.
		f.truncateAndFlush(s.Start.UserKey)
	}

	f.pending = append(f.pending, s)
}

// Covers returns true if the specified key is covered by one of the pending
// spans. The key must be consistent with the ordering of the
// spans. That is, it is invalid to specify a key here that is out of
// order with the span start keys passed to Add.
func (f *Fragmenter) Covers(key base.InternalKey, snapshot uint64) bool {
	if f.finished {
		panic("pebble: span fragmenter already finished")
	}
	if len(f.pending) == 0 {
		return false
	}

	if f.Cmp(f.pending[0].Start.UserKey, key.UserKey) > 0 {
		panic(fmt.Sprintf("pebble: keys must be in order: %s > %s",
			f.pending[0].Start.Pretty(f.Format), key.Pretty(f.Format)))
	}

	seqNum := key.SeqNum()
	for _, s := range f.pending {
		if f.Cmp(key.UserKey, s.End) < 0 {
			// NB: A range deletion tombstone does not delete a point operation
			// at the same sequence number, and broadly a span is not considered
			// to cover a point operation at the same sequence number.
			if s.Start.Visible(snapshot) && s.Start.SeqNum() > seqNum {
				return true
			}
		}
	}
	return false
}

// Empty returns true if all fragments added so far have finished flushing.
func (f *Fragmenter) Empty() bool {
	return f.finished || len(f.pending) == 0
}

// TruncateAndFlushTo flushes all of the fragments with a start key <= key,
// truncating spans to the specified end key. Used during compaction to force
// emitting of spans which straddle an sstable boundary. Consider
// the scenario:
//
//     a---------k#10
//          f#8
//          f#7
///
// Let's say the next user key after f is g. Calling TruncateAndFlushTo(g) will
// flush this span:
//
//    a-------g#10
//         f#8
//         f#7
//
// And leave this one in f.pending:
//
//            g----k#10
//
// WARNING: The fragmenter could hold on to the specified end key. Ensure it's
// a safe byte slice that could outlast the current sstable output, and one
// that will never be modified.
func (f *Fragmenter) TruncateAndFlushTo(key []byte) {
	if f.finished {
		panic("pebble: span fragmenter already finished")
	}
	if f.flushedKey != nil {
		switch c := f.Cmp(key, f.flushedKey); {
		case c < 0:
			panic(fmt.Sprintf("pebble: start key (%s) < flushed key (%s)",
				f.Format(key), f.Format(f.flushedKey)))
		}
	}
	if invariants.RaceEnabled {
		f.checkInvariants(f.pending)
		defer func() { f.checkInvariants(f.pending) }()
	}
	if len(f.pending) > 0 {
		// Since all of the pending spans have the same start key, we only need
		// to compare against the first one.
		switch c := f.Cmp(f.pending[0].Start.UserKey, key); {
		case c > 0:
			panic(fmt.Sprintf("pebble: keys must be added in order: %s > %s",
				f.Format(f.pending[0].Start.UserKey), f.Format(key)))
		case c == 0:
			return
		}
	}
	f.truncateAndFlush(key)
}

// Start returns the start key of the first span in the pending buffer, or nil
// if there are no pending spans. The start key of all pending spans is the same
// as that of the first one.
func (f *Fragmenter) Start() []byte {
	if len(f.pending) > 0 {
		return f.pending[0].Start.UserKey
	}
	return nil
}

// Flushes all pending spans up to key (exclusive).
//
// WARNING: The specified key is stored without making a copy, so all callers
// must ensure it is safe.
func (f *Fragmenter) truncateAndFlush(key []byte) {
	f.flushedKey = append(f.flushedKey[:0], key...)
	done := f.doneBuf[:0]
	pending := f.pending
	f.pending = f.pending[:0]

	// pending and f.pending share the same underlying storage. As we iterate
	// over pending we append to f.pending, but only one entry is appended in
	// each iteration, after we have read the entry being overwritten.
	for _, s := range pending {
		if f.Cmp(key, s.End) < 0 {
			//   s: a--+--e
			// new:    c------
			if f.Cmp(s.Start.UserKey, key) < 0 {
				done = append(done, Span{
					Start: s.Start,
					End:   key,
					Value: s.Value,
				})
			}
			f.pending = append(f.pending, Span{
				Start: base.MakeInternalKey(key, s.Start.SeqNum(), s.Start.Kind()),
				End:   s.End,
				Value: s.Value,
			})
		} else {
			//   s: a-----e
			// new:       e----
			done = append(done, s)
		}
	}

	f.doneBuf = done[:0]
	f.flush(done, nil)
}

// flush a group of range spans to the block. The spans are required to all have
// the same start key. We flush all span fragments until startKey > lastKey. If
// lastKey is nil, all span fragments are flushed. The specification of a
// non-nil lastKey occurs for range deletion tombstones during compaction where
// we want to flush (but not truncate) all range tombstones that start at or
// before the first key in the next sstable. Consider:
//
//   a---e#10
//   a------h#9
//
// If a compaction splits the sstables at key c we want the first sstable to
// contain the tombstones [a,e)#10 and [a,e)#9. Fragmentation would naturally
// produce a tombstone [e,h)#9, but we don't need to output that tombstone to
// the first sstable.
func (f *Fragmenter) flush(buf []Span, lastKey []byte) {
	if invariants.RaceEnabled {
		f.checkInvariants(buf)
	}

	// Sort the spans by end key. This will allow us to walk over the spans and
	// easily determine the next split point (the smallest end-key).
	f.sortBuf.cmp = f.Cmp
	f.sortBuf.buf = buf
	sort.Sort(&f.sortBuf)

	// Loop over the spans, splitting by end key.
	for len(buf) > 0 {
		// A prefix of spans will end at split. remove represents the count of
		// that prefix.
		remove := 1
		split := buf[0].End
		f.flushBuf = append(f.flushBuf[:0], buf[0])

		for i := 1; i < len(buf); i++ {
			if f.Cmp(split, buf[i].End) == 0 {
				remove++
			}
			f.flushBuf = append(f.flushBuf, Span{
				Start: buf[i].Start,
				End:   split,
				Value: buf[i].Value,
			})
		}

		sort.Sort(&f.flushBuf)
		f.Emit(f.flushBuf)

		if lastKey != nil && f.Cmp(split, lastKey) > 0 {
			break
		}

		// Adjust the start key for every remaining span.
		buf = buf[remove:]
		for i := range buf {
			buf[i].Start.UserKey = split
		}
	}
}

// Finish flushes any remaining fragments to the output. It is an error to call
// this if any other spans will be added.
func (f *Fragmenter) Finish() {
	if f.finished {
		panic("pebble: span fragmenter already finished")
	}
	f.flush(f.pending, nil)
	f.finished = true
}

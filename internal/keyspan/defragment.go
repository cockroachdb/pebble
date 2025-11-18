// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"context"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/treesteps"
)

// BufferReuseMaxCapacity is the maximum capacity of a DefragmentingIter buffer
// that DefragmentingIter will reuse. Buffers larger than this will be
// discarded and reallocated as necessary.
const BufferReuseMaxCapacity = 10 << 10 // 10 KB

// keysReuseMaxCapacity is the maximum capacity of a []keyspan.Key buffer that
// DefragmentingIter will reuse. Buffers larger than this will be discarded and
// reallocated as necessary.
const keysReuseMaxCapacity = 100

// DefragmentMethod configures the defragmentation performed by the
// DefragmentingIter.
type DefragmentMethod interface {
	// ShouldDefragment takes two abutting spans and returns whether the two
	// spans should be combined into a single, defragmented Span.
	ShouldDefragment(suffixCmp base.CompareRangeSuffixes, left, right *Span) bool
}

// The DefragmentMethodFunc type is an adapter to allow the use of ordinary
// functions as DefragmentMethods. If f is a function with the appropriate
// signature, DefragmentMethodFunc(f) is a DefragmentMethod that calls f.
type DefragmentMethodFunc func(suffixCmp base.CompareRangeSuffixes, left, right *Span) bool

// ShouldDefragment calls f(equal, left, right).
func (f DefragmentMethodFunc) ShouldDefragment(
	suffixCmp base.CompareRangeSuffixes, left, right *Span,
) bool {
	return f(suffixCmp, left, right)
}

// DefragmentInternal configures a DefragmentingIter to defragment spans only if
// they have identical keys. It requires spans' keys to be sorted in trailer
// descending order.
//
// This defragmenting method is intended for use in compactions that may see
// internal range keys fragments that may now be joined, because the state that
// required their fragmentation has been dropped.
var DefragmentInternal DefragmentMethod = DefragmentMethodFunc(func(suffixCmp base.CompareRangeSuffixes, a, b *Span) bool {
	if a.KeysOrder != ByTrailerDesc || b.KeysOrder != ByTrailerDesc {
		panic("pebble: span keys unexpectedly not in trailer descending order")
	}
	if len(a.Keys) != len(b.Keys) {
		return false
	}
	for i := range a.Keys {
		if a.Keys[i].Trailer != b.Keys[i].Trailer {
			return false
		}
		if suffixCmp(a.Keys[i].Suffix, b.Keys[i].Suffix) != 0 {
			return false
		}
		if !bytes.Equal(a.Keys[i].Value, b.Keys[i].Value) {
			return false
		}
	}
	return true
})

// DefragmentReducer merges the current and next Key slices, returning a new Key
// slice.
//
// Implementations should modify and return `cur` to save on allocations, or
// consider allocating a new slice, as the `cur` slice may be retained by the
// DefragmentingIter and mutated. The `next` slice must not be mutated.
//
// The incoming slices are sorted by (SeqNum, Kind) descending. The output slice
// must also have this sort order.
type DefragmentReducer func(cur, next []Key) []Key

// StaticDefragmentReducer is a no-op DefragmentReducer that simply returns the
// current key slice, effectively retaining the first set of keys encountered
// for a defragmented span.
//
// This reducer can be used, for example, when the set of Keys for each Span
// being reduced is not expected to change, and therefore the keys from the
// first span encountered can be used without considering keys in subsequent
// spans.
var StaticDefragmentReducer DefragmentReducer = func(cur, _ []Key) []Key {
	return cur
}

// iterPos is an enum indicating the position of the defragmenting iter's
// wrapped iter. The defragmenting iter must look ahead or behind when
// defragmenting forward or backwards respectively, and this enum records that
// current position.
type iterPos int8

const (
	iterPosPrev iterPos = -1
	iterPosCurr iterPos = 0
	iterPosNext iterPos = +1
)

// DefragmentingIter wraps a key span iterator, defragmenting physical
// fragmentation during iteration.
//
// During flushes and compactions, keys applied over a span may be split at
// sstable boundaries. This fragmentation can produce internal key bounds that
// do not match any of the bounds ever supplied to a user operation. This
// physical fragmentation is necessary to avoid excessively wide sstables.
//
// The defragmenting iterator undoes this physical fragmentation, joining spans
// with abutting bounds and equal state. The defragmenting iterator takes a
// DefragmentMethod to determine what is "equal state" for a span. The
// DefragmentMethod is a function type, allowing arbitrary comparisons between
// Span keys.
//
// Seeking (SeekGE, SeekLT) poses an obstacle to defragmentation. A seek may
// land on a physical fragment in the middle of several fragments that must be
// defragmented. A seek that lands in a fragment straddling the seek key must
// first degfragment in the opposite direction of iteration to find the
// beginning of the defragmented span, and then defragments in the iteration
// direction, ensuring it's found a whole defragmented span.
type DefragmentingIter struct {
	// DefragmentingBuffers holds buffers used for copying iterator state.
	*DefragmentingBuffers
	comparer *base.Comparer
	equal    base.Equal
	iter     FragmentIterator
	iterSpan *Span
	iterPos  iterPos

	// curr holds the span at the current iterator position.
	curr Span

	// method is a comparison function for two spans. method is called when two
	// spans are abutting to determine whether they may be defragmented.
	// method does not itself check for adjacency for the two spans.
	method DefragmentMethod

	// reduce is the reducer function used to collect Keys across all spans that
	// constitute a defragmented span.
	reduce DefragmentReducer
}

// DefragmentingBuffers holds buffers used for copying iterator state.
type DefragmentingBuffers struct {
	// currBuf is a buffer for use when copying user keys for curr. currBuf is
	// cleared between positioning methods.
	currBuf bytealloc.A
	// keysBuf is a buffer for use when copying Keys for DefragmentingIter.curr.
	keysBuf []Key
	// keyBuf is a buffer specifically for the defragmented start key when
	// defragmenting backwards or the defragmented end key when defragmenting
	// forwards. These bounds are overwritten repeatedly during defragmentation,
	// and the defragmentation routines overwrite keyBuf repeatedly to store
	// these extended bounds.
	keyBuf []byte
}

// PrepareForReuse discards any excessively large buffers.
func (bufs *DefragmentingBuffers) PrepareForReuse() {
	if cap(bufs.currBuf) > BufferReuseMaxCapacity {
		bufs.currBuf = nil
	}
	if cap(bufs.keyBuf) > BufferReuseMaxCapacity {
		bufs.keyBuf = nil
	}
	if cap(bufs.keysBuf) > keysReuseMaxCapacity {
		bufs.keysBuf = nil
	}
}

// Assert that *DefragmentingIter implements the FragmentIterator interface.
var _ FragmentIterator = (*DefragmentingIter)(nil)

// Init initializes the defragmenting iter using the provided defragment
// method.
func (i *DefragmentingIter) Init(
	comparer *base.Comparer,
	iter FragmentIterator,
	equal DefragmentMethod,
	reducer DefragmentReducer,
	bufs *DefragmentingBuffers,
) {
	*i = DefragmentingIter{
		DefragmentingBuffers: bufs,
		comparer:             comparer,
		equal:                comparer.Equal,
		iter:                 iter,
		method:               equal,
		reduce:               reducer,
	}
}

// SetContext is part of the FragmentIterator interface.
func (i *DefragmentingIter) SetContext(ctx context.Context) {
	i.iter.SetContext(ctx)
}

// Close closes the underlying iterators.
func (i *DefragmentingIter) Close() {
	i.iter.Close()
}

// SeekGE moves the iterator to the first span covering a key greater than or
// equal to the given key. This is equivalent to seeking to the first span with
// an end key greater than the given key.
func (i *DefragmentingIter) SeekGE(key []byte) (*Span, error) {
	var err error
	i.iterSpan, err = i.iter.SeekGE(key)
	switch {
	case err != nil:
		return nil, err
	case i.iterSpan == nil:
		i.iterPos = iterPosCurr
		return nil, nil
	case i.iterSpan.Empty():
		i.iterPos = iterPosCurr
		return i.iterSpan, nil
	}
	// If the span starts strictly after key, we know there mustn't be an
	// earlier span that ends at i.iterSpan.Start, otherwise i.iter would've
	// returned that span instead.
	if i.comparer.Compare(i.iterSpan.Start, key) > 0 {
		return i.defragmentForward()
	}

	// The span we landed on has a Start bound ≤ key. There may be additional
	// fragments before this span. Defragment backward to find the start of the
	// defragmented span.
	if _, err := i.defragmentBackward(); err != nil {
		return nil, err
	}
	if i.iterPos == iterPosPrev {
		// Next once back onto the span.
		var err error
		i.iterSpan, err = i.iter.Next()
		if err != nil {
			return nil, err
		}
	}
	// Defragment the full span from its start.
	return i.defragmentForward()
}

// SeekLT moves the iterator to the last span covering a key less than the
// given key. This is equivalent to seeking to the last span with a start
// key less than the given key.
func (i *DefragmentingIter) SeekLT(key []byte) (*Span, error) {
	var err error
	i.iterSpan, err = i.iter.SeekLT(key)
	switch {
	case err != nil:
		return nil, err
	case i.iterSpan == nil:
		i.iterPos = iterPosCurr
		return nil, nil
	case i.iterSpan.Empty():
		i.iterPos = iterPosCurr
		return i.iterSpan, nil
	}
	// If the span ends strictly before key, we know there mustn't be a later
	// span that starts at i.iterSpan.End, otherwise i.iter would've returned
	// that span instead.
	if i.comparer.Compare(i.iterSpan.End, key) < 0 {
		return i.defragmentBackward()
	}

	// The span we landed on has a End bound ≥ key. There may be additional
	// fragments after this span. Defragment forward to find the end of the
	// defragmented span.
	if _, err := i.defragmentForward(); err != nil {
		return nil, err
	}

	if i.iterPos == iterPosNext {
		// Prev once back onto the span.
		var err error
		i.iterSpan, err = i.iter.Prev()
		if err != nil {
			return nil, err
		}
	}
	// Defragment the full span from its end.
	return i.defragmentBackward()
}

// First seeks the iterator to the first span and returns it.
func (i *DefragmentingIter) First() (*Span, error) {
	var err error
	i.iterSpan, err = i.iter.First()
	switch {
	case err != nil:
		return nil, err
	case i.iterSpan == nil:
		i.iterPos = iterPosCurr
		return nil, nil
	default:
		return i.defragmentForward()
	}
}

// Last seeks the iterator to the last span and returns it.
func (i *DefragmentingIter) Last() (*Span, error) {
	var err error
	i.iterSpan, err = i.iter.Last()
	switch {
	case err != nil:
		return nil, err
	case i.iterSpan == nil:
		i.iterPos = iterPosCurr
		return nil, nil
	default:
		return i.defragmentBackward()
	}
}

// Next advances to the next span and returns it.
func (i *DefragmentingIter) Next() (*Span, error) {
	switch i.iterPos {
	case iterPosPrev:
		// Switching directions; The iterator is currently positioned over the
		// last span of the previous set of fragments. In the below diagram,
		// the iterator is positioned over the last span that contributes to
		// the defragmented x position. We want to be positioned over the first
		// span that contributes to the z position.
		//
		//   x x x y y y z z z
		//       ^       ^
		//      old     new
		//
		// Next once to move onto y, defragment forward to land on the first z
		// position.
		var err error
		i.iterSpan, err = i.iter.Next()
		if err != nil {
			return nil, err
		} else if i.iterSpan == nil {
			panic("pebble: invariant violation: no next span while switching directions")
		}
		// We're now positioned on the first span that was defragmented into the
		// current iterator position. Skip over the rest of the current iterator
		// position's constitutent fragments. In the above example, this would
		// land on the first 'z'.
		if _, err = i.defragmentForward(); err != nil {
			return nil, err
		}
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil, nil
		}

		// Now that we're positioned over the first of the next set of
		// fragments, defragment forward.
		return i.defragmentForward()
	case iterPosCurr:
		// iterPosCurr is only used when the iter is exhausted or when the iterator
		// is at an empty span.
		if invariants.Enabled && i.iterSpan != nil && !i.iterSpan.Empty() {
			panic("pebble: invariant violation: iterPosCurr with valid iterSpan")
		}

		var err error
		i.iterSpan, err = i.iter.Next()
		if i.iterSpan == nil {
			// NB: err may be nil or non-nil.
			return nil, err
		}
		return i.defragmentForward()
	case iterPosNext:
		// Already at the next span.
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil, nil
		}
		return i.defragmentForward()
	default:
		panic("unreachable")
	}
}

// Prev steps back to the previous span and returns it.
func (i *DefragmentingIter) Prev() (*Span, error) {
	switch i.iterPos {
	case iterPosPrev:
		// Already at the previous span.
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil, nil
		}
		return i.defragmentBackward()
	case iterPosCurr:
		// iterPosCurr is only used when the iter is exhausted or when the iterator
		// is at an empty span.
		if invariants.Enabled && i.iterSpan != nil && !i.iterSpan.Empty() {
			panic("pebble: invariant violation: iterPosCurr with valid iterSpan")
		}

		var err error
		i.iterSpan, err = i.iter.Prev()
		if i.iterSpan == nil {
			// NB: err may be nil or non-nil.
			return nil, err
		}
		return i.defragmentBackward()
	case iterPosNext:
		// Switching directions; The iterator is currently positioned over the
		// first fragment of the next set of fragments. In the below diagram,
		// the iterator is positioned over the first span that contributes to
		// the defragmented z position. We want to be positioned over the last
		// span that contributes to the x position.
		//
		//   x x x y y y z z z
		//       ^       ^
		//      new     old
		//
		// Prev once to move onto y, defragment backward to land on the last x
		// position.
		var err error
		i.iterSpan, err = i.iter.Prev()
		if err != nil {
			return nil, err
		} else if i.iterSpan == nil {
			panic("pebble: invariant violation: no previous span while switching directions")
		}
		// We're now positioned on the last span that was defragmented into the
		// current iterator position. Skip over the rest of the current iterator
		// position's constitutent fragments. In the above example, this would
		// land on the last 'x'.
		if _, err = i.defragmentBackward(); err != nil {
			return nil, err
		}

		// Now that we're positioned over the last of the prev set of
		// fragments, defragment backward.
		if i.iterSpan == nil {
			i.iterPos = iterPosCurr
			return nil, nil
		}
		return i.defragmentBackward()
	default:
		panic("unreachable")
	}
}

// checkEqual checks the two spans for logical equivalence. It uses the passed-in
// DefragmentMethod and ensures both spans are NOT empty; not defragmenting empty
// spans is an optimization that lets us load fewer sstable blocks.
func (i *DefragmentingIter) checkEqual(left, right *Span) bool {
	return (!left.Empty() && !right.Empty()) && i.method.ShouldDefragment(i.comparer.CompareRangeSuffixes, i.iterSpan, &i.curr)
}

// defragmentForward defragments spans in the forward direction, starting from
// i.iter's current position. The span at the current position must be non-nil,
// but may be Empty().
func (i *DefragmentingIter) defragmentForward() (*Span, error) {
	if i.iterSpan.Empty() {
		// An empty span will never be equal to another span; see checkEqual for
		// why. To avoid loading non-empty range keys further ahead by calling Next,
		// return early.
		i.iterPos = iterPosCurr
		return i.iterSpan, nil
	}
	i.saveCurrent()

	var err error
	i.iterPos = iterPosNext
	i.iterSpan, err = i.iter.Next()
	for i.iterSpan != nil {
		if !i.equal(i.curr.End, i.iterSpan.Start) {
			// Not a continuation.
			break
		}
		if !i.checkEqual(i.iterSpan, &i.curr) {
			// Not a continuation.
			break
		}
		i.keyBuf = append(i.keyBuf[:0], i.iterSpan.End...)
		i.curr.End = i.keyBuf
		i.keysBuf = i.reduce(i.keysBuf, i.iterSpan.Keys)
		i.iterSpan, err = i.iter.Next()
	}
	// i.iterSpan == nil
	if err != nil {
		return nil, err
	}
	i.curr.Keys = i.keysBuf
	return &i.curr, nil
}

// defragmentBackward defragments spans in the backward direction, starting from
// i.iter's current position. The span at the current position must be non-nil,
// but may be Empty().
func (i *DefragmentingIter) defragmentBackward() (*Span, error) {
	if i.iterSpan.Empty() {
		// An empty span will never be equal to another span; see checkEqual for
		// why. To avoid loading non-empty range keys further ahead by calling Next,
		// return early.
		i.iterPos = iterPosCurr
		return i.iterSpan, nil
	}
	i.saveCurrent()

	var err error
	i.iterPos = iterPosPrev
	i.iterSpan, err = i.iter.Prev()
	for i.iterSpan != nil {
		if !i.equal(i.curr.Start, i.iterSpan.End) {
			// Not a continuation.
			break
		}
		if !i.checkEqual(i.iterSpan, &i.curr) {
			// Not a continuation.
			break
		}
		i.keyBuf = append(i.keyBuf[:0], i.iterSpan.Start...)
		i.curr.Start = i.keyBuf
		i.keysBuf = i.reduce(i.keysBuf, i.iterSpan.Keys)
		i.iterSpan, err = i.iter.Prev()
	}
	// i.iterSpan == nil
	if err != nil {
		return nil, err
	}
	i.curr.Keys = i.keysBuf
	return &i.curr, nil
}

func (i *DefragmentingIter) saveCurrent() {
	i.currBuf.Reset()
	i.keysBuf = i.keysBuf[:0]
	i.keyBuf = i.keyBuf[:0]
	if i.iterSpan == nil {
		return
	}
	i.curr = Span{
		Start:     i.saveBytes(i.iterSpan.Start),
		End:       i.saveBytes(i.iterSpan.End),
		KeysOrder: i.iterSpan.KeysOrder,
	}
	for j := range i.iterSpan.Keys {
		i.keysBuf = append(i.keysBuf, Key{
			Trailer: i.iterSpan.Keys[j].Trailer,
			Suffix:  i.saveBytes(i.iterSpan.Keys[j].Suffix),
			Value:   i.saveBytes(i.iterSpan.Keys[j].Value),
		})
	}
	i.curr.Keys = i.keysBuf
}

func (i *DefragmentingIter) saveBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	i.currBuf, b = i.currBuf.Copy(b)
	return b
}

// WrapChildren implements FragmentIterator.
func (i *DefragmentingIter) WrapChildren(wrap WrapFn) {
	i.iter = wrap(i.iter)
}

// TreeStepsNode is part of the FragmentIterator interface.
func (i *DefragmentingIter) TreeStepsNode() treesteps.NodeInfo {
	info := treesteps.NodeInfof(i, "%T(%p)", i, i)
	info.AddChildren(i.iter)
	return info
}

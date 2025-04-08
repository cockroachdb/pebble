// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
)

// fragmentIter wraps an Iter, implementing the keyspan.FragmentIterator
// interface. It's used for reading range deletion and range key blocks.
//
// Range deletions and range keys are fragmented before they're persisted to the
// block. Overlapping fragments have identical bounds.  The fragmentIter gathers
// all the fragments with identical bounds within a block and returns a single
// keyspan.Span describing all the keys defined over the span.
//
// # Memory lifetime
//
// A Span returned by fragmentIter is only guaranteed to be stable until the
// next fragmentIter iteration positioning method. A Span's Keys slice may be
// reused, so the user must not assume it's stable.
//
// Blocks holding range deletions and range keys are configured to use a restart
// interval of 1. This provides key stability. The caller may treat the various
// byte slices (start, end, suffix, value) as stable for the lifetime of the
// iterator.
type fragmentIter struct {
	suffixCmp base.CompareRangeSuffixes
	blockIter Iter
	keyBuf    [2]keyspan.Key
	span      keyspan.Span
	dir       int8

	// fileNum is used for logging/debugging.
	fileNum base.DiskFileNum

	syntheticPrefixAndSuffix block.SyntheticPrefixAndSuffix
	// startKeyBuf is a buffer that is reused to store the start key of the span
	// when a synthetic prefix is used.
	startKeyBuf []byte
	// endKeyBuf is a buffer that is reused to generate the end key of the span
	// when a synthetic prefix is set. It always starts with syntheticPrefix.
	endKeyBuf []byte

	closeCheck invariants.CloseChecker
}

var _ keyspan.FragmentIterator = (*fragmentIter)(nil)

var fragmentBlockIterPool = sync.Pool{
	New: func() interface{} {
		i := &fragmentIter{}
		if invariants.UseFinalizers {
			invariants.SetFinalizer(i, checkFragmentBlockIterator)
		}
		return i
	},
}

// NewFragmentIter returns a new keyspan iterator that iterates over a block's
// spans.
func NewFragmentIter(
	fileNum base.DiskFileNum,
	comparer *base.Comparer,
	blockHandle block.BufferHandle,
	transforms block.FragmentIterTransforms,
) (keyspan.FragmentIterator, error) {
	i := fragmentBlockIterPool.Get().(*fragmentIter)

	i.suffixCmp = comparer.CompareRangeSuffixes
	// Use the i.keyBuf array to back the Keys slice to prevent an allocation
	// when the spans contain few keys.
	i.span.Keys = i.keyBuf[:0]
	i.fileNum = fileNum
	i.syntheticPrefixAndSuffix = transforms.SyntheticPrefixAndSuffix
	if transforms.HasSyntheticPrefix() {
		i.endKeyBuf = append(i.endKeyBuf[:0], transforms.SyntheticPrefix()...)
	}
	i.closeCheck = invariants.CloseChecker{}

	if err := i.blockIter.InitHandle(comparer, blockHandle, block.IterTransforms{
		SyntheticSeqNum: transforms.SyntheticSeqNum,
		// We let the blockIter prepend the prefix to span start keys; the fragment
		// iterator will prepend it for end keys. We could do everything in the
		// fragment iterator, but we'd have to duplicate the logic for adjusting the
		// seek key for SeekGE/SeekLT.
		SyntheticPrefixAndSuffix: transforms.SyntheticPrefixAndSuffix.RemoveSuffix(),
		// It's okay for HideObsoletePoints to be false here, even for shared
		// ingested sstables. This is because rangedels do not apply to points in
		// the same sstable at the same sequence number anyway, so exposing obsolete
		// rangedels is harmless.
		HideObsoletePoints: false,
	}); err != nil {
		i.Close()
		return nil, err
	}
	return i, nil
}

// initSpan initializes the span with a single fragment.
//
// Note that the span start and end keys and range key contents are aliased to
// the key or value when we don't have a synthetic prefix. This is ok because
// the range del/key block doesn't use prefix compression, so the key/value will
// be pointing directly into the buffer data.
func (i *fragmentIter) initSpan(ik base.InternalKey, internalValue []byte) error {
	if ik.Kind() == base.InternalKeyKindRangeDelete {
		i.span = rangedel.Decode(ik, internalValue, i.span.Keys[:0])
	} else {
		var err error
		i.span, err = rangekey.Decode(ik, internalValue, i.span.Keys[:0])
		if err != nil {
			return err
		}
	}
	// When synthetic prefix is used in the blockIter, the keys cannot be used
	// across multiple blockIter operations; we have to make a copy in this case.
	if i.syntheticPrefixAndSuffix.HasPrefix() || invariants.Sometimes(10) {
		i.startKeyBuf = append(i.startKeyBuf[:0], i.span.Start...)
		i.span.Start = i.startKeyBuf
	}
	return nil
}

// addToSpan adds a fragment to the existing span. The fragment must be for the
// same start/end keys.
func (i *fragmentIter) addToSpan(
	cmp base.Compare, ik base.InternalKey, internalValue []byte,
) error {
	var err error
	if ik.Kind() == base.InternalKeyKindRangeDelete {
		err = rangedel.DecodeIntoSpan(cmp, ik, internalValue, &i.span)
	} else {
		err = rangekey.DecodeIntoSpan(cmp, ik, internalValue, &i.span)
	}
	return err
}

// applySpanTransforms applies changes to the span that we decoded, if
// appropriate.
func (i *fragmentIter) applySpanTransforms() error {
	if i.syntheticPrefixAndSuffix.HasPrefix() || invariants.Sometimes(10) {
		syntheticPrefix := i.syntheticPrefixAndSuffix.Prefix()
		// We have to make a copy of the start key because it will not stay valid
		// across multiple blockIter operations.
		i.startKeyBuf = append(i.startKeyBuf[:0], i.span.Start...)
		i.span.Start = i.startKeyBuf
		if invariants.Enabled && !bytes.Equal(syntheticPrefix, i.endKeyBuf[:len(syntheticPrefix)]) {
			panic("pebble: invariant violation: synthetic prefix mismatch")
		}
		i.endKeyBuf = append(i.endKeyBuf[:len(syntheticPrefix)], i.span.End...)
		i.span.End = i.endKeyBuf
	}

	if i.syntheticPrefixAndSuffix.HasSuffix() {
		syntheticSuffix := i.syntheticPrefixAndSuffix.Suffix()
		for keyIdx := range i.span.Keys {
			k := &i.span.Keys[keyIdx]

			switch k.Kind() {
			case base.InternalKeyKindRangeKeySet:
				if len(k.Suffix) > 0 {
					if invariants.Enabled && i.suffixCmp(syntheticSuffix, k.Suffix) >= 0 {
						return base.AssertionFailedf("synthetic suffix %q >= RangeKeySet suffix %q",
							syntheticSuffix, k.Suffix)
					}
					k.Suffix = syntheticSuffix
				}
			case base.InternalKeyKindRangeKeyDelete:
				// Nothing to do.
			default:
				return base.AssertionFailedf("synthetic suffix not supported with key kind %s", k.Kind())
			}
		}
	}
	return nil
}

// gatherForward gathers internal keys with identical bounds. Keys defined over
// spans of the keyspace are fragmented such that any overlapping key spans have
// identical bounds. When these spans are persisted to a range deletion or range
// key block, they may be persisted as multiple internal keys in order to encode
// multiple sequence numbers or key kinds.
//
// gatherForward iterates forward, re-combining the fragmented internal keys to
// reconstruct a keyspan.Span that holds all the keys defined over the span.
func (i *fragmentIter) gatherForward(kv *base.InternalKV) (*keyspan.Span, error) {
	i.span = keyspan.Span{}
	if kv == nil || !i.blockIter.Valid() {
		return nil, nil
	}
	// Use the i.keyBuf array to back the Keys slice to prevent an allocation
	// when a span contains few keys.
	i.span.Keys = i.keyBuf[:0]

	// Decode the span's end key and individual keys from the value.
	if err := i.initSpan(kv.K, kv.InPlaceValue()); err != nil {
		return nil, err
	}

	// There might exist additional internal keys with identical bounds encoded
	// within the block. Iterate forward, accumulating all the keys with
	// identical bounds to s.

	// Overlapping fragments are required to have exactly equal start and
	// end bounds.
	for kv = i.blockIter.Next(); kv != nil && i.blockIter.cmp(kv.K.UserKey, i.span.Start) == 0; kv = i.blockIter.Next() {
		if err := i.addToSpan(i.blockIter.cmp, kv.K, kv.InPlaceValue()); err != nil {
			return nil, err
		}
	}
	if err := i.applySpanTransforms(); err != nil {
		return nil, err
	}

	// Apply a consistent ordering.
	keyspan.SortKeysByTrailer(i.span.Keys)

	// i.blockIter is positioned over the first internal key for the next span.
	return &i.span, nil
}

// gatherBackward gathers internal keys with identical bounds. Keys defined over
// spans of the keyspace are fragmented such that any overlapping key spans have
// identical bounds. When these spans are persisted to a range deletion or range
// key block, they may be persisted as multiple internal keys in order to encode
// multiple sequence numbers or key kinds.
//
// gatherBackward iterates backwards, re-combining the fragmented internal keys
// to reconstruct a keyspan.Span that holds all the keys defined over the span.
func (i *fragmentIter) gatherBackward(kv *base.InternalKV) (*keyspan.Span, error) {
	i.span = keyspan.Span{}
	if kv == nil || !i.blockIter.Valid() {
		return nil, nil
	}

	// Decode the span's end key and individual keys from the value.
	if err := i.initSpan(kv.K, kv.InPlaceValue()); err != nil {
		return nil, err
	}

	// There might exist additional internal keys with identical bounds encoded
	// within the block. Iterate backward, accumulating all the keys with
	// identical bounds to s.
	//
	// Overlapping fragments are required to have exactly equal start and
	// end bounds.
	for kv = i.blockIter.Prev(); kv != nil && i.blockIter.cmp(kv.K.UserKey, i.span.Start) == 0; kv = i.blockIter.Prev() {
		if err := i.addToSpan(i.blockIter.cmp, kv.K, kv.InPlaceValue()); err != nil {
			return nil, err
		}
	}
	// i.blockIter is positioned over the last internal key for the previous
	// span.

	// Apply a consistent ordering.
	keyspan.SortKeysByTrailer(i.span.Keys)

	if err := i.applySpanTransforms(); err != nil {
		return nil, err
	}
	return &i.span, nil
}

// SetContext is part of the FragmentIterator interface.
func (i *fragmentIter) SetContext(ctx context.Context) {}

// Close implements (keyspan.FragmentIterator).Close.
func (i *fragmentIter) Close() {
	_ = i.blockIter.Close()
	i.closeCheck.Close()

	if invariants.Sometimes(25) {
		// In invariants mode, sometimes don't add the object to the pool so that we
		// can check for double closes that take longer than the object stays in the
		// pool.
		return
	}
	i.span = keyspan.Span{}
	i.dir = 0
	i.fileNum = 0
	i.syntheticPrefixAndSuffix = block.SyntheticPrefixAndSuffix{}
	i.startKeyBuf = i.startKeyBuf[:0]
	i.endKeyBuf = i.endKeyBuf[:0]
	fragmentBlockIterPool.Put(i)
}

// First implements (keyspan.FragmentIterator).First
func (i *fragmentIter) First() (*keyspan.Span, error) {
	i.dir = +1
	return i.gatherForward(i.blockIter.First())
}

// Last implements (keyspan.FragmentIterator).Last.
func (i *fragmentIter) Last() (*keyspan.Span, error) {
	i.dir = -1
	return i.gatherBackward(i.blockIter.Last())
}

// Next implements (keyspan.FragmentIterator).Next.
func (i *fragmentIter) Next() (*keyspan.Span, error) {
	switch {
	case i.dir == -1 && !i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is exhausted, before the first key. Move onto the first.
		i.blockIter.First()
		i.dir = +1
	case i.dir == -1 && i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is currently positioned over the last internal key for
		// the previous span. Next it once to move to the first internal key
		// that makes up the current span, and gatherForwaad to land on the
		// first internal key making up the next span.
		//
		// In the diagram below, if the last span returned to the user during
		// reverse iteration was [b,c), i.blockIter is currently positioned at
		// [a,b). The block iter must be positioned over [d,e) to gather the
		// next span's fragments.
		//
		//    ... [a,b) [b,c) [b,c) [b,c) [d,e) ...
		//          ^                       ^
		//     i.blockIter                 want
		if x, err := i.gatherForward(i.blockIter.Next()); err != nil {
			return nil, err
		} else if invariants.Enabled && !x.Valid() {
			panic("pebble: invariant violation: next entry unexpectedly invalid")
		}
		i.dir = +1
	}
	// We know that this blockIter has in-place values.
	return i.gatherForward(i.blockIter.KV())
}

// Prev implements (keyspan.FragmentIterator).Prev.
func (i *fragmentIter) Prev() (*keyspan.Span, error) {
	switch {
	case i.dir == +1 && !i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is exhausted, after the last key. Move onto the last.
		i.blockIter.Last()
		i.dir = -1
	case i.dir == +1 && i.span.Valid():
		// Switching directions.
		//
		// i.blockIter is currently positioned over the first internal key for
		// the next span. Prev it once to move to the last internal key that
		// makes up the current span, and gatherBackward to land on the last
		// internal key making up the previous span.
		//
		// In the diagram below, if the last span returned to the user during
		// forward iteration was [b,c), i.blockIter is currently positioned at
		// [d,e). The block iter must be positioned over [a,b) to gather the
		// previous span's fragments.
		//
		//    ... [a,b) [b,c) [b,c) [b,c) [d,e) ...
		//          ^                       ^
		//        want                  i.blockIter
		if x, err := i.gatherBackward(i.blockIter.Prev()); err != nil {
			return nil, err
		} else if invariants.Enabled && !x.Valid() {
			panic("pebble: invariant violation: previous entry unexpectedly invalid")
		}
		i.dir = -1
	}
	// We know that this blockIter has in-place values.
	return i.gatherBackward(i.blockIter.KV())
}

// SeekGE implements (keyspan.FragmentIterator).SeekGE.
func (i *fragmentIter) SeekGE(k []byte) (*keyspan.Span, error) {
	if s, err := i.SeekLT(k); err != nil {
		return nil, err
	} else if s != nil && i.blockIter.cmp(k, s.End) < 0 {
		return s, nil
	}
	// TODO(jackson): If the above i.SeekLT(k) discovers a span but the span
	// doesn't meet the k < s.End comparison, then there's no need for the
	// SeekLT to gatherBackward.
	return i.Next()
}

// SeekLT implements (keyspan.FragmentIterator).SeekLT.
func (i *fragmentIter) SeekLT(k []byte) (*keyspan.Span, error) {
	i.dir = -1
	return i.gatherBackward(i.blockIter.SeekLT(k, base.SeekLTFlagsNone))
}

// String implements fmt.Stringer.
func (i *fragmentIter) String() string {
	return "fragment-block-iter"
}

// WrapChildren implements FragmentIterator.
func (i *fragmentIter) WrapChildren(wrap keyspan.WrapFn) {}

// DebugTree is part of the FragmentIterator interface.
func (i *fragmentIter) DebugTree(tp treeprinter.Node) {
	tp.Childf("%T(%p) fileNum=%s", i, i, i.fileNum)
}

func checkFragmentBlockIterator(obj interface{}) {
	i := obj.(*fragmentIter)
	if h := i.blockIter.Handle(); h.Valid() {
		fmt.Fprintf(os.Stderr, "fragmentBlockIter.blockIter.handle is not nil: %#v\n", h)
		os.Exit(1)
	}
}

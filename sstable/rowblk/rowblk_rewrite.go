// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rowblk

import (
	"bytes"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/sstable/block"
)

// NewRewriter constructs a new rewriter.
func NewRewriter(comparer *base.Comparer, restartInterval int) *Rewriter {
	rw := &Rewriter{comparer: comparer}
	rw.writer.RestartInterval = restartInterval
	return rw
}

// Rewriter may be used to rewrite row-based blocks.
type Rewriter struct {
	comparer *base.Comparer
	// bufs resued each call to Rewrite.
	writer     Writer
	iter       Iter
	scratchKey base.InternalKey
	// alloc grown throughout the lifetime of the rewriter.
	keyAlloc bytealloc.A
}

// RewriteSuffixes rewrites the input block. It expects the input block to only
// contain keys with the suffix `from`. It rewrites the block to contain the
// same keys with the suffix `to`.
//
// RewriteSuffixes returns the start and end keys of the rewritten block, and the
// finished rewritten block.
func (r *Rewriter) RewriteSuffixes(
	input []byte, from []byte, to []byte,
) (start, end base.InternalKey, rewritten []byte, err error) {
	if err := r.iter.Init(r.comparer.Compare, r.comparer.ComparePointSuffixes, r.comparer.Split, input, block.NoTransforms); err != nil {
		return base.InternalKey{}, base.InternalKey{}, nil, err
	}
	if cap(r.writer.restarts) < int(r.iter.restarts) {
		r.writer.restarts = make([]uint32, 0, r.iter.restarts)
	}
	if cap(r.writer.buf) == 0 {
		r.writer.buf = make([]byte, 0, len(input))
	}
	if cap(r.writer.restarts) < int(r.iter.numRestarts) {
		r.writer.restarts = make([]uint32, 0, r.iter.numRestarts)
	}
	for kv := r.iter.First(); kv != nil; kv = r.iter.Next() {
		if kv.Kind() != base.InternalKeyKindSet {
			return base.InternalKey{}, base.InternalKey{}, nil,
				errors.New("key does not have expected kind (set)")
		}
		si := r.comparer.Split(kv.K.UserKey)
		oldSuffix := kv.K.UserKey[si:]
		if !bytes.Equal(oldSuffix, from) {
			return base.InternalKey{}, base.InternalKey{}, nil,
				errors.Errorf("key has suffix %q, expected %q", oldSuffix, from)
		}
		newLen := si + len(to)
		if cap(r.scratchKey.UserKey) < newLen {
			r.scratchKey.UserKey = make([]byte, 0, len(kv.K.UserKey)*2+len(to)-len(from))
		}

		r.scratchKey.Trailer = kv.K.Trailer
		r.scratchKey.UserKey = r.scratchKey.UserKey[:newLen]
		copy(r.scratchKey.UserKey, kv.K.UserKey[:si])
		copy(r.scratchKey.UserKey[si:], to)

		// NB: for TableFormatPebblev3 and higher, since
		// !iter.lazyValueHandling.hasValuePrefix, it will return the raw value
		// in the block, which includes the 1-byte prefix. This is fine since bw
		// also does not know about the prefix and will preserve it in bw.add.
		v := kv.InPlaceValue()
		if err := r.writer.Add(r.scratchKey, v); err != nil {
			return base.InternalKey{}, base.InternalKey{}, nil, err
		}
		if start.UserKey == nil {
			// Copy the first key.
			start.Trailer = r.scratchKey.Trailer
			r.keyAlloc, start.UserKey = r.keyAlloc.Copy(r.scratchKey.UserKey)
		}
	}
	// Copy the last key.
	end.Trailer = r.scratchKey.Trailer
	r.keyAlloc, end.UserKey = r.keyAlloc.Copy(r.scratchKey.UserKey)

	_ = r.iter.Close() // infallible
	return start, end, r.writer.Finish(), nil
}

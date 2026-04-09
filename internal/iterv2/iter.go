// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"strings"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/buildtags"
	"github.com/cockroachdb/pebble/internal/keyspan"
)

// Enabled controls whether the V2 merging iterator is used in the iterator
// stack. When true, constructPointIter builds a mergingIterV2 over
// levelIterV2/InterleavingIter children instead of the V1 mergingIter.
const Enabled = buildtags.IterV2

// Iter is a base.InternalIterator that also exposes span information.
//
// # Key space partitioning
//
// Every Iter operates over a key range defined by optional lower and upper
// bounds. This range is partitioned into contiguous Spans (see iterv2.Span and
// keyspan.Span). Each span covers a half-open user key range and may contain
// zero or more span Keys (e.g. RANGEDEL keys). A span with no Keys is "empty"
// but still covers a region of the key space.
//
// Span().BoundaryType is never BoundaryNone when the iterator is positioned.
//
// Example: given an Iter with bounds [a, z), point keys {a, b, c, d, e, h, i},
// and a RANGEDEL span [c, g), the key space is partitioned into three Spans:
//
//	     [a, c) empty           [c, g) RANGEDEL          [g, z) empty
//	 |-------------------| |------------------------| |------------------
//	 a       b            c        d       e         g        h       i
//	 .       .            ^        .       .         ^        .       .
//	point   point      boundary   point   point   boundary   point   point
//	                     key                        key
//
// # Boundary keys
//
// When the iterator is about to leave the current Span, it returns a synthetic
// key with kind InternalKeyKindSpanBoundary and sequence number SeqNumMax. The
// user key is the Span boundary being crossed: Span().Boundary in the
// current iteration direction.
//
// Because boundary keys use SeqNumMax, they sort before any point key with the
// same user key. When a point key coincides with a span boundary (e.g. point
// key "c" at the start of span [c, g)), the boundary key for the previous span
// is returned first, followed by the point key (now in the new span).
//
// At a boundary key, Span() still returns the Span being exited; it is updated
// to the adjacent Span on the subsequent Next or Prev call.
//
// When the iterator has a lower/upper bound, there will be a boundary key
// emitted for that bound. If there is no bound, there is no boundary key
// emitted before exhaustion.
//
// # Forward iteration example
//
// Continuing the example above, Span().Boundary shows the next boundary
// in the forward direction (the span's End):
//
//	Call   | Returned key     | Span()
//	-------+------------------+----------------------------
//	First  | a#10,SET         | [?, c)  (empty span)
//	Next   | b#8,SET          | [?, c)  (empty span)
//	Next   | c#inf,BDRY       | [?, c)  (empty, exiting)
//	Next   | c#5,SET          | [?, g)  RANGEDEL
//	Next   | d#4,SET          | [?, g)  RANGEDEL
//	Next   | e#2,SET          | [?, g)  RANGEDEL
//	Next   | g#inf,BDRY       | [?, g)  RANGEDEL (exiting)
//	Next   | h#6,SET          | [?, z)  (empty span)
//	Next   | i#1,SET          | [?, z)  (empty span)
//	Next   | z#inf,BDRY       | [?, z)  (empty, exiting)
//	Next   | nil (exhausted)  |
//
// # Spurious span boundaries
//
// It is acceptable for an Iter to expose spurious span boundaries (i.e. two
// abutting Spans with the same keys). This is used when getting more
// information requires extra work. For example, the level iterator exposes
// information from the current file without opening the next file(s).
//
// # Amendments to the InternalIterator methods
//
// ## SeekPrefixGE
//
// Like SeekGE, but stops returning point keys once the prefix no longer matches
// and becomes exhausted after it returns the first boundary key with a
// different prefix. The result is not simply a truncation of what SeekGE would
// have returned - we will skip point keys with non-matching prefix up to the
// first non-matching boundary.
//
// To illustrate the reason for these semantics, imagine the iterator has no
// point keys and no boundaries with the given prefix:
//
//	Points:     c@2  c@1 ...      d#BOUNDARY
//	Spans    |--- [a, d):RANGEDEL ---|
//
// # If a SeekPrefixGE(b) returned nil, the caller would not discover that we
// have a RANGEDEL covering the entire key space for prefix b. Instead,
// SeekPrefixGE(b) returns the d#BOUNDARY key and exposes the RANGEDEL in the
// Span().
//
// A more nuanced example:
//
//	Points:  a@8 a@7 a@6#BOUNDARY  b@2  b@1 ...        d#BOUNDARY
//	Spans    --------------------|--- [a@6, d):RANGEDEL ---|
//
// Consider SeekPrefixGE(a). If it returns a@8, a@7, a@6#BOUNDARY and then
// becomes exhausted, we will never discover the [a@6, d):RANGEDEL. Inside the
// merging iterator, this RANGEDEL is important because it could shadow keys
// like a@3 on a lower level.
//
// ## NextPrefix
//
// NextPrefix(succKey) is still equivalent to SeekGE(succKey), but can only be
// called when the iterator is positioned at a *point* key, not at a boundary
// key.
type Iter interface {
	base.InternalIterator

	// Span returns information about the current span. When the iterator is
	// positioned, the span can be empty (len(Keys)==0). The span always contains
	// the last key returned by the iterator, except a synthetic boundary key when
	// iterating the forward direction.
	//
	// When the iterator is exhausted or not positioned (i.e. before any seeking
	// operation or after SetBounds), the Span has zero-value fields (BoundaryType
	// is BoundaryNone).
	//
	// Span returns a stable pointer to a Span embedded in the iterator (which is
	// updated after iterator operations). Callers can call it once and stash the
	// pointer. Span never returns nil, even if the iterator is exhausted or not
	// positioned.
	Span() *Span
}

// BoundaryType indicates which boundary of the span is exposed.
type BoundaryType int8

const (
	// BoundaryNone indicates the iterator is unpositioned or exhausted.
	BoundaryNone BoundaryType = iota
	// BoundaryEnd indicates forward iteration: Boundary is the exclusive end.
	BoundaryEnd
	// BoundaryStart indicates backward iteration: Boundary is the inclusive start.
	BoundaryStart
)

// Span is an abbreviated version of keyspan.Span, only exposing one boundary
// (the next boundary in the direction of iteration). A Span can have an empty
// set of keys.
//
// Motivation: after (say) a SeekGE, we typically don't care about what's before
// the seek key; obtaining the span start key might require work, like opening
// the previous file in the level iterator.
type Span struct {
	// BoundaryType indicates which boundary is exposed:
	//   - BoundaryEnd: forward iteration; Boundary is the exclusive end.
	//   - BoundaryStart: backward iteration; Boundary is the inclusive start.
	//   - BoundaryNone: unpositioned or exhausted.
	BoundaryType BoundaryType
	// Boundary is the exclusive End boundary of this span if BoundaryType is
	// BoundaryEnd (i.e. after a First/SeekGE/SeekPrefixGE/Next/NextPrefix call)
	// or the inclusive Start boundary of this span if BoundaryType is
	// BoundaryStart (i.e. after a Last/SeekLT/Prev call).
	//
	// Boundary can be nil when the iteration range is unbounded in the
	// corresponding direction: nil for BoundaryEnd means no upper bound, nil for
	// BoundaryStart means no lower bound. When Boundary is nil, no boundary key
	// is emitted at that edge; the iterator simply exhausts.
	Boundary []byte
	// Keys holds the set of keys for this span, by (SeqNum, Kind) descending.
	// Keys may be empty, even when BoundaryType is not BoundaryNone.
	Keys []keyspan.Key
}

// Valid returns true if the Span is positioned (BoundaryType is not
// BoundaryNone).
func (s *Span) Valid() bool {
	return s.BoundaryType != BoundaryNone
}

// String returns a human-readable representation of the Span.
func (s *Span) String() string {
	var b strings.Builder
	switch s.BoundaryType {
	case BoundaryNone:
		return "<no span>"
	case BoundaryEnd:
		b.WriteString("[?, ")
		if s.Boundary != nil {
			b.Write(s.Boundary)
		} else {
			b.WriteString("\u221e")
		}
		b.WriteByte(')')
	case BoundaryStart:
		b.WriteByte('[')
		if s.Boundary != nil {
			b.Write(s.Boundary)
		} else {
			b.WriteString("-\u221e")
		}
		b.WriteString(", ?)")
	default:
		return "<invalid BoundaryType>"
	}
	if len(s.Keys) > 0 {
		b.WriteString(" {")
		for i, k := range s.Keys {
			if i > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(k.String())
		}
		b.WriteByte('}')
	}
	return b.String()
}

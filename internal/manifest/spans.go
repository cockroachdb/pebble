// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"sort"

	"github.com/cockroachdb/pebble/internal/base"
)

// Span is a key span as part of a set of Spans. A span is inclusive on both ends,
// as motivated by file bounds in FileMetadata.
type Span struct {
	Start, End []byte
}

// Equal checks whether two spans are equal.
func (s *Span) Equal(other Span, cmp base.Compare) bool {
	return cmp(s.Start, other.Start) == 0 && cmp(s.End, other.End) == 0
}

// Spans is a de-duplicated set of disjoint spans. Adding a duplicate span
// using Add is a no-op, and adding an overlapping span merges it into existing
// overlapping spans. Not safe for concurrent use.
type Spans struct {
	Cmp base.Compare

	spans []Span
}

// Add appends the specified span to the end of the spanset. Sorting/merging
// is done as part of Finish if necessary.
func (s *Spans) Add(span Span) {
	s.spans = append(s.spans, span)
}

// Finish sorts the underlying spanset. Must be called after a sequence of Add
// operations that were not sorted, to maintain sort order. Does not need to be
// called if entries in Add were sorted.
func (s *Spans) Finish() {
	// Sort the spans by start key.
	sort.Slice(s.spans, func(i, j int) bool {
		return s.Cmp(s.spans[i].Start, s.spans[j].Start) < 0
	})

	// Iterate through the sorted spans. Three possible cases:
	// 1) The RHS span is completely covered by the LHS span -> delete RHS span
	// 2) The RHS span has a partial overlap with the LHS span -> change LHS
	//    span's end key and delete RHS span.
	// 3) The RHS span does not overlap at all with LHS span -> do nothing
	for i := 1; i < len(s.spans); {
		if s.Cmp(s.spans[i-1].End, s.spans[i].Start) < 0 {
			// No overlap - case 3.
			i++
			continue
		}
		if s.Cmp(s.spans[i-1].End, s.spans[i].End) < 0 {
			// Partial overlap - case 2. Extend i-1's end.
			s.spans[i-1].End = s.spans[i].End
		}
		// Cases 1 and 2. Delete RHS span. Note that we do *not* increment i.
		n := copy(s.spans[i:], s.spans[i+1:])
		s.spans = s.spans[:i+n]
	}
}

// Get returns the underlying, sorted SpanSet.
func (s *Spans) Get() []Span {
	return s.spans
}

// Empty returns if the spanset is empty.
func (s *Spans) Empty() bool {
	return len(s.spans) == 0
}

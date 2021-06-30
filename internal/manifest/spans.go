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

// Add merges the new span into the set of spans. If this span already existed
// in the span set, added is false, otherwise it's true.
func (s *Spans) Add(span Span) (added bool) {
	startIdx := sort.Search(len(s.spans), func(i int) bool {
		return s.Cmp(s.spans[i].End, span.Start) >= 0
	})
	endIdx := sort.Search(len(s.spans), func(i int) bool {
		return s.Cmp(s.spans[i].Start, span.End) > 0
	})

	if startIdx == len(s.spans) {
		// Fast path: append at the end.
		s.spans = append(s.spans, span)
		return true
	}
	if endIdx == startIdx {
		// No-replace case. Insert between endIdx and startIdx.
		s.spans = append(s.spans, Span{})
		copy(s.spans[startIdx+1:], s.spans[startIdx:])
		s.spans[startIdx] = span
		return true
	}
	// startIdx < endIdx.
	//
	// Check if we are adding a duplicate.
	if startIdx == endIdx - 1 && s.spans[startIdx].Equal(span, s.Cmp) {
		return false
	}
	// Replace everything between s.spans[startIdx:endIdx] with `span` after
	// modifying its bounds.
	firstSpan := s.spans[startIdx]
	lastSpan := s.spans[endIdx-1]
	if s.Cmp(firstSpan.Start, span.Start) < 0 {
		span.Start = firstSpan.Start
	}
	if s.Cmp(lastSpan.End, span.End) > 0 {
		span.End = lastSpan.End
	}
	s.spans[startIdx] = span
	n := copy(s.spans[startIdx+1:], s.spans[endIdx:])
	s.spans = s.spans[:startIdx+1+n]
	return true
}

// Get returns the underlying, sorted SpanSet.
func (s *Spans) Get() []Span {
	return s.spans
}

// Empty returns if the spanset is empty.
func (s *Spans) Empty() bool {
	return len(s.spans) == 0
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan // import "github.com/cockroachdb/pebble/internal/keyspan"

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// Span represents a key-kind over a span of user keys, along with the
// sequence number at which it was written and optionally a value.
//
// Conceptually it represents (key-kind, [start, end))#seqnum. The key
// kind, start and sequence number are stored in a base.InternalKey
// struct in the Start field out of convenience. The end user key is
// stored separately as the End field.
//
// Note that the start user key is inclusive and the end user key is
// exclusive.
//
// Currently the only supported key kinds are: RANGEDEL, RANGEKEYSET,
// RANGEKEYUNSET and RANGEKEYDEL.
type Span struct {
	Start base.InternalKey
	End   []byte
	Value []byte // optional
}

// Overlaps returns 0 if this span overlaps the other, -1 if there's no
// overlap and this span comes before the other, 1 if no overlap and
// this span comes after other.
func (s Span) Overlaps(cmp base.Compare, other Span) int {
	if cmp(s.Start.UserKey, other.Start.UserKey) == 0 && bytes.Equal(s.End, other.End) {
		if other.Start.SeqNum() < s.Start.SeqNum() {
			return -1
		}
		return 1
	}
	if cmp(s.End, other.Start.UserKey) <= 0 {
		return -1
	}
	if cmp(other.End, s.Start.UserKey) <= 0 {
		return 1
	}
	return 0
}

// Empty returns true if the span does not cover any keys.
func (s Span) Empty() bool {
	// Only RANGEDELs, RANGEKEYSETs, RANGEKEYUNSETs and RANGEKEYDELs may be used
	// as spans.
	switch s.Start.Kind() {
	case base.InternalKeyKindRangeDelete:
		return false
	case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
		return false
	default:
		return true
	}
}

// Contains returns true if the specified key resides within the span's
// bounds.
func (s Span) Contains(cmp base.Compare, key []byte) bool {
	return cmp(s.Start.UserKey, key) <= 0 && cmp(key, s.End) < 0
}

// Covers returns true if the span covers keys at seqNum.
func (s Span) Covers(seqNum uint64) bool {
	return !s.Empty() && s.Start.SeqNum() > seqNum
}

func (s Span) String() string {
	if s.Empty() {
		return "<empty>"
	}
	return fmt.Sprintf("%s-%s#%d", s.Start.UserKey, s.End, s.Start.SeqNum())
}

// Pretty returns a formatter for the span.
func (s Span) Pretty(f base.FormatKey) fmt.Formatter {
	return prettySpan{s, f}
}

type prettySpan struct {
	Span
	formatKey base.FormatKey
}

func (s prettySpan) Format(fs fmt.State, c rune) {
	if s.Empty() {
		fmt.Fprintf(fs, "<empty>")
	}
	fmt.Fprintf(fs, "%s-%s#%d", s.formatKey(s.Start.UserKey), s.formatKey(s.End), s.Start.SeqNum())
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan // import "github.com/cockroachdb/pebble/internal/keyspan"

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/pebble/internal/base"
)

// Span represents a set of keys over a span of user key space. All of the keys
// within a Span are applied across the span's key span indicated by Start and
// End. Each internal key applied over the user key span appears as a separate
// Key, with its own kind and sequence number. Optionally, each Key may also
// have a Suffix and/or Value.
//
// Note that the start user key is inclusive and the end user key is exclusive.
//
// Currently the only supported key kinds are:
//   RANGEDEL, RANGEKEYSET, RANGEKEYUNSET, RANGEKEYDEL.
type Span struct {
	// Start and End encode the user key range of all the contained items, with
	// an inclusive start key and exclusive end key. Both Start and End must be
	// non-nil, or both nil if representing an invalid Span.
	Start, End []byte
	// Keys holds the set of keys applied over the [Start, End) user key range.
	// Keys is sorted by (SeqNum, Kind) descending. If SeqNum and Kind are
	// equal, the order of Keys is undefined. Keys may be empty, even if Start
	// and End are non-nil.
	//
	// Keys are a decoded representation of the internal keys stored in batches
	// or sstable blocks. A single internal key in a range key block may produce
	// several decoded Keys.
	Keys []Key
}

// Key represents a single key applied over a span of user keys. A Key is
// contained by a Span which specifies the span of user keys over which the Key
// is applied.
type Key struct {
	// Trailer contains the key kind and sequence number.
	Trailer uint64
	// Suffix holds an optional suffix associated with the key. This is only
	// non-nil for RANGEKEYSET and RANGEKEYUNSET keys.
	Suffix []byte
	// Value holds a logical value associated with the Key. It is NOT the
	// internal value stored in a range key or range deletion block.  This is
	// only non-nil for RANGEKEYSET keys.
	Value []byte
}

// SeqNum returns the sequence number component of the key.
func (k Key) SeqNum() uint64 {
	return k.Trailer >> 8
}

// Kind returns the kind component of the key.
func (k Key) Kind() base.InternalKeyKind {
	return base.InternalKeyKind(k.Trailer & 0xff)
}

// Valid returns true if the span is defined.
func (s Span) Valid() bool {
	return s.Start != nil && s.End != nil
}

// Empty returns true if the span does not contain any keys. An empty span may
// still be Valid. A non-empty span must be Valid.
//
// An Empty span may be produced by Visible, or be produced by iterators in
// order to surface the gaps between keys.
func (s Span) Empty() bool {
	return len(s.Keys) == 0
}

// SmallestKey returns the smallest internal key defined by the span's keys.
// It panics if the span contains no keys.
func (s Span) SmallestKey() base.InternalKey {
	if len(s.Keys) == 0 {
		panic("pebble: Span contains no keys")
	}
	// The first key has the highest (sequence number,kind) tuple.
	return base.InternalKey{
		UserKey: s.Start,
		Trailer: s.Keys[0].Trailer,
	}
}

// LargestKey returns the largest internal key defined by the span's keys. The
// returned key will always be a "sentinel key" at the end boundary. The
// "sentinel key" models the exclusive end boundary by returning an InternalKey
// with the maximal sequence number, ensuring all InternalKeys with the same
// user key sort after the sentinel key.
//
// It panics if the span contains no keys.
func (s Span) LargestKey() base.InternalKey {
	if len(s.Keys) == 0 {
		panic("pebble: Span contains no keys")
	}
	// The last key has the lowest (sequence number,kind) tuple.
	kind := s.Keys[len(s.Keys)-1].Kind()
	return base.MakeExclusiveSentinelKey(kind, s.End)
}

// SmallestSeqNum returns the smallest sequence number of a key contained within
// the span. It panics if the span contains no keys.
func (s Span) SmallestSeqNum() uint64 {
	if len(s.Keys) == 0 {
		panic("pebble: Span contains no keys")
	}
	return s.Keys[len(s.Keys)-1].SeqNum()
}

// LargestSeqNum returns the largest sequence number of a key contained within
// the span. It panics if the span contains no keys.
func (s Span) LargestSeqNum() uint64 {
	if len(s.Keys) == 0 {
		panic("pebble: Span contains no keys")
	}
	return s.Keys[0].SeqNum()
}

// Visible returns a span with the subset of keys visible at the provided
// sequence number.
func (s Span) Visible(seqNum uint64) Span {
	ret := Span{Start: s.Start, End: s.End}
	for i := range s.Keys {
		if base.Visible(s.Keys[i].SeqNum(), seqNum) {
			ret.Keys = s.Keys[i:]
			break
		}
	}
	return ret
}

// ShallowClone returns the span with a Keys slice owned by the span itself.
// None of the key byte slices are cloned (see Span.DeepClone).
func (s Span) ShallowClone() Span {
	c := Span{
		Start: s.Start,
		End:   s.End,
		Keys:  make([]Key, len(s.Keys)),
	}
	copy(c.Keys, s.Keys)
	return c
}

// DeepClone clones the span, creating copies of all contained slices. DeepClone
// is intended for non-production code paths like tests, the level checker, etc
// because it is allocation heavy.
func (s Span) DeepClone() Span {
	c := Span{
		Start: make([]byte, len(s.Start)),
		End:   make([]byte, len(s.End)),
		Keys:  make([]Key, len(s.Keys)),
	}
	copy(c.Start, s.Start)
	copy(c.End, s.End)
	for i := range s.Keys {
		c.Keys[i].Trailer = s.Keys[i].Trailer
		if len(s.Keys[i].Suffix) > 0 {
			c.Keys[i].Suffix = make([]byte, len(s.Keys[i].Suffix))
			copy(c.Keys[i].Suffix, s.Keys[i].Suffix)
		}
		if len(s.Keys[i].Value) > 0 {
			c.Keys[i].Value = make([]byte, len(s.Keys[i].Value))
			copy(c.Keys[i].Value, s.Keys[i].Value)
		}
	}
	return c
}

// Contains returns true if the specified key resides within the span's bounds.
func (s Span) Contains(cmp base.Compare, key []byte) bool {
	return cmp(s.Start, key) <= 0 && cmp(key, s.End) < 0
}

// Covers returns true if the span covers keys at seqNum.
func (s Span) Covers(seqNum uint64) bool {
	return !s.Empty() && s.Keys[0].SeqNum() > seqNum
}

// String returns a string representation of the span.
func (s Span) String() string {
	return fmt.Sprint(prettySpan{Span: s, formatKey: base.DefaultFormatter})
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
	if !s.Valid() {
		fmt.Fprintf(fs, "<invalid>")
		return
	}
	fmt.Fprintf(fs, "%s-%s:{", s.formatKey(s.Start), s.formatKey(s.End))
	for i, k := range s.Keys {
		if i > 0 {
			fmt.Fprint(fs, " ")
		}
		fmt.Fprintf(fs, "(#%d,%s", k.SeqNum(), k.Kind())
		if len(k.Suffix) > 0 || len(k.Value) > 0 {
			fmt.Fprintf(fs, ",%s", k.Suffix)
		}
		if len(k.Value) > 0 {
			fmt.Fprintf(fs, ",%s", k.Value)
		}
		fmt.Fprint(fs, ")")
	}
	fmt.Fprintf(fs, "}")
}

// SortKeys sorts a keys slice by trailer.
func SortKeys(keys []Key) {
	sorted := keysBySeqNumKind(keys)
	sort.Sort(&sorted)
}

// ParseSpan parses the string representation of a Span. It's intended for
// tests. ParseSpan panics if passed a malformed span representation.
func ParseSpan(input string) Span {
	var s Span
	parts := strings.FieldsFunc(input, func(r rune) bool {
		switch r {
		case '-', ':', '{', '}':
			return true
		default:
			return unicode.IsSpace(r)
		}
	})
	s.Start, s.End = []byte(parts[0]), []byte(parts[1])

	// Each of the remaining parts represents a single Key.
	s.Keys = make([]Key, 0, len(parts)-2)
	for _, p := range parts[2:] {
		keyFields := strings.FieldsFunc(p, func(r rune) bool {
			switch r {
			case '#', ',', '(', ')':
				return true
			default:
				return unicode.IsSpace(r)
			}
		})

		var k Key
		// Parse the sequence number.
		seqNum, err := strconv.ParseUint(keyFields[0], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("invalid sequence number: %q: %s", keyFields[0], err))
		}
		// Parse the key kind.
		kind := base.ParseKind(keyFields[1])
		k.Trailer = base.MakeTrailer(seqNum, kind)
		// Parse the optional suffix.
		if len(keyFields) >= 3 {
			k.Suffix = []byte(keyFields[2])
		}
		// Parse the optional value.
		if len(keyFields) >= 4 {
			k.Value = []byte(keyFields[3])
		}
		s.Keys = append(s.Keys, k)
	}
	return s
}

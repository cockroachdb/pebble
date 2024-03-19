// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// BoundaryKind indicates if a boundary is exclusive or inclusive.
type BoundaryKind uint8

// The two possible values of BoundaryKind.
//
// Note that we prefer Exclusive to be the zero value, so that zero
// UserKeyBounds are not valid.
const (
	Exclusive BoundaryKind = iota
	Inclusive
)

// UserKeyEndBoundary is the end boundary of a user key interval; it can be
// exclusive or inclusive.
type UserKeyEndBoundary struct {
	Key  []byte
	Kind BoundaryKind
}

// UserKeyEndInclusive creates an inclusive end boundary.
func UserKeyEndInclusive(userKey []byte) UserKeyEndBoundary {
	return UserKeyEndBoundary{
		Key:  userKey,
		Kind: Inclusive,
	}
}

// UserKeyEndExclusive creates an exclusive end boundary.
func UserKeyEndExclusive(userKey []byte) UserKeyEndBoundary {
	return UserKeyEndBoundary{
		Key:  userKey,
		Kind: Exclusive,
	}
}

// UserKeyEndExclusiveIf creates a end boundary which can be either inclusive or
// exclusive.
func UserKeyEndExclusiveIf(userKey []byte, exclusive bool) UserKeyEndBoundary {
	kind := Inclusive
	if exclusive {
		kind = Exclusive
	}
	return UserKeyEndBoundary{
		Key:  userKey,
		Kind: kind,
	}
}

// Applies returns true if the given key is "before" the end boundary.
func (eb UserKeyEndBoundary) Applies(cmp Compare, userKey []byte) bool {
	c := cmp(userKey, eb.Key)
	return c < 0 || (c == 0 && eb.Kind == Inclusive)
}

// AppliesToInternalKey returns true if the given internal key is "before" the
// end boundary.
func (eb UserKeyEndBoundary) AppliesToInternalKey(cmp Compare, key InternalKey) bool {
	c := cmp(key.UserKey, eb.Key)
	return c < 0 || (c == 0 && (eb.Kind == Inclusive || key.IsExclusiveSentinel()))
}

// UserKeyBounds is a user key interval with an inclusive start boundary and
// with an end boundary that can be either inclusive or exclusive.
type UserKeyBounds struct {
	Start []byte
	End   UserKeyEndBoundary
}

// UserKeyBoundsInclusive creates the bounds [start, end].
func UserKeyBoundsInclusive(start []byte, end []byte) UserKeyBounds {
	return UserKeyBounds{
		Start: start,
		End:   UserKeyEndInclusive(end),
	}
}

// UserKeyBoundsEndExclusive creates the bounds [start, end).
func UserKeyBoundsEndExclusive(start []byte, end []byte) UserKeyBounds {
	return UserKeyBounds{
		Start: start,
		End:   UserKeyEndExclusive(end),
	}
}

// UserKeyBoundsEndExclusiveIf creates either [start, end] or [start, end) bounds.
func UserKeyBoundsEndExclusiveIf(start []byte, end []byte, exclusive bool) UserKeyBounds {
	return UserKeyBounds{
		Start: start,
		End:   UserKeyEndExclusiveIf(end, exclusive),
	}
}

// UserKeyBoundsFromInternal creates the bounds
// [smallest.UserKey, largest.UserKey] or [smallest.UserKey, largest.UserKey) if
// largest is an exclusive sentinel.
//
// smallest must not be an exclusive sentinel.
func UserKeyBoundsFromInternal(smallest, largest InternalKey) UserKeyBounds {
	if invariants.Enabled && smallest.IsExclusiveSentinel() {
		panic("smallest key is exclusive sentinel")
	}
	return UserKeyBoundsEndExclusiveIf(smallest.UserKey, largest.UserKey, largest.IsExclusiveSentinel())
}

// Valid returns true if the bounds contain at least a user key.
func (b *UserKeyBounds) Valid(cmp Compare) bool {
	return b.End.Applies(cmp, b.Start)
}

// Overlaps returns true if the bounds overlap.
func (b *UserKeyBounds) Overlaps(cmp Compare, other *UserKeyBounds) bool {
	// There is no overlap iff one interval starts after the other ends.
	return other.End.Applies(cmp, b.Start) && b.End.Applies(cmp, other.Start)
}

// ContainsBounds returns true if b completely overlaps other.
func (b *UserKeyBounds) ContainsBounds(cmp Compare, other *UserKeyBounds) bool {
	if cmp(b.Start, other.Start) > 0 {
		return false
	}
	c := cmp(other.End.Key, b.End.Key)
	return c < 0 || (c == 0 && (b.End.Kind == Inclusive || other.End.Kind == Exclusive))
}

// ContainsUserKey returns true if the user key is within the bounds.
func (b *UserKeyBounds) ContainsUserKey(cmp Compare, userKey []byte) bool {
	return cmp(b.Start, userKey) <= 0 && b.End.Applies(cmp, userKey)
}

// ContainsInternalKey returns true if the internal key is within the bounds.
func (b *UserKeyBounds) ContainsInternalKey(cmp Compare, key InternalKey) bool {
	c := cmp(b.Start, key.UserKey)
	return (c < 0 || (c == 0 && !key.IsExclusiveSentinel())) &&
		b.End.AppliesToInternalKey(cmp, key)
}

func (b UserKeyBounds) String() string {
	return b.Format(DefaultFormatter)
}

// Format converts the bounds to a string of the form "[foo, bar]" or
// "[foo, bar)", using the given key formatter.
func (b UserKeyBounds) Format(fmtKey FormatKey) string {
	endC := ']'
	if b.End.Kind == Exclusive {
		endC = ')'
	}
	return fmt.Sprintf("[%s, %s%c", fmtKey(b.Start), fmtKey(b.End.Key), endC)
}

// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/internal/invariants"
)

// KeyRange encodes a key range in user key space. A KeyRange's Start is
// inclusive while its End is exclusive.
//
// KeyRange is equivalent to UserKeyBounds with exclusive end.
type KeyRange struct {
	Start, End []byte
}

// Valid returns true if the KeyRange is defined.
func (k *KeyRange) Valid() bool {
	return k.Start != nil && k.End != nil
}

// Contains returns whether the specified key exists in the KeyRange.
func (k *KeyRange) Contains(cmp Compare, key InternalKey) bool {
	v := cmp(key.UserKey, k.End)
	return (v < 0 || (v == 0 && key.IsExclusiveSentinel())) && cmp(k.Start, key.UserKey) <= 0
}

// UserKeyBounds returns the KeyRange as UserKeyBounds. Also implements the internal `bounded` interface.
func (k KeyRange) UserKeyBounds() UserKeyBounds {
	return UserKeyBoundsEndExclusive(k.Start, k.End)
}

// OverlapsInternalKeyRange checks if the specified internal key range has an
// overlap with the KeyRange. Note that we aren't checking for full containment
// of smallest-largest within k, rather just that there's some intersection
// between the two ranges.
func (k *KeyRange) OverlapsInternalKeyRange(cmp Compare, smallest, largest InternalKey) bool {
	ukb := k.UserKeyBounds()
	b := UserKeyBoundsFromInternal(smallest, largest)
	return ukb.Overlaps(cmp, &b)
}

// OverlapsKeyRange checks if this span overlaps with the provided KeyRange.
// Note that we aren't checking for full containment of either span in the other,
// just that there's a key x that is in both key ranges.
func (k *KeyRange) OverlapsKeyRange(cmp Compare, span KeyRange) bool {
	return cmp(k.Start, span.End) < 0 && cmp(k.End, span.Start) > 0
}

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

// UserKeyBoundary represents the endpoint of a bound which can be exclusive or
// inclusive.
type UserKeyBoundary struct {
	Key  []byte
	Kind BoundaryKind
}

// UserKeyInclusive creates an inclusive user key boundary.
func UserKeyInclusive(userKey []byte) UserKeyBoundary {
	return UserKeyBoundary{
		Key:  userKey,
		Kind: Inclusive,
	}
}

// UserKeyExclusive creates an exclusive user key boundary.
func UserKeyExclusive(userKey []byte) UserKeyBoundary {
	return UserKeyBoundary{
		Key:  userKey,
		Kind: Exclusive,
	}
}

// UserKeyExclusiveIf creates a user key boundary which can be either inclusive
// or exclusive.
func UserKeyExclusiveIf(userKey []byte, exclusive bool) UserKeyBoundary {
	kind := Inclusive
	if exclusive {
		kind = Exclusive
	}
	return UserKeyBoundary{
		Key:  userKey,
		Kind: kind,
	}
}

// IsUpperBoundFor returns true if the boundary is an upper bound for the key;
// i.e. the key is less than the boundary key OR they are equal and the boundary
// is inclusive.
func (eb UserKeyBoundary) IsUpperBoundFor(cmp Compare, userKey []byte) bool {
	c := cmp(userKey, eb.Key)
	return c < 0 || (c == 0 && eb.Kind == Inclusive)
}

// IsUpperBoundForInternalKey returns true if boundary is an upper bound for the
// given internal key.
func (eb UserKeyBoundary) IsUpperBoundForInternalKey(cmp Compare, key InternalKey) bool {
	c := cmp(key.UserKey, eb.Key)
	return c < 0 || (c == 0 && (eb.Kind == Inclusive || key.IsExclusiveSentinel()))
}

// CompareUpperBounds compares two UserKeyBoundaries as upper bounds (e.g. when
// they are used for UserKeyBounds.End).
func (eb UserKeyBoundary) CompareUpperBounds(cmp Compare, other UserKeyBoundary) int {
	switch c := cmp(eb.Key, other.Key); {
	case c != 0:
		return c
	case eb.Kind == other.Kind:
		return 0
	case eb.Kind == Inclusive:
		// eb is inclusive, other is exclusive.
		return 1
	default:
		// eb is exclusive, other is inclusive.
		return -1
	}
}

// UserKeyBounds is a user key interval with an inclusive start boundary and
// with an end boundary that can be either inclusive or exclusive.
type UserKeyBounds struct {
	Start []byte
	End   UserKeyBoundary
}

// UserKeyBoundsInclusive creates the bounds [start, end].
func UserKeyBoundsInclusive(start []byte, end []byte) UserKeyBounds {
	return UserKeyBounds{
		Start: start,
		End:   UserKeyInclusive(end),
	}
}

// UserKeyBoundsEndExclusive creates the bounds [start, end).
func UserKeyBoundsEndExclusive(start []byte, end []byte) UserKeyBounds {
	return UserKeyBounds{
		Start: start,
		End:   UserKeyExclusive(end),
	}
}

// UserKeyBoundsEndExclusiveIf creates either [start, end] or [start, end) bounds.
func UserKeyBoundsEndExclusiveIf(start []byte, end []byte, exclusive bool) UserKeyBounds {
	return UserKeyBounds{
		Start: start,
		End:   UserKeyExclusiveIf(end, exclusive),
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
	return b.End.IsUpperBoundFor(cmp, b.Start)
}

// Overlaps returns true if the bounds overlap.
func (b *UserKeyBounds) Overlaps(cmp Compare, other *UserKeyBounds) bool {
	// There is no overlap iff one interval starts after the other ends.
	return other.End.IsUpperBoundFor(cmp, b.Start) && b.End.IsUpperBoundFor(cmp, other.Start)
}

// ContainsBounds returns true if b completely overlaps other.
func (b *UserKeyBounds) ContainsBounds(cmp Compare, other *UserKeyBounds) bool {
	if cmp(b.Start, other.Start) > 0 {
		return false
	}
	return other.End.CompareUpperBounds(cmp, b.End) <= 0
}

// ContainsUserKey returns true if the user key is within the bounds.
func (b *UserKeyBounds) ContainsUserKey(cmp Compare, userKey []byte) bool {
	return cmp(b.Start, userKey) <= 0 && b.End.IsUpperBoundFor(cmp, userKey)
}

// ContainsInternalKey returns true if the internal key is within the bounds.
func (b *UserKeyBounds) ContainsInternalKey(cmp Compare, key InternalKey) bool {
	c := cmp(b.Start, key.UserKey)
	return (c < 0 || (c == 0 && !key.IsExclusiveSentinel())) &&
		b.End.IsUpperBoundForInternalKey(cmp, key)
}

// Clone returns a copy of the bounds.
func (b UserKeyBounds) Clone() UserKeyBounds {
	return UserKeyBounds{
		Start: slices.Clone(b.Start),
		End:   UserKeyBoundary{Key: slices.Clone(b.End.Key), Kind: b.End.Kind},
	}
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

// Union returns bounds that encompass both the receiver and the provided bounds.
//
// If either bounds are empty, the other bounds are returned.
func (b *UserKeyBounds) Union(cmp Compare, other UserKeyBounds) UserKeyBounds {
	union := *b
	if other.Start != nil && (union.Start == nil || cmp(union.Start, other.Start) > 0) {
		union.Start = other.Start
	}
	if other.End.Key != nil && (union.End.Key == nil || union.End.CompareUpperBounds(cmp, other.End) < 0) {
		union.End = other.End
	}
	return union
}

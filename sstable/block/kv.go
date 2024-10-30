// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import "github.com/cockroachdb/pebble/internal/base"

// ValuePrefix is the single byte prefix in values indicating either an in-place
// value or a value encoding a valueHandle. It encodes multiple kinds of
// information (see below).
type ValuePrefix byte

const (
	// 2 most-significant bits of valuePrefix encodes the value-kind.
	valueKindMask           ValuePrefix = 0xC0
	valueKindIsValueHandle  ValuePrefix = 0x80
	valueKindIsInPlaceValue ValuePrefix = 0x00

	// 1 bit indicates SET has same key prefix as immediately preceding key that
	// is also a SET. If the immediately preceding key in the same block is a
	// SET, AND this bit is 0, the prefix must have changed.
	//
	// Note that the current policy of only storing older MVCC versions in value
	// blocks means that valueKindIsValueHandle => SET has same prefix. But no
	// code should rely on this behavior. Also, SET has same prefix does *not*
	// imply valueKindIsValueHandle.
	setHasSameKeyPrefixMask ValuePrefix = 0x20

	// 3 least-significant bits for the user-defined base.ShortAttribute.
	// Undefined for valueKindIsInPlaceValue.
	userDefinedShortAttributeMask ValuePrefix = 0x07
)

// IsValueHandle returns true if the ValuePrefix is for a valueHandle.
func (vp ValuePrefix) IsValueHandle() bool {
	return vp&valueKindMask == valueKindIsValueHandle
}

// SetHasSamePrefix returns true if the ValuePrefix encodes that the key is a
// set with the same prefix as the preceding key which also is a set.
func (vp ValuePrefix) SetHasSamePrefix() bool {
	return vp&setHasSameKeyPrefixMask == setHasSameKeyPrefixMask
}

// ShortAttribute returns the user-defined base.ShortAttribute encoded in the
// ValuePrefix.
//
// REQUIRES: IsValueHandle()
func (vp ValuePrefix) ShortAttribute() base.ShortAttribute {
	return base.ShortAttribute(vp & userDefinedShortAttributeMask)
}

// ValueHandlePrefix returns the ValuePrefix for a valueHandle.
func ValueHandlePrefix(setHasSameKeyPrefix bool, attribute base.ShortAttribute) ValuePrefix {
	prefix := valueKindIsValueHandle | ValuePrefix(attribute)
	if setHasSameKeyPrefix {
		prefix = prefix | setHasSameKeyPrefixMask
	}
	return prefix
}

// InPlaceValuePrefix returns the ValuePrefix for an in-place value.
func InPlaceValuePrefix(setHasSameKeyPrefix bool) ValuePrefix {
	prefix := valueKindIsInPlaceValue
	if setHasSameKeyPrefix {
		prefix = prefix | setHasSameKeyPrefixMask
	}
	return prefix
}

// GetLazyValueForPrefixAndValueHandler is an interface for getting a LazyValue
// from a value prefix and value.
type GetLazyValueForPrefixAndValueHandler interface {
	// GetLazyValueForPrefixAndValueHandle returns a LazyValue for the given value
	// prefix and value. The result is only valid until the next call to
	// GetLazyValueForPrefixAndValueHandle.
	GetLazyValueForPrefixAndValueHandle(handle []byte) base.LazyValue
}

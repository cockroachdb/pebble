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
	valueKindMask               ValuePrefix = 0xC0
	valueKindIsValueBlockHandle ValuePrefix = 0x80
	valueKindIsInPlaceValue     ValuePrefix = 0x00

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

// IsInPlaceValue returns true if the ValuePrefix is for an in-place value.
func (vp ValuePrefix) IsInPlaceValue() bool {
	return vp&valueKindMask == valueKindIsInPlaceValue
}

// IsValueBlockHandle returns true if the ValuePrefix is for a valblk.Handle.
func (vp ValuePrefix) IsValueBlockHandle() bool {
	return vp&valueKindMask == valueKindIsValueBlockHandle
}

// SetHasSamePrefix returns true if the ValuePrefix encodes that the key is a
// set with the same prefix as the preceding key which also is a set.
func (vp ValuePrefix) SetHasSamePrefix() bool {
	return vp&setHasSameKeyPrefixMask == setHasSameKeyPrefixMask
}

// ShortAttribute returns the user-defined base.ShortAttribute encoded in the
// ValuePrefix.
//
// REQUIRES: !IsInPlaceValue()
func (vp ValuePrefix) ShortAttribute() base.ShortAttribute {
	return base.ShortAttribute(vp & userDefinedShortAttributeMask)
}

// ValueBlockHandlePrefix returns the ValuePrefix for a valblk.Handle.
func ValueBlockHandlePrefix(setHasSameKeyPrefix bool, attribute base.ShortAttribute) ValuePrefix {
	prefix := valueKindIsValueBlockHandle | ValuePrefix(attribute)
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

// GetInternalValueForPrefixAndValueHandler is an interface for getting an
// InternalValue from a value prefix and value.
type GetInternalValueForPrefixAndValueHandler interface {
	// GetInternalValueForPrefixAndValueHandle returns a InternalValue for the
	// given value prefix and value.
	//
	// The result is only valid until the next call to
	// GetInternalValueForPrefixAndValueHandle. Use InternalValue.Clone if the
	// lifetime of the InternalValue needs to be extended. For more details, see
	// the "memory management" comment where LazyValue is declared.
	GetInternalValueForPrefixAndValueHandle(handle []byte) base.InternalValue
}

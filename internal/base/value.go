// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "github.com/cockroachdb/pebble/internal/invariants"

// An InternalValue represents a value. The value may be in-memory, immediately
// accessible, or it may be stored out-of-band and need to be fetched when
// required.
//
// InternalValue is distinct from LazyValue. The LazyValue type is used within
// Pebble's public interface, while InternalValue is an intermediary
// representation of a value used only internally within Pebble.
type InternalValue struct {
	lazyValue LazyValue
}

// MakeLazyValue constructs an InternalValue from a LazyValue.
func MakeLazyValue(v LazyValue) InternalValue {
	return InternalValue{lazyValue: v}
}

// MakeInPlaceValue constructs an in-place value.
func MakeInPlaceValue(val []byte) InternalValue {
	return InternalValue{lazyValue: LazyValue{ValueOrHandle: val}}
}

// IsBlobValueHandle returns true iff the value is a blob value handle, pointing
// to a value stored externally in a blob file.
func (v *InternalValue) IsBlobValueHandle() bool {
	f := v.lazyValue.Fetcher
	return f != nil && f.BlobFileNum > 0
}

// IsInPlaceValue returns true iff the value was stored in-place and does not
// need to be fetched externally.
func (v *InternalValue) IsInPlaceValue() bool {
	return v.lazyValue.Fetcher == nil
}

// InPlaceValue returns the value under the assumption that it is in-place.
// This is for Pebble-internal code.
func (v *InternalValue) InPlaceValue() []byte {
	if invariants.Enabled && v.lazyValue.Fetcher != nil {
		panic("value must be in-place")
	}
	return v.lazyValue.ValueOrHandle
}

// LazyValue returns the InternalValue as a LazyValue.
func (v *InternalValue) LazyValue() LazyValue {
	return v.lazyValue
}

// Len returns the length of the value. This is the length of the logical value
// (i.e., the length of the byte slice returned by .Value())
func (v *InternalValue) Len() int {
	return v.lazyValue.Len()
}

// InternalLen returns the length of the value, if the value is in-place, or the
// length of the handle describing the location of the value if the value is
// stored out-of-band.
func (v *InternalValue) InternalLen() int {
	return len(v.lazyValue.ValueOrHandle)
}

// ValueOrHandle returns the value or handle that is stored inlined. If the
// value is stored out-of-band, the returned slice contains a binary-encoded
// value handle.
func (v *InternalValue) ValueOrHandle() []byte {
	return v.lazyValue.ValueOrHandle
}

// Value returns the KV's underlying value.
func (v *InternalValue) Value(buf []byte) (val []byte, callerOwned bool, err error) {
	return v.lazyValue.Value(buf)
}

// Clone creates a stable copy of the value, by appending bytes to buf.  The
// fetcher parameter must be non-nil and may be over-written and used inside the
// returned InternalValue -- this is needed to avoid an allocation.
//
// See LazyValue.Clone for more details.
func (v *InternalValue) Clone(buf []byte, fetcher *LazyFetcher) (InternalValue, []byte) {
	lv, buf := v.lazyValue.Clone(buf, fetcher)
	return InternalValue{lazyValue: lv}, buf
}

// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/buildtags"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manual"
)

// ValueMetadataSize denotes the number of bytes of metadata allocated for a
// cache entry. Note that builds with cgo disabled allocate no metadata, and
// 32-bit builds allocate less for a cache.Value. However, we keep the value
// constant to reduce friction for writing tests.
const ValueMetadataSize = 32

// Assert that the size of a Value{} is less than or equal to the
// ValueMetadataSize.
var _ uint = ValueMetadataSize - uint(unsafe.Sizeof(Value{}))

// Value holds a reference counted immutable value.
type Value struct {
	// buf is allocated using the manual package.
	buf []byte
	// Reference count for the value. The value is freed when the reference count
	// drops to zero.
	ref refcnt
}

// The Value struct is normally allocated together with the buffer using manual
// memory.
//
// If cgo is not available, the Value must be a normal Go object to keep
// the buffer reference visible to the GC. We also use the Go allocator if we
// want to add finalizer assertions.
const valueEntryGoAllocated = !buildtags.Cgo || invariants.UseFinalizers

func newValue(n int) *Value {
	if n == 0 {
		return nil
	}

	if valueEntryGoAllocated {
		// Note: if cgo is not enabled, manual.New will do a regular Go allocation.
		b := manual.New(manual.BlockCacheData, n)
		v := &Value{buf: b}
		v.ref.init(1)
		// Note: this is a no-op if invariants and tracing are disabled or race is
		// enabled.
		invariants.SetFinalizer(v, func(obj interface{}) {
			v := obj.(*Value)
			if v.buf != nil {
				fmt.Fprintf(os.Stderr, "%p: cache value was not freed: refs=%d\n%s",
					v, v.refs(), v.ref.traces())
				os.Exit(1)
			}
		})
		return v
	}
	// When we're not performing leak detection, the lifetime of the returned
	// Value is exactly the lifetime of the backing buffer and we can manually
	// allocate both.
	b := manual.New(manual.BlockCacheData, ValueMetadataSize+n)
	v := (*Value)(unsafe.Pointer(&b[0]))
	v.buf = b[ValueMetadataSize:]
	v.ref.init(1)
	return v
}

func (v *Value) free() {
	if invariants.Enabled {
		// Poison the contents to help catch use-after-free bugs.
		for i := range v.buf {
			v.buf[i] = 0xff
		}
	}
	if valueEntryGoAllocated {
		manual.Free(manual.BlockCacheData, v.buf)
		v.buf = nil
		return
	}
	n := ValueMetadataSize + cap(v.buf)
	buf := unsafe.Slice((*byte)(unsafe.Pointer(v)), n)
	v.buf = nil
	manual.Free(manual.BlockCacheData, buf)
}

// Buf returns the buffer associated with the value. The contents of the buffer
// should not be changed once the value has been added to the cache. Instead, a
// new Value should be created and added to the cache to replace the existing
// value.
func (v *Value) Buf() []byte {
	if v == nil {
		return nil
	}
	return v.buf
}

// Truncate the buffer to the specified length. The buffer length should not be
// changed once the value has been added to the cache as there may be
// concurrent readers of the Value. Instead, a new Value should be created and
// added to the cache to replace the existing value.
func (v *Value) Truncate(n int) {
	v.buf = v.buf[:n]
}

func (v *Value) refs() int32 {
	return v.ref.refs()
}

func (v *Value) acquire() {
	v.ref.acquire()
}

func (v *Value) release() {
	if v != nil && v.ref.release() {
		v.free()
	}
}

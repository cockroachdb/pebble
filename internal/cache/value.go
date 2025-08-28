// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"math/rand/v2"
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
	// buf is part of the slice allocated using the manual package.
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
//
// valueEntryCanBeGoAllocated is used to allow the compiler to statically elide
// code blocks.
const valueEntryCanBeGoAllocated = !buildtags.Cgo || invariants.UseFinalizers

var valueEntryGoAllocated = !buildtags.Cgo || (invariants.UseFinalizers && rand.IntN(2) == 0)

// Alloc allocates a byte slice of the specified size, possibly reusing
// previously allocated but unused memory. The memory backing the value is
// manually managed. The caller MUST either add the value to the cache (via
// Cache.Set), or release the value (via Cache.Free). Failure to do so will
// result in a memory leak.
func Alloc(n int) *Value {
	if n == 0 {
		return nil
	}

	if valueEntryCanBeGoAllocated && valueEntryGoAllocated {
		// Note: if cgo is not enabled, manual.New will do a regular Go allocation.
		b := manual.New(manual.BlockCacheData, uintptr(n))
		v := &Value{buf: b.Slice()}
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
	b := manual.New(manual.BlockCacheData, ValueMetadataSize+uintptr(n))
	v := (*Value)(b.Data())
	v.buf = b.Slice()[ValueMetadataSize:]
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
	if valueEntryCanBeGoAllocated && valueEntryGoAllocated {
		buf := manual.MakeBufUnsafe(unsafe.Pointer(unsafe.SliceData(v.buf)), uintptr(cap(v.buf)))
		manual.Free(manual.BlockCacheData, buf)
		v.buf = nil
		return
	}
	n := ValueMetadataSize + uintptr(cap(v.buf))
	buf := manual.MakeBufUnsafe(unsafe.Pointer(v), n)
	v.buf = nil
	manual.Free(manual.BlockCacheData, buf)
}

// RawBuffer returns the buffer associated with the value. The contents of the buffer
// should not be changed once the value has been added to the cache. Instead, a
// new Value should be created and added to the cache to replace the existing
// value.
func (v *Value) RawBuffer() []byte {
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

// Release a ref count on the buffer. It is a no-op to call Release on a nil
// Value.
func (v *Value) Release() {
	if v != nil && v.ref.release() {
		v.free()
	}
}

// Free frees the specified value. The buffer associated with the value will
// possibly be reused, making it invalid to use the buffer after calling
// Free. Do not call Free on a value that has been added to the cache.
func Free(v *Value) {
	if n := v.refs(); n > 1 {
		panic(fmt.Sprintf("pebble: Value has been added to the cache: refs=%d", n))
	}
	v.Release()
}

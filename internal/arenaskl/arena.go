/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package arenaskl

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Arena should be lock-free.
type Arena struct {
	n   uint32
	buf []byte

	extValues struct {
		threshold uint32
		size      uint32
		sync.RWMutex
		vals [][]byte
	}
}

const (
	align4 = 3
)

var (
	ErrArenaFull = errors.New("allocation failed because arena is full")
)

// NewArena allocates a new arena of the specified size and returns it.
func NewArena(size, extValueThreshold uint32) *Arena {
	// Don't store data at position 0 in order to reserve offset=0 as a kind
	// of nil pointer.
	out := &Arena{
		n:   1,
		buf: make([]byte, size),
	}
	if extValueThreshold >= size {
		panic("invalid external value threshold")
	}
	if extValueThreshold == 0 {
		extValueThreshold = size / 4
	}
	out.extValues.threshold = extValueThreshold

	return out
}

func (a *Arena) Size() uint32 {
	return atomic.LoadUint32(&a.n) + atomic.LoadUint32(&a.extValues.size)
}

func (a *Arena) Capacity() uint32 {
	return uint32(len(a.buf))
}

func (a *Arena) alloc(size, align uint32) (uint32, error) {
	// Pad the allocation with enough bytes to ensure the requested alignment.
	padded := uint32(size) + align

	newSize := atomic.AddUint32(&a.n, padded)
	if int(newSize) > len(a.buf) {
		return 0, ErrArenaFull
	}

	// Return the aligned offset.
	offset := (newSize - padded + uint32(align)) & ^uint32(align)
	return offset, nil
}

func (a *Arena) allocExtValue(size uint32) int32 {
	atomic.AddUint32(&a.extValues.size, size)
	v := make([]byte, size)
	a.extValues.Lock()
	i := int32(len(a.extValues.vals))
	a.extValues.vals = append(a.extValues.vals, v)
	a.extValues.Unlock()
	return i
}

func (a *Arena) getBytes(offset uint32, size uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size : offset+size]
}

func (a *Arena) getPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) getPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func (a *Arena) getExtValue(i int32) []byte {
	a.extValues.RLock()
	v := a.extValues.vals[i]
	a.extValues.RUnlock()
	return v
}

// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"encoding/binary"
	"unsafe"
)

const cacheLineSize = 64
const cacheLineBits = cacheLineSize * 8

// filterBits contains the actual filter data, organized in blocks of
// cacheLineSize.
type filterBits struct {
	ptr      unsafe.Pointer
	numLines uint32
}

func aliasFilterBits(b []byte, nLines uint32) filterBits {
	_ = b[nLines*cacheLineSize]
	return filterBits{
		ptr:      unsafe.Pointer(unsafe.SliceData(b)),
		numLines: nLines,
	}
}

// cacheLine returns a pointer to the cache line corresponding to hash h.
func (f filterBits) cacheLine(h uint32) *[cacheLineSize]byte {
	lineIdx := h % f.numLines
	return (*[cacheLineSize]byte)(unsafe.Add(f.ptr, uintptr(lineIdx)*cacheLineSize))
}

//gcassert:inline
func (f filterBits) probe(nProbes uint8, h uint32) bool {
	delta := h>>17 | h<<15 // rotate right 17 bits
	line := f.cacheLine(h)
	for range nProbes {
		// The bit position within the line is (h % cacheLineBits).
		//  byte index: (h % cacheLineBits)/8 = (h/8) % cacheLineSize
		//  bit index: (h % cacheLineBits)%8 = h%8
		val := line[(h>>3)&(cacheLineSize-1)] & (1 << (h & 7)) //gcassert:bce
		if val == 0 {
			return false
		}
		h += delta
	}
	return true
}

//gcassert:inline
func (f filterBits) set(nProbes uint32, h uint32) {
	delta := h>>17 | h<<15 // rotate right 17 bits
	line := f.cacheLine(h)
	for range nProbes {
		// The bit position within the line is (h % cacheLineBits).
		//  byte index: (h % cacheLineBits)/8 = (h/8) % cacheLineSize
		//  bit index: (h % cacheLineBits)%8 = h%8
		line[(h>>3)&(cacheLineSize-1)] |= 1 << (h & 7) //gcassert:bce
		h += delta
	}
}

// buildFilter builds a bloom filter with the given number of lines and probes.
// The filter format matches the RocksDB full-file filter format:
//   - nLines * cacheLineSize bytes of filter bits
//   - 1 byte: number of probes
//   - 4 bytes: number of lines (little-endian)
func buildFilter(nLines, nProbes uint32, hc *hashCollector) []byte {
	nBytes := nLines * cacheLineSize
	// Format:
	//   - nBytes: filter bits
	//   - 1 byte: number of probes
	//   - 4 bytes: number of lines
	filter := make([]byte, nBytes+5)
	bits := aliasFilterBits(filter, nLines)
	for b := range hc.Blocks() {
		for _, h := range b {
			bits.set(nProbes, h)
		}
	}
	filter[nBytes] = byte(nProbes)
	binary.LittleEndian.PutUint32(filter[nBytes+1:], nLines)
	return filter
}

// mayContain reports whether the given key may be contained in the bloom
// filter, created by buildFilter.
func mayContain(filter []byte, h uint32) bool {
	if len(filter) <= 5 {
		return false
	}
	n := len(filter) - 5
	nProbes := filter[n]
	nLines := binary.LittleEndian.Uint32(filter[n+1:])
	if 8*(uint32(n)/nLines) != cacheLineBits {
		panic("bloom filter: unexpected cache line size")
	}
	bits := aliasFilterBits(filter, nLines)
	return bits.probe(nProbes, h)
}

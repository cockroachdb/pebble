// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package bloom implements Bloom filters.
package bloom // import "github.com/cockroachdb/pebble/bloom"

import (
	"encoding/binary"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
)

const (
	cacheLineSize = 64
	cacheLineBits = cacheLineSize * 8
)

type tableFilter []byte

func (f tableFilter) MayContain(key []byte) bool {
	if len(f) <= 5 {
		return false
	}
	n := len(f) - 5
	nProbes := f[n]
	nLines := binary.LittleEndian.Uint32(f[n+1:])
	if 8*(uint32(n)/nLines) != cacheLineBits {
		panic("bloom filter: unexpected cache line size")
	}
	h := hash(key)
	delta := h>>17 | h<<15
	lineIdx := h % nLines
	// Set up a pointer to a [cacheLineSize]byte array. This avoids bound
	// checks inside the loop.
	line := (*[cacheLineSize]byte)(f[lineIdx*cacheLineSize : (lineIdx+1)*cacheLineSize])
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

// This table contains the optimal number of probes for each bitsPerKey. For
// bits per key over 10, probes[10] should be used.
//
// The values are derived from simulations (see simulation.txt).
//
// The standard bloom filter formula does not yield the optimal number for our
// scheme, which constrains all probes to be inside the same cache line. This is
// especially true for larger bits-per-key values.
var probes = [11]uint32{
	1:  1,
	2:  1,
	3:  2,
	4:  3,
	5:  3,
	6:  4,
	7:  4,
	8:  5,
	9:  5,
	10: 6,
}

func calculateProbes(bitsPerKey uint32) uint32 {
	if bitsPerKey > 10 {
		return probes[10]
	}
	return probes[bitsPerKey]
}

// extend appends n zero bytes to b. It returns the overall slice (of length
// n+len(originalB)) and the slice of n trailing zeroes.
func extend(b []byte, n int) (overall, trailer []byte) {
	want := n + len(b)
	if want <= cap(b) {
		overall = b[:want]
		trailer = overall[len(b):]
		clear(trailer)
	} else {
		// Grow the capacity exponentially, with a 1KiB minimum.
		c := 1024
		for c < want {
			c += c / 4
		}
		overall = make([]byte, want, c)
		trailer = overall[len(b):]
		copy(overall, b)
	}
	return overall, trailer
}

// hash implements a hashing algorithm similar to the Murmur hash.
func hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(uint64(uint32(len(b))*m))
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}

	// The code below first casts each byte to a signed 8-bit integer. This is
	// necessary to match RocksDB's behavior. Note that the `byte` type in Go is
	// unsigned. What is the difference between casting a signed 8-bit value vs
	// unsigned 8-bit value into an unsigned 32-bit value?
	// Sign-extension. Consider the value 250 which has the bit pattern 11111010:
	//
	//   uint32(250)        = 00000000000000000000000011111010
	//   uint32(int8(250))  = 11111111111111111111111111111010
	//
	// Note that the original LevelDB code did not explicitly cast to a signed
	// 8-bit value which left the behavior dependent on whether C characters were
	// signed or unsigned which is a compiler flag for gcc (-funsigned-char).
	switch len(b) {
	case 3:
		h += uint32(int8(b[2])) << 16
		fallthrough
	case 2:
		h += uint32(int8(b[1])) << 8
		fallthrough
	case 1:
		h += uint32(int8(b[0]))
		h *= m
		h ^= h >> 24
	}
	return h
}

const hashBlockLen = 16384

type hashBlock [hashBlockLen]uint32

var hashBlockPool = sync.Pool{
	New: func() interface{} {
		return &hashBlock{}
	},
}

type tableFilterWriter struct {
	bitsPerKey uint32
	numProbes  uint32

	numHashes int
	// We store the hashes in blocks.
	blocks   []*hashBlock
	lastHash uint32

	// Initial "in-line" storage for the blocks slice (to avoid some small
	// allocations).
	blocksBuf [16]*hashBlock
}

func newTableFilterWriter(bitsPerKey uint32) *tableFilterWriter {
	w := &tableFilterWriter{
		bitsPerKey: bitsPerKey,
		numProbes:  calculateProbes(bitsPerKey),
	}
	w.blocks = w.blocksBuf[:0]
	return w
}

// AddKey implements the base.FilterWriter interface.
func (w *tableFilterWriter) AddKey(key []byte) {
	h := hash(key)
	if w.numHashes != 0 && h == w.lastHash {
		return
	}
	ofs := w.numHashes % hashBlockLen
	if ofs == 0 {
		// Time for a new block.
		w.blocks = append(w.blocks, hashBlockPool.Get().(*hashBlock))
	}
	w.blocks[len(w.blocks)-1][ofs] = h
	w.numHashes++
	w.lastHash = h
}

func calculateNumLines(numHashes int, bitsPerKey uint32) uint32 {
	if numHashes == 0 {
		return 0
	}
	nLines := (uint64(numHashes)*uint64(bitsPerKey) + cacheLineBits - 1) / (cacheLineBits)
	// Make nLines an odd number to make sure more bits are involved when
	// determining which block.
	return uint32(nLines | 1)
}

// Finish implements the base.FilterWriter interface.
func (w *tableFilterWriter) Finish(buf []byte) []byte {
	// The table filter format matches the RocksDB full-file filter format.
	nLines := calculateNumLines(w.numHashes, w.bitsPerKey)

	nBytes := nLines * cacheLineSize
	// +5: 4 bytes for num-lines, 1 byte for num-probes
	buf, filter := extend(buf, int(nBytes+5))

	if nLines != 0 {
		nProbes := w.numProbes
		for bIdx, b := range w.blocks {
			length := hashBlockLen
			if bIdx == len(w.blocks)-1 && w.numHashes%hashBlockLen != 0 {
				length = w.numHashes % hashBlockLen
			}
			for _, h := range b[:length] {
				delta := h>>17 | h<<15 // rotate right 17 bits
				lineIdx := h % uint32(nLines)
				// Set up a pointer to a [cacheLineSize]byte array. This avoids bound
				// checks inside the loop.
				line := (*[cacheLineSize]byte)(filter[lineIdx*cacheLineSize : (lineIdx+1)*cacheLineSize])
				for range nProbes {
					// The bit position within the line is (h % cacheLineBits).
					//  byte index: (h % cacheLineBits)/8 = (h/8) % cacheLineSize
					//  bit index: (h % cacheLineBits)%8 = h%8
					line[(h>>3)&(cacheLineSize-1)] |= 1 << (h & 7) //gcassert:bce
					h += delta
				}
			}
		}
		nBytes := nLines * cacheLineSize
		filter[nBytes] = byte(nProbes)
		binary.LittleEndian.PutUint32(filter[nBytes+1:], uint32(nLines))
	}

	// Release the hash blocks.
	for i, b := range w.blocks {
		hashBlockPool.Put(b)
		w.blocks[i] = nil
	}
	w.blocks = w.blocks[:0]
	w.numHashes = 0
	return buf
}

// FilterPolicy implements the FilterPolicy interface from the pebble package.
//
// The integer value is the approximate number of bits used per key. A good
// value is 10, which yields a filter with ~1% false positive rate.
//
// The table below contains false positive rates for various bits-per-key values
// (obtained from simulations).
//
//	Bits/key | Probes |       FPR
//	---------+--------+------------------
//	       1 |   1    | 61.4% (1 in 1.63)
//	       2 |   1    | 38.6% (1 in 2.59)
//	       3 |   2    | 23.2% (1 in 4.31)
//	       4 |   3    | 14.5% (1 in 6.91)
//	       5 |   3    | 9.16% (1 in 10.9)
//	       6 |   4    | 5.75% (1 in 17.4)
//	       7 |   4    | 3.76% (1 in 26.6)
//	       8 |   5    | 2.44% (1 in 40.9)
//	       9 |   5    | 1.66% (1 in 60.2)
//	      10 |   6    | 1.14% (1 in 87.5)
//	      11 |   6    | 0.815% (1 in 123)
//	      12 |   6    | 0.604% (1 in 166)
//	      13 |   6    | 0.463% (1 in 216)
//	      14 |   6    | 0.365% (1 in 274)
//	      15 |   6    | 0.296% (1 in 338)
//	      16 |   6    | 0.246% (1 in 407)
//	      17 |   6    | 0.208% (1 in 481)
//	      18 |   6    | 0.179% (1 in 557)
//	      19 |   6    | 0.158% (1 in 634)
//	      20 |   6    | 0.140% (1 in 713)
type FilterPolicy int

var _ base.FilterPolicy = FilterPolicy(0)

// Name implements the pebble.FilterPolicy interface.
func (p FilterPolicy) Name() string {
	// This string looks arbitrary, but its value is written to LevelDB .sst
	// files, and should be this exact value to be compatible with those files
	// and with the C++ LevelDB code.
	return "rocksdb.BuiltinBloomFilter"
}

// MayContain implements the pebble.FilterPolicy interface.
func (p FilterPolicy) MayContain(f, key []byte) bool {
	return tableFilter(f).MayContain(key)
}

// NewWriter implements the pebble.FilterPolicy interface.
func (p FilterPolicy) NewWriter() base.FilterWriter {
	return newTableFilterWriter(uint32(p))
}

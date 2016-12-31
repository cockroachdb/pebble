// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bloom implements Bloom filters.
package bloom // import "github.com/golang/leveldb/bloom"

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(key []byte) bool {
	if len(f) < 2 {
		return false
	}
	k := f[len(f)-1]
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (len(f) - 1))
	h := hash(key)
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// TODO: delete the buf argument to NewFilter, if it's always going to be nil?

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately. The returned Filter may be
// a sub-slice of buf[:cap(buf)] if it is large enough, otherwise the Filter
// will be allocated separately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(buf []byte, keys [][]byte, bitsPerKey int) Filter {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	// 0.69 is approximately ln(2).
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	nBits := len(keys) * int(bitsPerKey)
	// For small n, we can see a very high false positive rate. Fix it
	// by enforcing a minimum bloom filter length.
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8

	if nBytes+1 <= cap(buf) {
		buf = buf[:nBytes+1]
		for i := range buf {
			buf[i] = 0
		}
	} else {
		buf = make([]byte, nBytes+1)
	}

	for _, key := range keys {
		h := hash(key)
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			buf[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}
	buf[nBytes] = uint8(k)
	return Filter(buf)
}

// hash implements a hashing algorithm similar to the Murmur hash.
func hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b)*m)
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}

// FilterPolicy implements the db.FilterPolicy interface from the leveldb/db
// package.
//
// The integer value is the approximate number of bits used per key. A good
// value is 10, which yields a filter with ~ 1% false positive rate.
//
// It is valid to use the other API in this package (leveldb/bloom) without
// using this type or the leveldb/db package.
type FilterPolicy int

// Name implements the db.FilterPolicy interface.
func (p FilterPolicy) Name() string {
	// This string looks arbitrary, but its value is written to LevelDB .ldb
	// files, and should be this exact value to be compatible with those files
	// and with the C++ LevelDB code.
	return "leveldb.BuiltinBloomFilter2"
}

// NewFilter implements the db.FilterPolicy interface.
func (p FilterPolicy) NewFilter(keys [][]byte) []byte {
	return NewFilter(nil, keys, int(p))
}

// MayContain implements the db.FilterPolicy interface.
func (p FilterPolicy) MayContain(filter, key []byte) bool {
	return Filter(filter).MayContain(key)
}

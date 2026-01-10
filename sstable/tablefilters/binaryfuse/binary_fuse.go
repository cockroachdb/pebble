// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/zeebo/xxh3"
)

// Family name for binary fuse filters.
const Family base.TableFilterFamily = "binaryfuse"

// FilterPolicy returns a base.TableFilterPolicy that creates binary fuse
// filters with the given number of bits per fingerprint. Only 4, 8, 12, and 16
// bits per fingerprint are supported.
//
// A binary fuse filter has false positive rate 1/2^bitsPerFingerprint and
// overhead ~13% (i.e. contains ~1.13 fingerprints per key):
//
//	Bits/fingeprint | Bits/key | FPR
//	----------------+----------+---------------------
//	              4 |   ~4.5   | 6.25% (1 in 16)
//	              8 |   ~9     | 0.39% (1 in 256)
//	             12 |   ~13.5  | 0.024% (1 in 4096)
//	             16 |   ~18    | 0.0015% (1 in 65536)
//
// Notes:
//   - Older Pebble versions do not understand binary fuse filters and will not
//     use these filters.
//   - The Bits/key above applies once we have a lot (hundreds of thousands) of
//     keys. For smaller sets, we get 5-10% larger filters. See simulation.md
//     for exact figures.
func FilterPolicy(bitsPerFingerprint int) base.TableFilterPolicy {
	switch bitsPerFingerprint {
	case 4, 8, 12, 16:
	default:
		panic(fmt.Sprintf("invalid bitsPerFingerprint %d", bitsPerFingerprint))
	}
	return filterPolicyImpl{BitsPerFingerprint: bitsPerFingerprint}
}

type filterPolicyImpl struct {
	BitsPerFingerprint int
}

var _ base.TableFilterPolicy = filterPolicyImpl{}

// Name is part of the base.TableFilterPolicy interface.
func (p filterPolicyImpl) Name() string {
	return fmt.Sprintf("binaryfuse(%d)", p.BitsPerFingerprint)
}

// NewWriter is part of the base.TableFilterPolicy interface.
func (p filterPolicyImpl) NewWriter() base.TableFilterWriter {
	return newTableFilterWriter(p.BitsPerFingerprint)
}

// PolicyFromName returns the filterPolicyImpl corresponding to the given
// name, or false if the string is not recognized as a binary fuse filter policy.
func PolicyFromName(name string) (_ base.TableFilterPolicy, ok bool) {
	var fpBits int
	if n, err := fmt.Sscanf(name, "binaryfuse(%d)", &fpBits); err == nil && n == 1 && fpBits >= 1 {
		return FilterPolicy(fpBits), true
	}
	return nil, false
}

// Decoder implements base.TableFilterDecoder for binary fuse filters.
var Decoder base.TableFilterDecoder = decoderImpl{}

type decoderImpl struct{}

func (d decoderImpl) Family() base.TableFilterFamily {
	return Family
}

func (d decoderImpl) MayContain(filter, key []byte) bool {
	return mayContain(filter, xxh3.Hash(key))
}

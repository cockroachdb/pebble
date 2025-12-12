// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// AdaptivePolicy implements base.TableFilterPolicy for Bloom filters. It
// automatically reduces the number of bits per key so the filter size stays
// under a given limit.
func AdaptivePolicy(targetBitsPerKey uint32, maxFilterSize uint64) base.TableFilterPolicy {
	return adaptivePolicyImpl{TargetBitsPerKey: targetBitsPerKey, MaxSize: maxFilterSize}
}

type adaptivePolicyImpl struct {
	TargetBitsPerKey uint32
	MaxSize          uint64
}

var _ base.TableFilterPolicy = adaptivePolicyImpl{}

func (p adaptivePolicyImpl) Name() string {
	return fmt.Sprintf("adaptive_bloom(%d,%d)", p.TargetBitsPerKey, p.MaxSize)
}

func (p adaptivePolicyImpl) NewWriter() base.TableFilterWriter {
	return newAdaptiveFilterWriter(p.TargetBitsPerKey, p.MaxSize)
}

// adaptiveFilterWriter is a TableFilterWriter that uses up to w.bitsPerKey to
// create a filter of up to maxSize bytes.
type adaptiveFilterWriter struct {
	w       tableFilterWriter
	maxSize uint64
}

func (aw *adaptiveFilterWriter) AddKey(key []byte) {
	aw.w.AddKey(key)
}

func (aw *adaptiveFilterWriter) Finish() (_ []byte, _ base.TableFilterFamily, ok bool) {
	if aw.w.numHashes == 0 {
		return nil, "", false
	}
	if filterSize := FilterSize(aw.w.numHashes, aw.w.bitsPerKey); filterSize > aw.maxSize {
		aw.w.bitsPerKey = MaxBitsPerKey(aw.w.numHashes, aw.maxSize)
		// A single-bit filter is not very useful; it is large (at least maxSize/2) so
		// it wastes memory and bandwidth.
		if aw.w.bitsPerKey < 2 {
			return nil, "", false
		}
		aw.w.numProbes = calculateProbes(aw.w.bitsPerKey)
	}
	return aw.w.Finish()
}

var _ base.TableFilterWriter = (*adaptiveFilterWriter)(nil)

func newAdaptiveFilterWriter(targetBitsPerKey uint32, maxSize uint64) *adaptiveFilterWriter {
	aw := &adaptiveFilterWriter{}
	aw.w.init(targetBitsPerKey)
	aw.maxSize = maxSize
	return aw
}

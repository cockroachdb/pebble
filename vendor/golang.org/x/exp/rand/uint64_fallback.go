// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build !go1.12

package rand

func (pcg *PCGSource) add() {
	old := pcg.low
	pcg.low += incLow
	if pcg.low < old {
		// Carry occurred.
		pcg.high++
	}
	pcg.high += incHigh
}

func (pcg *PCGSource) multiply() {
	// Break each lower word into two separate 32-bit 'digits' each stored
	// in a 64-bit word with 32 high zero bits.  This allows the overflow
	// into the high word to be computed.
	s0 := (pcg.low >> 00) & maxUint32
	s1 := (pcg.low >> 32) & maxUint32

	const (
		m0    = (multiplier >> 00) & maxUint32
		m1    = (multiplier >> 32) & maxUint32
		mLow  = multiplier & (1<<64 - 1)
		mHigh = multiplier >> 64 & (1<<64 - 1)
	)

	high := pcg.low*mHigh + pcg.high*mLow
	s0m0 := s0 * m0
	s0m1 := s0 * m1
	s1m0 := s1 * m0
	s1m1 := s1 * m1
	high += (s0m1 >> 32) + (s1m0 >> 32)
	carry := (s0m1 & maxUint32) + (s1m0 & maxUint32) + s0m0>>32
	high += (carry >> 32)

	pcg.low *= mLow
	pcg.high = high + s1m1
}

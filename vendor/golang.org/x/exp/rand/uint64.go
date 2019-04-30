// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.12

package rand

import "math/bits"

func (pcg *PCGSource) add() {
	var carry uint64
	pcg.low, carry = bits.Add64(pcg.low, incLow, 0)
	pcg.high, _ = bits.Add64(pcg.high, incHigh, carry)
}

func (pcg *PCGSource) multiply() {
	hi, lo := bits.Mul64(pcg.low, mulLow)
	hi += pcg.high * mulLow
	hi += pcg.low * mulHigh
	pcg.low = lo
	pcg.high = hi
}

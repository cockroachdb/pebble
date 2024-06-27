// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package colblk implements a columnar block format.
package colblk

import "golang.org/x/exp/constraints"

func align[T constraints.Integer](offset, val T) T {
	return (offset + val - 1) & ^(val - 1)
}

func alignWithZeroes[T constraints.Integer](buf []byte, offset, val T) T {
	aligned := (offset + val - 1) & ^(val - 1)
	for i := offset; i < aligned; i++ {
		buf[i] = 0
	}
	return aligned
}

const (
	align16 = 2
	align32 = 4
	align64 = 8
)

// When multiplying or dividing by align{16,32,64}, it's faster to shift to the
// left to multiply or shift to the right to divide. The below constants define
// the shift amounts corresponding to the above align constants.
// (eg, alignNShift = log(alignN)).
const (
	align16Shift = 1
	align32Shift = 2
	align64Shift = 3
)

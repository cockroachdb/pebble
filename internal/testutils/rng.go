// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import "math/rand/v2"

// RandIntInRange returns a value in [min, max)
func RandIntInRange(r *rand.Rand, min, max int) int {
	return min + r.IntN(max-min)
}

var randLetters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandBytes returns a byte slice of the given length with random
// data.
func RandBytes(r *rand.Rand, size int) []byte {
	if size <= 0 {
		return nil
	}

	arr := make([]byte, size)
	for i := 0; i < len(arr); i++ {
		arr[i] = randLetters[r.IntN(len(randLetters))]
	}
	return arr
}

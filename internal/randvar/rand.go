// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package randvar

import "math/rand/v2"

// NewRand creates a new random number generator with a random seed.
func NewRand() *rand.Rand {
	return rand.New(rand.NewPCG(0, rand.Uint64()))
}

func ensureRand(rng *rand.Rand) *rand.Rand {
	if rng != nil {
		return rng
	}
	return NewRand()
}

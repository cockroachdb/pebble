// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package randvar

import (
	"fmt"
	"math/rand/v2"
)

// Static models a random variable that pulls from a static distribution.
type Static interface {
	// Uint64 returns a random uint64 value.
	Uint64(rng *rand.Rand) uint64
}

// StaticBytes models a random variable that generates byte slices.
type StaticBytes interface {
	fmt.Stringer

	// Bytes returns a random bytes value.
	Bytes(rng *rand.Rand, buf []byte) []byte
}

// Dynamic models a random variable that pulls from a distribution with an
// upper bound that can change dynamically using the IncMax method.
type Dynamic interface {
	Static

	// IncMax increments the max value the variable will return.
	IncMax(delta uint64)

	// Max returns the current max value the variable will return.
	Max() uint64
}

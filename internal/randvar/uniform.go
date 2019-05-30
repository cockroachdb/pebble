// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package randvar

import (
	"sync"

	"golang.org/x/exp/rand"
)

// Uniform is a random number generator that generates draws from a uniform
// distribution.
type Uniform struct {
	min uint64
	mu  struct {
		sync.Mutex
		rng *rand.Rand
		max uint64
	}
}

// NewUniform constructs a new Uniform generator with the given
// parameters. Returns an error if the parameters are outside the accepted
// range.
func NewUniform(rng *rand.Rand, min, max uint64) *Uniform {
	g := &Uniform{min: min}
	g.mu.rng = ensureRand(rng)
	g.mu.max = max
	return g
}

// IncMax increments max.
func (g *Uniform) IncMax(delta int) {
	g.mu.Lock()
	g.mu.max += uint64(delta)
	g.mu.Unlock()
}

// Uint64 returns a random Uint64 between min and max, drawn from a uniform
// distribution.
func (g *Uniform) Uint64() uint64 {
	g.mu.Lock()
	result := g.mu.rng.Uint64n(g.mu.max-g.min+1) + g.min
	g.mu.Unlock()
	return result
}

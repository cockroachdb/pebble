// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/internal/randvar"
)

func TestGenerateRandKeyInRange(t *testing.T) {
	rng := randvar.NewRand()
	g := newKeyGenerator(newKeyManager(1 /* numInstances */), rng, DefaultOpConfig())
	// Seed 100 initial keys.
	for i := 0; i < 100; i++ {
		_ = g.RandKey(1.0)
	}
	for i := 0; i < 100; i++ {
		a := g.RandKey(0.01)
		b := g.RandKey(0.01)
		// Ensure unique prefixes; required by RandKeyInRange.
		for g.equal(g.prefix(a), g.prefix(b)) {
			b = g.RandKey(0.01)
		}
		if v := g.cmp(a, b); v > 0 {
			a, b = b, a
		}
		kr := pebble.KeyRange{Start: a, End: b}
		for j := 0; j < 10; j++ {
			k := g.RandKeyInRange(0.05, kr)
			if g.cmp(k, a) < 0 {
				t.Errorf("generated random key %q outside range %s", k, kr)
			} else if g.cmp(k, b) >= 0 {
				t.Errorf("generated random key %q outside range %s", k, kr)
			}
		}
	}
}

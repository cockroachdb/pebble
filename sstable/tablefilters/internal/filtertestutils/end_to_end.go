// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package filtertestutils

import (
	crand "crypto/rand"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
)

// RunEndToEndTest generates filters and checks for correctness, and also that the false positive ratio is below the
// given threshold. The threshold should be set generously high to avoid flakes.
func RunEndToEndTest(
	t *testing.T, policy base.TableFilterPolicy, decoder base.TableFilterDecoder, maxFPR float64,
) {
	for range 10 {
		var numKeys int
		switch x := rand.IntN(100); {
		case x < 50:
			numKeys = 1 + rand.IntN(1000)
		case x < 85:
			numKeys = 1 + rand.IntN(10_000)
		case x < 99:
			numKeys = 1 + rand.IntN(100_000)
		default:
			numKeys = 1 + rand.IntN(1_000_000)
		}
		keys := make([][]byte, numKeys)
		for i := range keys {
			keys[i] = randKey(10 + rand.IntN(10))
		}

		w := policy.NewWriter()
		for _, key := range keys {
			w.AddKey(key)
		}
		filter, _, ok := w.Finish()
		if !ok {
			t.Fatalf("failed to create filter")
		}
		for _, k := range keys {
			if !decoder.MayContain(filter, k) {
				t.Fatalf("false negative")
			}
		}
		var falsePositives int
		for range 1000 {
			k := randKey(10 + rand.IntN(10))
			if decoder.MayContain(filter, k) {
				falsePositives++
			}
		}
		if float64(falsePositives)/1000 > maxFPR {
			t.Fatalf("false positive rate > %f", maxFPR)
		}
	}
}

func randKey(keyLen int) []byte {
	k := make([]byte, keyLen)
	_, _ = crand.Read(k)
	return k
}

// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestSingleSpanIterRandom(t *testing.T) {
	// To reproduce a failure:
	// runSingleSpanRandomTest(t, <seed>)
	for range 200 {
		runSingleSpanRandomTest(t, rand.Uint64())
	}
}

func runSingleSpanRandomTest(t *testing.T, seed uint64) {
	const numOps = 500

	rng := rand.New(rand.NewPCG(seed, seed))
	cmp := testkeys.Comparer

	cfg := RandKeyConfig(rng)

	// Pick a random non-empty span [spanStart, spanEnd).
	var spanStart, spanEnd []byte
	for {
		a := cfg.RandKey(rng)
		b := cfg.RandKey(rng)
		c := cmp.Compare(a, b)
		if c == 0 {
			continue
		}
		if c < 0 {
			spanStart, spanEnd = a, b
		} else {
			spanStart, spanEnd = b, a
		}
		break
	}

	// Pick a random RANGEDEL trailer.
	seqNum := cfg.MinSeqNum
	if cfg.MaxSeqNum > cfg.MinSeqNum {
		seqNum += base.SeqNum(rng.Uint64N(uint64(cfg.MaxSeqNum - cfg.MinSeqNum + 1)))
	}
	trailer := base.MakeTrailer(seqNum, base.InternalKeyKindRangeDelete)

	lower, upper := RandBounds(rng, cfg, nil, nil)

	var iter SingleSpanIter
	iter.Init(cmp, spanStart, spanEnd, trailer, lower, upper)

	defer func() {
		if t.Failed() {
			t.Logf("seed: %d", seed)
			t.Logf("cfg: %+v", cfg)
			t.Logf("span: [%q, %q) trailer=%s", spanStart, spanEnd, base.InternalKeyKind(trailer&0xff))
			t.Logf("lower, upper: %q %q", lower, upper)
		}
	}()

	checkCfg := CheckIterConfig{
		Comparer:     cmp,
		KeyGenConfig: cfg,
		OpWeights:    AllTestOps,
		NumOps:       numOps,
	}
	expected := TestIterData{
		Spans: []keyspan.Span{{
			Start: spanStart,
			End:   spanEnd,
			Keys:  []keyspan.Key{{Trailer: trailer}},
		}},
		Lower: lower,
		Upper: upper,
	}
	CheckIter(t, rng, checkCfg, expected, &iter)
}

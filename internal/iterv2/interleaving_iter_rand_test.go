// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invalidating"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestInterleavingIterRandom(t *testing.T) {
	// To reproduce a failure:
	// runRandomTest(t, <seed>)
	for range 200 {
		runRandomTest(t, rand.Uint64())
	}
}

func runRandomTest(t *testing.T, seed uint64) {
	const numOps = 500

	rng := rand.New(rand.NewPCG(seed, seed))

	cmp := testkeys.Comparer

	cfg := RandKeyConfig(rng)
	points := RandPointKeys(rng, cfg, 50)
	spans := RandSpans(rng, cfg, 10)

	var startKey, endKey []byte
	switch rng.IntN(3) {
	case 0:
		// Sometimes nil.
	case 1:
		// Sometimes a loose bound.
		startKey = []byte("a")
	case 2:
		// Sometimes a tight bound.
		if len(points) > 0 {
			startKey = points[0].K.UserKey
		}
		if len(spans) > 0 {
			if startKey == nil || cmp.Compare(startKey, spans[0].Start) > 0 {
				startKey = spans[0].Start
			}
		}
	}
	switch rng.IntN(3) {
	case 0:
		// Sometimes nil.
	case 1:
		// Sometimes a loose bound.
		endKey = []byte("z~")
	case 2:
		// Sometimes a tight bound.
		if len(points) > 0 {
			endKey = cmp.ImmediateSuccessor(nil, cmp.Split.Prefix(points[len(points)-1].K.UserKey))
		}
		if len(spans) > 0 {
			if endKey == nil || cmp.Compare(endKey, spans[len(spans)-1].End) < 0 {
				endKey = spans[len(spans)-1].End
			}
		}
	}

	lower, upper := RandBounds(rng, cfg, startKey, endKey)

	var pointIter base.InternalIterator
	pointIter = base.NewFakeIter(cmp, points)
	if lower != nil || upper != nil {
		pointIter.SetBounds(lower, upper)
	}
	if rand.IntN(2) == 0 {
		pointIter = invalidating.NewIter(pointIter)
	}

	var spanIter keyspan.FragmentIterator
	if len(spans) == 0 && rng.IntN(2) == 0 {
		// Use nil span iterator.
	} else {
		spanIter = keyspan.NewIter(cmp.Compare, spans)
		if rand.IntN(2) == 0 {
			spanIter = keyspan.NewInvalidatingIter(spanIter)
		}
	}
	var interleavingIter InterleavingIter
	interleavingIter.Init(
		cmp,
		pointIter,
		spanIter,
		startKey,
		endKey,
		lower,
		upper,
	)

	defer func() {
		if t.Failed() {
			t.Logf("seed: %d", seed)
			t.Logf("cfg: %+v", cfg)
			t.Logf("start, end: %q %q", startKey, endKey)
			t.Logf("lower, upper: %q %q", lower, upper)
			t.Logf("points: %s", base.FormatKeys(points))
			t.Logf("spans: %v", spans)
		}
	}()

	checkCfg := CheckIterConfig{
		Comparer:     cmp,
		KeyGenConfig: cfg,
		OpWeights:    AllTestOps,
		NumOps:       numOps,
	}
	expected := TestIterData{
		Points:   points,
		Spans:    spans,
		StartKey: startKey,
		EndKey:   endKey,
		Lower:    lower,
		Upper:    upper,
	}
	CheckIter(t, rng, checkCfg, expected, &interleavingIter)
}

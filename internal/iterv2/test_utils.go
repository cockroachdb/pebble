// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// TB is a subset of testing.TB used by CheckIter, allowing it to be used
// without a direct dependency on the testing package.
type TB interface {
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool
}

// TestOp enumerates the iterator operations used by CheckIter.
// TODO(radu): add TestOpSeekPrefixGEWithTrySeekUsingNext
type TestOp int

const (
	TestOpFirst TestOp = iota
	TestOpLast
	TestOpSeekGE
	TestOpSeekPrefixGE
	TestOpSeekLT
	TestOpNext
	TestOpPrev
	TestOpNextPrefix
	TestOpSetBounds
	NumTestOps
)

// TestOpWeights defines a weight for each TestOp. Operations with weight 0 are
// never selected. The values are relative (they don't need to sum to any
// particular number).
type TestOpWeights [NumTestOps]int

// AllTestOps is the default set of weights, which exercises all operations.
var AllTestOps = TestOpWeights{
	TestOpFirst:        5,
	TestOpLast:         5,
	TestOpSeekGE:       15,
	TestOpSeekPrefixGE: 15,
	TestOpSeekLT:       15,
	TestOpNext:         15,
	TestOpPrev:         15,
	TestOpNextPrefix:   10,
	TestOpSetBounds:    5,
}

// CheckIter constructs a TestIter with the given points and spans and runs
// numOps random positioning operations on TestIter and iter, comparing their
// outputs after each operation. The TestIter is wrapped in an OpCheckIter and
// iter is wrapped in a LoggingIter. On mismatch, the operation history is
// logged and t.Fatalf is called.
func CheckIter(
	t TB,
	rng *rand.Rand,
	cmp *base.Comparer,
	cfg KeyGenConfig,
	weights TestOpWeights,
	points []base.InternalKV,
	spans []keyspan.Span,
	iter Iter,
	startKey, endKey []byte,
	lower, upper []byte,
	numOps int,
) {
	var totalWeight int
	for _, w := range weights {
		totalWeight += w
	}
	if totalWeight == 0 {
		panic("CheckIter: all weights are zero")
	}

	testIter := NewTestIter(points, spans, startKey, endKey, lower, upper)
	checkIter := NewOpCheckIter(testIter, cmp, lower, upper)
	logIter := NewLoggingIter(iter)
	defer func() {
		_ = checkIter.Close()
		_ = logIter.Close()
	}()

	var lastKV *base.InternalKV
	for range numOps {
		// Pick a random operation based on weights.
		r := rng.IntN(totalWeight)
		var op TestOp
		for op = 0; op < NumTestOps-1; op++ {
			r -= weights[op]
			if r < 0 {
				break
			}
		}

		var refKV, intKV *base.InternalKV

		illegal := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					if _, ok := r.(*IllegalOpError); ok {
						illegal = true
						return
					}
					t.Logf("Ops:\n%s", logIter.String())
					panic(r)
				}
			}()

			switch op {
			case TestOpFirst:
				refKV = checkIter.First()
				intKV = logIter.First()

			case TestOpLast:
				refKV = checkIter.Last()
				intKV = logIter.Last()

			case TestOpSeekGE:
				key := cfg.RandKey(rng)
				refKV = checkIter.SeekGE(key, base.SeekGEFlagsNone)
				intKV = logIter.SeekGE(key, base.SeekGEFlagsNone)

			case TestOpSeekPrefixGE:
				key := cfg.RandKey(rng)
				prefix := cmp.Split.Prefix(key)
				refKV = checkIter.SeekPrefixGE(prefix, key, base.SeekGEFlagsNone)
				intKV = logIter.SeekPrefixGE(prefix, key, base.SeekGEFlagsNone)

			case TestOpSeekLT:
				key := cfg.RandKey(rng)
				refKV = checkIter.SeekLT(key, base.SeekLTFlagsNone)
				intKV = logIter.SeekLT(key, base.SeekLTFlagsNone)

			case TestOpNext:
				refKV = checkIter.Next()
				intKV = logIter.Next()

			case TestOpPrev:
				refKV = checkIter.Prev()
				intKV = logIter.Prev()

			case TestOpNextPrefix:
				if lastKV == nil || lastKV.Kind() == base.InternalKeyKindSpanBoundary {
					// NextPrefix not applicable; fall through to SetBounds.
					lower, upper = RandBounds(rng, cfg, startKey, endKey)
					checkIter.SetBounds(lower, upper)
					logIter.SetBounds(lower, upper)
					return
				}
				succKey := cmp.ImmediateSuccessor(nil, cmp.Split.Prefix(lastKV.K.UserKey))
				refKV = checkIter.NextPrefix(succKey)
				intKV = logIter.NextPrefix(succKey)

			case TestOpSetBounds:
				lower, upper = RandBounds(rng, cfg, startKey, endKey)
				checkIter.SetBounds(lower, upper)
				logIter.SetBounds(lower, upper)
			}
		}()

		if illegal {
			continue
		}
		lastKV = refKV

		// Check for mismatches.
		expSpan := checkIter.Span().String()
		actualSpan := logIter.Span().String()

		if expSpan != actualSpan {
			t.Logf("Ops:\n%s", logIter.String())
			t.Fatalf("expected: %v %s  got: %v %s", refKV, expSpan, intKV, actualSpan)
		}
		if refKV == nil || intKV == nil ||
			!bytes.Equal(refKV.K.UserKey, intKV.K.UserKey) ||
			refKV.K.Trailer != intKV.K.Trailer {
			if refKV == nil && intKV == nil {
				continue
			}
			t.Logf("Ops:\n%s", logIter.String())
			t.Fatalf("expected: %v %s  got: %v %s", refKV, expSpan, intKV, actualSpan)
		}
	}
}

// KeyGenConfig controls how random keys and spans are generated.
type KeyGenConfig struct {
	MaxPrefixLen int
	MaxSuffix    int         // 0 = no suffix
	MinSeqNum    base.SeqNum // minimum sequence number (inclusive)
	MaxSeqNum    base.SeqNum // maximum sequence number (inclusive)
}

// SimpleKeyConfig generates single-character prefix keys with no suffix and
// sequence number 1.
var SimpleKeyConfig = KeyGenConfig{
	MaxPrefixLen: 1,
	MaxSuffix:    0,
	MinSeqNum:    1,
	MaxSeqNum:    1,
}

// RandKeyConfig returns a randomized KeyGenConfig.
func RandKeyConfig(rng *rand.Rand) KeyGenConfig {
	cfg := KeyGenConfig{
		MaxPrefixLen: 1 + rng.IntN(4),
		MinSeqNum:    1,
		MaxSeqNum:    10,
	}
	if rng.IntN(2) == 0 {
		cfg.MaxSuffix = rng.IntN(20)
	}
	return cfg
}

// RandKey generates a random user key from the configured keyspace, with an
// optional MVCC suffix.
func (cfg KeyGenConfig) RandKey(rng *rand.Rand) []byte {
	ks := testkeys.Alpha(cfg.MaxPrefixLen)
	idx := rng.Uint64N(ks.Count())
	if cfg.MaxSuffix > 0 {
		return testkeys.KeyAt(ks, idx, int64(1+rng.IntN(cfg.MaxSuffix)))
	}
	return testkeys.Key(ks, idx)
}

// RandPointKeys generates a random sorted, deduplicated slice of point keys.
// The result may contain fewer than maxNum keys due to deduplication.
func RandPointKeys(rng *rand.Rand, cfg KeyGenConfig, maxNum int) []base.InternalKV {
	n := rng.IntN(maxNum + 1)
	if n == 0 {
		return nil
	}
	keys := make([]base.InternalKey, n)
	for i := range n {
		userKey := cfg.RandKey(rng)
		kind := base.InternalKeyKindSet
		if rng.IntN(2) == 0 {
			kind = base.InternalKeyKindDelete
		}
		seqNum := cfg.MinSeqNum
		if cfg.MaxSeqNum > cfg.MinSeqNum {
			seqNum += base.SeqNum(rng.Uint64N(uint64(cfg.MaxSeqNum - cfg.MinSeqNum + 1)))
		}
		keys[i] = base.MakeInternalKey(userKey, seqNum, kind)
	}

	// Sort by internal key ordering.
	slices.SortFunc(keys, func(a, b base.InternalKey) int {
		return base.InternalCompare(testkeys.Comparer.Compare, a, b)
	})

	// Deduplicate adjacent keys with identical (UserKey, Trailer).
	keys = slices.CompactFunc(keys, func(a, b base.InternalKey) bool {
		return testkeys.Comparer.Compare(a.UserKey, b.UserKey) == 0 && a.Trailer == b.Trailer
	})
	kvs := make([]base.InternalKV, len(keys))
	for i, k := range keys {
		kvs[i].K = k
		if k.Kind() == base.InternalKeyKindSet {
			kvs[i].V = base.MakeInPlaceValue([]byte(fmt.Sprintf("%s#%s", k.UserKey, k.SeqNum())))
		}
	}
	return kvs
}

// RandSpans generates a random slice of non-overlapping RANGEDEL spans. The
// result may contain fewer than maxNum spans due to deduplication or boundary
// exhaustion.
func RandSpans(rng *rand.Rand, cfg KeyGenConfig, maxNum int) []keyspan.Span {
	n := rng.IntN(maxNum + 1)
	if n == 0 {
		return nil
	}
	// Generate 2*n+2 random boundary keys.
	numBoundaries := 2*n + 2
	boundaries := make([][]byte, numBoundaries)
	for i := range numBoundaries {
		boundaries[i] = cfg.RandKey(rng)
	}

	// Sort and deduplicate.
	slices.SortFunc(boundaries, func(a, b []byte) int {
		return testkeys.Comparer.Compare(a, b)
	})
	boundaries = slices.CompactFunc(boundaries, func(a, b []byte) bool {
		return testkeys.Comparer.Compare(a, b) == 0
	})

	var spans []keyspan.Span
	for i := 0; i+1 < len(boundaries) && len(spans) < maxNum; {
		var start, end []byte
		if len(spans) > 0 && rng.IntN(10) == 0 {
			// 10% chance: reuse previous span's end key as current start
			// (adjacent spans).
			start = spans[len(spans)-1].End
			end = boundaries[i]
			i++
		} else {
			start = boundaries[i]
			end = boundaries[i+1]
			i += 2
		}
		// Ensure start < end; skip if not.
		if testkeys.Comparer.Compare(start, end) >= 0 {
			continue
		}

		// Generate 1-3 RANGEDEL keys for this span.
		numKeys := 1 + rng.IntN(3)
		spanKeys := make([]keyspan.Key, numKeys)
		for j := range numKeys {
			seqNum := cfg.MinSeqNum
			if cfg.MaxSeqNum > cfg.MinSeqNum {
				seqNum += base.SeqNum(rng.Uint64N(uint64(cfg.MaxSeqNum - cfg.MinSeqNum + 1)))
			}
			spanKeys[j] = keyspan.Key{
				Trailer: base.MakeTrailer(seqNum, base.InternalKeyKindRangeDelete),
			}
		}
		// Sort keys by trailer descending (required by keyspan.Span contract).
		slices.SortFunc(spanKeys, func(a, b keyspan.Key) int {
			return cmp.Compare(b.Trailer, a.Trailer)
		})

		spans = append(spans, keyspan.Span{
			Start: start,
			End:   end,
			Keys:  spanKeys,
		})
	}
	return spans
}

func RandBounds(rng *rand.Rand, cfg KeyGenConfig, startKey, endKey []byte) (lower, upper []byte) {
	if rng.IntN(2) == 0 {
		lower = cfg.RandKey(rng)
		if (startKey != nil && testkeys.Comparer.Compare(lower, startKey) < 0) ||
			(endKey != nil && testkeys.Comparer.Compare(lower, endKey) >= 0) {
			lower = nil
		}
	}
	if rng.IntN(2) == 0 {
		upper = cfg.RandKey(rng)
		if (endKey != nil && testkeys.Comparer.Compare(upper, endKey) > 0) ||
			(startKey != nil && testkeys.Comparer.Compare(upper, startKey) <= 0) ||
			(lower != nil && testkeys.Comparer.Compare(lower, upper) >= 0) {
			upper = nil
		}
	}
	return lower, upper
}

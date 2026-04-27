// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package iterv2

import (
	"bytes"
	"cmp"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

// TB is a subset of testing.TB used by CheckIter, allowing it to be used
// without a direct dependency on the testing package.
type TB interface {
	Helper()
	Logf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Failed() bool
}

// TestOp enumerates the iterator operations used by CheckIter.
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
	TestOpSeekGEWithTrySeekUsingNext
	TestOpSeekPrefixGEWithTrySeekUsingNext
	NumTestOps
)

// TestOpWeights defines a weight for each TestOp. Operations with weight 0 are
// never selected. The values are relative (they don't need to sum to any
// particular number).
type TestOpWeights [NumTestOps]int

// AllTestOps is the default set of weights, which exercises all operations.
var AllTestOps = TestOpWeights{
	TestOpFirst:                            5,
	TestOpLast:                             5,
	TestOpSeekGE:                           10,
	TestOpSeekGEWithTrySeekUsingNext:       10,
	TestOpSeekPrefixGE:                     10,
	TestOpSeekPrefixGEWithTrySeekUsingNext: 10,
	TestOpSeekLT:                           15,
	TestOpNext:                             30,
	TestOpPrev:                             10,
	TestOpNextPrefix:                       10,
	TestOpSetBounds:                        5,
}

// CheckIterConfig contains parameters for CheckIter.
type CheckIterConfig struct {
	Comparer     *base.Comparer
	KeyGenConfig KeyGenConfig
	OpWeights    TestOpWeights
	NumOps       int
	// RequirePrefixChangeForTrySeekUsingNext, when true, requires that
	// SeekPrefixGE(TrySeekUsingNext) be called with a prefix that differs from
	// the prefix of the most recent SeekPrefixGE.
	//
	// This flag should not be used with general iterv2.Iter implementations which
	// must support TrySeekUsingNext without changing the prefix. It is used to
	// test mergingIterV2 which is not an iterv2.Iter (but uses the iterv2 testing
	// infrastructure).
	RequirePrefixChangeForTrySeekUsingNext bool
}

// CheckIter constructs a TestIter with the given points and spans and runs
// numOps random positioning operations on TestIter and iter, comparing their
// outputs after each operation. The TestIter is wrapped in an OpCheckIter and
// iter is wrapped in a LoggingIter. On mismatch, the operation history is
// logged and t.Fatalf is called.
func CheckIter(t TB, rng *rand.Rand, cfg CheckIterConfig, expected TestIterData, iter Iter) {
	startKey, endKey := expected.StartKey, expected.EndKey
	lower, upper := expected.Lower, expected.Upper
	cmp := cfg.Comparer
	if startKey != nil && endKey != nil && cmp.Compare(startKey, endKey) >= 0 {
		panic(errors.AssertionFailedf("invalid range [%q, %q)", startKey, endKey))
	}
	var totalWeight int
	for _, w := range cfg.OpWeights {
		totalWeight += w
	}
	if totalWeight == 0 {
		panic(errors.AssertionFailedf("CheckIter: all weights are zero"))
	}

	testIter := NewTestIter(expected)
	checkIter := NewOpCheckIter(testIter, cmp, lower, upper)
	if cfg.RequirePrefixChangeForTrySeekUsingNext {
		checkIter.RequirePrefixChangeForTrySeekUsingNext()
	}
	logIter := NewLoggingIter(iter)
	defer func() {
		_ = checkIter.Close()
		_ = logIter.Close()
		if p := recover(); p != nil {
			t.Logf("Ops:\n%s", logIter.String())
			t.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

	var lastKV *base.InternalKV
	for range cfg.NumOps {
		// Pick a random operation based on weights.
		r := rng.IntN(totalWeight)
		var op TestOp
		for op = 0; op < NumTestOps-1; op++ {
			r -= cfg.OpWeights[op]
			if r < 0 {
				break
			}
		}

		var opKey []byte

		// Initialize doOp to perform the selected operation on any Iter.
		var doOp func(iter Iter) *base.InternalKV
		switch op {
		case TestOpFirst:
			doOp = func(iter Iter) *base.InternalKV { return iter.First() }

		case TestOpLast:
			doOp = func(iter Iter) *base.InternalKV { return iter.Last() }

		case TestOpSeekGE:
			opKey = cfg.KeyGenConfig.RandKey(rng)
			doOp = func(iter Iter) *base.InternalKV {
				return iter.SeekGE(opKey, base.SeekGEFlagsNone)
			}

		case TestOpSeekGEWithTrySeekUsingNext:
			opKey = cfg.KeyGenConfig.RandKey(rng)
			doOp = func(iter Iter) *base.InternalKV {
				return iter.SeekGE(opKey, base.SeekGEFlagsNone.EnableTrySeekUsingNext())
			}

		case TestOpSeekPrefixGE:
			opKey = cfg.KeyGenConfig.RandKey(rng)
			prefix := cmp.Split.Prefix(opKey)
			doOp = func(iter Iter) *base.InternalKV {
				return iter.SeekPrefixGE(prefix, opKey, base.SeekGEFlagsNone)
			}

		case TestOpSeekPrefixGEWithTrySeekUsingNext:
			opKey = cfg.KeyGenConfig.RandKey(rng)
			prefix := cmp.Split.Prefix(opKey)
			doOp = func(iter Iter) *base.InternalKV {
				return iter.SeekPrefixGE(prefix, opKey, base.SeekGEFlagsNone.EnableTrySeekUsingNext())
			}

		case TestOpSeekLT:
			key := cfg.KeyGenConfig.RandKey(rng)
			doOp = func(iter Iter) *base.InternalKV {
				return iter.SeekLT(key, base.SeekLTFlagsNone)
			}

		case TestOpNext:
			doOp = func(iter Iter) *base.InternalKV { return iter.Next() }

		case TestOpPrev:
			doOp = func(iter Iter) *base.InternalKV { return iter.Prev() }

		case TestOpNextPrefix:
			if lastKV == nil || lastKV.Kind() == base.InternalKeyKindSpanBoundary {
				// NextPrefix not applicable.
				continue
			}
			succKey := cmp.ImmediateSuccessor(nil, cmp.Split.Prefix(lastKV.K.UserKey))
			doOp = func(iter Iter) *base.InternalKV {
				return iter.NextPrefix(succKey)
			}

		case TestOpSetBounds:
			lower, upper = RandBounds(rng, cfg.KeyGenConfig, startKey, endKey)
			doOp = func(iter Iter) *base.InternalKV {
				iter.SetBounds(lower, upper)
				return nil
			}

		default:
			t.Fatalf("unknown op %d", op)
		}

		// Run doOp on checkIter, catching illegal ops.
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
			refKV = doOp(checkIter)
		}()
		if illegal {
			continue
		}

		// Run doOp on logIter; any illegal op here fails the test.
		intKV = doOp(logIter)

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

func RunIterOps(t TB, iter Iter, input string) string {
	t.Helper()
	var lastKV *base.InternalKV
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 1, ' ', 0)
	for line := range crstrings.LinesSeq(input) {
		parts := strings.Fields(line)
		cmd := parts[0]
		fmt.Fprintf(tw, "%s:\t", line)

		var kv *base.InternalKV
		switch cmd {
		case "first":
			kv = iter.First()
		case "last":
			kv = iter.Last()
		case "next":
			kv = iter.Next()
		case "prev":
			kv = iter.Prev()
		case "next-prefix":
			if lastKV == nil || lastKV.K.Kind() == base.InternalKeyKindSpanBoundary {
				t.Fatalf("next-prefix requires iterator to be positioned at point key")
			}
			prefix := testkeys.Comparer.Split.Prefix(lastKV.K.UserKey)
			succKey := testkeys.Comparer.ImmediateSuccessor(nil, prefix)
			kv = iter.NextPrefix(succKey)
		case "seek-ge":
			if len(parts) < 2 {
				t.Fatalf("ERROR: seek-ge requires a key argument")
			}
			flags := base.SeekGEFlagsNone
			if len(parts) >= 3 && parts[2] == "try-seek-using-next" {
				flags = flags.EnableTrySeekUsingNext()
			}
			kv = iter.SeekGE([]byte(parts[1]), flags)
		case "seek-prefix-ge":
			if len(parts) < 2 {
				t.Fatalf("ERROR: seek-prefix-ge requires a key argument")
			}
			prefix := testkeys.Comparer.Split.Prefix([]byte(parts[1]))
			flags := base.SeekGEFlagsNone
			if len(parts) >= 3 && parts[2] == "try-seek-using-next" {
				flags = flags.EnableTrySeekUsingNext()
			}
			kv = iter.SeekPrefixGE(prefix, []byte(parts[1]), flags)
		case "seek-lt":
			if len(parts) < 2 {
				t.Fatalf("ERROR: seek-lt requires a key argument")
			}
			kv = iter.SeekLT([]byte(parts[1]), base.SeekLTFlagsNone)
		case "set-bounds":
			if len(parts) < 3 {
				t.Fatalf("ERROR: set-bounds requires lower and upper args")
			}
			var lower, upper []byte
			if parts[1] != "." {
				lower = []byte(parts[1])
			}
			if parts[2] != "." {
				upper = []byte(parts[2])
			}
			iter.SetBounds(lower, upper)
			fmt.Fprintf(tw, "ok\n")
			continue
		default:
			t.Fatalf("ERROR: unknown op %q", cmd)
		}
		if err := iter.Error(); err != nil {
			fmt.Fprintf(tw, "err=%v\n", err)
			continue
		}
		if kv == nil {
			fmt.Fprintf(tw, ".\n")
		} else {
			fmt.Fprintf(tw, "%s %s\n", kv.K, iter.Span())
		}
		lastKV = kv
	}
	_ = tw.Flush()
	return buf.String()
}

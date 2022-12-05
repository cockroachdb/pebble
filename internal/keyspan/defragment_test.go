// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/pmezard/go-difflib/difflib"
)

func TestDefragmentingIter(t *testing.T) {
	comparer := testkeys.Comparer
	cmp := comparer.Compare
	internalEqual := DefragmentInternal
	alwaysEqual := DefragmentMethodFunc(func(_ base.Equal, _, _ *Span) bool { return true })
	staticReducer := StaticDefragmentReducer
	collectReducer := func(cur, next []Key) []Key {
		c := keysBySeqNumKind(append(cur, next...))
		sort.Sort(&c)
		return c
	}

	var buf bytes.Buffer
	var spans []Span
	datadriven.RunTest(t, "testdata/defragmenting_iter", func(t *testing.T, td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define":
			spans = spans[:0]
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, ParseSpan(line))
			}
			return ""
		case "iter":
			equal := internalEqual
			reducer := staticReducer
			for _, cmdArg := range td.CmdArgs {
				switch cmd := cmdArg.Key; cmd {
				case "equal":
					if len(cmdArg.Vals) != 1 {
						return fmt.Sprintf("only one equal func expected; got %d", len(cmdArg.Vals))
					}
					switch val := cmdArg.Vals[0]; val {
					case "internal":
						equal = internalEqual
					case "always":
						equal = alwaysEqual
					default:
						return fmt.Sprintf("unknown reducer %s", val)
					}
				case "reducer":
					if len(cmdArg.Vals) != 1 {
						return fmt.Sprintf("only one reducer expected; got %d", len(cmdArg.Vals))
					}
					switch val := cmdArg.Vals[0]; val {
					case "collect":
						reducer = collectReducer
					case "static":
						reducer = staticReducer
					default:
						return fmt.Sprintf("unknown reducer %s", val)
					}
				default:
					return fmt.Sprintf("unknown command: %s", cmd)
				}
			}
			var innerIter MergingIter
			innerIter.Init(cmp, noopTransform, new(MergingBuffers), NewIter(cmp, spans))
			var iter DefragmentingIter
			iter.Init(comparer, &innerIter, equal, reducer, new(DefragmentingBuffers))
			for _, line := range strings.Split(td.Input, "\n") {
				runIterOp(&buf, &iter, line)
			}
			return strings.TrimSpace(buf.String())
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestDefragmentingIter_Randomized(t *testing.T) {
	seed := time.Now().UnixNano()
	for i := int64(0); i < 100; i++ {
		testDefragmentingIteRandomizedOnce(t, seed+i)
	}
}

func TestDefragmentingIter_RandomizedFixedSeed(t *testing.T) {
	const seed = 1648173101214881000
	testDefragmentingIteRandomizedOnce(t, seed)
}

func testDefragmentingIteRandomizedOnce(t *testing.T, seed int64) {
	comparer := testkeys.Comparer
	cmp := comparer.Compare
	formatKey := comparer.FormatKey

	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed = %d", seed)

	// Use a key space of alphanumeric strings, with a random max length between
	// 1-2. Repeat keys are more common at the lower max lengths.
	ks := testkeys.Alpha(rng.Intn(2) + 1)

	// Generate between 1-15 range keys.
	const maxRangeKeys = 15
	var original, fragmented []Span
	numRangeKeys := 1 + rng.Intn(maxRangeKeys)
	for i := 0; i < numRangeKeys; i++ {
		startIdx := rng.Intn(ks.Count())
		endIdx := rng.Intn(ks.Count())
		for startIdx == endIdx {
			endIdx = rng.Intn(ks.Count())
		}
		if startIdx > endIdx {
			startIdx, endIdx = endIdx, startIdx
		}

		key := Key{
			Trailer: base.MakeTrailer(uint64(i), base.InternalKeyKindRangeKeySet),
			Value:   []byte(fmt.Sprintf("v%d", rng.Intn(3))),
		}
		// Generate suffixes 0, 1, 2, or 3 with 0 indicating none.
		if suffix := rng.Intn(4); suffix > 0 {
			key.Suffix = testkeys.Suffix(suffix)
		}
		original = append(original, Span{
			Start: testkeys.Key(ks, startIdx),
			End:   testkeys.Key(ks, endIdx),
			Keys:  []Key{key},
		})

		for startIdx < endIdx {
			width := rng.Intn(endIdx-startIdx) + 1
			fragmented = append(fragmented, Span{
				Start: testkeys.Key(ks, startIdx),
				End:   testkeys.Key(ks, startIdx+width),
				Keys:  []Key{key},
			})
			startIdx += width
		}
	}

	// Both the original and the deliberately fragmented spans may contain
	// overlaps, so we need to sort and fragment them.
	original = fragment(cmp, formatKey, original)
	fragmented = fragment(cmp, formatKey, fragmented)

	var originalInner MergingIter
	originalInner.Init(cmp, noopTransform, new(MergingBuffers), NewIter(cmp, original))
	var fragmentedInner MergingIter
	fragmentedInner.Init(cmp, noopTransform, new(MergingBuffers), NewIter(cmp, fragmented))

	var referenceIter, fragmentedIter DefragmentingIter
	referenceIter.Init(comparer, &originalInner, DefragmentInternal, StaticDefragmentReducer, new(DefragmentingBuffers))
	fragmentedIter.Init(comparer, &fragmentedInner, DefragmentInternal, StaticDefragmentReducer, new(DefragmentingBuffers))

	// Generate 100 random operations and run them against both iterators.
	const numIterOps = 100
	type opKind struct {
		weight int
		fn     func() string
	}
	ops := []opKind{
		{weight: 2, fn: func() string { return "first" }},
		{weight: 2, fn: func() string { return "last" }},
		{weight: 50, fn: func() string { return "next" }},
		{weight: 50, fn: func() string { return "prev" }},
		{weight: 5, fn: func() string {
			k := testkeys.Key(ks, rng.Intn(ks.Count()))
			return fmt.Sprintf("seekge(%s)", k)
		}},
		{weight: 5, fn: func() string {
			k := testkeys.Key(ks, rng.Intn(ks.Count()))
			return fmt.Sprintf("seeklt(%s)", k)
		}},
	}
	var totalWeight int
	for _, op := range ops {
		totalWeight += op.weight
	}
	var referenceHistory, fragmentedHistory bytes.Buffer
	for i := 0; i < numIterOps; i++ {
		p := rng.Intn(totalWeight)
		opIndex := 0
		if i == 0 {
			// First op is always a First().
		} else {
			for i, op := range ops {
				if p < op.weight {
					opIndex = i
					break
				}
				p -= op.weight
			}
		}
		op := ops[opIndex].fn()
		runIterOp(&referenceHistory, &referenceIter, op)
		runIterOp(&fragmentedHistory, &fragmentedIter, op)
		if !bytes.Equal(referenceHistory.Bytes(), fragmentedHistory.Bytes()) {
			t.Fatal(debugContext(cmp, formatKey, original, fragmented,
				referenceHistory.String(), fragmentedHistory.String()))
		}
	}
}

func fragment(cmp base.Compare, formatKey base.FormatKey, spans []Span) []Span {
	Sort(cmp, spans)
	var fragments []Span
	f := Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit: func(f Span) {
			fragments = append(fragments, f)
		},
	}
	for _, s := range spans {
		f.Add(s)
	}
	f.Finish()
	return fragments
}

func debugContext(
	cmp base.Compare,
	formatKey base.FormatKey,
	original, fragmented []Span,
	refHistory, fragHistory string,
) string {
	var buf bytes.Buffer
	fmt.Fprintln(&buf, "Reference:")
	for _, s := range original {
		fmt.Fprintln(&buf, s)
	}
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "Fragmented:")
	for _, s := range fragmented {
		fmt.Fprintln(&buf, s)
	}
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "\nOperations diff:")
	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:       difflib.SplitLines(refHistory),
		B:       difflib.SplitLines(fragHistory),
		Context: 5,
	})
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(&buf, diff)
	return buf.String()
}

var iterDelim = map[rune]bool{',': true, ' ': true, '(': true, ')': true, '"': true}

func runIterOp(w io.Writer, it FragmentIterator, op string) {
	fields := strings.FieldsFunc(op, func(r rune) bool { return iterDelim[r] })
	var s *Span
	switch strings.ToLower(fields[0]) {
	case "first":
		s = it.First()
	case "last":
		s = it.Last()
	case "seekge":
		s = it.SeekGE([]byte(fields[1]))
	case "seeklt":
		s = it.SeekLT([]byte(fields[1]))
	case "next":
		s = it.Next()
	case "prev":
		s = it.Prev()
	default:
		panic(fmt.Sprintf("unrecognized iter op %q", fields[0]))
	}
	fmt.Fprintf(w, "%-10s", op)
	if s == nil {
		fmt.Fprintln(w, ".")
		return
	}
	fmt.Fprintln(w, s)
}

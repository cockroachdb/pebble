// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

func TestDefragmentingIter(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	fmtKey := testkeys.Comparer.FormatKey

	var buf bytes.Buffer
	var spans []keyspan.Span
	datadriven.RunTest(t, "testdata/defragmenting_iter", func(td *datadriven.TestData) string {
		buf.Reset()
		switch td.Cmd {
		case "define":
			spans = spans[:0]
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				startKey, value := Parse(line)
				endKey, v, ok := DecodeEndKey(startKey.Kind(), value)
				require.True(t, ok)
				spans = append(spans, keyspan.Span{
					Start: startKey,
					End:   endKey,
					Value: v,
				})
			}
			return ""
		case "iter":
			var methodStr string
			var method DefragmentMethod
			td.ScanArgs(t, "method", &methodStr)
			switch methodStr {
			case "internal":
				method = DefragmentInternal
			case "logical":
				method = DefragmentLogical
			default:
				panic(fmt.Sprintf("unrecognized defragment method %q", methodStr))
			}
			var innerIter Iter
			innerIter.Init(cmp, fmtKey, base.InternalKeySeqNumMax, keyspan.NewIter(cmp, spans))
			var iter DefragmentingIter
			iter.Init(cmp, &innerIter, method)
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
	const seed = 1644617922104685000
	testDefragmentingIteRandomizedOnce(t, seed)
}

func testDefragmentingIteRandomizedOnce(t *testing.T, seed int64) {
	cmp := testkeys.Comparer.Compare
	formatKey := testkeys.Comparer.FormatKey

	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed = %d", seed)

	// Use a key space of alphanumeric strings, with a random max length between
	// 1-2. Repeat keys are more common at the lower max lengths.
	ks := testkeys.Alpha(rng.Intn(2) + 1)

	// Generate between 1-15 range keys.
	const maxRangeKeys = 15
	var original, fragmented []keyspan.Span
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

		var sv SuffixValue
		// Generate suffixes 0, 1, 2, or 3 with 0 indicating none.
		if suffix := rng.Intn(4); suffix > 0 {
			sv.Suffix = testkeys.Suffix(suffix)
		}
		sv.Value = []byte(fmt.Sprintf("v%d", rng.Intn(3)))
		encodedSuffixValue := make([]byte, EncodedSetSuffixValuesLen([]SuffixValue{sv}))
		EncodeSetSuffixValues(encodedSuffixValue, []SuffixValue{sv})

		start := testkeys.Key(ks, startIdx)
		end := testkeys.Key(ks, endIdx)
		original = append(original, keyspan.Span{
			Start: base.MakeInternalKey(start, uint64(i), base.InternalKeyKindRangeKeySet),
			End:   end,
			Value: encodedSuffixValue,
		})

		for startIdx < endIdx {
			width := rng.Intn(endIdx-startIdx) + 1
			start := testkeys.Key(ks, startIdx)
			end := testkeys.Key(ks, startIdx+width)
			fragmented = append(fragmented, keyspan.Span{
				Start: base.MakeInternalKey(start, uint64(i), base.InternalKeyKindRangeKeySet),
				End:   end,
				Value: encodedSuffixValue,
			})
			startIdx += width
		}
	}

	// Both the original and the deliberately fragmented spans may contain
	// overlaps, so we need to sort and fragment them.
	original = fragment(cmp, formatKey, original)
	fragmented = fragment(cmp, formatKey, fragmented)

	var originalInner Iter
	originalInner.Init(cmp, formatKey, base.InternalKeySeqNumMax, keyspan.NewIter(cmp, original))
	var fragmentedInner Iter
	fragmentedInner.Init(cmp, formatKey, base.InternalKeySeqNumMax, keyspan.NewIter(cmp, fragmented))

	var referenceIter, fragmentedIter DefragmentingIter
	referenceIter.Init(cmp, &originalInner, DefragmentLogical)
	fragmentedIter.Init(cmp, &fragmentedInner, DefragmentLogical)

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

func fragment(cmp base.Compare, formatKey base.FormatKey, spans []keyspan.Span) []keyspan.Span {
	keyspan.Sort(cmp, spans)
	var fragments []keyspan.Span
	f := keyspan.Fragmenter{
		Cmp:    cmp,
		Format: formatKey,
		Emit: func(emittedFrags []keyspan.Span) {
			fragments = append(fragments, emittedFrags...)
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
	original, fragmented []keyspan.Span,
	refHistory, fragHistory string,
) string {
	var buf bytes.Buffer
	fmt.Fprintln(&buf, "Reference:")
	printRangeKeys(&buf, cmp, formatKey, original)
	fmt.Fprintln(&buf)
	fmt.Fprintln(&buf, "Fragmented:")
	printRangeKeys(&buf, cmp, formatKey, fragmented)
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

func runIterOp(w io.Writer, it *DefragmentingIter, op string) {
	fields := strings.FieldsFunc(op, func(r rune) bool { return iterDelim[r] })
	var s *CoalescedSpan
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
	printRangeKey(w, s)
}

func printRangeKeys(w io.Writer, cmp base.Compare, formatKey base.FormatKey, spans []keyspan.Span) {
	var c Coalescer
	c.Init(cmp, formatKey, base.InternalKeySeqNumMax, func(s CoalescedSpan) {
		printRangeKey(w, &s)
	})
	for _, s := range spans {
		c.Add(s)
	}
	c.Finish()
}

func printRangeKey(w io.Writer, s *CoalescedSpan) {
	fmt.Fprintf(w, "[%s, %s): ", s.Start, s.End)
	if s.Delete {
		fmt.Fprintf(w, " DEL")
	}
	for j, item := range s.Items {
		if j > 0 || s.Delete {
			fmt.Fprint(w, ", ")
		}
		if item.Unset {
			fmt.Fprintf(w, "%q = <unset>", item.Suffix)
		} else {
			fmt.Fprintf(w, "%q = %q", item.Suffix, item.Value)
		}
	}
	fmt.Fprintln(w)
}

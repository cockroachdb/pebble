// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspanimpl

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestMergingIter(t *testing.T) {
	cmp := base.DefaultComparer.Compare

	var definedIters []keyspan.FragmentIterator
	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/merging_iter", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			definedIters = definedIters[:0]
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			var spans []keyspan.Span
			for _, line := range lines {
				if line == "--" {
					definedIters = append(definedIters, keyspan.NewInvalidatingIter(keyspan.NewIter(cmp, spans)))
					spans = nil
					continue
				}
				spans = append(spans, keyspan.ParseSpan(line))
			}
			if len(spans) > 0 {
				definedIters = append(definedIters, keyspan.NewInvalidatingIter(keyspan.NewIter(cmp, spans)))
			}
			return fmt.Sprintf("%d levels", len(definedIters))
		case "iter":
			buf.Reset()
			snapshot := base.SeqNumMax
			iters := slices.Clone(definedIters)
			for _, cmdArg := range td.CmdArgs {
				switch cmdArg.Key {
				case "snapshot":
					snapshot = base.ParseSeqNum(cmdArg.Vals[0])
				case "probes":
					// The first value indicates which of the merging iterator's
					// child iterators is the target.
					i, err := strconv.Atoi(cmdArg.Vals[0])
					if err != nil {
						return err.Error()
					}
					// The remaining values define probes to attach.
					iters[i] = keyspan.ParseAndAttachProbes(iters[i], &buf, cmdArg.Vals[1:]...)
				default:
					return fmt.Sprintf("unrecognized arg %q", cmdArg.Key)
				}
			}
			var iter MergingIter
			iter.Init(base.DefaultComparer, keyspan.VisibleTransform(snapshot), new(MergingBuffers), iters...)
			keyspan.RunIterCmd(td.Input, &iter, &buf)
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// TestMergingIter_FragmenterEquivalence tests for equivalence between the
// fragmentation performed on-the-fly by the MergingIter and the fragmentation
// performed by the Fragmenter.
//
// It does this by producing 1-10 levels of well-formed fragments. Generated
// fragments may overlap other levels arbitrarily, but within their level
// generated fragments may only overlap other fragments that share the same user
// key bounds.
//
// The test then feeds all the fragments, across all levels, into a Fragmenter
// and produces a Iter over those fragments. The test also constructs a
// MergingIter with a separate Iter for each level. It runs a random
// series of operations, applying each operation to both. It asserts that each
// operation has identical results on both iterators.
func TestMergingIter_FragmenterEquivalence(t *testing.T) {
	seed := time.Now().UnixNano()
	for i := int64(0); i < 10; i++ {
		testFragmenterEquivalenceOnce(t, seed+i)
	}
}

func TestMergingIter_FragmenterEquivalence_Seed(t *testing.T) {
	// This test uses a fixed seed. It's useful to manually edit its seed when
	// debugging a test failure of the variable-seed test.
	const seed = 1644517830186873000
	testFragmenterEquivalenceOnce(t, seed)
}

func testFragmenterEquivalenceOnce(t *testing.T, seed int64) {
	cmp := testkeys.Comparer.Compare
	rng := rand.New(rand.NewPCG(0, uint64(seed)))
	t.Logf("seed = %d", seed)

	// Use a key space of alphanumeric strings, with a random max length between
	// 1-3. Repeat keys are more common at the lower max lengths.
	ks := testkeys.Alpha(rng.IntN(3) + 1)

	// Generate between 1 and 10 levels of fragment iterators.
	levels := make([][]keyspan.Span, rng.IntN(10)+1)
	iters := make([]keyspan.FragmentIterator, len(levels))
	var allSpans []keyspan.Span
	var buf bytes.Buffer
	for l := 0; l < len(levels); l++ {
		fmt.Fprintf(&buf, "level %d: ", l)
		for keyspaceStartIdx := int64(0); keyspaceStartIdx < ks.Count(); {
			// Generate spans of lengths of up to a third of the keyspace.
			spanStartIdx := keyspaceStartIdx + rng.Int64N(ks.Count()/3)
			spanEndIdx := spanStartIdx + rng.Int64N(ks.Count()/3) + 1

			if spanEndIdx < ks.Count() {
				keyCount := uint64(rng.IntN(3) + 1)
				s := keyspan.Span{
					Start: testkeys.Key(ks, spanStartIdx),
					End:   testkeys.Key(ks, spanEndIdx),
					Keys:  make([]keyspan.Key, 0, keyCount),
				}
				for k := keyCount; k > 0; k-- {
					seqNum := base.SeqNum((len(levels)-l)*3) + base.SeqNum(k)
					s.Keys = append(s.Keys, keyspan.Key{
						Trailer: base.MakeTrailer(seqNum, base.InternalKeyKindRangeKeySet),
					})
				}
				if len(levels[l]) > 0 {
					fmt.Fprint(&buf, ", ")
				}
				fmt.Fprintf(&buf, "%s", s)

				levels[l] = append(levels[l], s)
				allSpans = append(allSpans, s)
			}
			keyspaceStartIdx = spanEndIdx
		}
		iters[l] = keyspan.NewInvalidatingIter(keyspan.NewIter(cmp, levels[l]))
		fmt.Fprintln(&buf)
	}

	// Fragment the spans across the levels.
	var allFragmented []keyspan.Span
	f := keyspan.Fragmenter{
		Cmp:    cmp,
		Format: testkeys.Comparer.FormatKey,
		Emit: func(span keyspan.Span) {
			allFragmented = append(allFragmented, span)
		},
	}
	keyspan.SortSpansByStartKey(f.Cmp, allSpans)
	for _, s := range allSpans {
		f.Add(s)
	}
	f.Finish()

	// Log all the levels and their fragments, as well as the fully-fragmented
	// spans produced by the Fragmenter.
	fmt.Fprintln(&buf, "Fragmenter fragments:")
	for i, s := range allFragmented {
		if i > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprint(&buf, s)
	}
	t.Logf("%d levels:\n%s\n", len(levels), buf.String())

	fragmenterIter := keyspan.NewIter(f.Cmp, allFragmented)
	mergingIter := &MergingIter{}
	mergingIter.Init(testkeys.Comparer, keyspan.VisibleTransform(base.SeqNumMax), new(MergingBuffers), iters...)

	// Position both so that it's okay to perform relative positioning
	// operations immediately.
	mergingIter.First()
	fragmenterIter.First()

	type opKind struct {
		weight int
		fn     func() (str string, f *keyspan.Span, m *keyspan.Span)
	}
	must := func(s *keyspan.Span, err error) *keyspan.Span {
		require.NoError(t, err)
		return s
	}
	ops := []opKind{
		{weight: 2, fn: func() (string, *keyspan.Span, *keyspan.Span) {
			return "First()", must(fragmenterIter.First()), must(mergingIter.First())
		}},
		{weight: 2, fn: func() (string, *keyspan.Span, *keyspan.Span) {
			return "Last()", must(fragmenterIter.Last()), must(mergingIter.Last())
		}},
		{weight: 5, fn: func() (string, *keyspan.Span, *keyspan.Span) {
			k := testkeys.Key(ks, rng.Int64N(ks.Count()))
			return fmt.Sprintf("SeekGE(%q)", k),
				must(fragmenterIter.SeekGE(k)),
				must(mergingIter.SeekGE(k))
		}},
		{weight: 5, fn: func() (string, *keyspan.Span, *keyspan.Span) {
			k := testkeys.Key(ks, rng.Int64N(ks.Count()))
			return fmt.Sprintf("SeekLT(%q)", k),
				must(fragmenterIter.SeekLT(k)),
				must(mergingIter.SeekLT(k))
		}},
		{weight: 50, fn: func() (string, *keyspan.Span, *keyspan.Span) {
			return "Next()", must(fragmenterIter.Next()), must(mergingIter.Next())
		}},
		{weight: 50, fn: func() (string, *keyspan.Span, *keyspan.Span) {
			return "Prev()", must(fragmenterIter.Prev()), must(mergingIter.Prev())
		}},
	}
	var totalWeight int
	for _, op := range ops {
		totalWeight += op.weight
	}

	var fragmenterBuf bytes.Buffer
	var mergingBuf bytes.Buffer
	opCount := rng.IntN(200) + 50
	for i := 0; i < opCount; i++ {
		p := rng.IntN(totalWeight)
		opIndex := 0
		for i, op := range ops {
			if p < op.weight {
				opIndex = i
				break
			}
			p -= op.weight
		}

		opString, fs, ms := ops[opIndex].fn()

		fragmenterBuf.Reset()
		mergingBuf.Reset()
		fmt.Fprint(&fragmenterBuf, fs)
		fmt.Fprint(&mergingBuf, ms)
		if fragmenterBuf.String() != mergingBuf.String() {
			t.Fatalf("seed %d, op %d: %s = %s, fragmenter iterator returned %s",
				seed, i, opString, mergingBuf.String(), fragmenterBuf.String())
		}
		t.Logf("op %d: %s = %s", i, opString, fragmenterBuf.String())
	}
}

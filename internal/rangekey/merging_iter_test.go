// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangekey

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestMergingIter(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	var buf bytes.Buffer
	var iter MergingIter

	formatKey := func(k *base.InternalKey, _ []byte) {
		if k == nil {
			fmt.Fprint(&buf, ".")
			return
		}
		s := iter.Current()
		fmt.Fprintf(&buf, "%s.%s", s, s.Start.Kind())
	}

	datadriven.RunTest(t, "testdata/merging_iter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var iters []keyspan.FragmentIterator
			var spans []keyspan.Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				if line == "--" {
					iters = append(iters, keyspan.NewIter(cmp, spans))
					spans = nil
					continue
				}

				startKey, value := Parse(line)
				endKey, v, ok := DecodeEndKey(startKey.Kind(), value)
				require.True(t, ok)
				spans = append(spans, keyspan.Span{
					Start: startKey,
					End:   endKey,
					Value: v,
				})
			}
			if len(spans) > 0 {
				iters = append(iters, keyspan.NewIter(cmp, spans))
			}
			iter.Init(cmp, iters...)
			return fmt.Sprintf("%d levels", len(iters))
		case "iter":
			buf.Reset()
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				bufLen := buf.Len()
				line = strings.TrimSpace(line)
				i := strings.IndexByte(line, ' ')
				iterCmd := line
				if i > 0 {
					iterCmd = string(line[:i])
				}
				switch iterCmd {
				case "first":
					formatKey(iter.First())
				case "last":
					formatKey(iter.Last())
				case "next":
					formatKey(iter.Next())
				case "prev":
					formatKey(iter.Prev())
				case "seek-ge":
					formatKey(iter.SeekGE([]byte(strings.TrimSpace(line[i:])), false))
				case "seek-lt":
					formatKey(iter.SeekLT([]byte(strings.TrimSpace(line[i:]))))
				case "set-bounds":
					bounds := strings.Fields(line[i:])
					if len(bounds) != 2 {
						return fmt.Sprintf("set-bounds expects 2 bounds, got %d", len(bounds))
					}
					l, u := []byte(bounds[0]), []byte(bounds[1])
					if bounds[0] == "." {
						l = nil
					}
					if bounds[1] == "." {
						u = nil
					}
					iter.SetBounds(l, u)
				default:
					return fmt.Sprintf("unrecognized iter command %q", iterCmd)
				}
				require.NoError(t, iter.Error())
				if buf.Len() > bufLen {
					fmt.Fprintln(&buf)
				}
			}
			return strings.TrimSpace(buf.String())

		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// TestMergingIter_FragmenterEquivalence tests for equivalence between the
// fragmentation performed on-the-fly by the MergingIter and the fragmentation
// performed by the keyspan.Fragmenter.
//
// It does this by producing 1-10 levels of well-formed fragments. Generated
// fragments may overlap other levels arbitrarily, but within their level
// generated fragments may only overlap other fragments that share the same user
// key bounds.
//
// The test then feeds all the fragments, across all levels, into a Fragmenter
// and produces a keyspan.Iter over those fragments. The test also constructs a
// MergingIter with a separate keyspan.Iter for each level. It runs a random
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
	const seed = 1642108194329956001
	testFragmenterEquivalenceOnce(t, seed)
}

func testFragmenterEquivalenceOnce(t *testing.T, seed int64) {
	cmp := testkeys.Comparer.Compare
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed = %d", seed)

	// Use a key space of alphanumeric strings, with a random max length between
	// 1-3. Repeat keys are more common at the lower max lengths.
	ks := testkeys.Alpha(rng.Intn(3) + 1)

	// Generate between 1 and 10 levels of fragment iterators.
	levels := make([][]keyspan.Span, rng.Intn(10)+1)
	iters := make([]keyspan.FragmentIterator, len(levels))
	var allSpans []keyspan.Span
	var buf bytes.Buffer
	for l := 0; l < len(levels); l++ {
		fmt.Fprintf(&buf, "level %d: ", l)
		for keyspaceStartIdx := 0; keyspaceStartIdx < ks.Count(); {
			// Generate spans of lengths of up to a third of the keyspace.
			spanStartIdx := keyspaceStartIdx + rng.Intn(ks.Count()/3)
			spanEndIdx := spanStartIdx + rng.Intn(ks.Count()/3) + 1

			if spanEndIdx < ks.Count() {
				startUserKey := testkeys.Key(ks, spanStartIdx)
				endUserKey := testkeys.Key(ks, spanEndIdx)
				fragmentCount := uint64(rng.Intn(3) + 1)

				for f := fragmentCount; f > 0; f-- {
					s := keyspan.Span{
						Start: base.MakeInternalKey(
							startUserKey,
							uint64((len(levels)-l)*3)+f,
							base.InternalKeyKindRangeKeySet,
						),
						End: endUserKey,
					}

					if len(levels[l]) > 0 {
						fmt.Fprint(&buf, ", ")
					}
					fmt.Fprintf(&buf, "%s", s)

					levels[l] = append(levels[l], s)
					allSpans = append(allSpans, s)
				}
			}
			keyspaceStartIdx = spanEndIdx
		}
		iters[l] = keyspan.NewIter(cmp, levels[l])
		fmt.Fprintln(&buf)
	}

	// Fragment the spans across the levels.
	var allFragmented []keyspan.Span
	f := keyspan.Fragmenter{
		Cmp:    cmp,
		Format: testkeys.Comparer.FormatKey,
		Emit: func(spans []keyspan.Span) {
			allFragmented = append(allFragmented, spans...)
		},
	}
	keyspan.Sort(f.Cmp, allSpans)
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
	mergingIter.Init(f.Cmp, iters...)

	// Position both so that it's okay to perform relative positioning
	// operations immediately.
	mergingIter.First()
	fragmenterIter.First()

	type opKind struct {
		weight int
		fn     func() string
	}
	ops := []opKind{
		{weight: 2, fn: func() string {
			fragmenterIter.First()
			mergingIter.First()
			return "First()"
		}},
		{weight: 2, fn: func() string {
			fragmenterIter.Last()
			mergingIter.Last()
			return "Last()"
		}},
		{weight: 5, fn: func() string {
			k := testkeys.Key(ks, rng.Intn(ks.Count()))
			fragmenterIter.SeekGE(k, false)
			mergingIter.SeekGE(k, false)
			return fmt.Sprintf("SeekGE(%q)", k)
		}},
		{weight: 5, fn: func() string {
			k := testkeys.Key(ks, rng.Intn(ks.Count()))
			fragmenterIter.SeekLT(k)
			mergingIter.SeekLT(k)
			return fmt.Sprintf("SeekLT(%q)", k)
		}},
		{weight: 50, fn: func() string {
			fragmenterIter.Next()
			mergingIter.Next()
			return "Next()"
		}},
		{weight: 50, fn: func() string {
			fragmenterIter.Prev()
			mergingIter.Prev()
			return "Prev()"
		}},
	}
	var totalWeight int
	for _, op := range ops {
		totalWeight += op.weight
	}

	var fragmenterBuf bytes.Buffer
	var mergingBuf bytes.Buffer
	opCount := rng.Intn(200) + 50
	for i := 0; i < opCount; i++ {
		p := rng.Intn(totalWeight)
		opIndex := 0
		for i, op := range ops {
			if p < op.weight {
				opIndex = i
				break
			}
			p -= op.weight
		}

		opString := ops[opIndex].fn()
		fs := fragmenterIter.Current()
		ms := mergingIter.Current()

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

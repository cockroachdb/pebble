// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func parseSpanWithKind(t testing.TB, s string) Span {
	// parseSpanWithKind expects a string like
	//
	//  a.RANGEKEYSET.10  : c <value>
	//
	// The value is optional.
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == ':' || unicode.IsSpace(r) })
	if len(fields) < 2 {
		t.Fatalf("key span string representation should have 2+ fields, found %d in %q", len(fields), s)
	}
	var value []byte
	if len(fields[2:]) > 0 {
		value = []byte(strings.Join(fields[2:], " "))
	}
	return Span{
		Start: base.ParseInternalKey(fields[0]),
		End:   []byte(fields[1]),
		Value: value,
	}
}

func TestMergingIter(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	var buf bytes.Buffer
	var iter MergingIter

	formatFragments := func(frags Fragments) {
		if frags.Empty() {
			fmt.Fprint(&buf, ".\n-\n")
			return
		}
		for i := 0; i < frags.Count(); i++ {
			s := frags.At(i)
			fmt.Fprintf(&buf, "%s.%s\n", s, s.Start.Kind())
		}
		fmt.Fprintln(&buf, "-")
	}

	datadriven.RunTest(t, "testdata/merging_iter", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var iters []FragmentIterator
			var spans []Span
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				if line == "--" {
					iters = append(iters, NewIter(cmp, spans))
					spans = nil
					continue
				}

				spans = append(spans, parseSpanWithKind(t, line))
			}
			if len(spans) > 0 {
				iters = append(iters, NewIter(cmp, spans))
			}
			iter.Init(cmp, iters...)
			return fmt.Sprintf("%d levels", len(iters))
		case "iter":
			buf.Reset()
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				i := strings.IndexByte(line, ' ')
				iterCmd := line
				if i > 0 {
					iterCmd = string(line[:i])
				}
				switch iterCmd {
				case "first":
					formatFragments(iter.First())
				case "last":
					formatFragments(iter.Last())
				case "next":
					formatFragments(iter.Next())
				case "prev":
					formatFragments(iter.Prev())
				case "seek-ge":
					formatFragments(iter.SeekGE([]byte(strings.TrimSpace(line[i:])), false))
				case "seek-lt":
					formatFragments(iter.SeekLT([]byte(strings.TrimSpace(line[i:]))))
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
			}
			return strings.TrimSpace(buf.String())

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
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed = %d", seed)

	// Use a key space of alphanumeric strings, with a random max length between
	// 1-3. Repeat keys are more common at the lower max lengths.
	ks := testkeys.Alpha(rng.Intn(3) + 1)

	// Generate between 1 and 10 levels of fragment iterators.
	levels := make([][]Span, rng.Intn(10)+1)
	iters := make([]FragmentIterator, len(levels))
	var allSpans []Span
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
					s := Span{
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
		iters[l] = NewIter(cmp, levels[l])
		fmt.Fprintln(&buf)
	}

	// Fragment the spans across the levels.
	var allFragmented []Span
	f := Fragmenter{
		Cmp:    cmp,
		Format: testkeys.Comparer.FormatKey,
		Emit: func(spans []Span) {
			allFragmented = append(allFragmented, spans...)
		},
	}
	Sort(f.Cmp, allSpans)
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

	fragmenterIter := NewIter(f.Cmp, allFragmented)
	mergingIter := &mergingIterAdapter{MergingIter: &MergingIter{}}
	mergingIter.MergingIter.Init(f.Cmp, iters...)

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

// mergingIterAdapter adapts MergingIter, which returns Fragments, to fulfill
// the FragmentIterator interface. It's used by
// TestMergingIter_FragmenterEquivalence to compare a Iter with a
// MergingIter, despite their different interfaces.
type mergingIterAdapter struct {
	*MergingIter
	index int
	dir   int8
	frags Fragments
}

var _ FragmentIterator = &mergingIterAdapter{}

func (i *mergingIterAdapter) Clone() FragmentIterator {
	panic("unimplemented")
}

func (i *mergingIterAdapter) End() []byte {
	return i.Current().End
}

func (i *mergingIterAdapter) Current() Span {
	if i.index < 0 || i.index >= i.frags.Count() {
		return Span{}
	}
	return i.frags.At(i.index)
}

func (i *mergingIterAdapter) Valid() bool {
	return i.index >= 0 && i.index < i.frags.Count()
}

func (i *mergingIterAdapter) firstFrag() (*base.InternalKey, []byte) {
	if i.frags.Empty() {
		return nil, nil
	}
	i.index = 0
	f := i.frags.At(i.index)
	return &f.Start, nil
}

func (i *mergingIterAdapter) lastFrag() (*base.InternalKey, []byte) {
	if i.frags.Empty() {
		return nil, nil
	}
	i.index = i.frags.Count() - 1
	f := i.frags.At(i.index)
	return &f.Start, nil
}

func (i *mergingIterAdapter) First() (*base.InternalKey, []byte) {
	i.frags = i.MergingIter.First()
	i.dir = +1
	return i.firstFrag()
}

func (i *mergingIterAdapter) Last() (*base.InternalKey, []byte) {
	i.frags = i.MergingIter.Last()
	i.dir = -1
	return i.lastFrag()
}

func (i *mergingIterAdapter) SeekGE(key []byte, trySeekUsingNext bool) (*base.InternalKey, []byte) {
	i.frags = i.MergingIter.SeekGE(key, trySeekUsingNext)
	i.dir = +1
	return i.firstFrag()
}

func (i *mergingIterAdapter) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*base.InternalKey, []byte) {
	panic("unimplemented")
}

func (i *mergingIterAdapter) SeekLT(key []byte) (*base.InternalKey, []byte) {
	i.frags = i.MergingIter.SeekLT(key)
	i.dir = -1
	return i.lastFrag()
}

func (i *mergingIterAdapter) Next() (*base.InternalKey, []byte) {
	if i.dir == +1 && i.frags.Empty() {
		// Already exhausted in the forward direction.
		return nil, nil
	} else if i.dir == -1 && i.frags.Empty() {
		i.dir = +1
		i.frags = i.MergingIter.Next()
		return i.firstFrag()
	}
	i.dir = +1
	i.index++
	if i.index >= i.frags.Count() {
		i.frags = i.MergingIter.Next()
		return i.firstFrag()
	}
	s := i.frags.At(i.index)
	return &s.Start, nil
}

func (i *mergingIterAdapter) Prev() (*base.InternalKey, []byte) {
	if i.dir == -1 && i.frags.Empty() {
		// Already exhausted in the backward direction.
		return nil, nil
	} else if i.dir == +1 && i.frags.Empty() {
		i.dir = -1
		i.frags = i.MergingIter.Prev()
		return i.lastFrag()
	}
	i.dir = -1
	i.index--
	if i.index < 0 {
		i.frags = i.MergingIter.Prev()
		return i.lastFrag()
	}
	s := i.frags.At(i.index)
	return &s.Start, nil
}

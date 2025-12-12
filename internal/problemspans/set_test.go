// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package problemspans

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
)

func TestSet(t *testing.T) {
	now := crtime.Mono(0)
	nowFn := func() crtime.Mono { return now }
	var set Set
	set.init(base.DefaultComparer.Compare, nowFn)

	datadriven.RunTest(t, "testdata/set", func(t *testing.T, td *datadriven.TestData) string {
		var nowStr string
		if td.MaybeScanArgs(t, "now", &nowStr) {
			var nowVal int
			if n, err := fmt.Sscanf(nowStr, "%ds", &nowVal); err != nil || n != 1 {
				td.Fatalf(t, "error parsing now %q: %v", nowStr, err)
			}
			v := crtime.Mono(time.Duration(nowVal) * time.Second)
			if v < now {
				td.Fatalf(t, "now cannot go backwards")
			}
			now = v
		}

		var out bytes.Buffer
		switch td.Cmd {
		case "reset":
			set.init(base.DefaultComparer.Compare, nowFn)
			now = 0

		case "add":
			for line := range crstrings.LinesSeq(td.Input) {
				bounds, expiration := parseSetLine(line, true /* withTime */)
				// The test uses absolute expiration times.
				set.Add(bounds, expiration.Sub(now))
			}

		case "excise":
			for line := range crstrings.LinesSeq(td.Input) {
				bounds, _ := parseSetLine(line, false /* withTime */)
				set.Excise(bounds)
			}

		case "overlap":
			for line := range crstrings.LinesSeq(td.Input) {
				bounds, _ := parseSetLine(line, false /* withTime */)
				res := "overlap"
				if !set.Overlaps(bounds) {
					res = "no overlap"
				}
				fmt.Fprintf(&out, "%s: %s\n", bounds, res)
			}

		case "is-empty":
			if set.IsEmpty() {
				out.WriteString("empty\n")
			} else {
				out.WriteString("not empty\n")
			}
		default:
			td.Fatalf(t, "unknown command %q", td.Cmd)
		}
		out.WriteString("Set:\n")
		for l := range crstrings.LinesSeq(set.String()) {
			fmt.Fprintf(&out, "  %s\n", l)
		}
		return out.String()
	})
}

func parseSetLine(line string, withTime bool) (base.UserKeyBounds, crtime.Mono) {
	var span1, span2 string
	var timeVal int
	if withTime {
		n, err := fmt.Sscanf(line, "%s %s %ds", &span1, &span2, &timeVal)
		if err != nil || n != 3 {
			panic(fmt.Sprintf("error parsing line %q: n=%d err=%v", line, n, err))
		}
	} else {
		n, err := fmt.Sscanf(line, "%s %s", &span1, &span2)
		if err != nil || n != 2 {
			panic(fmt.Sprintf("error parsing line %q: n=%d err=%v", line, n, err))
		}
	}
	bounds := base.ParseUserKeyBounds(span1 + " " + span2)
	return bounds, crtime.Mono(time.Duration(timeVal) * time.Second)
}

// TestSetRandomized cross-checks the Set implementation against a trivial
// implementation.
func TestSetRandomized(t *testing.T) {
	now := crtime.Mono(0)
	nowFn := func() crtime.Mono { return now }

	for test := 0; test < 1000; test++ {
		var set Set
		set.init(base.DefaultComparer.Compare, nowFn)
		var naive naiveSet

		keys := 4 + rand.Intn(100)
		key := func(k int) []byte {
			return []byte(fmt.Sprintf("%04d", k))
		}
		for op := 0; op < 300; op++ {
			k1, k2 := rand.Intn(keys), rand.Intn(keys)
			if k1 > k2 {
				k1, k2 = k2, k1
			}
			bounds := base.UserKeyBoundsInclusive(key(k1), key(k2))
			if k1 < k2 && rand.Intn(2) == 0 {
				bounds.End.Kind = base.Exclusive
			}

			if n := rand.Intn(10); n < 2 {
				expiration := time.Duration(rand.Intn(100))
				set.Add(bounds, expiration)
				naive.Add(bounds, now+crtime.Mono(expiration))
			} else if bounds.End.Kind == base.Exclusive && n == 9 {
				set.Excise(bounds)
				naive.Excise(bounds)
			} else {
				overlap := set.Overlaps(bounds)
				expected := naive.Overlaps(bounds, now)
				if expected != overlap {
					t.Fatalf("expected overlap=%t, got %t for bounds %s", expected, overlap, bounds)
				}
			}

			if rand.Intn(4) == 0 {
				now += crtime.Mono(rand.Intn(40))
			}
		}
	}
}

type naiveSpan struct {
	bounds     base.UserKeyBounds
	expiration crtime.Mono
}

type naiveSet struct {
	spans []naiveSpan
}

func (ns *naiveSet) Add(bounds base.UserKeyBounds, expiration crtime.Mono) {
	ns.spans = append(ns.spans, naiveSpan{bounds, expiration})
}

func (ns *naiveSet) Overlaps(bounds base.UserKeyBounds, now crtime.Mono) bool {
	for _, sp := range ns.spans {
		if sp.expiration > now && sp.bounds.Overlaps(base.DefaultComparer.Compare, bounds) {
			return true
		}
	}
	return false
}

func (ns *naiveSet) Excise(bounds base.UserKeyBounds) {
	if bounds.End.Kind != base.Exclusive {
		panic("inclusive end not supported")
	}
	var overlapping []naiveSpan

	cmp := base.DefaultComparer.Compare
	ns.spans = slices.DeleteFunc(ns.spans, func(sp naiveSpan) bool {
		if sp.bounds.Overlaps(cmp, bounds) {
			overlapping = append(overlapping, sp)
			return true
		}
		return false
	})
	for _, sp := range overlapping {
		if cmp(sp.bounds.Start, bounds.Start) < 0 {
			ns.spans = append(ns.spans, naiveSpan{
				bounds:     base.UserKeyBoundsEndExclusive(sp.bounds.Start, bounds.Start),
				expiration: sp.expiration,
			})
		}
		if bounds.End.CompareUpperBounds(cmp, sp.bounds.End) < 0 {
			ns.spans = append(ns.spans, naiveSpan{
				bounds: base.UserKeyBounds{
					Start: bounds.End.Key,
					End:   sp.bounds.End,
				},
				expiration: sp.expiration,
			})
		}
	}
}

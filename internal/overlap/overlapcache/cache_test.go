// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package overlapcache

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/datadriven/diagram"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestCacheDataDriven(t *testing.T) {
	var c C
	datadriven.RunTest(t, "testdata/cache", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "report":
			var r1, r2 base.UserKeyBounds
			lines := strings.Split(d.Input, "\n")
			if lines[0] != "[]" {
				r1 = base.ParseUserKeyBounds(lines[0])
			}
			if lines[1] != "[]" {
				r2 = base.ParseUserKeyBounds(lines[1])
			}
			c.ReportEmptyRegion(cmp, r1, r2)

		case "report-data":
			r := base.ParseUserKeyBounds(d.Input)
			c.ReportDataRegion(cmp, r)

		case "reset":
			c = C{}

		default:
			d.Fatalf(t, "unknown command: %s", d.Cmd)
		}
		return toStr(&c)
	})
}

var cmp = testkeys.Comparer.Compare

// toStr returns a diagram of the current state of the cache.
// Each region is marked with a "*" for data regions, "-" for empty regions, and
// "?" for unknown regions.
func toStr(c *C) string {
	var wb diagram.Whiteboard
	const spacing = 5
	col := 0
	region := func(ch string) {
		wb.Write(0, col, strings.Repeat(ch, spacing))
		col += spacing
	}
	ifElse := func(cond bool, a, b string) string {
		if cond {
			return a
		}
		return b
	}
	writeKey := func(key []byte) {
		str := string(key)
		wb.Write(1, col, "|")
		wb.Write(2, col-(len(str)-1)/2, str)
	}

	region(ifElse(c.mu.emptyBeforeRegion[0], "-", "?"))
	for i, r := range c.mu.dataRegions[:c.mu.n] {
		wb.Write(0, col, "*")
		writeKey(r.Start)
		col++
		if cmp(r.Start, r.End.Key) != 0 {
			region("*")
			wb.Write(0, col, ifElse(r.End.Kind == base.Exclusive, "|", "*"))
			writeKey(r.End.Key)
			col++
		}
		region(ifElse(c.mu.emptyBeforeRegion[i+1], "-", "?"))
	}
	return wb.Indented(2)
}

type region struct {
	start   int
	end     int
	endKind base.BoundaryKind
}

func (r region) UserKeyBounds() base.UserKeyBounds {
	return base.UserKeyBounds{
		Start: keys[r.start],
		End: base.UserKeyBoundary{
			Key:  keys[r.end],
			Kind: r.endKind,
		},
	}
}

func (r *region) SetRandKind() {
	r.endKind = base.Inclusive
	if r.start != r.end && rand.Intn(2) == 0 {
		r.endKind = base.Exclusive
	}
}

func (r *region) MaybeTrimRightRand() {
	if rand.Intn(2) == 0 {
		return
	}
	oldEnd := r.end
	r.end = randInRange(r.start, r.end+1)
	if r.start == r.end {
		r.endKind = base.Inclusive
	} else if oldEnd > r.end || r.endKind == base.Inclusive {
		r.SetRandKind()
	}
}

func (r *region) MaybeTrimLeftRand() {
	if rand.Intn(2) == 0 {
		return
	}
	if r.endKind == base.Inclusive {
		r.start = randInRange(r.start, r.end+1)
	} else {
		r.start = randInRange(r.start, r.end)
	}
}

func TestCacheRandomized(t *testing.T) {
	for n := 0; n < 100; n++ {
		runRandomizedTest(t)
	}
}

func runRandomizedTest(t *testing.T) {
	const debug = false

	// Generate data regions.
	numRegions := rand.Intn(20)
	regions := make([]region, numRegions)
	randKeys := rand.Perm(len(keys))[:numRegions+1]
	sort.Ints(randKeys)
	for i := range regions {
		regions[i].start = randKeys[i]
		regions[i].end = regions[i].start
		if rand.Intn(4) > 0 {
			regions[i].end = randInRange(regions[i].start, randKeys[i+1])
		}
		regions[i].SetRandKind()
	}
	if debug {
		fmt.Printf("Regions:")
		for i := range regions {
			fmt.Printf("  %s", regions[i].UserKeyBounds())
		}
	}
	c := &C{}
	for j := 0; j < 100; j++ {
		var knownRegion base.UserKeyBounds
		if rand.Intn(4) == 0 && len(regions) > 0 {
			r := regions[rand.Intn(len(regions))]
			r.MaybeTrimLeftRand()
			r.MaybeTrimRightRand()
			knownRegion = r.UserKeyBounds()
			if debug {
				fmt.Printf("ReportDataRegion(%s)\n", r.UserKeyBounds())
			}
			c.ReportDataRegion(cmp, r.UserKeyBounds())
		} else {
			var r1, r2 base.UserKeyBounds
			i := rand.Intn(len(regions)+1) - 1
			if i >= 0 {
				r := regions[i]
				r.MaybeTrimLeftRand()
				r1 = r.UserKeyBounds()
				knownRegion.Start = r1.Start
			}
			if i+1 < len(regions) {
				r := regions[i+1]
				r.MaybeTrimRightRand()
				r2 = r.UserKeyBounds()
				knownRegion.End = r2.End
			}
			if debug {
				fmt.Printf("ReportEmptyRegion(%s, %s)\n", r1, r2)
			}
			c.ReportEmptyRegion(cmp, r1, r2)
		}
		if debug {
			fmt.Printf("%s", toStr(c))
		}

		for j := 0; j < 100; j++ {
			r := randRegion().UserKeyBounds()

			result, ok := c.CheckDataOverlap(cmp, r)
			if !ok {
				// The cache must be able to answer queries for any region that overlaps
				// the knownRegion.
				if (knownRegion.Start == nil || r.End.IsUpperBoundFor(cmp, knownRegion.Start)) &&
					(knownRegion.End.Key == nil || knownRegion.End.IsUpperBoundFor(cmp, r.Start)) {
					t.Fatalf("cache should know if %s contains data", r)
				}
				continue
			}
			// Check the result.
			idx := sort.Search(len(regions), func(i int) bool {
				return regions[i].UserKeyBounds().End.IsUpperBoundFor(cmp, r.Start)
			})
			correct := idx < len(regions) && r.End.IsUpperBoundFor(cmp, keys[regions[idx].start])
			require.Equalf(t, correct, result, "incorrect ContainsData result for %s", r)
		}
	}
}

// Returns a random integer in [start, end).
func randInRange(start, end int) int {
	return start + rand.Intn(end-start)
}

func randRegion() region {
	var r region
	r.start = randInRange(0, len(keys))
	r.end = randInRange(r.start, len(keys))
	r.SetRandKind()
	return r
}

var keys = func() [][]byte {
	keys := make([][]byte, 100)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k%02d", i))
	}
	return keys
}()

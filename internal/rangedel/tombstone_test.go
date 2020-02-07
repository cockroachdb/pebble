// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
)

func TestTombstone_Overlaps(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	testCases := []struct {
		a    string
		b    string
		want int
	}{
		{ // Same start, end.
			a:    "a.RANGEDEL.2:b",
			b:    "a.RANGEDEL.1:b",
			want: -1,
		},
		{ // No overlap.
			a:    "a.RANGEDEL.2:b",
			b:    "b.RANGEDEL.1:c",
			want: -1,
		},
		{ // Overlap with one range inside other.
			a:    "a.RANGEDEL.2:d",
			b:    "b.RANGEDEL.1:c",
			want: 0,
		},
		{ // Overlap with intersection.
			a:    "a.RANGEDEL.2:c",
			b:    "b.RANGEDEL.1:d",
			want: 0,
		},
	}
	parse := func(s string) (start, end string) {
		keyValue := strings.Split(s, ":")
		if len(keyValue) != 2 {
			t.Fatal("incorrectly formatted tombstone")
		}
		return keyValue[0], keyValue[1]
	}
	for _, tc := range testCases {
		start, end := parse(tc.a)
		a := Tombstone{
			Start: base.ParseInternalKey(start),
			End:   []byte(end),
		}
		start, end = parse(tc.b)
		b := Tombstone{
			Start: base.ParseInternalKey(start),
			End:   []byte(end),
		}
		got := a.Overlaps(cmp, b)
		if got != tc.want {
			t.Fatalf("expected %d, but got %d", tc.want, got)
		}
		// Check inverted result if params are inverted.
		want := got * -1
		got = b.Overlaps(cmp, a)
		if got != want {
			t.Fatalf("expected %d, but got %d", want, got)
		}
	}
}

// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import "testing"

// TODO(jackson): Add unit tests for all of the various Span methods.

func TestSpan_ParseRoundtrip(t *testing.T) {
	spans := []string{
		"a-c:{(#5,RANGEDEL)}",
		"a-c:{(#5,RANGEDEL) (#2,RANGEDEL)}",
		"h-z:{(#20,RANGEKEYSET,@5,foo) (#15,RANGEKEYUNSET,@9) (#2,RANGEKEYDEL)}",
	}
	for _, input := range spans {
		got := ParseSpan(input).String()
		if got != input {
			t.Errorf("ParseSpan(%q).String() = %q", input, got)
		}
	}
}

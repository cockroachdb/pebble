// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestAssertBoundsIter(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	var spans []Span
	datadriven.RunTest(t, "testdata/assert_iter", func(t *testing.T, td *datadriven.TestData) string {
		switch cmd := td.Cmd; cmd {
		case "define":
			spans = spans[:0]
			lines := strings.Split(strings.TrimSpace(td.Input), "\n")
			for _, line := range lines {
				spans = append(spans, ParseSpan(line))
			}
			return ""

		case "assert-bounds", "assert-userkey-bounds":
			lines := strings.Split(td.Input, "\n")
			require.Equal(t, 2, len(lines))
			upper := []byte(lines[1])
			innerIter := NewIter(cmp, spans)
			var iter FragmentIterator
			if cmd == "assert-bounds" {
				lower := base.ParseInternalKey(lines[0])
				iter = AssertBounds(innerIter, lower, upper, cmp)
			} else {
				lower := []byte(lines[0])
				iter = AssertUserKeyBounds(innerIter, lower, upper, cmp)
			}
			defer iter.Close()

			return func() (res string) {
				defer func() {
					if r := recover(); r != nil {
						res = fmt.Sprintf("%v", r)
					}
				}()
				var span *Span
				var err error
				for span, err = iter.First(); span != nil; span, err = iter.Next() {
				}
				require.NoError(t, err)
				return "OK"
			}()

		default:
			return fmt.Sprintf("unknown command: %s", cmd)
		}
	})
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func TestSeek(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	fmtKey := base.DefaultComparer.FormatKey
	var iter FragmentIterator

	datadriven.RunTest(t, "testdata/seek", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := buildSpans(t, cmp, fmtKey, d.Input, base.InternalKeyKindRangeDelete)
			iter = NewIter(cmp, tombstones)
			return formatSpans(tombstones)

		case "seek-ge", "seek-le":
			seek := SeekGE
			if d.Cmd == "seek-le" {
				seek = SeekLE
			}

			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return fmt.Sprintf("malformed input: %s", line)
				}
				seq, err := strconv.ParseUint(parts[1], 10, 64)
				if err != nil {
					return err.Error()
				}
				tombstone := seek(cmp, iter, []byte(parts[0]), seq)
				fmt.Fprintf(&buf, "%s", strings.TrimSpace(formatSpans([]Span{tombstone})))
				fmt.Fprintf(&buf, "\n")
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package keyspan

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func TestIter(t *testing.T) {
	var tombstones []Span
	datadriven.RunTest(t, "testdata/iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			tombstones = nil
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				tombstones = append(tombstones, Span{
					Start: base.ParseInternalKey(key[:j]),
					End:   []byte(key[j+1:]),
				})
			}
			return ""

		case "iter":
			iter := NewIter(base.DefaultComparer.Compare, tombstones)
			defer iter.Close()

			var b bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				var start *base.InternalKey
				switch parts[0] {
				case "seek-ge":
					if len(parts) != 2 {
						return "seek-ge <key>\n"
					}
					start, _ = iter.SeekGE([]byte(strings.TrimSpace(parts[1])), false /* trySeekUsingNext */)
				case "seek-lt":
					if len(parts) != 2 {
						return "seek-lt <key>\n"
					}
					start, _ = iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
				case "first":
					start, _ = iter.First()
				case "last":
					start, _ = iter.Last()
				case "next":
					start, _ = iter.Next()
				case "prev":
					start, _ = iter.Prev()
				case "set-bounds":
					if len(parts) != 3 {
						return fmt.Sprintf("set-bounds expects 2 bounds, got %d", len(parts)-1)
					}
					l, u := []byte(parts[1]), []byte(parts[2])
					if parts[1] == "." {
						l = nil
					}
					if parts[2] == "." {
						u = nil
					}
					iter.SetBounds(l, u)
					fmt.Fprintf(&b, ".\n")
					continue
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if iter.Valid() {
					fmt.Fprintf(&b, "%s-%s#%d\n", start.UserKey, iter.End(), start.SeqNum())
				} else if err := iter.Error(); err != nil {
					fmt.Fprintf(&b, "err=%v\n", err)
				} else {
					fmt.Fprintf(&b, ".\n")
				}
			}
			return b.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

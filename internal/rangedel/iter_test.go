// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
)

func TestIter(t *testing.T) {
	var tombstones []Tombstone
	datadriven.RunTest(t, "testdata/iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			tombstones = nil
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				tombstones = append(tombstones, Tombstone{
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
				switch parts[0] {
				case "seek-ge":
					if len(parts) != 2 {
						return "seek-ge <key>\n"
					}
					iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
				case "seek-lt":
					if len(parts) != 2 {
						return "seek-lt <key>\n"
					}
					iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
				case "first":
					iter.First()
				case "last":
					iter.Last()
				case "next":
					iter.Next()
				case "prev":
					iter.Prev()
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if iter.Valid() {
					fmt.Fprintf(&b, "%s-%s#%d\n", iter.Key().UserKey, iter.Value(), iter.Key().SeqNum())
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

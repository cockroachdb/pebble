// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
)

func TestSeek(t *testing.T) {
	cmp := db.DefaultComparer.Compare
	var iter iterator

	datadriven.RunTest(t, "testdata/seek", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := buildTombstones(t, cmp, d.Input)
			iter = NewIter(cmp, tombstones)
			return formatTombstones(tombstones)

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
				fmt.Fprintf(&buf, "%s",
					strings.TrimSpace(formatTombstones([]Tombstone{tombstone})))
				// Check that the returned tombstone and the tombstone the iterator is
				// pointed at are identical.
				var iTombstone Tombstone
				if iter.Valid() {
					iTombstone = Tombstone{
						Start: iter.Key(),
						End:   iter.Value(),
					}
				}
				if !reflect.DeepEqual(tombstone, iTombstone) {
					fmt.Fprintf(&buf, " [%s]",
						strings.TrimSpace(formatTombstones([]Tombstone{tombstone})))
				}
				fmt.Fprintf(&buf, "\n")
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

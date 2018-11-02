// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
)

func runInternalIterCmd(d *datadriven.TestData, iter db.InternalIterator) string {
	var b bytes.Buffer
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-ge <key>\n")
			}
			iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-lt":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-lt <key>\n")
			}
			iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			iter.First()
		case "last":
			iter.Last()
		case "next":
			iter.Next()
		case "next-user-key":
			iter.NextUserKey()
		case "prev":
			iter.Prev()
		case "prev-user-key":
			iter.PrevUserKey()
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if iter.Valid() {
			fmt.Fprintf(&b, "%s:%s\n", iter.Key().UserKey, iter.Value())
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else {
			fmt.Fprintf(&b, ".\n")
		}
	}
	return b.String()
}

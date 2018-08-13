// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/petermattis/pebble/datadriven"
	"github.com/petermattis/pebble/db"
)

func TestCompactionIter(t *testing.T) {
	var keys []db.InternalKey
	var vals [][]byte

	newIter := func() *compactionIter {
		return &compactionIter{
			cmp:   db.DefaultComparer.Compare,
			merge: db.DefaultMerger.Merge,
			iter:  &fakeIter{keys: keys, vals: vals},
		}
	}

	datadriven.RunTest(t, "testdata/compaction_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			keys = keys[:0]
			vals = vals[:0]
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				keys = append(keys, makeIkey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			return ""

		case "iter":
			iter := newIter()
			var b bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "first":
					iter.First()
				case "next":
					iter.Next()
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				if iter.Valid() {
					fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
				} else if err := iter.Error(); err != nil {
					fmt.Fprintf(&b, "err=%v\n", err)
				} else {
					fmt.Fprintf(&b, ".\n")
				}
			}
			return b.String()

		default:
			t.Fatalf("unknown command: %s", d.Cmd)
		}

		return ""
	})
}

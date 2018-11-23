// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
)

func TestFragmenter(t *testing.T) {
	var tombstoneRe = regexp.MustCompile(`(\w+)-(\w+)#(\d+)`)

	parseTombstone := func(t *testing.T, s string) Tombstone {
		m := tombstoneRe.FindStringSubmatch(s)
		if len(m) != 4 {
			t.Fatalf("expected 4 components, but found %d", len(m))
		}
		seqNum, err := strconv.Atoi(m[3])
		if err != nil {
			t.Fatal(err)
		}
		return Tombstone{
			Start: db.MakeInternalKey([]byte(m[1]), uint64(seqNum), 0),
			End:   []byte(m[2]),
		}
	}

	build := func(t *testing.T, s string) []Tombstone {
		var tombstones []Tombstone
		f := &Fragmenter{
			Cmp: db.DefaultComparer.Compare,
			Emit: func(fragmented []Tombstone) {
				tombstones = append(tombstones, fragmented...)
			},
		}
		for _, p := range strings.Split(s, ",") {
			t := parseTombstone(t, p)
			f.Add(t.Start, t.End)
		}
		f.Finish()
		return tombstones
	}

	datadriven.RunTest(t, "testdata/fragmenter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones := build(t, d.Input)
			var buf bytes.Buffer
			for _, v := range tombstones {
				fmt.Fprintf(&buf, "%s-%s#%d\n",
					v.Start.UserKey, v.End, v.Start.SeqNum())
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// TODO(peter): Test Fragmenter.Deleted

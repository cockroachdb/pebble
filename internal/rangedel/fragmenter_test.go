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

var tombstoneRe = regexp.MustCompile(`(\w+)-(\w+)#(\d+)`)

func parseTombstone(t *testing.T, s string) Tombstone {
	m := tombstoneRe.FindStringSubmatch(s)
	if len(m) != 4 {
		t.Fatalf("expected 4 components, but found %d", len(m))
	}
	seqNum, err := strconv.Atoi(m[3])
	if err != nil {
		t.Fatal(err)
	}
	return Tombstone{
		Start: db.MakeInternalKey([]byte(m[1]), uint64(seqNum), db.InternalKeyKindRangeDelete),
		End:   []byte(m[2]),
	}
}

func TestFragmenter(t *testing.T) {
	cmp := db.DefaultComparer.Compare

	build := func(t *testing.T, s string) []Tombstone {
		var tombstones []Tombstone
		f := &Fragmenter{
			Cmp: cmp,
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

	var getRe = regexp.MustCompile(`(\w+)#(\d+)`)

	parseGet := func(t *testing.T, s string) (string, int) {
		m := getRe.FindStringSubmatch(s)
		if len(m) != 3 {
			t.Fatalf("expected 3 components, but found %d", len(m))
		}
		seq, err := strconv.Atoi(m[2])
		if err != nil {
			t.Fatal(err)
		}
		return m[1], seq
	}

	var tombstones []Tombstone

	// Returns true if the specified <key,seq> pair is deleted at the specified
	// read sequence number. Get ignores tombstones newer than the read sequence
	// number. This is a simple version of what full processing of range
	// tombstones looks like.
	deleted := func(key []byte, seq, readSeq uint64) bool {
		i := NewIter(cmp, tombstones)

		// NB: We use SeekLT in order to land on the proper tombstone for a search
		// key that resides in the middle of a tombstone. Consider the scenario:
		//
		//     a---e
		//         e---i
		//
		// The tombstones are indexed by their start keys `a` and `e`. If the
		// search key is `c` we want to land on the tombstone [a,e). If we were to
		// use SeekGE then the search key `c` would land on the tombstone [e,i) and
		// we'd have to backtrack. The one complexity here is what happens for the
		// search key `e`. In that case SeekLT will land us on the tombstone [a,e)
		// and we'll have to move forward.
		i.SeekLT(key)
		if !i.Valid() {
			i.Next()
			if !i.Valid() {
				return false
			}
		}

		// Advance the iterator as long as the search key lies past the end of the
		// tombstone. See the comment above about why this is necessary.
		for cmp(key, i.Value()) >= 0 {
			i.Next()
			if !i.Valid() || cmp(key, i.Key().UserKey) < 0 {
				// We've run out of tombstones or we've moved on to a tombstone which
				// starts after our search key.
				return false
			}
		}

		// At this point, key >= tombstone-start and key < tombstone-end. Walk
		// through the tombstones to find one that is both visible and newer than
		// our key's sequence number. SeekLT returns the oldest entry for a key, so
		// back up to find the newest.
		for {
			i.Prev()
			if !i.Valid() || cmp(key, i.Value()) >= 0 {
				i.Next()
				break
			}
		}

		for {
			tSeqNum := i.Key().SeqNum()
			if tSeqNum <= readSeq {
				// The tombstone is visible at our read sequence number.
				if tSeqNum >= seq {
					// The key lies within the tombstone and the tombstone is newer than
					// the key.
					return true
				}
			}
			i.Next()
			if !i.Valid() || cmp(key, i.Key().UserKey) < 0 {
				// We've run out of tombstones or we've moved on to a tombstone which
				// starts after our search key.
				return false
			}
		}
	}

	datadriven.RunTest(t, "testdata/fragmenter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			tombstones = build(t, d.Input)
			var buf bytes.Buffer
			for _, v := range tombstones {
				fmt.Fprintf(&buf, "%s-%s#%d\n",
					v.Start.UserKey, v.End, v.Start.SeqNum())
			}
			return buf.String()

		case "get":
			if len(d.CmdArgs) != 1 {
				t.Fatalf("expected 1 argument, but found %s", d.CmdArgs)
			}
			if d.CmdArgs[0].Key != "t" {
				t.Fatalf("expected timestamp argument, but found %s", d.CmdArgs[0])
			}
			readSeq, err := strconv.Atoi(d.CmdArgs[0].Vals[0])
			if err != nil {
				t.Fatal(err)
			}

			var results []string
			for _, p := range strings.Split(d.Input, " ") {
				key, seq := parseGet(t, p)
				if deleted([]byte(key), uint64(seq), uint64(readSeq)) {
					results = append(results, "deleted")
				} else {
					results = append(results, "alive")
				}
			}
			return strings.Join(results, " ")

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestFragmenterDeleted(t *testing.T) {
	datadriven.RunTest(t, "testdata/fragmenter_deleted", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "build":
			f := &Fragmenter{
				Cmp: db.DefaultComparer.Compare,
				Emit: func(fragmented []Tombstone) {
				},
			}
			var buf bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					return fmt.Sprintf("malformed input: %s", line)
				}
				switch fields[0] {
				case "add":
					t := parseTombstone(t, fields[1])
					f.Add(t.Start, t.End)
				case "deleted":
					key := db.ParseInternalKey(fields[1])
					func() {
						defer func() {
							if r := recover(); r != nil {
								fmt.Fprintf(&buf, "%s: %s\n", key, r)
							}
						}()
						fmt.Fprintf(&buf, "%s: %t\n", key, f.Deleted(key))
					}()
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

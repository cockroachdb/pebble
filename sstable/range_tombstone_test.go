// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/datadriven"
	"github.com/petermattis/pebble/db"
)

func TestRangeTombstone(t *testing.T) {
	cmp := db.DefaultComparer.Compare

	var tombstoneRe = regexp.MustCompile(`(\w+)-(\w+)#(\d+)`)

	parseTombstone := func(t *testing.T, s string) rangeTombstone {
		m := tombstoneRe.FindStringSubmatch(s)
		if len(m) != 4 {
			t.Fatalf("expected 4 components, but found %d", len(m))
		}
		seqNum, err := strconv.Atoi(m[3])
		if err != nil {
			t.Fatal(err)
		}
		return rangeTombstone{
			start: db.MakeInternalKey([]byte(m[1]), uint64(seqNum), 0),
			end:   []byte(m[2]),
		}
	}

	build := func(t *testing.T, s string) block {
		w := rangeTombstoneBlockWriter{cmp: cmp}
		w.block.restartInterval = 1
		for _, p := range strings.Split(s, ",") {
			t := parseTombstone(t, p)
			w.add(t.start.UserKey, t.end, t.start.SeqNum())
		}
		return w.finish()
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

	var block block

	// Returns true if the specified <key,seq> pair is deleted at the specified
	// read sequence number. Get ignores tombstones newer than the read sequence
	// number. This is a simple version of what full processing of range
	// tombstones looks like.
	deleted := func(key []byte, seq, readSeq uint64) bool {
		i, err := newBlockIter(cmp, block)
		if err != nil {
			t.Fatal(err)
		}

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
		// our key's sequence number.

		// TODO(peter): SeekLT needs to return the newest entry for a key and
		// currently it returns the oldest. So back up to find the newest.
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

	datadriven.RunTest(t, "testdata/range_tombstone", func(d *datadriven.TestData) string {
		switch d.Cmd {
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
			return strings.Join(results, " ") + "\n"

		case "build":
			block = build(t, d.Input)
			i, err := newBlockIter(cmp, block)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			for i.First(); i.Valid(); i.Next() {
				fmt.Fprintf(&buf, "%s-%s#%d\n",
					i.Key().UserKey, i.Value(), i.Key().SeqNum())
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

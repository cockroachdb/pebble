// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/sstable"
)

type mockSplitter struct {
	shouldSplitVal ShouldSplit
}

func (m *mockSplitter) ShouldSplitBefore(key *base.InternalKey, tw *sstable.Writer) ShouldSplit {
	return m.shouldSplitVal
}

func (m *mockSplitter) OnNewOutput(key []byte) []byte {
	return nil
}

func TestOutputSplitters(t *testing.T) {
	var main, child0, child1 OutputSplitter
	var prevUserKey []byte
	cmp := base.DefaultComparer.Compare
	pickSplitter := func(input string) *OutputSplitter {
		switch input {
		case "main":
			return &main
		case "child0":
			return &child0
		case "child1":
			return &child1
		default:
			t.Fatalf("invalid splitter slot: %s", input)
			return nil
		}
	}

	datadriven.RunTest(t, "testdata/output_splitters",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "reset":
				main = nil
				child0 = nil
				child1 = nil
			case "init":
				if len(d.CmdArgs) < 2 {
					return "expected at least 2 args"
				}
				splitterToInit := pickSplitter(d.CmdArgs[0].Key)
				switch d.CmdArgs[1].Key {
				case "array":
					*splitterToInit = CombineSplitters(cmp, child0, child1)
				case "mock":
					*splitterToInit = &mockSplitter{}
				case "userkey":
					*splitterToInit = UserKeyChangeSplitter(cmp, child0, func() []byte { return prevUserKey })
				}
				(*splitterToInit).OnNewOutput(nil)
			case "set-should-split":
				if len(d.CmdArgs) < 2 {
					return "expected at least 2 args"
				}
				splitterToSet := (*pickSplitter(d.CmdArgs[0].Key)).(*mockSplitter)
				var val ShouldSplit
				switch d.CmdArgs[1].Key {
				case "split-now":
					val = SplitNow
				case "no-split":
					val = NoSplit
				default:
					t.Fatalf("unexpected value for should-split: %s", d.CmdArgs[1].Key)
				}
				splitterToSet.shouldSplitVal = val
			case "should-split-before":
				if len(d.CmdArgs) < 1 {
					return "expected at least 1 arg"
				}
				key := base.ParseInternalKey(d.CmdArgs[0].Key)
				shouldSplit := main.ShouldSplitBefore(&key, nil)
				if shouldSplit == SplitNow {
					main.OnNewOutput(key.UserKey)
					prevUserKey = nil
				} else {
					prevUserKey = key.UserKey
				}
				return shouldSplit.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
			return "ok"
		})
}

func TestFrontiers(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	var keySets [][][]byte
	datadriven.RunTest(t, "testdata/frontiers", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			// Init configures a frontier per line of input. Each line should
			// contain a sorted whitespace-separated list of keys that the
			// frontier will use.
			//
			// For example, the following input creates two separate monitored
			// frontiers: one that sets its key successively to 'd', 'e', 'j'
			// and one that sets its key to 'a', 'p', 'n', 'z':
			//
			//    init
			//    b e j
			//    a p n z

			keySets = keySets[:0]
			for _, line := range strings.Split(td.Input, "\n") {
				keySets = append(keySets, bytes.Fields([]byte(line)))
			}
			return ""
		case "scan":
			f := &Frontiers{cmp: cmp}
			for _, keys := range keySets {
				initTestFrontier(f, keys...)
			}
			var buf bytes.Buffer
			for _, kStr := range strings.Fields(td.Input) {
				k := []byte(kStr)
				f.Advance(k)
				fmt.Fprintf(&buf, "%s : { %s }\n", kStr, f.String())
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// initTestFrontiers adds a new frontier to f that iterates through the provided
// keys. The keys slice must be sorted.
func initTestFrontier(f *Frontiers, keys ...[]byte) *frontier {
	ff := &frontier{}
	var key []byte
	if len(keys) > 0 {
		key, keys = keys[0], keys[1:]
	}
	reached := func(k []byte) (nextKey []byte) {
		if len(keys) > 0 {
			nextKey, keys = keys[0], keys[1:]
		}
		return nextKey
	}
	ff.Init(f, key, reached)
	return ff
}

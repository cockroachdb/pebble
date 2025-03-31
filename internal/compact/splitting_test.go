// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestOutputSplitter(t *testing.T) {
	var s *OutputSplitter
	var grandparents manifest.LevelMetadata
	datadriven.RunTest(t, "testdata/output_splitter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init-grandparents":
			// We create a version with all tables in L1.
			var files [manifest.NumLevels][]*manifest.TableMetadata
			if d.Input != "" {
				for _, l := range strings.Split(d.Input, "\n") {
					f, err := manifest.ParseTableMetadataDebug(l)
					if err != nil {
						d.Fatalf(t, "error parsing %q: %v", l, err)
					}
					files[1] = append(files[1], f)
				}
			}
			l0Organizer := manifest.NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
			v := manifest.NewVersionForTesting(base.DefaultComparer, l0Organizer, files)
			if err := v.CheckOrdering(); err != nil {
				d.Fatalf(t, "%v", err)
			}
			grandparents = v.Levels[1]

		case "run":
			var startKey, limitKey string
			var targetFileSize uint64
			f := &Frontiers{cmp: base.DefaultComparer.Compare}
			d.ScanArgs(t, "start-key", &startKey)
			d.MaybeScanArgs(t, "limit-key", &limitKey)
			d.ScanArgs(t, "target-size", &targetFileSize)
			s = NewOutputSplitter(
				base.DefaultComparer.Compare, []byte(startKey), []byte(limitKey),
				targetFileSize, grandparents.Iter(), f,
			)
			var last string
			for i, l := range strings.Split(d.Input, "\n") {
				var key string
				var estimatedSize uint64
				fmt.Sscanf(l, "%s %d", &key, &estimatedSize)
				// Advance the frontier, except (sometimes) for the first key where the
				// splitter allows for the frontier to already be at the next user key.
				if i > 0 || rand.IntN(2) == 0 {
					f.Advance([]byte(key))
				}
				if s.ShouldSplitBefore([]byte(key), estimatedSize, func(k []byte) bool { return f.cmp(k, []byte(last)) == 0 }) {
					return fmt.Sprintf("%s %d: split at %q", key, estimatedSize, s.SplitKey())
				}
				last = key
			}
			return fmt.Sprintf("split at %q", s.SplitKey())

		default:
			d.Fatalf(t, "unknown command: %s", d.Cmd)
		}
		return ""
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

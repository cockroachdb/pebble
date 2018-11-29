// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
)

func TestDBIter(t *testing.T) {
	var keys []db.InternalKey
	var vals [][]byte

	newIter := func(seqNum uint64, opts *db.IterOptions) *dbIter {
		return &dbIter{
			opts:     opts,
			cmp:      db.DefaultComparer.Compare,
			merge:    db.DefaultMerger.Merge,
			iter:     &fakeIter{keys: keys, vals: vals},
			snapshot: seqNum,
		}
	}

	datadriven.RunTest(t, "testdata/db_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			keys = keys[:0]
			vals = vals[:0]
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				keys = append(keys, db.ParseInternalKey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			return ""

		case "iter":
			var seqNum int
			var opts db.IterOptions

			for _, arg := range d.CmdArgs {
				if len(arg.Vals) != 1 {
					t.Fatalf("%s: %s=<value>", d.Cmd, arg.Key)
				}
				switch arg.Key {
				case "seq":
					var err error
					seqNum, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						return err.Error()
					}
				case "lower":
					opts.LowerBound = []byte(arg.Vals[0])
				case "upper":
					opts.UpperBound = []byte(arg.Vals[0])
				default:
					t.Fatalf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}

			iter := newIter(uint64(seqNum), &opts)
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
				case "prev":
					iter.Prev()
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

func BenchmarkDBIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := &dbIter{
		cmp:      db.DefaultComparer.Compare,
		iter:     m.newIter(nil),
		snapshot: db.InternalKeySeqNumMax,
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[rng.Intn(len(keys))]
		iter.SeekGE(key)
	}
}

func BenchmarkDBIterNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := &dbIter{
		cmp:      db.DefaultComparer.Compare,
		iter:     m.newIter(nil),
		snapshot: db.InternalKeySeqNumMax,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.First()
		}
		iter.Next()
	}
}

func BenchmarkDBIterPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := &dbIter{
		cmp:      db.DefaultComparer.Compare,
		iter:     m.newIter(nil),
		snapshot: db.InternalKeySeqNumMax,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.Last()
		}
		iter.Prev()
	}
}

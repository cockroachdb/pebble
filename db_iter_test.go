// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
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
		cmp := db.DefaultComparer.Compare
		// NB: Use a mergingIter to filter entries newer than seqNum.
		iter := newMergingIter(cmp, &fakeIter{keys: keys, vals: vals})
		iter.snapshot = seqNum
		return &dbIter{
			opts:  opts,
			cmp:   cmp,
			merge: db.DefaultMerger.Merge,
			iter:  iter,
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
					return fmt.Sprintf("%s: %s=<value>", d.Cmd, arg.Key)
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
					return fmt.Sprintf("%s: unknown arg: %s", d.Cmd, arg.Key)
				}
			}

			iter := newIter(uint64(seqNum), &opts)
			defer iter.Close()
			return runIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func BenchmarkDBIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := &dbIter{
		cmp:  db.DefaultComparer.Compare,
		iter: m.newIter(nil),
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
		cmp:  db.DefaultComparer.Compare,
		iter: m.newIter(nil),
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
		cmp:  db.DefaultComparer.Compare,
		iter: m.newIter(nil),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.Last()
		}
		iter.Prev()
	}
}

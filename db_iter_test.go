package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/datadriven"
	"github.com/petermattis/pebble/db"
)

func TestDBIter(t *testing.T) {
	var keys []db.InternalKey
	var vals [][]byte

	datadriven.RunTest(t, "testdata/db_iter", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			keys = nil
			vals = nil
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				keys = append(keys, makeIkey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			return ""

		case "next":
			seek := fakeIkey(strings.TrimSpace(d.Input))
			iter := &dbIter{
				cmp:    db.DefaultComparer.Compare,
				iter:   &fakeIter{keys: keys, vals: vals},
				seqNum: seek.SeqNum(),
			}

			var b bytes.Buffer
			for iter.SeekGE([]byte(seek.UserKey)); iter.Valid(); iter.Next() {
				fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
			}
			if err := iter.Error(); err != nil {
				fmt.Fprintf(&b, "err=%v\n", err)
			}
			return b.String()

		case "prev":
			seek := fakeIkey(strings.TrimSpace(d.Input))
			iter := &dbIter{
				cmp:    db.DefaultComparer.Compare,
				iter:   &fakeIter{keys: keys, vals: vals},
				seqNum: seek.SeqNum(),
			}

			var b bytes.Buffer
			for iter.SeekLT([]byte(seek.UserKey)); iter.Valid(); iter.Prev() {
				fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
			}
			if err := iter.Error(); err != nil {
				fmt.Fprintf(&b, "err=%v\n", err)
			}
			return b.String()
		}

		return ""
	})
}

func BenchmarkDBIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := &dbIter{
		cmp:    db.DefaultComparer.Compare,
		iter:   m.NewIter(nil),
		seqNum: db.InternalKeySeqNumMax,
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
		cmp:    db.DefaultComparer.Compare,
		iter:   m.NewIter(nil),
		seqNum: db.InternalKeySeqNumMax,
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
		cmp:    db.DefaultComparer.Compare,
		iter:   m.NewIter(nil),
		seqNum: db.InternalKeySeqNumMax,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.Last()
		}
		iter.Prev()
	}
}

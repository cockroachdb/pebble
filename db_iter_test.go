package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
)

func TestDBIterNextPrev(t *testing.T) {
	testCases := []struct {
		seek         string
		keys         string
		expectedNext string
		expectedPrev string
	}{
		{
			"a:1",
			"a.SET.1:b",
			"<a:b>.",
			".",
		},
		{
			"a:1",
			"a.SET.2:c a.SET.1:b",
			"<a:b>.",
			".",
		},
		{
			"a:2",
			"a.SET.2:c a.SET.1:b",
			"<a:c>.",
			".",
		},
		{
			"a:2",
			"a.DEL.2: a.SET.1:b",
			".",
			".",
		},
		{
			"a:3",
			"a.DEL.2: a.SET.1:b b.SET.3:c",
			"<b:c>.",
			".",
		},
		{
			"a:3",
			"a.SET.1:a b.SET.2:b c.SET.3:c",
			"<a:a><b:b><c:c>.",
			".",
		},
		{
			"b:3",
			"a.SET.1:a b.SET.2:b c.SET.3:c",
			"<b:b><c:c>.",
			"<a:a>.",
		},
		{
			"c:3",
			"a.SET.1:a b.SET.2:b c.SET.3:c",
			"<c:c>.",
			"<b:b><a:a>.",
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var keys []db.InternalKey
			var vals [][]byte
			for _, key := range strings.Split(c.keys, " ") {
				j := strings.Index(key, ":")
				keys = append(keys, makeIkey(key[:j]))
				vals = append(vals, []byte(key[j+1:]))
			}
			seek := fakeIkey(c.seek)
			iter := &dbIter{
				cmp:    db.DefaultComparer.Compare,
				iter:   &fakeIter{keys: keys, vals: vals},
				seqNum: seek.SeqNum(),
			}

			var b bytes.Buffer
			for iter.SeekGE([]byte(seek.UserKey)); iter.Valid(); iter.Next() {
				fmt.Fprintf(&b, "<%s:%s>", iter.Key(), iter.Value())
			}
			if err := iter.Error(); err != nil {
				fmt.Fprintf(&b, "err=%v", err)
			} else {
				b.WriteByte('.')
			}
			if got := b.String(); got != c.expectedNext {
				t.Errorf("got  %q\nwant %q", got, c.expectedNext)
			}

			b.Reset()
			for iter.SeekLT([]byte(seek.UserKey)); iter.Valid(); iter.Prev() {
				fmt.Fprintf(&b, "<%s:%s>", iter.Key(), iter.Value())
			}
			if err := iter.Close(); err != nil {
				fmt.Fprintf(&b, "err=%v", err)
			} else {
				b.WriteByte('.')
			}
			if got := b.String(); got != c.expectedPrev {
				t.Errorf("got  %q\nwant %q", got, c.expectedPrev)
			}
		})
	}
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

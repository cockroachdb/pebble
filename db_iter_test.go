package pebble

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

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
			"<a:b>.",
		},
		{
			"a:1",
			"a.SET.2:c a.SET.1:b",
			"<a:b>.",
			"<a:b>.",
		},
		{
			"a:2",
			"a.SET.2:c a.SET.1:b",
			"<a:c>.",
			"<a:c>.",
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
				seqnum: seek.Seqnum(),
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
			for iter.SeekLE([]byte(seek.UserKey)); iter.Valid(); iter.Prev() {
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

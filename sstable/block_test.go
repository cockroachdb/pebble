// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/datadriven"
	"github.com/petermattis/pebble/db"
)

func TestBlockWriter(t *testing.T) {
	ikey := func(s string) db.InternalKey {
		return db.InternalKey{UserKey: []byte(s)}
	}

	w := &rawBlockWriter{
		blockWriter: blockWriter{restartInterval: 16},
	}
	w.add(ikey("apple"), nil)
	w.add(ikey("apricot"), nil)
	w.add(ikey("banana"), nil)
	block := w.finish()

	expected := []byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00")
	if bytes.Compare(expected, block) != 0 {
		t.Fatalf("expected\n%q\nfound\n%q", expected, block)
	}
}

func TestBlockIter(t *testing.T) {
	// k is a block that maps three keys "apple", "apricot", "banana" to empty strings.
	k := block([]byte(
		"\x00\x05\x00apple" +
			"\x02\x05\x00ricot" +
			"\x00\x06\x00banana" +
			"\x00\x00\x00\x00\x01\x00\x00\x00"))
	var testcases = []struct {
		index int
		key   string
	}{
		{0, ""},
		{0, "a"},
		{0, "aaaaaaaaaaaaaaa"},
		{0, "app"},
		{0, "apple"},
		{1, "appliance"},
		{1, "apricos"},
		{1, "apricot"},
		{2, "azzzzzzzzzzzzzz"},
		{2, "b"},
		{2, "banan"},
		{2, "banana"},
		{3, "banana\x00"},
		{3, "c"},
	}
	for _, tc := range testcases {
		i, err := newRawBlockIter(bytes.Compare, k)
		if err != nil {
			t.Fatal(err)
		}
		i.SeekGE([]byte(tc.key))
		for j, kWant := range []string{"apple", "apricot", "banana"}[tc.index:] {
			if !i.Valid() {
				t.Fatalf("key=%q, index=%d, j=%d: Valid got false, want true", tc.key, tc.index, j)
			}
			if kGot := string(i.Key().UserKey); kGot != kWant {
				t.Fatalf("key=%q, index=%d, j=%d: got %q, want %q", tc.key, tc.index, j, kGot, kWant)
			}
			i.Next()
		}
		if i.Valid() {
			t.Fatalf("key=%q, index=%d: Valid got true, want false", tc.key, tc.index)
		}
		if err := i.Close(); err != nil {
			t.Fatalf("key=%q, index=%d: got err=%v", tc.key, tc.index, err)
		}
	}

	{
		i, err := newRawBlockIter(bytes.Compare, k)
		if err != nil {
			t.Fatal(err)
		}
		i.Last()
		for j, kWant := range []string{"banana", "apricot", "apple"} {
			if !i.Valid() {
				t.Fatalf("j=%d: Valid got false, want true", j)
			}
			if kGot := string(i.Key().UserKey); kGot != kWant {
				t.Fatalf("j=%d: got %q, want %q", j, kGot, kWant)
			}
			i.Prev()
		}
		if i.Valid() {
			t.Fatalf("Valid got true, want false")
		}
		if err := i.Close(); err != nil {
			t.Fatalf("got err=%v", err)
		}
	}
}

func TestBlockIter2(t *testing.T) {
	makeIkey := func(s string) db.InternalKey {
		j := strings.Index(s, ":")
		seqNum, err := strconv.Atoi(s[j+1:])
		if err != nil {
			panic(err)
		}
		return db.MakeInternalKey([]byte(s[:j]), uint64(seqNum), db.InternalKeyKindSet)
	}

	var block []byte

	for _, r := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("restart=%d", r), func(t *testing.T) {
			datadriven.RunTest(t, "testdata/block", func(d *datadriven.TestData) string {
				switch d.Cmd {
				case "define":
					w := &blockWriter{restartInterval: r}
					for _, e := range strings.Split(strings.TrimSpace(d.Input), ",") {
						w.add(makeIkey(e), nil)
					}
					block = w.finish()

				case "iter":
					iter, err := newBlockIter(bytes.Compare, block)
					if err != nil {
						t.Fatal(err)
					}

					for _, arg := range d.CmdArgs {
						switch arg.Key {
						case "globalSeqNum":
							if len(arg.Vals) != 1 {
								t.Fatalf("%s: arg %s expects 1 value", d.Cmd, arg.Key)
							}
							v, err := strconv.Atoi(arg.Vals[0])
							if err != nil {
								t.Fatal(err)
							}
							iter.globalSeqNum = uint64(v)
						default:
							t.Fatalf("%s: unknown arg: %s", d.Cmd, arg.Key)
						}
					}

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
						}
						if iter.Valid() {
							fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
						} else if err := iter.Error(); err != nil {
							fmt.Fprintf(&b, "<err=%v>", err)
						} else {
							fmt.Fprintf(&b, ".")
						}
					}
					b.WriteString("\n")
					return b.String()

				default:
					t.Fatalf("unknown command: %s", d.Cmd)
				}
				return ""
			})
		})
	}
}

func BenchmarkBlockIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				w := &blockWriter{
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				var keys [][]byte
				for i := 0; w.estimatedSize() < blockSize; i++ {
					key := []byte(fmt.Sprintf("%05d", i))
					keys = append(keys, key)
					ikey.UserKey = key
					w.add(ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, w.finish())
				if err != nil {
					b.Fatal(err)
				}
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					k := keys[rng.Intn(len(keys))]
					it.SeekGE(k)
					if testing.Verbose() {
						if !it.Valid() {
							b.Fatal("expected to find key")
						}
						if !bytes.Equal(k, it.Key().UserKey) {
							b.Fatalf("expected %s, but found %s", k, it.Key().UserKey)
						}
					}
				}
			})
	}
}

func BenchmarkBlockIterSeekLT(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				w := &blockWriter{
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				var keys [][]byte
				for i := 0; w.estimatedSize() < blockSize; i++ {
					key := []byte(fmt.Sprintf("%05d", i))
					keys = append(keys, key)
					ikey.UserKey = key
					w.add(ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, w.finish())
				if err != nil {
					b.Fatal(err)
				}
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					j := rng.Intn(len(keys))
					it.SeekLT(keys[j])
					if testing.Verbose() {
						if j == 0 {
							if it.Valid() {
								b.Fatal("unexpected key")
							}
						} else {
							if !it.Valid() {
								b.Fatal("expected to find key")
							}
							k := keys[j-1]
							if !bytes.Equal(k, it.Key().UserKey) {
								b.Fatalf("expected %s, but found %s", k, it.Key().UserKey)
							}
						}
					}
				}
			})
	}
}

func BenchmarkBlockIterNext(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				w := &blockWriter{
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				for i := 0; w.estimatedSize() < blockSize; i++ {
					ikey.UserKey = []byte(fmt.Sprintf("%05d", i))
					w.add(ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, w.finish())
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !it.Valid() {
						it.First()
					}
					it.Next()
				}
			})
	}
}

func BenchmarkBlockIterPrev(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{16} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				w := &blockWriter{
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				for i := 0; w.estimatedSize() < blockSize; i++ {
					ikey.UserKey = []byte(fmt.Sprintf("%05d", i))
					w.add(ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, w.finish())
				if err != nil {
					b.Fatal(err)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if !it.Valid() {
						it.Last()
					}
					it.Prev()
				}
			})
	}
}

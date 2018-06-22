package table

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
)

func TestBlockWriter(t *testing.T) {
	ikey := func(s string) *db.InternalKey {
		return &db.InternalKey{UserKey: []byte(s)}
	}

	w := &blockWriter{
		coder:           raw{},
		restartInterval: 16,
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
		i, err := newBlockIter(bytes.Compare, raw{}, k)
		if err != nil {
			t.Fatal(err)
		}
		ik := db.InternalKey{UserKey: []byte(tc.key)}
		i.SeekGE(&ik)
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
		i, err := newBlockIter(bytes.Compare, raw{}, k)
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

func BenchmarkBlockIterSeekGE(b *testing.B) {
	const blockSize = 32 << 10

	for _, restartInterval := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("restart=%d", restartInterval),
			func(b *testing.B) {
				w := &blockWriter{
					coder:           raw{},
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				var keys [][]byte
				for i := 0; w.estimatedSize() < blockSize; i++ {
					key := []byte(fmt.Sprintf("%05d", i))
					keys = append(keys, key)
					ikey.UserKey = key
					w.add(&ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, raw{}, w.finish())
				if err != nil {
					b.Fatal(err)
				}
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					ikey.UserKey = keys[rng.Intn(len(keys))]
					it.SeekGE(&ikey)
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
					coder:           raw{},
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				for i := 0; w.estimatedSize() < blockSize; i++ {
					ikey.UserKey = []byte(fmt.Sprintf("%05d", i))
					w.add(&ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, raw{}, w.finish())
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
					coder:           raw{},
					restartInterval: restartInterval,
				}

				var ikey db.InternalKey
				for i := 0; w.estimatedSize() < blockSize; i++ {
					ikey.UserKey = []byte(fmt.Sprintf("%05d", i))
					w.add(&ikey, nil)
				}

				it, err := newBlockIter(bytes.Compare, raw{}, w.finish())
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

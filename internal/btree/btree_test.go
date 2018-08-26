// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
)

func key(i, v int) db.InternalKey {
	return db.MakeInternalKey([]byte(fmt.Sprintf("%04d", i)), uint64(v), 0)
}

func checkIter(t *testing.T, it Iterator, start, end int) {
	// t.Helper()
	i := start
	for it.First(); it.Valid(); it.Next() {
		item := it.Item()
		expected := key(i, i)
		if db.InternalCompare(bytes.Compare, expected, item) != 0 {
			t.Fatalf("expected %s, but found %s", expected, item)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}

	for it.Last(); it.Valid(); it.Prev() {
		i--
		item := it.Item()
		expected := key(i, i)
		if db.InternalCompare(bytes.Compare, expected, item) != 0 {
			t.Fatalf("expected %s, but found %s", expected, item)
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v parent=%p", start, i, it, it.n.parent)
	}
}

func TestBTree(t *testing.T) {
	tr := New(bytes.Compare)

	// With degree == 16 (max-items/node == 31) we need 513 items in order for
	// there to be 3 levels in the tree. The count here is comfortably above
	// that.
	const count = 768
	// Add keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Set(key(i, i))
		tr.Verify()
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.NewIter(), 0, i+1)
	}
	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(key(i, i))
		tr.Verify()
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.NewIter(), i+1, count)
	}

	// Add keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Set(key(count-i, count-i))
		tr.Verify()
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.NewIter(), count-i, count+1)
	}
	// Delete keys in reverse sorted order.
	for i := 0; i < count; i++ {
		tr.Delete(key(count-i, count-i))
		tr.Verify()
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.NewIter(), 1, count-i)
	}
}

func TestBTreeSeek(t *testing.T) {
	const count = 513

	tr := New(bytes.Compare)
	for i := 0; i < count; i++ {
		tr.Set(key(i*2, i*2))
	}

	it := tr.NewIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(key(i, i))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Item()
		expected := key(2*((i+1)/2), 2*((i+1)/2))
		if db.InternalCompare(bytes.Compare, expected, item) != 0 {
			t.Fatalf("%d: expected %s, but found %s", i, expected, item)
		}
	}
	it.SeekGE(key(2*count-1, 2*count-1))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(key(i, i))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Item()
		expected := key(2*((i-1)/2), 2*((i-1)/2))
		if db.InternalCompare(bytes.Compare, expected, item) != 0 {
			t.Fatalf("%d: expected %s, but found %s", i, expected, item)
		}
	}
	it.SeekLT(key(0, 0))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

func randomKey(rng *rand.Rand, b []byte) []byte {
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}

func BenchmarkIterSeekGE(b *testing.B) {
	for _, count := range []int{16, 128, 1024} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			var keys [][]byte
			tr := New(bytes.Compare)

			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("%05d", i))
				keys = append(keys, key)
				tr.Set(db.InternalKey{UserKey: key})
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			it := tr.NewIter()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[rng.Intn(len(keys))]
				it.SeekGE(db.InternalKey{UserKey: k})
				if testing.Verbose() {
					if !it.Valid() {
						b.Fatal("expected to find key")
					}
					if !bytes.Equal(k, it.Item().UserKey) {
						b.Fatalf("expected %s, but found %s", k, it.Item().UserKey)
					}
				}
			}
		})
	}
}

func BenchmarkIterSeekLT(b *testing.B) {
	for _, count := range []int{16, 128, 1024} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			var keys [][]byte
			tr := New(bytes.Compare)

			for i := 0; i < count; i++ {
				key := []byte(fmt.Sprintf("%05d", i))
				keys = append(keys, key)
				tr.Set(db.InternalKey{UserKey: key})
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			it := tr.NewIter()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				j := rng.Intn(len(keys))
				k := keys[j]
				it.SeekLT(db.InternalKey{UserKey: k})
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
						if !bytes.Equal(k, it.Item().UserKey) {
							b.Fatalf("expected %s, but found %s", k, it.Item().UserKey)
						}
					}
				}
			}
		})
	}
}

func BenchmarkIterNext(b *testing.B) {
	buf := make([]byte, 64<<10)
	tr := New(bytes.Compare)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len(buf); i += 8 {
		key := randomKey(rng, buf[i:i+8])
		tr.Set(db.InternalKey{UserKey: key})
	}

	it := tr.NewIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Next()
	}
}

func BenchmarkIterPrev(b *testing.B) {
	buf := make([]byte, 64<<10)
	tr := New(bytes.Compare)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < len(buf); i += 8 {
		key := randomKey(rng, buf[i:i+8])
		tr.Set(db.InternalKey{UserKey: key})
	}

	it := tr.NewIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.Last()
		}
		it.Prev()
	}
}

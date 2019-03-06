// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/arenaskl"
	"github.com/petermattis/pebble/internal/datadriven"
)

// Set sets the value for the given key. It overwrites any previous value for
// that key; a DB is not a multi-map. NB: this might have unexpected
// interaction with prepare/apply. Caveat emptor!
func (m *memTable) set(key db.InternalKey, value []byte) error {
	if key.Kind() == db.InternalKeyKindRangeDelete {
		if err := m.rangeDelSkl.Add(key, value); err != nil {
			return err
		}
		atomic.AddUint32(&m.tombstones.count, 1)
		atomic.StorePointer(&m.tombstones.vals, nil)
		return nil
	}
	return m.skl.Add(key, value)
}

// count returns the number of entries in a DB.
func (m *memTable) count() (n int) {
	x := m.newIter(nil)
	for valid := x.First(); valid; valid = x.Next() {
		n++
	}
	if x.Close() != nil {
		return -1
	}
	return n
}

func ikey(s string) db.InternalKey {
	return db.MakeInternalKey([]byte(s), 0, db.InternalKeyKindSet)
}

func TestMemTableBasic(t *testing.T) {
	// Check the empty DB.
	m := newMemTable(nil)
	if got, want := m.count(), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}
	v, err := m.get([]byte("cherry"))
	if string(v) != "" || err != db.ErrNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", v, err, "", db.ErrNotFound)
	}
	// Add some key/value pairs.
	m.set(ikey("cherry"), []byte("red"))
	m.set(ikey("peach"), []byte("yellow"))
	m.set(ikey("grape"), []byte("red"))
	m.set(ikey("grape"), []byte("green"))
	m.set(ikey("plum"), []byte("purple"))
	if got, want := m.count(), 4; got != want {
		t.Fatalf("2.count: got %v, want %v", got, want)
	}
	// Get keys that are and aren't in the DB.
	v, err = m.get([]byte("plum"))
	if string(v) != "purple" || err != nil {
		t.Fatalf("6.get: got (%q, %v), want (%q, %v)", v, err, "purple", error(nil))
	}
	v, err = m.get([]byte("lychee"))
	if string(v) != "" || err != db.ErrNotFound {
		t.Fatalf("7.get: got (%q, %v), want (%q, %v)", v, err, "", db.ErrNotFound)
	}
	// Check an iterator.
	s, x := "", m.newIter(nil)
	for valid := x.SeekGE([]byte("mango")); valid; valid = x.Next() {
		s += fmt.Sprintf("%s/%s.", x.Key().UserKey, x.Value())
	}
	if want := "peach/yellow.plum/purple."; s != want {
		t.Fatalf("8.iter: got %q, want %q", s, want)
	}
	if err = x.Close(); err != nil {
		t.Fatalf("9.close: %v", err)
	}
	// Check some more sets and deletes.
	if err := m.set(ikey("apricot"), []byte("orange")); err != nil {
		t.Fatalf("12.set: %v", err)
	}
	if got, want := m.count(), 5; got != want {
		t.Fatalf("13.count: got %v, want %v", got, want)
	}
	// Clean up.
	if err := m.close(); err != nil {
		t.Fatalf("14.close: %v", err)
	}
}

func TestMemTableCount(t *testing.T) {
	m := newMemTable(nil)
	for i := 0; i < 200; i++ {
		if j := m.count(); j != i {
			t.Fatalf("count: got %d, want %d", j, i)
		}
		m.set(db.InternalKey{UserKey: []byte{byte(i)}}, nil)
	}
	if err := m.close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemTableEmpty(t *testing.T) {
	m := newMemTable(nil)
	if !m.empty() {
		t.Errorf("got !empty, want empty")
	}
	// Add one key/value pair with an empty key and empty value.
	m.set(db.InternalKey{}, nil)
	if m.empty() {
		t.Errorf("got empty, want !empty")
	}
}

func TestMemTable1000Entries(t *testing.T) {
	// Initialize the DB.
	const N = 1000
	m0 := newMemTable(nil)
	for i := 0; i < N; i++ {
		k := ikey(strconv.Itoa(i))
		v := []byte(strings.Repeat("x", i))
		m0.set(k, v)
	}
	// Check the DB count.
	if got, want := m0.count(), 1000; got != want {
		t.Fatalf("count: got %v, want %v", got, want)
	}
	// Check random-access lookup.
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 3*N; i++ {
		j := r.Intn(N)
		k := []byte(strconv.Itoa(j))
		v, err := m0.get(k)
		if err != nil {
			t.Fatal(err)
		}
		if len(v) != cap(v) {
			t.Fatalf("get: j=%d, got len(v)=%d, cap(v)=%d", j, len(v), cap(v))
		}
		var c uint8
		if len(v) != 0 {
			c = v[0]
		} else {
			c = 'x'
		}
		if len(v) != j || c != 'x' {
			t.Fatalf("get: j=%d, got len(v)=%d,c=%c, want %d,%c", j, len(v), c, j, 'x')
		}
	}
	// Check that iterating through the middle of the DB looks OK.
	// Keys are in lexicographic order, not numerical order.
	// Multiples of 3 are not present.
	wants := []string{
		"499",
		"5",
		"50",
		"500",
		"501",
		"502",
		"503",
		"504",
		"505",
		"506",
		"507",
	}
	x := m0.newIter(nil)
	x.SeekGE([]byte(wants[0]))
	for _, want := range wants {
		if !x.Valid() {
			t.Fatalf("iter: next failed, want=%q", want)
		}
		if got := string(x.Key().UserKey); got != want {
			t.Fatalf("iter: got %q, want %q", got, want)
		}
		if k := x.Key().UserKey; len(k) != cap(k) {
			t.Fatalf("iter: len(k)=%d, cap(k)=%d", len(k), cap(k))
		}
		if v := x.Value(); len(v) != cap(v) {
			t.Fatalf("iter: len(v)=%d, cap(v)=%d", len(v), cap(v))
		}
		x.Next()
	}
	if err := x.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Clean up.
	if err := m0.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestMemTableIter(t *testing.T) {
	var mem *memTable
	datadriven.RunTest(t, "testdata/internal_iter_next", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			mem = newMemTable(nil)
			for _, key := range strings.Split(d.Input, "\n") {
				j := strings.Index(key, ":")
				if err := mem.set(db.ParseInternalKey(key[:j]), []byte(key[j+1:])); err != nil {
					return err.Error()
				}
			}
			return ""

		case "iter":
			iter := mem.newIter(nil)
			defer iter.Close()
			return runInternalIterCmd(d, iter)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestMemTableDeleteRange(t *testing.T) {
	var mem *memTable
	var seqNum uint64

	datadriven.RunTest(t, "testdata/delete_range", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "clear":
			mem = nil
			seqNum = 0
			return ""

		case "define":
			b := newBatch(nil)
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if mem == nil {
				mem = newMemTable(nil)
			}
			if err := mem.apply(b, seqNum); err != nil {
				return err.Error()
			}
			seqNum += uint64(b.count())
			return ""

		case "scan":
			var iter internalIterator
			if len(td.CmdArgs) > 1 {
				return fmt.Sprintf("%s expects at most 1 argument", td.Cmd)
			}
			if len(td.CmdArgs) == 1 {
				if td.CmdArgs[0].String() != "range-del" {
					return fmt.Sprintf("%s unknown argument %s", td.Cmd, td.CmdArgs[0])
				}
				iter = mem.newRangeDelIter(nil)
			} else {
				iter = mem.newIter(nil)
			}
			defer iter.Close()

			var buf bytes.Buffer
			for valid := iter.First(); valid; valid = iter.Next() {
				fmt.Fprintf(&buf, "%s:%s\n", iter.Key(), iter.Value())
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func buildMemTable(b *testing.B) (*memTable, [][]byte) {
	m := newMemTable(nil)
	var keys [][]byte
	var ikey db.InternalKey
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		keys = append(keys, key)
		ikey = db.MakeInternalKey(key, 0, db.InternalKeyKindSet)
		if m.set(ikey, nil) == arenaskl.ErrArenaFull {
			break
		}
	}
	return m, keys
}

func BenchmarkMemTableIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(nil)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter.SeekGE(keys[rng.Intn(len(keys))])
	}
}

func BenchmarkMemTableIterNext(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.First()
		}
		iter.Next()
	}
}

func BenchmarkMemTableIterPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !iter.Valid() {
			iter.Last()
		}
		iter.Prev()
	}
}

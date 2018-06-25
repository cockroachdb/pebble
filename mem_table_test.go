// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pebble

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
)

// count returns the number of entries in a DB.
func count(d *memTable) (n int) {
	x := d.NewIter(nil)
	for x.First(); x.Valid(); x.Next() {
		n++
	}
	if x.Close() != nil {
		return -1
	}
	return n
}

func ikey(s string) *db.InternalKey {
	k := db.MakeInternalKey([]byte(s), 0, db.InternalKeyKindSet)
	return &k
}

// compact compacts a MemTable.
func compact(m *memTable) (*memTable, error) {
	n, x := newMemTable(nil), m.NewIter(nil)
	for x.First(); x.Valid(); x.Next() {
		if err := n.Set(x.Key(), x.Value(), nil); err != nil {
			return nil, err
		}
	}
	if err := x.Close(); err != nil {
		return nil, err
	}
	return n, nil
}

func TestMemTableBasic(t *testing.T) {
	// Check the empty DB.
	m := newMemTable(nil)
	if got, want := count(m), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}
	v, err := m.Get(ikey("cherry"), nil)
	if string(v) != "" || err != db.ErrNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", v, err, "", db.ErrNotFound)
	}
	// Add some key/value pairs.
	m.Set(ikey("cherry"), []byte("red"), nil)
	m.Set(ikey("peach"), []byte("yellow"), nil)
	m.Set(ikey("grape"), []byte("red"), nil)
	m.Set(ikey("grape"), []byte("green"), nil)
	m.Set(ikey("plum"), []byte("purple"), nil)
	if got, want := count(m), 4; got != want {
		t.Fatalf("2.count: got %v, want %v", got, want)
	}
	// Get keys that are and aren't in the DB.
	v, err = m.Get(ikey("plum"), nil)
	if string(v) != "purple" || err != nil {
		t.Fatalf("6.get: got (%q, %v), want (%q, %v)", v, err, "purple", error(nil))
	}
	v, err = m.Get(ikey("lychee"), nil)
	if string(v) != "" || err != db.ErrNotFound {
		t.Fatalf("7.get: got (%q, %v), want (%q, %v)", v, err, "", db.ErrNotFound)
	}
	// Check an iterator.
	s, x := "", m.NewIter(nil)
	for x.SeekGE(ikey("mango")); x.Valid(); x.Next() {
		s += fmt.Sprintf("%s/%s.", x.Key().UserKey, x.Value())
	}
	if want := "peach/yellow.plum/purple."; s != want {
		t.Fatalf("8.iter: got %q, want %q", s, want)
	}
	if err = x.Close(); err != nil {
		t.Fatalf("9.close: %v", err)
	}
	// Check some more sets and deletes.
	if err := m.Set(ikey("apricot"), []byte("orange"), nil); err != nil {
		t.Fatalf("12.set: %v", err)
	}
	if got, want := count(m), 5; got != want {
		t.Fatalf("13.count: got %v, want %v", got, want)
	}
	// Clean up.
	if err := m.Close(); err != nil {
		t.Fatalf("14.close: %v", err)
	}
}

func TestMemTableCount(t *testing.T) {
	m := newMemTable(nil)
	for i := 0; i < 200; i++ {
		if j := count(m); j != i {
			t.Fatalf("count: got %d, want %d", j, i)
		}
		m.Set(&db.InternalKey{UserKey: []byte{byte(i)}}, nil, nil)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemTableEmpty(t *testing.T) {
	m := newMemTable(nil)
	if !m.Empty() {
		t.Errorf("got !empty, want empty")
	}
	// Add one key/value pair with an empty key and empty value.
	m.Set(&db.InternalKey{}, nil, nil)
	if m.Empty() {
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
		m0.Set(k, v, nil)
	}
	// Check the DB count.
	if got, want := count(m0), 1000; got != want {
		t.Fatalf("count: got %v, want %v", got, want)
	}
	// Check random-access lookup.
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 3*N; i++ {
		j := r.Intn(N)
		k := ikey(strconv.Itoa(j))
		v, err := m0.Get(k, nil)
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
	x := m0.NewIter(nil)
	x.SeekGE(ikey(wants[0]))
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
	if err := m0.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

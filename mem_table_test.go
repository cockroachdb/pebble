// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"golang.org/x/exp/rand"
)

// Set sets the value for the given key. It overwrites any previous value for
// that key; a DB is not a multi-map. NB: this might have unexpected
// interaction with prepare/apply. Caveat emptor!
func (m *memTable) set(key InternalKey, value []byte) error {
	if key.Kind() == InternalKeyKindRangeDelete {
		if err := m.rangeDelSkl.Add(key, value); err != nil {
			return err
		}
		m.tombstones.invalidate(1)
		return nil
	}
	return m.skl.Add(key, value)
}

// count returns the number of entries in a DB.
func (m *memTable) count() (n int) {
	x := newInternalIterAdapter(m.newIter(nil))
	for valid := x.First(); valid; valid = x.Next() {
		n++
	}
	if x.Close() != nil {
		return -1
	}
	return n
}

// bytesIterated returns the number of bytes iterated in a DB.
func (m *memTable) bytesIterated(t *testing.T) (bytesIterated uint64) {
	x := newInternalIterAdapter(m.newFlushIter(nil, &bytesIterated))
	var prevIterated uint64
	for valid := x.First(); valid; valid = x.Next() {
		if bytesIterated < prevIterated {
			t.Fatalf("bytesIterated moved backward: %d < %d", bytesIterated, prevIterated)
		}
		prevIterated = bytesIterated
	}
	if x.Close() != nil {
		return 0
	}
	return bytesIterated
}

func ikey(s string) InternalKey {
	return base.MakeInternalKey([]byte(s), 0, InternalKeyKindSet)
}

func TestMemTableBasic(t *testing.T) {
	// Check the empty DB.
	m := newMemTable(nil)
	if got, want := m.count(), 0; got != want {
		t.Fatalf("0.count: got %v, want %v", got, want)
	}
	v, err := m.get([]byte("cherry"))
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("1.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
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
	if string(v) != "" || err != ErrNotFound {
		t.Fatalf("7.get: got (%q, %v), want (%q, %v)", v, err, "", ErrNotFound)
	}
	// Check an iterator.
	s, x := "", newInternalIterAdapter(m.newIter(nil))
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
		m.set(InternalKey{UserKey: []byte{byte(i)}}, nil)
	}
	if err := m.close(); err != nil {
		t.Fatal(err)
	}
}

func TestMemTableBytesIterated(t *testing.T) {
	m := newMemTable(nil)
	for i := 0; i < 200; i++ {
		bytesIterated := m.bytesIterated(t)
		expected := m.totalBytes()
		if bytesIterated != expected {
			t.Fatalf("bytesIterated: got %d, want %d", bytesIterated, expected)
		}
		m.set(InternalKey{UserKey: []byte{byte(i)}}, nil)
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
	m.set(InternalKey{}, nil)
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
	x := newInternalIterAdapter(m0.newIter(nil))
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
	for _, testdata := range []string{
		"testdata/internal_iter_next", "testdata/internal_iter_bounds"} {
		datadriven.RunTest(t, testdata, func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				mem = newMemTable(nil)
				for _, key := range strings.Split(d.Input, "\n") {
					j := strings.Index(key, ":")
					if err := mem.set(base.ParseInternalKey(key[:j]), []byte(key[j+1:])); err != nil {
						return err.Error()
					}
				}
				return ""

			case "iter":
				var options IterOptions
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "lower":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf(
								"%s expects at most 1 value for lower", d.Cmd)
						}
						options.LowerBound = []byte(arg.Vals[0])
					case "upper":
						if len(arg.Vals) != 1 {
							return fmt.Sprintf(
								"%s expects at most 1 value for upper", d.Cmd)
						}
						options.UpperBound = []byte(arg.Vals[0])
					default:
						return fmt.Sprintf("unknown arg: %s", arg.Key)
					}
				}
				iter := mem.newIter(&options)
				defer iter.Close()
				return runInternalIterCmd(d, iter)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	}
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
			seqNum += uint64(b.Count())
			return ""

		case "scan":
			var iter internalIterAdapter
			if len(td.CmdArgs) > 1 {
				return fmt.Sprintf("%s expects at most 1 argument", td.Cmd)
			}
			if len(td.CmdArgs) == 1 {
				if td.CmdArgs[0].String() != "range-del" {
					return fmt.Sprintf("%s unknown argument %s", td.Cmd, td.CmdArgs[0])
				}
				iter.internalIterator = mem.newRangeDelIter(nil)
			} else {
				iter.internalIterator = mem.newIter(nil)
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

func TestMemTableConcurrentDeleteRange(t *testing.T) {
	// Concurrently write and read range tombstones. Workers add range
	// tombstones, and then immediately retrieve them verifying that the
	// tombstones they've added are all present.

	m := newMemTable(&Options{MemTableSize: 64 << 20})

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	seqNum := uint64(1)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			start := ([]byte)(fmt.Sprintf("%03d", i))
			end := ([]byte)(fmt.Sprintf("%03d", i+1))
			for j := 0; j < 100; j++ {
				b := newBatch(nil)
				b.DeleteRange(start, end, nil)
				n := atomic.AddUint64(&seqNum, 1) - 1
				if err := m.apply(b, n); err != nil {
					t.Fatal(err)
				}
				b.release()

				var count int
				it := newInternalIterAdapter(m.newRangeDelIter(nil))
				for valid := it.SeekGE(start); valid; valid = it.Next() {
					if m.cmp(it.Key().UserKey, end) >= 0 {
						break
					}
					count++
				}
				if j+1 != count {
					t.Fatalf("%d: expected %d tombstones, but found %d", i, j+1, count)
				}
			}
		}(i)
	}
	wg.Wait()
}

func buildMemTable(b *testing.B) (*memTable, [][]byte) {
	m := newMemTable(nil)
	var keys [][]byte
	var ikey InternalKey
	for i := 0; ; i++ {
		key := []byte(fmt.Sprintf("%08d", i))
		keys = append(keys, key)
		ikey = base.MakeInternalKey(key, 0, InternalKeyKindSet)
		if m.set(ikey, nil) == arenaskl.ErrArenaFull {
			break
		}
	}
	return m, keys
}

func BenchmarkMemTableIterSeekGE(b *testing.B) {
	m, keys := buildMemTable(b)
	iter := m.newIter(nil)
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

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
		key, _ := iter.Next()
		if key == nil {
			key, _ = iter.First()
		}
		_ = key
	}
}

func BenchmarkMemTableIterPrev(b *testing.B) {
	m, _ := buildMemTable(b)
	iter := m.newIter(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := iter.Prev()
		if key == nil {
			key, _ = iter.Last()
		}
		_ = key
	}
}

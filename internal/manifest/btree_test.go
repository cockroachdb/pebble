// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func newItem(k InternalKey) *FileMetadata {
	return &FileMetadata{
		Smallest: k,
		Largest:  k,
	}
}

func cmp(a, b *FileMetadata) int {
	return cmpKey(a.Smallest, b.Smallest)
}

func cmpKey(a, b InternalKey) int {
	return base.InternalCompare(base.DefaultComparer.Compare, a, b)
}

//////////////////////////////////////////
//        Invariant verification        //
//////////////////////////////////////////

// Verify asserts that the tree's structural invariants all hold.
func (t *btree) Verify(tt *testing.T) {
	if t.length == 0 {
		require.Nil(tt, t.root)
		return
	}
	t.verifyLeafSameDepth(tt)
	t.verifyCountAllowed(tt)
	t.isSorted(tt)
}

func (t *btree) verifyLeafSameDepth(tt *testing.T) {
	h := t.Height()
	t.root.verifyDepthEqualToHeight(tt, 1, h)
}

func (n *node) verifyDepthEqualToHeight(t *testing.T, depth, height int) {
	if n.leaf {
		require.Equal(t, height, depth, "all leaves should have the same depth as the tree height")
	}
	n.recurse(func(child *node, _ int16) {
		child.verifyDepthEqualToHeight(t, depth+1, height)
	})
}

func (t *btree) verifyCountAllowed(tt *testing.T) {
	t.root.verifyCountAllowed(tt, true)
}

func (n *node) verifyCountAllowed(t *testing.T, root bool) {
	if !root {
		require.GreaterOrEqual(t, n.count, int16(minItems), "item count %d must be in range [%d,%d]", n.count, minItems, maxItems)
		require.LessOrEqual(t, n.count, int16(maxItems), "item count %d must be in range [%d,%d]", n.count, minItems, maxItems)
	}
	for i, item := range n.items {
		if i < int(n.count) {
			require.NotNil(t, item, "item below count")
		} else {
			require.Nil(t, item, "item above count")
		}
	}
	if !n.leaf {
		for i, child := range n.children {
			if i <= int(n.count) {
				require.NotNil(t, child, "node below count")
			} else {
				require.Nil(t, child, "node above count")
			}
		}
	}
	n.recurse(func(child *node, _ int16) {
		child.verifyCountAllowed(t, false)
	})
}

func (t *btree) isSorted(tt *testing.T) {
	t.root.isSorted(tt, t.cmp)
}

func (n *node) isSorted(t *testing.T, cmp func(*FileMetadata, *FileMetadata) int) {
	for i := int16(1); i < n.count; i++ {
		require.LessOrEqual(t, cmp(n.items[i-1], n.items[i]), 0)
	}
	if !n.leaf {
		for i := int16(0); i < n.count; i++ {
			prev := n.children[i]
			next := n.children[i+1]

			require.LessOrEqual(t, cmp(prev.items[prev.count-1], n.items[i]), 0)
			require.LessOrEqual(t, cmp(n.items[i], next.items[0]), 0)
		}
	}
	n.recurse(func(child *node, _ int16) {
		child.isSorted(t, cmp)
	})
}

func (n *node) recurse(f func(child *node, pos int16)) {
	if !n.leaf {
		for i := int16(0); i <= n.count; i++ {
			f(n.children[i], i)
		}
	}
}

//////////////////////////////////////////
//              Unit Tests              //
//////////////////////////////////////////

func key(i int) InternalKey {
	if i < 0 || i > 99999 {
		panic("key out of bounds")
	}
	return base.MakeInternalKey([]byte(fmt.Sprintf("%05d", i)), 0, base.InternalKeyKindSet)
}

func keyWithMemo(i int, memo map[int]InternalKey) InternalKey {
	if s, ok := memo[i]; ok {
		return s
	}
	s := key(i)
	memo[i] = s
	return s
}

func checkIter(t *testing.T, it iterator, start, end int, keyMemo map[int]InternalKey) {
	t.Helper()
	i := start
	for it.First(); it.Valid(); it.Next() {
		item := it.Cur()
		expected := keyWithMemo(i, keyMemo)
		if cmpKey(expected, item.Smallest) != 0 {
			t.Fatalf("expected %s, but found %s", expected, item.Smallest)
		}
		i++
	}
	if i != end {
		t.Fatalf("expected %d, but at %d", end, i)
	}

	for it.Last(); it.Valid(); it.Prev() {
		i--
		item := it.Cur()
		expected := keyWithMemo(i, keyMemo)
		if cmpKey(expected, item.Smallest) != 0 {
			t.Fatalf("expected %s, but found %s", expected, item.Smallest)
		}
	}
	if i != start {
		t.Fatalf("expected %d, but at %d: %+v", start, i, it)
	}
}

// TestBTree tests basic btree operations.
func TestBTree(t *testing.T) {
	var tr btree
	tr.cmp = cmp
	keyMemo := make(map[int]InternalKey)

	// With degree == 16 (max-items/node == 31) we need 513 items in order for
	// there to be 3 levels in the tree. The count here is comfortably above
	// that.
	const count = 768
	items := rang(0, count-1)

	// Add keys in sorted order.
	for i := 0; i < count; i++ {
		tr.Insert(items[i])
		tr.Verify(t)
		if e := i + 1; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, i+1, keyMemo)
	}

	// Delete keys in sorted order.
	for i := 0; i < count; i++ {
		obsolete := tr.Delete(items[i])
		if !obsolete {
			t.Fatalf("expected item %d to be obsolete", i)
		}
		tr.Verify(t)
		if e := count - (i + 1); e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), i+1, count, keyMemo)
	}

	// Add keys in reverse sorted order.
	for i := 1; i <= count; i++ {
		tr.Insert(items[count-i])
		tr.Verify(t)
		if i != tr.Len() {
			t.Fatalf("expected length %d, but found %d", i, tr.Len())
		}
		checkIter(t, tr.MakeIter(), count-i, count, keyMemo)
	}

	// Delete keys in reverse sorted order.
	for i := 1; i <= count; i++ {
		obsolete := tr.Delete(items[count-i])
		if !obsolete {
			t.Fatalf("expected item %d to be obsolete", i)
		}
		tr.Verify(t)
		if e := count - i; e != tr.Len() {
			t.Fatalf("expected length %d, but found %d", e, tr.Len())
		}
		checkIter(t, tr.MakeIter(), 0, count-i, keyMemo)
	}
}

// TestBTreeSeek tests basic btree iterator operations.
func TestBTreeSeek(t *testing.T) {
	const count = 513

	var tr btree
	tr.cmp = cmp
	for i := 0; i < count; i++ {
		tr.Insert(newItem(key(i * 2)))
	}

	it := tr.MakeIter()
	for i := 0; i < 2*count-1; i++ {
		it.SeekGE(newItem(key(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Cur()
		expected := key(2 * ((i + 1) / 2))
		if cmpKey(expected, item.Smallest) != 0 {
			t.Fatalf("%d: expected %s, but found %s", i, expected, item.Smallest)
		}
	}
	it.SeekGE(newItem(key(2*count - 1)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}

	for i := 1; i < 2*count; i++ {
		it.SeekLT(newItem(key(i)))
		if !it.Valid() {
			t.Fatalf("%d: expected valid iterator", i)
		}
		item := it.Cur()
		expected := key(2 * ((i - 1) / 2))
		if cmpKey(expected, item.Smallest) != 0 {
			t.Fatalf("%d: expected %s, but found %s", i, expected, item.Smallest)
		}
	}
	it.SeekLT(newItem(key(0)))
	if it.Valid() {
		t.Fatalf("expected invalid iterator")
	}
}

func TestBTreeInsertDuplicatePanic(t *testing.T) {
	var tr btree
	tr.cmp = cmp
	tr.Insert(newItem(key(1)))
	tr.Insert(newItem(key(2)))
	tr.Insert(newItem(key(3)))
	require.PanicsWithValue(t, "file key collision: existing metadata 000000, inserting 000000",
		func() { tr.Insert(newItem(key(2))) })
}

// TestBTreeCloneConcurrentOperations tests that cloning a btree returns a new
// btree instance which is an exact logical copy of the original but that can be
// modified independently going forward.
func TestBTreeCloneConcurrentOperations(t *testing.T) {
	const cloneTestSize = 1000
	p := perm(cloneTestSize)

	var trees []*btree
	treeC, treeDone := make(chan *btree), make(chan struct{})
	go func() {
		for b := range treeC {
			trees = append(trees, b)
		}
		close(treeDone)
	}()

	var wg sync.WaitGroup
	var populate func(tr *btree, start int)
	populate = func(tr *btree, start int) {
		t.Logf("Starting new clone at %v", start)
		treeC <- tr
		for i := start; i < cloneTestSize; i++ {
			tr.Insert(p[i])
			if i%(cloneTestSize/5) == 0 {
				wg.Add(1)
				c := tr.Clone()
				go populate(&c, i+1)
			}
		}
		wg.Done()
	}

	wg.Add(1)
	var tr btree
	tr.cmp = cmp
	go populate(&tr, 0)
	wg.Wait()
	close(treeC)
	<-treeDone

	t.Logf("Starting equality checks on %d trees", len(trees))
	want := rang(0, cloneTestSize-1)
	for i, tree := range trees {
		if got := all(tree); !reflect.DeepEqual(strReprs(got), strReprs(want)) {
			t.Errorf("tree %v mismatch", i)
		}
	}

	t.Log("Removing half of items from first half")
	toRemove := want[cloneTestSize/2:]
	for i := 0; i < len(trees)/2; i++ {
		tree := trees[i]
		wg.Add(1)
		go func() {
			for _, item := range toRemove {
				tree.Delete(item)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	t.Log("Checking all values again")
	for i, tree := range trees {
		var wantpart []*FileMetadata
		if i < len(trees)/2 {
			wantpart = want[:cloneTestSize/2]
		} else {
			wantpart = want
		}
		if got := all(tree); !reflect.DeepEqual(strReprs(got), strReprs(wantpart)) {
			t.Errorf("tree %v mismatch, want %#v got %#v", i, strReprs(wantpart), strReprs(got))
		}
	}

	var obsolete []base.FileNum
	for i := range trees {
		obsolete = append(obsolete, trees[i].Release()...)
	}
	if len(obsolete) != len(p) {
		t.Errorf("got %d obsolete trees, expected %d", len(obsolete), len(p))
	}
}

// TestIterStack tests the interface of the iterStack type.
func TestIterStack(t *testing.T) {
	f := func(i int) iterFrame { return iterFrame{pos: int16(i)} }
	var is iterStack
	for i := 1; i <= 2*len(iterStackArr{}); i++ {
		var j int
		for j = 0; j < i; j++ {
			is.push(f(j))
		}
		require.Equal(t, j, is.len())
		for j--; j >= 0; j-- {
			require.Equal(t, f(j), is.pop())
		}
		is.reset()
	}
}

//////////////////////////////////////////
//              Benchmarks              //
//////////////////////////////////////////

// perm returns a random permutation of items with keys in the range [0, n).
func perm(n int) (out []*FileMetadata) {
	for _, i := range rand.Perm(n) {
		out = append(out, newItem(key(i)))
	}
	return out
}

// rang returns an ordered list of items with keys in the range [m, n].
func rang(m, n int) (out []*FileMetadata) {
	for i := m; i <= n; i++ {
		out = append(out, newItem(key(i)))
	}
	return out
}

func strReprs(items []*FileMetadata) []string {
	s := make([]string, len(items))
	for i := range items {
		s[i] = items[i].String()
	}
	return s
}

// all extracts all items from a tree in order as a slice.
func all(tr *btree) (out []*FileMetadata) {
	it := tr.MakeIter()
	it.First()
	for it.Valid() {
		out = append(out, it.Cur())
		it.Next()
	}
	return out
}

func forBenchmarkSizes(b *testing.B, f func(b *testing.B, count int)) {
	for _, count := range []int{16, 128, 1024, 8192, 65536} {
		b.Run(fmt.Sprintf("count=%d", count), func(b *testing.B) {
			f(b, count)
		})
	}
}

// BenchmarkBTreeInsert measures btree insertion performance.
func BenchmarkBTreeInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(count)
		b.ResetTimer()
		for i := 0; i < b.N; {
			var tr btree
			tr.cmp = cmp
			for _, item := range insertP {
				tr.Insert(item)
				i++
				if i >= b.N {
					return
				}
			}
		}
	})
}

// BenchmarkBTreeDelete measures btree deletion performance.
func BenchmarkBTreeDelete(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP, removeP := perm(count), perm(count)
		b.ResetTimer()
		for i := 0; i < b.N; {
			b.StopTimer()
			var tr btree
			tr.cmp = cmp
			for _, item := range insertP {
				tr.Insert(item)
			}
			b.StartTimer()
			for _, item := range removeP {
				tr.Delete(item)
				i++
				if i >= b.N {
					return
				}
			}
			if tr.Len() > 0 {
				b.Fatalf("tree not empty: %s", &tr)
			}
		}
	})
}

// BenchmarkBTreeDeleteInsert measures btree deletion and insertion performance.
func BenchmarkBTreeDeleteInsert(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(count)
		var tr btree
		tr.cmp = cmp
		for _, item := range insertP {
			tr.Insert(item)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := insertP[i%count]
			tr.Delete(item)
			tr.Insert(item)
		}
	})
}

// BenchmarkBTreeDeleteInsertCloneOnce measures btree deletion and insertion
// performance after the tree has been copy-on-write cloned once.
func BenchmarkBTreeDeleteInsertCloneOnce(b *testing.B) {
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		insertP := perm(count)
		var tr btree
		tr.cmp = cmp
		for _, item := range insertP {
			tr.Insert(item)
		}
		tr = tr.Clone()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			item := insertP[i%count]
			tr.Delete(item)
			tr.Insert(item)
		}
	})
}

// BenchmarkBTreeDeleteInsertCloneEachTime measures btree deletion and insertion
// performance while the tree is repeatedly copy-on-write cloned.
func BenchmarkBTreeDeleteInsertCloneEachTime(b *testing.B) {
	for _, release := range []bool{false, true} {
		b.Run(fmt.Sprintf("release=%t", release), func(b *testing.B) {
			forBenchmarkSizes(b, func(b *testing.B, count int) {
				insertP := perm(count)
				var tr, trRelease btree
				tr.cmp = cmp
				trRelease.cmp = cmp
				for _, item := range insertP {
					tr.Insert(item)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					item := insertP[i%count]
					if release {
						trRelease.Release()
						trRelease = tr
					}
					tr = tr.Clone()
					tr.Delete(item)
					tr.Insert(item)
				}
			})
		})
	}
}

// BenchmarkBTreeMakeIter measures the cost of creating a btree iterator.
func BenchmarkBTreeMakeIter(b *testing.B) {
	var tr btree
	tr.cmp = cmp
	for i := 0; i < b.N; i++ {
		it := tr.MakeIter()
		it.First()
	}
}

// BenchmarkBTreeIterSeekGE measures the cost of seeking a btree iterator
// forward.
func BenchmarkBTreeIterSeekGE(b *testing.B) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var keys []InternalKey
		var tr btree
		tr.cmp = cmp

		for i := 0; i < count; i++ {
			s := key(i)
			keys = append(keys, s)
			tr.Insert(newItem(s))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			k := keys[rng.Intn(len(keys))]
			it := tr.MakeIter()
			it.SeekGE(newItem(k))
			if testing.Verbose() {
				if !it.Valid() {
					b.Fatal("expected to find key")
				}
				if cmpKey(k, it.Cur().Smallest) != 0 {
					b.Fatalf("expected %s, but found %s", k, it.Cur().Smallest)
				}
			}
		}
	})
}

// BenchmarkBTreeIterSeekLT measures the cost of seeking a btree iterator
// backward.
func BenchmarkBTreeIterSeekLT(b *testing.B) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	forBenchmarkSizes(b, func(b *testing.B, count int) {
		var keys []InternalKey
		var tr btree
		tr.cmp = cmp

		for i := 0; i < count; i++ {
			k := key(i)
			keys = append(keys, k)
			tr.Insert(newItem(k))
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := rng.Intn(len(keys))
			k := keys[j]
			it := tr.MakeIter()
			it.SeekLT(newItem(k))
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
					if cmpKey(k, it.Cur().Smallest) != 0 {
						b.Fatalf("expected %s, but found %s", k, it.Cur().Smallest)
					}
				}
			}
		}
	})
}

// BenchmarkBTreeIterNext measures the cost of seeking a btree iterator to the
// next item in the tree.
func BenchmarkBTreeIterNext(b *testing.B) {
	var tr btree
	tr.cmp = cmp

	const count = 8 << 10
	const size = 2 * maxItems
	for i := 0; i < count; i++ {
		item := newItem(key(i))
		tr.Insert(item)
	}

	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Next()
	}
}

// BenchmarkBTreeIterPrev measures the cost of seeking a btree iterator to the
// previous item in the tree.
func BenchmarkBTreeIterPrev(b *testing.B) {
	var tr btree
	tr.cmp = cmp

	const count = 8 << 10
	const size = 2 * maxItems
	for i := 0; i < count; i++ {
		item := newItem(key(i))
		tr.Insert(item)
	}

	it := tr.MakeIter()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !it.Valid() {
			it.First()
		}
		it.Prev()
	}
}

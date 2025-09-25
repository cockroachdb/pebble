// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package concurrentset

import (
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetBasic(t *testing.T) {
	s := New[int]()
	expect := func(vals ...int) {
		t.Helper()
		a := s.AppendAll(nil)
		slices.Sort(a)
		require.Equal(t, vals, a)
	}
	expect()
	h1 := s.Add(1)
	expect(1)
	h1000 := s.Add(1000)
	expect(1, 1000)
	s.Remove(h1)
	expect(1000)
	s.Remove(h1000)
	expect()
	h := make([]Handle, rand.IntN(10000))
	for i := range h {
		h[i] = s.Add(i)
	}
	for i := range h {
		if i%2 == 0 {
			s.Remove(h[i])
		}
	}
	var exp []int
	for i := range h {
		if i%2 == 1 {
			exp = append(exp, i)
		}
	}
	expect(exp...)
}

type refSet[T any] struct {
	mu sync.Mutex
	m  map[Handle]T
}

func newRefSet[T any]() *refSet[T] {
	return &refSet[T]{m: make(map[Handle]T)}
}
func (r *refSet[T]) Add(h Handle, v T) {
	r.mu.Lock()
	r.m[h] = v
	r.mu.Unlock()
}
func (r *refSet[T]) Remove(h Handle) {
	r.mu.Lock()
	delete(r.m, h)
	r.mu.Unlock()
}
func (r *refSet[T]) Snapshot() map[Handle]T {
	r.mu.Lock()
	cp := make(map[Handle]T, len(r.m))
	for k, v := range r.m {
		cp[k] = v
	}
	r.mu.Unlock()
	return cp
}

// handleBag tracks handles available for deletion; used to bias the op mix.
type handleBag struct {
	mu sync.Mutex
	hs []Handle
}

func (b *handleBag) add(h Handle) {
	b.mu.Lock()
	b.hs = append(b.hs, h)
	b.mu.Unlock()
}

// maybePop removes and returns a random handle, or false if empty.
func (b *handleBag) maybePop(r *rand.Rand) (Handle, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.hs) == 0 {
		return 0, false
	}
	i := r.IntN(len(b.hs))
	h := b.hs[i]
	// swap-delete
	b.hs[i] = b.hs[len(b.hs)-1]
	b.hs = b.hs[:len(b.hs)-1]
	return h, true
}

// compareMultiset compares the multiset of values in the Set (via Append) with the reference.
func compareMultiset(t *testing.T, gotVals []int, ref map[Handle]int) {
	gotCount := make(map[int]int, len(gotVals))
	for _, v := range gotVals {
		gotCount[v]++
	}
	wantCount := make(map[int]int, len(ref))
	for _, v := range ref {
		wantCount[v]++
	}
	if !reflect.DeepEqual(gotCount, wantCount) {
		t.Fatalf("multiset mismatch\n got:  %+v\n want: %+v", gotCount, wantCount)
	}
}

func TestSetConcurrentAddRemove(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

	workers := 1 + rng.IntN(2*runtime.GOMAXPROCS(0))
	opsPerWorker := rand.IntN(100000)

	s := New[int]()
	ref := newRefSet[int]()
	var bag handleBag
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
			for i := 0; i < opsPerWorker; i++ {
				if r.IntN(100) < 60 { // 60% adds, 40% removes
					v := r.Int()
					h := s.Add(v)
					ref.Add(h, v)
					bag.add(h)
				} else {
					if h, ok := bag.maybePop(r); ok {
						s.Remove(h)
						ref.Remove(h)
					}
				}
				// Occasionally yield to stir the scheduler.
				if i%257 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()

	// Quiescent: take a snapshot and compare.
	got := s.AppendAll(nil)
	compareMultiset(t, got, ref.Snapshot())
}

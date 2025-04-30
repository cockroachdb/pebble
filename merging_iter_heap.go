// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/pebble/internal/invariants"
)

// mergingIterHeap is a heap of mergingIterLevels. It only reads
// mergingIterLevel.iterKV.K.
//
// REQUIRES: Every mergingIterLevel.iterKV is non-nil.
//
// TODO(sumeer): use golang generics.
type mergingIterHeap struct {
	cmp     Compare
	reverse bool
	items   []mergingIterHeapItem
	// TODO: remove.
	cmpNeededCount int
	cmpCalledCount int
}

type mergingIterHeapItem struct {
	*mergingIterLevel
	winnerChild winnerChild
}

type winnerChild uint8

const (
	winnerChildUnknown winnerChild = iota
	winnerChildLeft
	winnerChildRight
)

// len returns the number of elements in the heap.
func (h *mergingIterHeap) len() int {
	return len(h.items)
}

// clear empties the heap.
func (h *mergingIterHeap) clear() {
	h.items = h.items[:0]
}

// less is an internal method, to compare the elements at i and j.
func (h *mergingIterHeap) less(i, j int) bool {
	ikv, jkv := h.items[i].iterKV, h.items[j].iterKV
	if c := h.cmp(ikv.K.UserKey, jkv.K.UserKey); c != 0 {
		if h.reverse {
			return c > 0
		}
		return c < 0
	}
	if h.reverse {
		return ikv.K.Trailer < jkv.K.Trailer
	}
	return ikv.K.Trailer > jkv.K.Trailer
}

// swap is an internal method, used to swap the elements at i and j.
func (h *mergingIterHeap) swap(i, j int) {
	h.items[i].mergingIterLevel, h.items[j].mergingIterLevel =
		h.items[j].mergingIterLevel, h.items[i].mergingIterLevel
}

// init initializes the heap.
func (h *mergingIterHeap) init() {
	// heapify
	n := h.len()
	for i := n/2 - 1; i >= 0; i-- {
		h.down(i, n)
	}
}

// fixTop restores the heap property after the top of the heap has been
// modified.
func (h *mergingIterHeap) fixTop() {
	h.down(0, h.len())
}

// pop removes the top of the heap.
func (h *mergingIterHeap) pop() *mergingIterLevel {
	n := h.len() - 1
	h.swap(0, n)
	// Parent of n does not know which child is the winner. But since index n is
	// removed, the parent of n will have at most one child, and so the value of
	// winnerChild is irrelevant, and we don't need to do:
	//  h.items[(n-1)/2].winnerChild = winnerChildUnknown
	h.down(0, n)
	item := h.items[n]
	h.items = h.items[:n]
	return item.mergingIterLevel
}

// down is an internal method. It moves i down the heap, which has length n,
// until the heap property is restored.
func (h *mergingIterHeap) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n {
			h.cmpNeededCount++
			if h.items[i].winnerChild == winnerChildUnknown {
				h.cmpCalledCount++
				if h.less(j2, j1) {
					h.items[i].winnerChild = winnerChildRight
				} else {
					h.items[i].winnerChild = winnerChildLeft
				}
			} else if invariants.Enabled {
				wc := winnerChildUnknown
				if h.less(j1, j2) {
					wc = winnerChildLeft
				} else if h.less(j2, j1) {
					wc = winnerChildRight
				}
				if wc != winnerChildUnknown && wc != h.items[i].winnerChild {
					panic("winnerChild mismatch")
				}
			}
			if h.items[i].winnerChild == winnerChildRight {
				j = j2 // = 2*i + 2  // right child
			}
		}
		h.cmpNeededCount++
		h.cmpCalledCount++
		if !h.less(j, i) {
			break
		}
		// NB: j is a child of i.
		h.swap(i, j)
		h.items[i].winnerChild = winnerChildUnknown
		i = j
	}
}

// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package btree

import (
	"fmt"
	"sort"
	"unsafe"

	"github.com/petermattis/pebble/db"
)

const (
	degree   = 16
	maxItems = 2*degree - 1
	minItems = degree - 1
)

type item struct {
	ptr unsafe.Pointer
	len int
	val uint64
}

func makeItem(k db.InternalKey) item {
	return item{
		ptr: unsafe.Pointer(&k.UserKey[0]),
		len: len(k.UserKey),
		val: k.Trailer,
	}
}

func (i item) ukey() []byte {
	const maxArrayLen = 1 << 31
	return (*[maxArrayLen]byte)(unsafe.Pointer(i.ptr))[:i.len:i.len]
}

func (i item) ikey() db.InternalKey {
	return db.InternalKey{
		UserKey: i.ukey(),
		Trailer: i.val,
	}
}

type leafNode struct {
	parent *node
	pos    int16
	count  int16
	leaf   bool
	items  [maxItems]item
}

func newLeafNode() *node {
	return (*node)(unsafe.Pointer(&leafNode{leaf: true}))
}

// node is an internal node in a tree.
type node struct {
	leafNode
	children [maxItems + 1]*node
}

func (n *node) updatePos(start, end int) {
	for i := start; i < end; i++ {
		n.children[i].pos = int16(i)
	}
}

func (n *node) insertAt(index int, item item, nd *node) {
	if index < int(n.count) {
		copy(n.items[index+1:n.count+1], n.items[index:n.count])
		if !n.leaf {
			copy(n.children[index+2:n.count+2], n.children[index+1:n.count+1])
			n.updatePos(index+2, int(n.count+2))
		}
	}
	n.items[index] = item
	if !n.leaf {
		n.children[index+1] = nd
		nd.parent = n
		nd.pos = int16(index + 1)
	}
	n.count++
}

func (n *node) pushBack(item item, nd *node) {
	n.items[n.count] = item
	if !n.leaf {
		n.children[n.count+1] = nd
		nd.parent = n
		nd.pos = n.count + 1
	}
	n.count++
}

func (n *node) pushFront(item item, nd *node) {
	if !n.leaf {
		copy(n.children[1:n.count+2], n.children[:n.count+1])
		n.updatePos(1, int(n.count+2))
		n.children[0] = nd
		nd.parent = n
		nd.pos = 0
	}
	copy(n.items[1:n.count+1], n.items[:n.count])
	n.items[0] = item
	n.count++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (n *node) removeAt(index int) (item, *node) {
	var child *node
	if !n.leaf {
		child = n.children[index+1]
		copy(n.children[index+1:n.count], n.children[index+2:n.count+1])
		n.updatePos(index+1, int(n.count))
		n.children[n.count] = nil
	}
	n.count--
	out := n.items[index]
	copy(n.items[index:n.count], n.items[index+1:n.count+1])
	n.items[n.count] = item{}
	return out, child
}

// popBack removes and returns the last element in the list.
func (n *node) popBack() (item, *node) {
	n.count--
	out := n.items[n.count]
	n.items[n.count] = item{}
	if n.leaf {
		return out, nil
	}
	child := n.children[n.count+1]
	n.children[n.count+1] = nil
	return out, child
}

// popFront removes and returns the first element in the list.
func (n *node) popFront() (item, *node) {
	n.count--
	var child *node
	if !n.leaf {
		child = n.children[0]
		copy(n.children[:n.count+1], n.children[1:n.count+2])
		n.updatePos(0, int(n.count+1))
		n.children[n.count+1] = nil
	}
	out := n.items[0]
	copy(n.items[:n.count], n.items[1:n.count+1])
	n.items[n.count] = item{}
	return out, child
}

// find returns the index where the given item should be inserted into this
// list. 'found' is true if the item already exists in the list at the given
// index.
func (n *node) find(cmp db.Compare, item item) (index int, found bool) {
	ukey := item.ukey()
	i := sort.Search(int(n.count), func(i int) bool {
		return cmp(ukey, n.items[i].ukey()) < 0
	})
	if i > 0 && cmp(n.items[i-1].ukey(), ukey) == 0 {
		return i - 1, true
	}
	return i, false
}

// split splits the given node at the given index. The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *node) split(i int) (item, *node) {
	out := n.items[i]
	var next *node
	if n.leaf {
		next = newLeafNode()
	} else {
		next = &node{}
	}
	next.count = n.count - int16(i+1)
	copy(next.items[:], n.items[i+1:n.count])
	for j := int16(i); j < n.count; j++ {
		n.items[j] = item{}
	}
	if !n.leaf {
		copy(next.children[:], n.children[i+1:n.count+1])
		for j := int16(i + 1); j < n.count; j++ {
			n.children[j] = nil
		}
		for j := int16(0); j <= next.count; j++ {
			next.children[j].parent = next
			next.children[j].pos = j
		}
	}
	n.count = int16(i)
	return out, next
}

// insert inserts an item into the subtree rooted at this node, making sure no
// nodes in the subtree exceed maxItems items. Returns true if an item was
// inserted and false if an existing item was replaced.
func (n *node) insert(cmp db.Compare, item item) bool {
	i, found := n.find(cmp, item)
	if found {
		n.items[i] = item
		return false
	}
	if n.leaf {
		n.insertAt(i, item, nil)
		return true
	}
	if n.children[i].count >= maxItems {
		splitItem, splitNode := n.children[i].split(maxItems / 2)
		n.insertAt(i, splitItem, splitNode)

		switch c := cmp(item.ukey(), n.items[i].ukey()); {
		case c < 0:
			// no change, we want first split node
		case c > 0:
			i++ // we want second split node
		default:
			n.items[i] = item
			return false
		}
	}
	return n.children[i].insert(cmp, item)
}

func (n *node) removeMax() item {
	if n.leaf {
		n.count--
		out := n.items[n.count]
		n.items[n.count] = item{}
		return out
	}
	child := n.children[n.count]
	if child.count <= minItems {
		n.rebalanceOrMerge(int(n.count))
		return n.removeMax()
	}
	return child.removeMax()
}

// remove removes an item from the subtree rooted at this node.
func (n *node) remove(cmp db.Compare, key item) (item, bool) {
	i, found := n.find(cmp, key)
	if n.leaf {
		if found {
			item, _ := n.removeAt(i)
			return item, true
		}
		return item{}, false
	}
	child := n.children[i]
	if child.count <= minItems {
		n.rebalanceOrMerge(i)
		return n.remove(cmp, key)
	}
	if found {
		// Replace the item being removed with the max item in our left child.
		out := n.items[i]
		n.items[i] = child.removeMax()
		return out, true
	}
	return child.remove(cmp, key)
}

func (n *node) rebalanceOrMerge(i int) {
	switch {
	case i > 0 && n.children[i-1].count > minItems:
		// Rebalance from left sibling.
		left := n.children[i-1]
		child := n.children[i]
		item, grandChild := left.popBack()
		child.pushFront(n.items[i-1], grandChild)
		n.items[i-1] = item

	case i < int(n.count) && n.children[i+1].count > minItems:
		// Rebalance from right sibling.
		right := n.children[i+1]
		child := n.children[i]
		item, grandChild := right.popFront()
		child.pushBack(n.items[i], grandChild)
		n.items[i] = item

	default:
		// Merge with either the left or right sibling.
		if i >= int(n.count) {
			i = int(n.count - 1)
		}
		child := n.children[i]
		mergeItem, mergeChild := n.removeAt(i)
		child.items[child.count] = mergeItem
		copy(child.items[child.count+1:], mergeChild.items[:mergeChild.count])
		if !child.leaf {
			copy(child.children[child.count+1:], mergeChild.children[:mergeChild.count+1])
			for i := int16(0); i <= mergeChild.count; i++ {
				mergeChild.children[i].parent = child
			}
			child.updatePos(int(child.count+1), int(child.count+mergeChild.count+2))
		}
		child.count += mergeChild.count + 1
	}
}

func (n *node) verify(cmp db.Compare) {
	for i := int16(1); i < n.count; i++ {
		if cmp(n.items[i-1].ukey(), n.items[i].ukey()) >= 0 {
			panic(fmt.Sprintf("items are not sorted @ %d: %s >= %s",
				i, n.items[i-1].ukey(), n.items[i].ukey()))
		}
	}
	if !n.leaf {
		for i := int16(0); i < n.count; i++ {
			prev := n.children[i]
			if cmp(prev.items[prev.count-1].ukey(), n.items[i].ukey()) >= 0 {
				panic(fmt.Sprintf("items are not sorted @ %d: %s >= %s",
					i, n.items[i].ukey(), prev.items[prev.count-1].ukey()))
			}
			next := n.children[i+1]
			if cmp(n.items[i].ukey(), next.items[0].ukey()) >= 0 {
				panic(fmt.Sprintf("items are not sorted @ %d: %s >= %s",
					i, n.items[i].ukey(), next.items[0].ukey()))
			}
		}
		for i := int16(0); i <= n.count; i++ {
			if n.children[i].pos != i {
				panic(fmt.Sprintf("child has incorrect pos: %d != %d/%d", n.children[i].pos, i, n.count))
			}
			if n.children[i].parent != n {
				panic(fmt.Sprintf("child does not point to parent: %d/%d", i, n.count))
			}
			n.children[i].verify(cmp)
		}
	}
}

// BTree is an implementation of a B-Tree.
//
// BTree stores keys in an ordered structure, allowing easy insertion, removal,
// and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree struct {
	cmp    db.Compare
	root   *node
	length int
}

// New ...
func New(cmp db.Compare) *BTree {
	return &BTree{
		cmp: cmp,
	}
}

// Reset removes all items from the btree.
func (t *BTree) Reset(cmp db.Compare) {
	t.cmp = cmp
	t.root = nil
	t.length = 0
}

// Delete removes an item equal to the passed in item from the tree.
func (t *BTree) Delete(key db.InternalKey) {
	if t.root == nil || t.root.count == 0 {
		return
	}
	if _, found := t.root.remove(t.cmp, makeItem(key)); found {
		t.length--
	}
	if t.root.count == 0 && !t.root.leaf {
		t.root = t.root.children[0]
	}
}

// Set adds the given item to the tree. If an item in the tree already equals
// the given one, it is replaced with the new item.
func (t *BTree) Set(key db.InternalKey) {
	item := makeItem(key)
	if t.root == nil {
		t.root = newLeafNode()
	} else if t.root.count >= maxItems {
		splitItem, splitNode := t.root.split(maxItems / 2)
		newRoot := &node{}
		newRoot.count = 1
		newRoot.items[0] = splitItem
		newRoot.children[0] = t.root
		newRoot.children[1] = splitNode
		t.root.parent = newRoot
		t.root.pos = 0
		splitNode.parent = newRoot
		splitNode.pos = 1
		t.root = newRoot
	}
	if t.root.insert(t.cmp, item) {
		t.length++
	}
}

// NewIter returns a new Iterator object. Note that it is safe for an iterator
// to be copied by value.
func (t *BTree) NewIter() Iterator {
	return Iterator{t: t, pos: -1}
}

// Height returns the height of the tree.
func (t *BTree) Height() int {
	if t.root == nil {
		return 0
	}
	h := 1
	n := t.root
	for !n.leaf {
		n = n.children[0]
		h++
	}
	return h
}

// Len returns the number of items currently in the tree.
func (t *BTree) Len() int {
	return t.length
}

// Verify ...
func (t *BTree) Verify() {
	if t.root == nil {
		return
	}
	t.root.verify(t.cmp)
}

// Iterator ...
type Iterator struct {
	t   *BTree
	n   *node
	pos int16
}

// SeekGE ...
func (i *Iterator) SeekGE(key db.InternalKey) {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	item := makeItem(key)
	for {
		pos, found := i.n.find(i.t.cmp, item)
		i.pos = int16(pos)
		if found {
			return
		}
		if i.n.leaf {
			if i.pos == i.n.count {
				i.Next()
			}
			return
		}
		i.n = i.n.children[i.pos]
	}
}

// SeekLT ...
func (i *Iterator) SeekLT(key db.InternalKey) {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	item := makeItem(key)
	for {
		pos, found := i.n.find(i.t.cmp, item)
		i.pos = int16(pos)
		if found || i.n.leaf {
			i.Prev()
			return
		}
		i.n = i.n.children[i.pos]
	}
}

// First ...
func (i *Iterator) First() {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.n = i.n.children[0]
	}
	i.pos = 0
}

// Last ...
func (i *Iterator) Last() {
	i.n = i.t.root
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.n = i.n.children[i.n.count]
	}
	i.pos = i.n.count - 1
}

// Next ...
func (i *Iterator) Next() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos++
		if i.pos < i.n.count {
			return
		}
		for i.n.parent != nil && i.pos >= i.n.count {
			i.pos = i.n.pos
			i.n = i.n.parent
		}
		return
	}

	i.n = i.n.children[i.pos+1]
	for !i.n.leaf {
		i.n = i.n.children[0]
	}
	i.pos = 0
}

// Prev ...
func (i *Iterator) Prev() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos--
		if i.pos >= 0 {
			return
		}
		for i.n.parent != nil && i.pos < 0 {
			i.pos = i.n.pos - 1
			i.n = i.n.parent
		}
		return
	}

	i.n = i.n.children[i.pos]
	for !i.n.leaf {
		i.n = i.n.children[i.n.count]
	}
	i.pos = i.n.count - 1
}

// Valid ...
func (i *Iterator) Valid() bool {
	return i.pos >= 0 && i.pos < i.n.count
}

// Item ...
func (i *Iterator) Item() db.InternalKey {
	return i.n.items[i.pos].ikey()
}

// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
)

const (
	degree   = 16
	maxItems = 2*degree - 1
	minItems = degree - 1
)

type leafNode struct {
	ref   int32
	count int16
	leaf  bool
	items [maxItems]*FileMetadata
}

type node struct {
	leafNode
	children [maxItems + 1]*node
}

//go:nocheckptr casts a ptr to a smaller struct to a ptr to a larger struct.
func leafToNode(ln *leafNode) *node {
	return (*node)(unsafe.Pointer(ln))
}

func nodeToLeaf(n *node) *leafNode {
	return (*leafNode)(unsafe.Pointer(n))
}

var leafPool = sync.Pool{
	New: func() interface{} {
		return new(leafNode)
	},
}

var nodePool = sync.Pool{
	New: func() interface{} {
		return new(node)
	},
}

func newLeafNode() *node {
	n := leafToNode(leafPool.Get().(*leafNode))
	n.leaf = true
	n.ref = 1
	return n
}

func newNode() *node {
	n := nodePool.Get().(*node)
	n.ref = 1
	return n
}

// mut creates and returns a mutable node reference. If the node is not shared
// with any other trees then it can be modified in place. Otherwise, it must be
// cloned to ensure unique ownership. In this way, we enforce a copy-on-write
// policy which transparently incorporates the idea of local mutations, like
// Clojure's transients or Haskell's ST monad, where nodes are only copied
// during the first time that they are modified between Clone operations.
//
// When a node is cloned, the provided pointer will be redirected to the new
// mutable node.
func mut(n **node) *node {
	if atomic.LoadInt32(&(*n).ref) == 1 {
		// Exclusive ownership. Can mutate in place.
		return *n
	}
	// If we do not have unique ownership over the node then we
	// clone it to gain unique ownership. After doing so, we can
	// release our reference to the old node. We pass recursive
	// as true because even though we just observed the node's
	// reference count to be greater than 1, we might be racing
	// with another call to decRef on this node.
	c := (*n).clone()
	(*n).decRef(true /* recursive */, nil)
	*n = c
	return *n
}

// incRef acquires a reference to the node.
func (n *node) incRef() {
	atomic.AddInt32(&n.ref, 1)
}

// decRef releases a reference to the node. If requested, the method
// will recurse into child nodes and decrease their refcounts as well.
// When a node is released, its contained files are dereferenced.
func (n *node) decRef(recursive bool, obsolete *[]base.FileNum) {
	if atomic.AddInt32(&n.ref, -1) > 0 {
		// Other references remain. Can't free.
		return
	}

	// Dereference the node's metadata.
	if recursive {
		for _, f := range n.items[:n.count] {
			if atomic.AddInt32(&f.refs, -1) == 0 {
				// There are two sources of node dereferences: tree mutations
				// and Version dereferences. Files should only be made obsolete
				// during Version dereferences, during which `obsolete` will be
				// non-nil.
				if obsolete == nil {
					panic(fmt.Sprintf("file metadata %s dereferenced to zero during tree mutation", f.FileNum))
				}
				*obsolete = append(*obsolete, f.FileNum)
			}
		}
	}

	// Clear and release node into memory pool.
	if n.leaf {
		ln := nodeToLeaf(n)
		*ln = leafNode{}
		leafPool.Put(ln)
	} else {
		// Release child references first, if requested.
		if recursive {
			for i := int16(0); i <= n.count; i++ {
				n.children[i].decRef(true /* recursive */, obsolete)
			}
		}
		*n = node{}
		nodePool.Put(n)
	}
}

// clone creates a clone of the receiver with a single reference count.
func (n *node) clone() *node {
	var c *node
	if n.leaf {
		c = newLeafNode()
	} else {
		c = newNode()
	}
	// NB: copy field-by-field without touching n.ref to avoid
	// triggering the race detector and looking like a data race.
	c.count = n.count
	c.items = n.items
	// Increase the refcount of each contained item.
	for _, f := range n.items[:n.count] {
		atomic.AddInt32(&f.refs, 1)
	}
	if !c.leaf {
		// Copy children and increase each refcount.
		c.children = n.children
		for i := int16(0); i <= c.count; i++ {
			c.children[i].incRef()
		}
	}
	return c
}

func (n *node) insertAt(index int, item *FileMetadata, nd *node) {
	if index < int(n.count) {
		copy(n.items[index+1:n.count+1], n.items[index:n.count])
		if !n.leaf {
			copy(n.children[index+2:n.count+2], n.children[index+1:n.count+1])
		}
	}
	n.items[index] = item
	if !n.leaf {
		n.children[index+1] = nd
	}
	n.count++
}

func (n *node) pushBack(item *FileMetadata, nd *node) {
	n.items[n.count] = item
	if !n.leaf {
		n.children[n.count+1] = nd
	}
	n.count++
}

func (n *node) pushFront(item *FileMetadata, nd *node) {
	if !n.leaf {
		copy(n.children[1:n.count+2], n.children[:n.count+1])
		n.children[0] = nd
	}
	copy(n.items[1:n.count+1], n.items[:n.count])
	n.items[0] = item
	n.count++
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (n *node) removeAt(index int) (*FileMetadata, *node) {
	var child *node
	if !n.leaf {
		child = n.children[index+1]
		copy(n.children[index+1:n.count], n.children[index+2:n.count+1])
		n.children[n.count] = nil
	}
	n.count--
	out := n.items[index]
	copy(n.items[index:n.count], n.items[index+1:n.count+1])
	n.items[n.count] = nil
	return out, child
}

// popBack removes and returns the last element in the list.
func (n *node) popBack() (*FileMetadata, *node) {
	n.count--
	out := n.items[n.count]
	n.items[n.count] = nil
	if n.leaf {
		return out, nil
	}
	child := n.children[n.count+1]
	n.children[n.count+1] = nil
	return out, child
}

// popFront removes and returns the first element in the list.
func (n *node) popFront() (*FileMetadata, *node) {
	n.count--
	var child *node
	if !n.leaf {
		child = n.children[0]
		copy(n.children[:n.count+1], n.children[1:n.count+2])
		n.children[n.count+1] = nil
	}
	out := n.items[0]
	copy(n.items[:n.count], n.items[1:n.count+1])
	n.items[n.count] = nil
	return out, child
}

// find returns the index where the given item should be inserted into this
// list. 'found' is true if the item already exists in the list at the given
// index.
func (n *node) find(cmp func(*FileMetadata, *FileMetadata) int, item *FileMetadata) (index int, found bool) {
	// Logic copied from sort.Search. Inlining this gave
	// an 11% speedup on BenchmarkBTreeDeleteInsert.
	i, j := 0, int(n.count)
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i ≤ h < j
		v := cmp(item, n.items[h])
		if v == 0 {
			return h, true
		} else if v > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, false
}

// split splits the given node at the given index. The current node shrinks,
// and this function returns the item that existed at that index and a new
// node containing all items/children after it.
//
// Before:
//
//          +-----------+
//          |   x y z   |
//          +--/-/-\-\--+
//
// After:
//
//          +-----------+
//          |     y     |
//          +----/-\----+
//              /   \
//             v     v
// +-----------+     +-----------+
// |         x |     | z         |
// +-----------+     +-----------+
//
func (n *node) split(i int) (*FileMetadata, *node) {
	out := n.items[i]
	var next *node
	if n.leaf {
		next = newLeafNode()
	} else {
		next = newNode()
	}
	next.count = n.count - int16(i+1)
	copy(next.items[:], n.items[i+1:n.count])
	for j := int16(i); j < n.count; j++ {
		n.items[j] = nil
	}
	if !n.leaf {
		copy(next.children[:], n.children[i+1:n.count+1])
		for j := int16(i + 1); j <= n.count; j++ {
			n.children[j] = nil
		}
	}
	n.count = int16(i)
	return out, next
}

// insert inserts a item into the subtree rooted at this node, making sure no
// nodes in the subtree exceed maxItems items.
func (n *node) insert(cmp func(*FileMetadata, *FileMetadata) int, item *FileMetadata) {
	i, found := n.find(cmp, item)
	if found {
		// cmp provides a total ordering of the files within a level.
		// If we're inserting a metadata that's equal to an existing item
		// in the tree, we're inserting a file into a level twice.
		panic(fmt.Sprintf("file key collision: existing metadata %s, inserting %s",
			n.items[i].FileNum, item.FileNum))
	}
	if n.leaf {
		n.insertAt(i, item, nil)
		return
	}
	if n.children[i].count >= maxItems {
		splitLa, splitNode := mut(&n.children[i]).split(maxItems / 2)
		n.insertAt(i, splitLa, splitNode)

		switch cmp := cmp(item, n.items[i]); {
		case cmp < 0:
			// no change, we want first split node
		case cmp > 0:
			i++ // we want second split node
		default:
			// cmp provides a total ordering of the files within a level.
			// If we're inserting a metadata that's equal to an existing item
			// in the tree, we're inserting a file into a level twice.
			panic(fmt.Sprintf("file key collision: existing metadata %s, inserting %s",
				n.items[i].FileNum, item.FileNum))
		}
	}
	mut(&n.children[i]).insert(cmp, item)
}

// removeMax removes and returns the maximum item from the subtree rooted
// at this node.
func (n *node) removeMax() *FileMetadata {
	if n.leaf {
		n.count--
		out := n.items[n.count]
		n.items[n.count] = nil
		return out
	}
	child := mut(&n.children[n.count])
	if child.count <= minItems {
		n.rebalanceOrMerge(int(n.count))
		return n.removeMax()
	}
	return child.removeMax()
}

// remove removes a item from the subtree rooted at this node. Returns
// the item that was removed or nil if no matching item was found.
func (n *node) remove(cmp func(*FileMetadata, *FileMetadata) int, item *FileMetadata) (out *FileMetadata) {
	i, found := n.find(cmp, item)
	if n.leaf {
		if found {
			out, _ = n.removeAt(i)
			return out
		}
		return nil
	}
	if n.children[i].count <= minItems {
		// Child not large enough to remove from.
		n.rebalanceOrMerge(i)
		return n.remove(cmp, item)
	}
	child := mut(&n.children[i])
	if found {
		// Replace the item being removed with the max item in our left child.
		out = n.items[i]
		n.items[i] = child.removeMax()
		return out
	}
	// Latch is not in this node and child is large enough to remove from.
	out = child.remove(cmp, item)
	return out
}

// rebalanceOrMerge grows child 'i' to ensure it has sufficient room to remove
// a item from it while keeping it at or above minItems.
func (n *node) rebalanceOrMerge(i int) {
	switch {
	case i > 0 && n.children[i-1].count > minItems:
		// Rebalance from left sibling.
		//
		//          +-----------+
		//          |     y     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         x |     |           |
		// +----------\+     +-----------+
		//             \
		//              v
		//              a
		//
		// After:
		//
		//          +-----------+
		//          |     x     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |           |     | y         |
		// +-----------+     +/----------+
		//                   /
		//                  v
		//                  a
		//
		left := mut(&n.children[i-1])
		child := mut(&n.children[i])
		xLa, grandChild := left.popBack()
		yLa := n.items[i-1]
		child.pushFront(yLa, grandChild)
		n.items[i-1] = xLa

	case i < int(n.count) && n.children[i+1].count > minItems:
		// Rebalance from right sibling.
		//
		//          +-----------+
		//          |     y     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |           |     | x         |
		// +-----------+     +/----------+
		//                   /
		//                  v
		//                  a
		//
		// After:
		//
		//          +-----------+
		//          |     x     |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         y |     |           |
		// +----------\+     +-----------+
		//             \
		//              v
		//              a
		//
		right := mut(&n.children[i+1])
		child := mut(&n.children[i])
		xLa, grandChild := right.popFront()
		yLa := n.items[i]
		child.pushBack(yLa, grandChild)
		n.items[i] = xLa

	default:
		// Merge with either the left or right sibling.
		//
		//          +-----------+
		//          |   u y v   |
		//          +----/-\----+
		//              /   \
		//             v     v
		// +-----------+     +-----------+
		// |         x |     | z         |
		// +-----------+     +-----------+
		//
		// After:
		//
		//          +-----------+
		//          |    u v    |
		//          +-----|-----+
		//                |
		//                v
		//          +-----------+
		//          |   x y z   |
		//          +-----------+
		//
		if i >= int(n.count) {
			i = int(n.count - 1)
		}
		child := mut(&n.children[i])
		// Make mergeChild mutable, bumping the refcounts on its children if necessary.
		_ = mut(&n.children[i+1])
		mergeLa, mergeChild := n.removeAt(i)
		child.items[child.count] = mergeLa
		copy(child.items[child.count+1:], mergeChild.items[:mergeChild.count])
		if !child.leaf {
			copy(child.children[child.count+1:], mergeChild.children[:mergeChild.count+1])
		}
		child.count += mergeChild.count + 1

		mergeChild.decRef(false /* recursive */, nil)
	}
}

// btree is an implementation of a B-Tree.
//
// btree stores FileMetadata in an ordered structure, allowing easy insertion,
// removal, and iteration. The B-Tree stores items in order based on cmp. The
// first level of the LSM uses a cmp function that compares sequence numbers.
// All other levels compare using the FileMetadata.Smallest.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type btree struct {
	root   *node
	length int
	cmp    func(*FileMetadata, *FileMetadata) int
}

// Release dereferences and clears the root node of the btree, removing all
// items from the btree. In doing so, it allows memory held by the btree to be
// recycled. It returns a slice of file numbers of newly obsolete files, if
// any.
func (t *btree) Release() (obsolete []base.FileNum) {
	if t.root != nil {
		t.root.decRef(true /* recursive */, &obsolete)
		t.root = nil
	}
	t.length = 0
	return obsolete
}

// Clone clones the btree, lazily. It does so in constant time.
func (t *btree) Clone() btree {
	c := *t
	if c.root != nil {
		// Incrementing the reference count on the root node is sufficient to
		// ensure that no node in the cloned tree can be mutated by an actor
		// holding a reference to the original tree and vice versa. This
		// property is upheld because the root node in the receiver btree and
		// the returned btree will both necessarily have a reference count of at
		// least 2 when this method returns. All tree mutations recursively
		// acquire mutable node references (see mut) as they traverse down the
		// tree. The act of acquiring a mutable node reference performs a clone
		// if a node's reference count is greater than one. Cloning a node (see
		// clone) increases the reference count on each of its children,
		// ensuring that they have a reference count of at least 2. This, in
		// turn, ensures that any of the child nodes that are modified will also
		// be copied-on-write, recursively ensuring the immutability property
		// over the entire tree.
		c.root.incRef()
	}
	return c
}

// Delete removes the provided file from the tree.
// It returns true if the file now has a zero reference count.
func (t *btree) Delete(item *FileMetadata) (obsolete bool) {
	if t.root == nil || t.root.count == 0 {
		return false
	}
	if out := mut(&t.root).remove(t.cmp, item); out != nil {
		t.length--
		obsolete = atomic.AddInt32(&out.refs, -1) == 0
	}
	if t.root.count == 0 {
		old := t.root
		if t.root.leaf {
			t.root = nil
		} else {
			t.root = t.root.children[0]
		}
		old.decRef(false /* recursive */, nil)
	}
	return obsolete
}

// Insert adds the given item to the tree. If a item in the tree already
// equals the given one, Insert panics.
func (t *btree) Insert(item *FileMetadata) {
	if t.root == nil {
		t.root = newLeafNode()
	} else if t.root.count >= maxItems {
		splitLa, splitNode := mut(&t.root).split(maxItems / 2)
		newRoot := newNode()
		newRoot.count = 1
		newRoot.items[0] = splitLa
		newRoot.children[0] = t.root
		newRoot.children[1] = splitNode
		t.root = newRoot
	}
	atomic.AddInt32(&item.refs, 1)
	mut(&t.root).insert(t.cmp, item)
	t.length++
}

// MakeIter returns a new iterator object. It is not safe to continue using an
// iterator after modifications are made to the tree. If modifications are made,
// create a new iterator.
func (t *btree) MakeIter() iterator {
	return iterator{r: t.root, pos: -1, cmp: t.cmp}
}

// Height returns the height of the tree.
func (t *btree) Height() int {
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
func (t *btree) Len() int {
	return t.length
}

// String returns a string description of the tree. The format is
// similar to the https://en.wikipedia.org/wiki/Newick_format.
func (t *btree) String() string {
	if t.length == 0 {
		return ";"
	}
	var b strings.Builder
	t.root.writeString(&b)
	return b.String()
}

func (n *node) writeString(b *strings.Builder) {
	if n.leaf {
		for i := int16(0); i < n.count; i++ {
			if i != 0 {
				b.WriteString(",")
			}
			b.WriteString(n.items[i].String())
		}
		return
	}
	for i := int16(0); i <= n.count; i++ {
		b.WriteString("(")
		n.children[i].writeString(b)
		b.WriteString(")")
		if i < n.count {
			b.WriteString(n.items[i].String())
		}
	}
}

// iterStack represents a stack of (node, pos) tuples, which captures
// iteration state as an iterator descends a btree.
type iterStack struct {
	a    iterStackArr
	aLen int16 // -1 when using s
	s    []iterFrame
}

// Used to avoid allocations for stacks below a certain size.
type iterStackArr [3]iterFrame

type iterFrame struct {
	n   *node
	pos int16
}

func (is *iterStack) push(f iterFrame) {
	if is.aLen == -1 {
		is.s = append(is.s, f)
	} else if int(is.aLen) == len(is.a) {
		is.s = make([]iterFrame, int(is.aLen)+1, 2*int(is.aLen))
		copy(is.s, is.a[:])
		is.s[int(is.aLen)] = f
		is.aLen = -1
	} else {
		is.a[is.aLen] = f
		is.aLen++
	}
}

func (is *iterStack) pop() iterFrame {
	if is.aLen == -1 {
		f := is.s[len(is.s)-1]
		is.s = is.s[:len(is.s)-1]
		return f
	}
	is.aLen--
	return is.a[is.aLen]
}

func (is *iterStack) len() int {
	if is.aLen == -1 {
		return len(is.s)
	}
	return int(is.aLen)
}

func (is *iterStack) clone() iterStack {
	// If the iterator is using the embedded iterStackArr, we only need to
	// copy the struct itself.
	if is.s == nil {
		return *is
	}
	clone := *is
	clone.s = make([]iterFrame, len(is.s))
	copy(clone.s, is.s)
	return clone
}

func (is *iterStack) reset() {
	if is.aLen == -1 {
		is.s = is.s[:0]
	} else {
		is.aLen = 0
	}
}

// iterator is responsible for search and traversal within a btree.
type iterator struct {
	r   *node
	n   *node
	pos int16
	cmp func(*FileMetadata, *FileMetadata) int
	s   iterStack
}

func (i *iterator) clone() iterator {
	c := *i
	c.s = i.s.clone()
	return c
}

func (i *iterator) reset() {
	i.n = i.r
	i.pos = -1
	i.s.reset()
}

func (i *iterator) descend(n *node, pos int16) {
	i.s.push(iterFrame{n: n, pos: pos})
	i.n = n.children[pos]
	i.pos = 0
}

// ascend ascends up to the current node's parent and resets the position
// to the one previously set for this parent node.
func (i *iterator) ascend() {
	f := i.s.pop()
	i.n = f.n
	i.pos = f.pos
}

// SeekGE seeks to the first item greater-than or equal to the provided
// item.
func (i *iterator) SeekGE(item *FileMetadata) {
	i.reset()
	if i.n == nil {
		return
	}
	for {
		pos, found := i.n.find(i.cmp, item)
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
		i.descend(i.n, i.pos)
	}
}

// SeekLT seeks to the first item less-than the provided item.
func (i *iterator) SeekLT(item *FileMetadata) {
	i.reset()
	if i.n == nil {
		return
	}
	for {
		pos, found := i.n.find(i.cmp, item)
		i.pos = int16(pos)
		if found || i.n.leaf {
			i.Prev()
			return
		}
		i.descend(i.n, i.pos)
	}
}

// First seeks to the first item in the btree.
func (i *iterator) First() {
	i.reset()
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.descend(i.n, 0)
	}
	i.pos = 0
}

// Last seeks to the last item in the btree.
func (i *iterator) Last() {
	i.reset()
	if i.n == nil {
		return
	}
	for !i.n.leaf {
		i.descend(i.n, i.n.count)
	}
	i.pos = i.n.count - 1
}

// Next positions the iterator to the item immediately following
// its current position.
func (i *iterator) Next() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos++
		if i.pos < i.n.count {
			return
		}
		for i.s.len() > 0 && i.pos >= i.n.count {
			i.ascend()
		}
		return
	}

	i.descend(i.n, i.pos+1)
	for !i.n.leaf {
		i.descend(i.n, 0)
	}
	i.pos = 0
}

// Prev positions the iterator to the item immediately preceding
// its current position.
func (i *iterator) Prev() {
	if i.n == nil {
		return
	}

	if i.n.leaf {
		i.pos--
		if i.pos >= 0 {
			return
		}
		for i.s.len() > 0 && i.pos < 0 {
			i.ascend()
			i.pos--
		}
		return
	}

	i.descend(i.n, i.pos)
	for !i.n.leaf {
		i.descend(i.n, i.n.count)
	}
	i.pos = i.n.count - 1
}

// Valid returns whether the iterator is positioned at a valid position.
func (i *iterator) Valid() bool {
	return i.pos >= 0 && i.pos < i.n.count
}

// Cur returns the item at the iterator's current position. It is illegal
// to call Cur if the iterator is not valid.
func (i *iterator) Cur() *FileMetadata {
	return i.n.items[i.pos]
}

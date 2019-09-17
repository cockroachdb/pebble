/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 * Modifications copyright (C) 2017 Andy Kimball and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Adapted from RocksDB inline skiplist.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.
*/

/*
Further adapted from Badger: https://github.com/dgraph-io/badger.

Key differences:
- Support for previous pointers - doubly linked lists. Note that it's up to higher
  level code to deal with the intermediate state that occurs during insertion,
  where node A is linked to node B, but node B is not yet linked back to node A.
- Iterator includes mutator functions.
*/

/*
Further adapted from arenaskl: https://github.com/andy-kimball/arenaskl

Key differences:
- Removed support for deletion.
- Removed support for concurrency.
- External storage of keys.
- Node storage grows to an arbitrary size.
*/

package batchskl // import "github.com/cockroachdb/pebble/internal/batchskl"

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"time"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"golang.org/x/exp/rand"
)

const (
	maxHeight   = 20
	maxNodeSize = int(unsafe.Sizeof(node{}))
	linksSize   = int(unsafe.Sizeof(links{}))
)

// ErrExists indicates that a duplicate record was inserted. This should never
// happen for normal usage of batchskl as every key should have a unique
// sequence number.
var ErrExists = errors.New("record with this key already exists")

type links struct {
	next uint32
	prev uint32
}

type node struct {
	// The offset of the key in storage. See Storage.Get.
	key uint32
	// A fixed 8-byte abbreviation of the key, used to avoid retrieval of the key
	// during seek operations. The key retrieval can be expensive purely due to
	// cache misses while the abbreviatedKey stored here will be in the same
	// cache line as the key and the links making accessing and comparing against
	// it almost free.
	abbreviatedKey uint64
	// Most nodes do not need to use the full height of the link tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated, its memory footprint is deliberately
	// truncated to not include unneeded link elements.
	links [maxHeight]links
}

// Storage defines the storage interface for retrieval and comparison of keys.
type Storage interface {
	// Get returns the key stored at the specified offset.
	Get(offset uint32) base.InternalKey

	// AbbreviatedKey returns a fixed length prefix of the specified key such
	// that AbbreviatedKey(a) < AbbreviatedKey(b) iff a < b and AbbreviatedKey(a)
	// > AbbreviatedKey(b) iff a > b. If AbbreviatedKey(a) == AbbreviatedKey(b)
	// an additional comparison is required to determine if the two keys are
	// actually equal.
	AbbreviatedKey(key []byte) uint64

	// Compare returns -1, 0, or +1 depending on whether a is 'less than', 'equal
	// to', or 'greater than' the key stored at b.
	Compare(a []byte, b uint32) int
}

// Skiplist is a fast, non-cocnurrent skiplist implementation that supports
// forward and backward iteration. See arenaskl.Skiplist for a concurrent
// skiplist. Keys and values are stored externally from the skiplist via the
// Storage interface. Deletion is not supported. Instead, higher-level code is
// expected to perform deletion via tombstones and needs to process those
// tombstones appropriately during retrieval operations.
type Skiplist struct {
	storage Storage
	nodes   []byte
	head    uint32
	tail    uint32
	height  uint32 // Current height: 1 <= height <= maxHeight
	rand    rand.PCGSource
}

var (
	probabilities [maxHeight]uint32
)

func init() {
	const pValue = 1 / math.E

	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := float64(1.0)
	for i := 0; i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

// NewSkiplist constructs and initializes a new, empty skiplist.
func NewSkiplist(storage Storage, initBufSize int) *Skiplist {
	if initBufSize < 256 {
		initBufSize = 256
	}
	s := &Skiplist{
		storage: storage,
		nodes:   make([]byte, 0, initBufSize),
		height:  1,
	}
	s.rand.Seed(uint64(time.Now().UnixNano()))

	// Allocate head and tail nodes.
	s.head = s.newNode(maxHeight, 0, 0)
	s.tail = s.newNode(maxHeight, 0, 0)

	// Link all head/tail levels together.
	for i := uint32(0); i < maxHeight; i++ {
		s.setNext(s.head, i, s.tail)
		s.setPrev(s.tail, i, s.head)
	}

	return s
}

// Reset the skiplist to empty and re-initialize.
func (s *Skiplist) Reset(storage Storage, initBufSize int) {
	if initBufSize < 256 {
		initBufSize = 256
	}
	*s = Skiplist{
		storage: storage,
		nodes:   make([]byte, 0, initBufSize),
		height:  1,
	}

	// Allocate head and tail nodes.
	s.head = s.newNode(maxHeight, 0, 0)
	s.tail = s.newNode(maxHeight, 0, 0)

	// Link all head/tail levels together.
	for i := uint32(0); i < maxHeight; i++ {
		s.setNext(s.head, i, s.tail)
		s.setPrev(s.tail, i, s.head)
	}
}

// Add adds a new key to the skiplist if it does not yet exist. If the record
// already exists, then Add returns ErrRecordExists.
func (s *Skiplist) Add(keyOffset uint32) error {
	key := s.storage.Get(keyOffset)
	abbreviatedKey := s.storage.AbbreviatedKey(key.UserKey)

	var spl [maxHeight]splice
	if s.findSplice(key.UserKey, abbreviatedKey, &spl) {
		return ErrExists
	}

	height := s.randomHeight()
	nd := s.newNode(height, keyOffset, abbreviatedKey)
	// Increase s.height as necessary.
	for ; s.height < height; s.height++ {
		spl[s.height].next = s.tail
		spl[s.height].prev = s.head
	}

	// We always insert from the base level and up. After you add a node in base
	// level, we cannot create a node in the level above because it would have
	// discovered the node in the base level.
	for i := uint32(0); i < height; i++ {
		next := spl[i].next
		prev := spl[i].prev
		s.setNext(nd, i, next)
		s.setPrev(nd, i, prev)
		s.setNext(prev, i, nd)
		s.setPrev(next, i, nd)
	}

	return nil
}

// NewIter returns a new Iterator object. The lower and upper bound parameters
// control the range of keys the iterator will return. Specifying for nil for
// lower or upper bound disables the check for that boundary. Note that lower
// bound is not checked on {SeekGE,First} and upper bound is not check on
// {SeekLT,Last}. The user is expected to perform that check. Note that it is
// safe for an iterator to be copied by value.
func (s *Skiplist) NewIter(lower, upper []byte) Iterator {
	return Iterator{list: s, lower: lower, upper: upper}
}

func (s *Skiplist) newNode(height, key uint32, abbreviatedKey uint64) uint32 {
	if height < 1 || height > maxHeight {
		panic("height cannot be less than one or greater than the max height")
	}

	unusedSize := (maxHeight - int(height)) * linksSize
	offset := s.alloc(uint32(maxNodeSize - unusedSize))
	nd := s.node(offset)

	nd.key = key
	nd.abbreviatedKey = abbreviatedKey
	return offset
}

func (s *Skiplist) alloc(size uint32) uint32 {
	offset := uint32(len(s.nodes))
	newSize := offset + size
	if cap(s.nodes) < int(newSize) {
		allocSize := uint32(cap(s.nodes) * 2)
		if allocSize < newSize {
			allocSize = newSize
		}
		tmp := make([]byte, len(s.nodes), allocSize)
		copy(tmp, s.nodes)
		s.nodes = tmp
	}

	s.nodes = s.nodes[:newSize]
	return offset
}

func (s *Skiplist) node(offset uint32) *node {
	return (*node)(unsafe.Pointer(&s.nodes[offset]))
}

func (s *Skiplist) randomHeight() uint32 {
	rnd := uint32(s.rand.Uint64())
	h := uint32(1)
	for h < maxHeight && rnd <= probabilities[h] {
		h++
	}
	return h
}

func (s *Skiplist) findSplice(
	key []byte, abbreviatedKey uint64, spl *[maxHeight]splice,
) (found bool) {
	var prev, next uint32
	prev = s.head

	for level := s.height - 1; ; level-- {
		prev, next, found = s.findSpliceForLevel(key, abbreviatedKey, level, prev)
		spl[level].init(prev, next)
		if level == 0 {
			break
		}
	}

	return
}

func (s *Skiplist) findSpliceForLevel(
	key []byte, abbreviatedKey uint64, level, start uint32,
) (prev, next uint32, found bool) {
	prev = start

	for {
		// Assume prev.key < key.
		next = s.getNext(prev, level)
		if next == s.tail {
			// Tail node, so done.
			break
		}

		nextAbbreviatedKey := s.getAbbreviatedKey(next)
		if abbreviatedKey < nextAbbreviatedKey {
			// We are done for this level, since prev.key < key < next.key.
			break
		}
		if abbreviatedKey == nextAbbreviatedKey {
			cmp := s.storage.Compare(key, s.getKey(next))
			if cmp == 0 {
				// Equality case.
				found = true
				break
			}
			if cmp < 0 {
				// We are done for this level, since prev.key < key < next.key.
				break
			}
		}

		// Keep moving right on this level.
		prev = next
	}

	return
}

func (s *Skiplist) getKey(nd uint32) uint32 {
	return s.node(nd).key
}

func (s *Skiplist) getAbbreviatedKey(nd uint32) uint64 {
	return s.node(nd).abbreviatedKey
}

func (s *Skiplist) getNext(nd, h uint32) uint32 {
	return s.node(nd).links[h].next
}

func (s *Skiplist) getPrev(nd, h uint32) uint32 {
	return s.node(nd).links[h].prev
}

func (s *Skiplist) setNext(nd, h, next uint32) {
	s.node(nd).links[h].next = next
}

func (s *Skiplist) setPrev(nd, h, prev uint32) {
	s.node(nd).links[h].prev = prev
}

func (s *Skiplist) debug() string {
	var buf bytes.Buffer
	for level := uint32(0); level < s.height; level++ {
		var count int
		for nd := s.head; nd != s.tail; nd = s.getNext(nd, level) {
			count++
		}
		fmt.Fprintf(&buf, "%d: %d\n", level, count)
	}
	return buf.String()
}

// Silence unused warning.
var _ = (*Skiplist).debug

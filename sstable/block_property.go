// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
)

// Block properties are an optional user-facing feature that can be used to
// filter data blocks (and whole sstables) from an Iterator before they are
// loaded. They do not apply to range delete blocks. These are expected to
// very concisely represent a set of some attribute value contained within the
// key or value, such that the set includes all the attribute values in the
// block. This has some similarities with OLAP pruning approaches that
// maintain min-max attribute values for some column (which concisely
// represent a set), that is then used to prune at query time. In Pebble's
// case, data blocks are small, typically 25-50KB, so these properties should
// reduce their precision in order to be concise -- a good rule of thumb is to
// not consume more than 50-100 bytes across all properties maintained for a
// block, i.e., a 500x reduction compared to loading the data block.
//
// A block property must be assigned a unique name, which is encoded and
// stored in the sstable. This name must be unique among all user-properties
// encoded in an sstable.
//
// A property is represented as a []byte. A nil value or empty byte slice are
// considered semantically identical. The caller is free to choose the
// semantics of an empty byte slice e.g. they could use it to represent the
// empty set or the universal set, whichever they think is more common and
// therefore better to encode more concisely. The serialization of the
// property for the various Finish*() calls in a BlockPropertyCollector
// implementation should be identical, since the corresponding
// BlockPropertyFilter implementation is not told the context in which it is
// deserializing the property.
//
// Block properties are more general than table properties and should be
// preferred over using table properties. A BlockPropertyCollector can achieve
// identical behavior to table properties by returning the nil slice from
// FinishDataBlock and FinishIndexBlock, and interpret them as the universal
// set in BlockPropertyFilter, and return a non-universal set in FinishTable.

// BlockPropertyCollector is used when writing a sstable.
// - All calls to Add are included in the next FinishDataBlock, after which
//   the next data block is expected to start.
//
// - The index entry generated for the data block, which contains the return
//   value from FinishDataBlock, is not immediately included in the current
//   index block. It is included when AddPrevDataBlockToIndexBlock is called.
//   An alternative would be to return an opaque handle from FinishDataBlock
//   and pass it to a new AddToIndexBlock method, which requires more
//   plumbing, and passing of an interface{} results in a undesirable heap
//   allocation. AddPrevDataBlockToIndexBlock must be called before keys are
//   added to the new data block.
type BlockPropertyCollector interface {
	// Name returns the name of the block property collector.
	Name() string
	// Add is called with each new entry added to a data block in the sstable.
	// The callee can assume that these are in sorted order.
	Add(key InternalKey, value []byte) error
	// FinishDataBlock is called when all the entries have been added to a
	// data block. Subsequent Add calls will be for the next data block. It
	// returns the property value for the finished block.
	FinishDataBlock(buf []byte) ([]byte, error)
	// AddPrevDataBlockToIndexBlock adds the entry corresponding to the
	// previous FinishDataBlock to the current index block.
	AddPrevDataBlockToIndexBlock()
	// FinishIndexBlock is called when an index block, containing all the
	// key-value pairs since the last FinishIndexBlock, will no longer see new
	// entries. It returns the property value for the index block.
	FinishIndexBlock(buf []byte) ([]byte, error)
	// FinishTable is called when the sstable is finished, and returns the
	// property value for the sstable.
	FinishTable(buf []byte) ([]byte, error)
}

// BlockPropertyFilter is used in an Iterator to filter sstables and blocks
// within the sstable. It should not maintain any per-sstable state, and must
// be thread-safe.
type BlockPropertyFilter interface {
	// Name returns the name of the block property collector.
	Name() string
	// Intersects returns true if the set represented by prop intersects with
	// the set in the filter.
	Intersects(prop []byte) (bool, error)
}

// BlockIntervalCollector is a helper implementation of BlockPropertyCollector
// for users who want to represent a set of the form [lower,upper) where both
// lower and upper are uint64, and lower <= upper.
//
// The set is encoded as:
// - Two varint integers, (lower,upper-lower), when upper-lower > 0
// - Nil, when upper-lower=0
//
// Users must not expect this to preserve differences between empty sets --
// they will all get turned into the semantically equivalent [0,0).
type BlockIntervalCollector struct {
	name    string
	dbic    DataBlockIntervalCollector

	blockInterval interval
	indexInterval interval
	tableInterval interval
}

var _ BlockPropertyCollector = &BlockIntervalCollector{}

// DataBlockIntervalCollector is the interface used by BlockIntervalCollector
// that contains the actual logic pertaining to the property. It only
// maintains state for the current data block, and resets that state in
// FinishDataBlock. This interface can be used to reduce parsing costs.
type DataBlockIntervalCollector interface {
	// Add is called with each new entry added to a data block in the sstable.
	// The callee can assume that these are in sorted order.
	Add(key InternalKey, value []byte) error
	// FinishDataBlock is called when all the entries have been added to a
	// data block. Subsequent Add calls will be for the next data block. It
	// returns the [lower, upper) for the finished block.
	FinishDataBlock() (lower uint64, upper uint64, err error)
}

// NewBlockIntervalCollector constructs a BlockIntervalCollector, with the
// given name and data block collector.
func NewBlockIntervalCollector(
	name string, blockAttributeCollector DataBlockIntervalCollector) *BlockIntervalCollector {
	return &BlockIntervalCollector{
		name: name, dbic: blockAttributeCollector}
}

// Name implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) Name() string {
	return b.name
}

// Add implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) Add(key InternalKey, value []byte) error {
	return b.dbic.Add(key, value)
}

// FinishDataBlock implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	var err error
	b.blockInterval.lower, b.blockInterval.upper, err = b.dbic.FinishDataBlock()
	if err != nil {
		return buf, err
	}
	buf = b.blockInterval.encode(buf)
	b.tableInterval.union(b.blockInterval)
	return buf, nil
}

// AddPrevDataBlockToIndexBlock implements the BlockPropertyCollector
// interface.
func (b *BlockIntervalCollector) AddPrevDataBlockToIndexBlock() {
	b.indexInterval.union(b.blockInterval)
	b.blockInterval = interval{}
}

// FinishIndexBlock implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	buf = b.indexInterval.encode(buf)
	b.indexInterval = interval{}
	return buf, nil
}

// FinishTable implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishTable(buf []byte) ([]byte, error) {
	return b.tableInterval.encode(buf), nil
}

type interval struct {
	lower uint64
	upper uint64
}

func (i interval) encode(buf []byte) []byte {
	if i.lower < i.upper {
		var encoded [binary.MaxVarintLen64*2]byte
		n := binary.PutUvarint(encoded[:], i.lower)
		n += binary.PutUvarint(encoded[n:], i.upper-i.lower)
		buf = append(buf, encoded[:n]...)
	}
	return buf
}

func (i *interval) decode(buf []byte) error {
	if len(buf) == 0 {
		*i = interval{}
		return nil
	}
	var n int
	i.lower, n = binary.Uvarint(buf)
	if n <= 0 || n >= len(buf) {
		return base.CorruptionErrorf("cannot decode interval from buf %x", buf)
	}
	pos := n
	i.upper, n = binary.Uvarint(buf[pos:])
	pos += n
	if pos != len(buf) || n <= 0 {
		return base.CorruptionErrorf("cannot decode interval from buf %x", buf)
	}
	// Delta decode.
	i.upper += i.lower
	if i.upper < i.lower {
		return base.CorruptionErrorf("unexpected overflow, upper %d < lower %d", i.upper, i.lower)
	}
	return nil
}

func (i *interval) union(x interval) {
	if x.lower >= x.upper {
		// x is the empty set.
		return
	}
	if i.lower >= i.upper {
		// i is the empty set.
		*i = x
		return
	}
	// Both sets are non-empty.
	if x.lower < i.lower {
		i.lower = x.lower
	}
	if x.upper > i.upper {
		i.upper = x.upper
	}
}

func (i interval) intersects(x interval) bool {
	if i.lower >= i.upper || x.lower >= x.upper {
		// At least one of the sets is empty.
		return false
	}
	// Neither set is empty.
	return i.upper > x.lower && i.lower < x.upper
}

// BlockIntervalFilter is an implementation of BlockPropertyFilter when the
// corresponding collector is a BlockIntervalCollector. That is, the set is of
// the form [lower, upper).
type BlockIntervalFilter struct {
	name string
	filterInterval interval
}

// NewBlockIntervalFilter constructs a BlockIntervalFilter with the given name
// and [lower, upper) bounds.
func NewBlockIntervalFilter(
	name string, lower uint64, upper uint64) *BlockIntervalFilter {
	return &BlockIntervalFilter{
		name: name,
		filterInterval: interval{lower: lower, upper: upper},
	}
}

// Name implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) Name() string {
	return b.name
}

// Intersects implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) Intersects(prop []byte) (bool, error) {
	var i interval
	if err := i.decode(prop); err != nil {
		return false, err
	}
	return i.intersects(b.filterInterval), nil
}

// When encoding block properties for each block, we cannot afford to encode
// the name. Instead, the name is mapped to a shortID, in the scope of that
// sstable, and the shortID is encoded. Since we use a uint8, there is a limit
// of 256 block property collectors per sstable.
type shortID uint8

type blockPropertiesEncoder struct {
	propsBuf []byte
	scratch []byte
}

func (e *blockPropertiesEncoder) getScratchForProp() []byte {
	return e.scratch[:0]
}

func (e *blockPropertiesEncoder) resetProps() {
	e.propsBuf = e.propsBuf[:0]
}

func (e *blockPropertiesEncoder) addProp(id shortID, scratch []byte) {
	const lenID = 1
	lenProp := uvarintLen(uint32(len(scratch)))
	n :=  lenID + lenProp + len(scratch)
	if cap(e.propsBuf) - len(e.propsBuf) < n {
		size := len(e.propsBuf) + 2*n
		if size < 2*cap(e.propsBuf) {
			size = 2*cap(e.propsBuf)
		}
		buf := make([]byte, len(e.propsBuf), size)
		copy(buf, e.propsBuf)
		e.propsBuf = buf
	}
	pos := len(e.propsBuf)
	b := e.propsBuf[pos:pos+lenID]
	b[0] = byte(id)
	pos += lenID
	b = e.propsBuf[pos:pos+lenProp]
	n = binary.PutUvarint(b, uint64(len(scratch)))
	pos += n
	b = e.propsBuf[pos:pos+len(scratch)]
	pos += len(scratch)
	copy(b, scratch)
	e.propsBuf = e.propsBuf[0:pos]
	e.scratch = scratch
}

func (e *blockPropertiesEncoder) unsafeProps() []byte {
	return e.propsBuf
}

func (e *blockPropertiesEncoder) props() []byte {
	buf := make([]byte, len(e.propsBuf))
	copy(buf, e.propsBuf)
	return buf
}

type blockPropertiesDecoder struct {
	props []byte
}

func (d *blockPropertiesDecoder) done() bool {
	return len(d.props) == 0
}

// REQUIRES: !done()
func (d *blockPropertiesDecoder) next() (id shortID, prop []byte, err error) {
	const lenID = 1
	id = shortID(d.props[0])
	propLen, m := binary.Uvarint(d.props[lenID:])
	n := lenID + m
	if m <= 0 || propLen == 0 || (n + int(propLen)) > len(d.props) {
		return 0, nil, base.CorruptionErrorf("corrupt block property length")
	}
	prop = d.props[n:n+int(propLen)]
	d.props = d.props[n+int(propLen):]
	return id, prop, nil
}

// BlockPropertiesFilterer provides filtering support when reading an sstable
// in the context of an iterator that has a slice of BlockPropertyFilters.
// After the call to NewBlockPropertiesFilterer, the caller must call
// IntersectsUserPropsAndFinishInit to check if the sstable intersects with
// the filters. If it does intersect, this function also finishes initializing
// the BlockPropertiesFilterer using the shortIDs for the relevant filters.
// Subsequent checks for relevance of a block should use the intersects
// method.
type BlockPropertiesFilterer struct {
	filters []BlockPropertyFilter
	// Maps shortID => index in filters. This can be sparse, and shortIDs for
	// which there is no filter are represented with an index of -1. The
	// length of this can be shorter than the shortIDs allocated in the
	// sstable. e.g. if the sstable used shortIDs 0, 1, 2, 3, and the iterator
	// has two filters, corresponding to shortIDs 2, 0, this would be:
	// len(shortIDToFiltersIndex)==3, 0=>1, 1=>-1, 2=>0.
	shortIDToFiltersIndex []int
}

var blockPropertiesFiltererPool = sync.Pool{
	New: func() interface{} {
		return &BlockPropertiesFilterer{}
	},
}

// NewBlockPropertiesFilterer returns a partially initialized filterer. To complete
// initialization, call IntersectsUserPropsAndFinishInit.
func NewBlockPropertiesFilterer(filters []BlockPropertyFilter) *BlockPropertiesFilterer {
	filterer := blockPropertiesFiltererPool.Get().(*BlockPropertiesFilterer)
	*filterer = BlockPropertiesFilterer{filters: filters}
	return filterer
}

func releaseBlockPropertiesFilterer(filterer *BlockPropertiesFilterer) {
	*filterer = BlockPropertiesFilterer{
		shortIDToFiltersIndex: filterer.shortIDToFiltersIndex[:0],
	}
	blockPropertiesFiltererPool.Put(filterer)
}

// IntersectsUserPropsAndFinishInit is called with the user properties map for
// the sstable and returns whether the sstable intersects the filters. It
// additionally initializes the shortIDToFiltersIndex for the filters that are
// relevant to this sstable.
func (f *BlockPropertiesFilterer) IntersectsUserPropsAndFinishInit(
	userProperties map[string]string) (bool, error) {
	for i := range f.filters {
		props, ok := userProperties[f.filters[i].Name()]
		if !ok {
			// Collector was not used when writing this file, so it is
			// considered intersecting.
			continue
		}
		byteProps := []byte(props)
		if len(byteProps) < 1 {
			return false, base.CorruptionErrorf(
				"block properties for %s is corrupted", f.filters[i].Name())
		}
		shortID := shortID(byteProps[0])
		intersects, err := f.filters[i].Intersects(byteProps[1:])
		if err != nil || !intersects {
			return false, err
		}
		// Intersects the sstable, so need to use this filter when
		// deciding whether to read blocks.
		n := len(f.shortIDToFiltersIndex)
		if n <= int(shortID) {
			if cap(f.shortIDToFiltersIndex) <= int(shortID) {
				index := make([]int, shortID+1, 2*(shortID+1))
				copy(index, f.shortIDToFiltersIndex)
				f.shortIDToFiltersIndex = index
			} else {
				f.shortIDToFiltersIndex = f.shortIDToFiltersIndex[:shortID+1]
			}
			for j := n; j < int(shortID); j++ {
				f.shortIDToFiltersIndex[j] = -1
			}
		}
		f.shortIDToFiltersIndex[shortID] = i
	}
	return true, nil
}

func (f *BlockPropertiesFilterer) intersects(props []byte) (bool, error) {
	i := 0
	decoder := blockPropertiesDecoder{props: props}
	for i < len(f.shortIDToFiltersIndex) {
		var id int
		var prop []byte
		if !decoder.done() {
			var shortID shortID
			var err error
			shortID, prop, err = decoder.next()
			if err != nil {
				return false, err
			}
			id = int(shortID)
		} else {
			id = math.MaxUint8+1
		}
		for i < len(f.shortIDToFiltersIndex) && id > i {
			if f.shortIDToFiltersIndex[i] >= 0 {
				// There is a filter for this id, but the property for this id
				// is not encoded for this block.
				intersects, err := f.filters[f.shortIDToFiltersIndex[i]].Intersects(nil)
				if err != nil {
					return false, err
				}
				if !intersects {
					return false, nil
				}
			}
			i++
		}
		if i >= len(f.shortIDToFiltersIndex) {
			return true, nil
		}
		// INVARIANT: id <= i. And since i is always incremented by 1, id==i.
		if id != i {
			panic(fmt.Sprintf("%d != %d", id, i))
		}
		if f.shortIDToFiltersIndex[i] >= 0 {
			intersects, err := f.filters[f.shortIDToFiltersIndex[i]].Intersects(prop)
			if err != nil {
				return false, err
			}
			if !intersects {
				return false, nil
			}
		}
		i++
	}
	return true, nil
}

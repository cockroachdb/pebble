// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/cockroachdb/errors"
	"math"
)

// Block properties are an optional user-facing feature that can be used to
// filter data blocks (and whole sstables) from an Iterator before they are
// loaded. They do not apply to range delete blocks. These are expected to
// very concisely represent a set of some attribute value contained within the
// key or value, such that the set includes all the attribute values in the
// block. This has some similarities with OLAP pruning approaches that
// maintain min-max attribute values for some column (which concisely
// represent a set), that is then used to prune at query time. In our case,
// data blocks are small, typically 50-100KB, so these properties should
// reduce their precision in order to be concise -- a good rule of thumb is to
// not consume more than 50-100 bytes across all properties maintained for a
// block, i.e., a 1000x reduction compared to loading the data block.
//
// A block property is assigned two unique identifiers:
// - A name, that is encoded and stored once in the sstable. This name must
//   be unique among all user-properties encoded in an sstable.
// - A small integer index, that is encoded and stored once per data block and
//   lower level index block.
// The maintenance of both identifiers is the responsibility of the DB user.
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
//   index block. It is included when AddLastDataBlockToIndexBlock. An
//   alternative would be to return an opaque handle from FinishDataBlock and
//   pass it to a new AddToIndexBlock method, which requires more plumbing,
//   and passing of an interface{} results in a undesirable heap allocation.
//   AddLastDataBlockToIndexBlock must be called before keys are added to the
//   new data block.
type BlockPropertyCollector interface {
	// Name returns the name of the block property collector.
	Name() string
	// ShortID returns the integer identifier.
	ShortID() uint16
	// Add is called with each new entry added to a data block in the sstable.
	// The callee can assume that these are in sorted order.
	Add(key InternalKey, value []byte) error
	// FinishDataBlock is called when all the entries have been added to a
	// data block. Subsequent Add calls will be for the next data block. It
	// returns the property value for the finished block.
	FinishDataBlock(buf []byte) ([]byte, error)
	// AddLastDataBlockToIndexBlock adds the entry corresponding to the last
	// FinishDataBlock to the current index block.
	AddLastDataBlockToIndexBlock()
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
	// ShortID returns the integer identifier.
	ShortID() uint16
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
	name                    string
	shortID                 uint16
	blockAttributeCollector DataBlockIntervalCollector

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
// given name, shortID, and data block collector.
func NewBlockIntervalCollector(
	name string, shortID uint16, blockAttributeCollector DataBlockIntervalCollector) *BlockIntervalCollector {
	return &BlockIntervalCollector{
		name: name, shortID: shortID, blockAttributeCollector: blockAttributeCollector}
}

// Name implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) Name() string {
	return b.name
}

// ShortID implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) ShortID() uint16 {
	return b.shortID
}

// Add implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) Add(key InternalKey, value []byte) error {
	return b.blockAttributeCollector.Add(key, value)
}

// FinishDataBlock implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	var err error
	b.blockInterval.lower, b.blockInterval.upper, err = b.blockAttributeCollector.FinishDataBlock()
	if err != nil {
		return buf, err
	}
	buf = b.blockInterval.encode(buf)
	b.tableInterval.union(b.blockInterval)
	return buf, nil
}

// AddLastDataBlockToIndexBlock implements the BlockPropertyCollector
// interface.
func (b *BlockIntervalCollector) AddLastDataBlockToIndexBlock() {
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
		return errors.Errorf("cannot decode interval from buf %x", buf)
	}
	pos := n
	i.upper, n = binary.Uvarint(buf[pos:])
	pos += n
	if pos != len(buf) || n <= 0 {
		return errors.Errorf("cannot decode interval from buf %x", buf)
	}
	// Delta decode.
	i.upper += i.lower
	if i.upper < i.lower {
		return errors.Errorf("unexpected overflow, upper %d < lower %d", i.upper, i.lower)
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
	shortID uint16
	filterInterval interval
}

// NewBlockIntervalFilter constructs a BlockIntervalFilter with the given
// name, shortID, and [lower, upper) bounds.
func NewBlockIntervalFilter(
	name string, shortID uint16, lower uint64, upper uint64) *BlockIntervalFilter {
	return &BlockIntervalFilter{
		name: name,
		shortID: shortID,
		filterInterval: interval{lower: lower, upper: upper},
	}
}

// Name implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) Name() string {
	return b.name
}

// ShortID implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) ShortID() uint16 {
	return b.shortID
}

// Intersects implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) Intersects(prop []byte) (bool, error) {
	var i interval
	if err := i.decode(prop); err != nil {
		return false, err
	}
	return i.intersects(b.filterInterval), nil
}

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

func (e *blockPropertiesEncoder) addProp(id uint16, scratch []byte) {
	lenID := uvarintLen(uint32(id))
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
	n = binary.PutUvarint(b, uint64(id))
	if n != lenID {
		panic(fmt.Sprintf("unexpected length %d is not equal to %d", n, lenID))
	}
	pos += n
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
func (d *blockPropertiesDecoder) next() (shortID uint16, prop []byte, err error) {
	// Decode.
	id, n := binary.Uvarint(d.props)
	if n <= 0 || id > math.MaxUint16 {
		return 0, nil, errors.Errorf("corrupt block property filter id")
	}
	shortID = uint16(id)
	propLen, m := binary.Uvarint(d.props[n:])
	if m <= 0 || propLen == 0 || (n + m + int(propLen)) > len(d.props) {
		return 0, nil, errors.Errorf("corrupt block property length")
	}
	n += m
	prop = d.props[n:n+int(propLen)]
	d.props = d.props[n+int(propLen):]
	return shortID, prop, nil
}

// Must be sorted in increasing order of shortID().
type blockPropertiesFilterer []BlockPropertyFilter

func (f blockPropertiesFilterer) intersects(props []byte) (bool, error) {
	i := 0
	decoder := blockPropertiesDecoder{props: props}
	for i < len(f) {
		var id int
		var prop []byte
		if !decoder.done() {
			var shortID uint16
			var err error
			shortID, prop, err = decoder.next()
			if err != nil {
				return false, err
			}
			id = int(shortID)
		} else {
			id = math.MaxUint16+1
		}
		for i < len(f) && id > int(f[i].ShortID()) {
			// Not encoded for this block.
			intersects, err := f[i].Intersects(nil)
			if err != nil {
				return false, err
			}
			if !intersects {
				return false, nil
			}
			i++
		}
		if i >= len(f) {
			return true, nil
		}
		if id == int(f[i].ShortID()) {
			intersects, err := f[i].Intersects(prop)
			if err != nil {
				return false, err
			}
			if !intersects {
				return false, nil
			}
			i++
		}
	}
	return true, nil
}

// Following will move to the CockroachDB code. It is here only for
// illustration.

type crdbDataBlockTimestampCollector struct {
	// Keep the encoded timestamps in min, max and decode in FinishDataBlock.
	min, max []byte
}

var _ DataBlockIntervalCollector = &crdbDataBlockTimestampCollector{}

const engineKeyVersionWallTimeLen = 8
const engineKeyVersionWallAndLogicalTimeLen = 12
const engineKeyVersionWallLogicalAndSyntheticTimeLen = 13

func (tc *crdbDataBlockTimestampCollector) Add(key InternalKey, value []byte) error {
	k := key.UserKey
	if len(k) == 0 {
		return nil
	}
	// Last byte is the version length + 1 when there is a version,
	// else it is 0.
	versionLen := int(k[len(k)-1])
	// keyPartEnd points to the sentinel byte.
	keyPartEnd := len(k) - 1 - versionLen
	if keyPartEnd < 0 {
		return errors.Errorf("invalid key")
	}
	if versionLen > 0 && (versionLen == engineKeyVersionWallTimeLen ||
		versionLen == engineKeyVersionWallAndLogicalTimeLen ||
		versionLen == engineKeyVersionWallLogicalAndSyntheticTimeLen) {
		// Version consists of the bytes after the sentinel and before the length.
		k = k[keyPartEnd+1 : len(k)-1]
		if len(tc.min) == 0 || bytes.Compare(k, tc.min) < 0 {
			tc.min = append(tc.min[:0], k...)
		}
		if len(tc.max) == 0 || bytes.Compare(k, tc.max) > 0 {
			tc.max = append(tc.max[:0], k...)
		}
	}
	return nil
}

func decodeWallTime(ts []byte) uint64 {
	return binary.BigEndian.Uint64(ts[0:8])
}

func (tc *crdbDataBlockTimestampCollector) FinishDataBlock(
	) (lower uint64, upper uint64, err error) {
	if len(tc.min) == 0 {
		// No calls to Add that contained a timestamped key.
		return 0, 0, nil
	}
	lower = decodeWallTime(tc.min)
	tc.min = tc.min[:0]
	// The actual value encoded into walltime is an int64, so +1 will not
	// overflow.
	upper = decodeWallTime(tc.max) + 1
	tc.max = tc.max[:0]
	if lower >= upper {
		return 0, 0,
		errors.Errorf("corrupt timestamps lower %d >= upper %d", lower, upper)
	}
	return lower, upper, nil
}


// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
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
//
// Block property filtering is nondeterministic because the separation of keys
// into blocks is nondeterministic. Clients use block-property filters to
// implement efficient application of a filter F that applies to key-value pairs
// (abbreviated as kv-filter). Consider correctness defined as surfacing exactly
// the same key-value pairs that would be surfaced if one applied the filter F
// above normal iteration. With this correctness definition, block property
// filtering may introduce two kinds of errors:
//
//   a) Block property filtering that uses a kv-filter may produce additional
//      key-value pairs that don't satisfy the filter because of the separation
//      of keys into blocks. Clients may remove these extra key-value pairs by
//      re-applying the kv filter while reading results back from Pebble.
//
//   b) Block property filtering may surface deleted key-value pairs if the
//      kv filter is not a strict function of the key's user key. A block
//      containing k.DEL may be filtered, while a block containing the deleted
//      key k.SET may not be filtered, if the kv filter applies to one but not
//      the other.
//
//      This error may be avoided trivially by using a kv filter that is a pure
//      function of the user key. A filter that examines values or key kinds
//      requires care to ensure F(k.SET, <value>) = F(k.DEL) = F(k.SINGLEDEL).
//
// The combination of range deletions and filtering by table-level properties
// add another opportunity for deleted point keys to be surfaced. The pebble
// Iterator stack takes care to correctly apply filtered tables' range deletions
// to lower tables, preventing this form of nondeterministic error.
//
// In addition to the non-determinism discussed in (b), which limits the use
// of properties over values, we now have support for values that are not
// stored together with the key, and may not even be retrieved during
// compactions. If Pebble is configured with such value separation, block
// properties must only apply to the key, and will be provided a nil value.

// BlockPropertyCollector is used when writing a sstable.
//
//   - All calls to Add are included in the next FinishDataBlock, after which
//     the next data block is expected to start.
//
//   - The index entry generated for the data block, which contains the return
//     value from FinishDataBlock, is not immediately included in the current
//     index block. It is included when AddPrevDataBlockToIndexBlock is called.
//     An alternative would be to return an opaque handle from FinishDataBlock
//     and pass it to a new AddToIndexBlock method, which requires more
//     plumbing, and passing of an interface{} results in a undesirable heap
//     allocation. AddPrevDataBlockToIndexBlock must be called before keys are
//     added to the new data block.
type BlockPropertyCollector interface {
	// Name returns the name of the block property collector.
	Name() string

	// AddPointKey is called with each new key added to a data block in the
	// sstable. The callee can assume that these are in sorted order.
	AddPointKey(key InternalKey, value []byte) error

	// AddRangeKeys is called for each range span added to the sstable. The range
	// key properties are stored separately and don't contribute to data block
	// properties. They are only used when FinishTable is called.
	// TODO(radu): clean up this subtle semantic.
	AddRangeKeys(span Span) error

	// AddCollectedWithSuffixReplacement adds previously collected property data
	// and updates it to reflect a change of suffix on all keys: the old property
	// data is assumed to be constructed from keys that all have the same
	// oldSuffix and is recalculated to reflect the same keys but with newSuffix.
	//
	// A collector which supports this method must be able to derive its updated
	// value from its old value and the change being made to the suffix, without
	// needing to be passed each updated K/V.
	//
	// For example, a collector that only inspects values can simply copy its
	// previously computed property as-is, since key-suffix replacement does not
	// change values, while a collector that depends only on key suffixes, like
	// one which collected mvcc-timestamp bounds from timestamp-suffixed keys, can
	// just set its new bounds from the new suffix, as it is common to all keys,
	// without needing to recompute it from every key.
	//
	// This method is optional (if it is not implemented, it always returns an
	// error). SupportsSuffixReplacement() can be used to check if this method is
	// implemented.
	AddCollectedWithSuffixReplacement(oldProp []byte, oldSuffix, newSuffix []byte) error

	// SupportsSuffixReplacement returns whether the collector supports the
	// AddCollectedWithSuffixReplacement method.
	SupportsSuffixReplacement() bool

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
type BlockPropertyFilter = base.BlockPropertyFilter

// BoundLimitedBlockPropertyFilter implements the block-property filter but
// imposes an additional constraint on its usage, requiring that only blocks
// containing exclusively keys between its lower and upper bounds may be
// filtered. The bounds may be change during iteration, so the filter doesn't
// expose the bounds, instead implementing KeyIsWithin[Lower,Upper]Bound methods
// for performing bound comparisons.
//
// To be used, a BoundLimitedBlockPropertyFilter must be supplied directly
// through NewBlockPropertiesFilterer's dedicated parameter. If supplied through
// the ordinary slice of block property filters, this filter's bounds will be
// ignored.
//
// The current [lower,upper) bounds of the filter are unknown, because they may
// be changing. During forward iteration the lower bound is externally
// guaranteed, meaning Intersects only returns false if the sstable iterator is
// already known to be positioned at a key ≥ lower. The sstable iterator is then
// only responsible for ensuring filtered blocks also meet the upper bound, and
// should only allow a block to be filtered if all its keys are < upper. The
// sstable iterator may invoke KeyIsWithinUpperBound(key) to perform this check,
// where key is an inclusive upper bound on the block's keys.
//
// During backward iteration the upper bound is externally guaranteed, and
// Intersects only returns false if the sstable iterator is already known to be
// positioned at a key < upper. The sstable iterator is responsible for ensuring
// filtered blocks also meet the lower bound, enforcing that a block is only
// filtered if all its keys are ≥ lower. This check is made through passing the
// block's inclusive lower bound to KeyIsWithinLowerBound.
//
// Implementations may become active or inactive through implementing Intersects
// to return true whenever the filter is disabled.
//
// Usage of BoundLimitedBlockPropertyFilter is subtle, and Pebble consumers
// should not implement this interface directly. This interface is an internal
// detail in the implementation of block-property range-key masking.
type BoundLimitedBlockPropertyFilter interface {
	BlockPropertyFilter

	// KeyIsWithinLowerBound tests whether the provided internal key falls
	// within the current lower bound of the filter. A true return value
	// indicates that the filter may be used to filter blocks that exclusively
	// contain keys ≥ `key`, so long as the blocks' keys also satisfy the upper
	// bound.
	KeyIsWithinLowerBound(key []byte) bool
	// KeyIsWithinUpperBound tests whether the provided internal key falls
	// within the current upper bound of the filter. A true return value
	// indicates that the filter may be used to filter blocks that exclusively
	// contain keys ≤ `key`, so long as the blocks' keys also satisfy the lower
	// bound.
	KeyIsWithinUpperBound(key []byte) bool
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
//
// A BlockIntervalCollector that collects over point and range keys needs to
// have both the point and range DataBlockIntervalCollector specified, since
// point and range keys are fed to the BlockIntervalCollector in an interleaved
// fashion, independently of one another. This also implies that the
// DataBlockIntervalCollectors for point and range keys should be references to
// independent instances, rather than references to the same collector, as point
// and range keys are tracked independently.
type BlockIntervalCollector struct {
	name           string
	mapper         IntervalMapper
	suffixReplacer BlockIntervalSuffixReplacer

	blockInterval BlockInterval
	indexInterval BlockInterval
	tableInterval BlockInterval
}

var _ BlockPropertyCollector = &BlockIntervalCollector{}

// IntervalMapper is an interface through which a user can define the mapping
// between keys and intervals. The interval for any collection of keys (e.g. a
// data block, a table) is the union of intervals for all keys.
type IntervalMapper interface {
	// MapPointKey maps a point key to an interval. The interval can be empty, which
	// means that this key will effectively be ignored.
	MapPointKey(key InternalKey, value []byte) (BlockInterval, error)

	// MapRangeKeys maps a range key span to an interval. The interval can be
	// empty, which means that this span will effectively be ignored.
	MapRangeKeys(span Span) (BlockInterval, error)
}

// NewBlockIntervalCollector constructs a BlockIntervalCollector with the given
// name. The BlockIntervalCollector makes use of the given point and range key
// DataBlockIntervalCollectors when encountering point and range keys,
// respectively.
//
// The caller may pass a nil DataBlockIntervalCollector for one of the point or
// range key collectors, in which case keys of those types will be ignored. This
// allows for flexible construction of BlockIntervalCollectors that operate on
// just point keys, just range keys, or both point and range keys.
//
// If both point and range keys are to be tracked, two independent collectors
// should be provided, rather than the same collector passed in twice (see the
// comment on BlockIntervalCollector for more detail)
// XXX update
func NewBlockIntervalCollector(
	name string, mapper IntervalMapper, suffixReplacer BlockIntervalSuffixReplacer,
) BlockPropertyCollector {
	if mapper == nil {
		panic("mapper must be provided")
	}
	return &BlockIntervalCollector{
		name:           name,
		mapper:         mapper,
		suffixReplacer: suffixReplacer,
	}
}

// Name is part of the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) Name() string {
	return b.name
}

// AddPointKey is part of the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) AddPointKey(key InternalKey, value []byte) error {
	interval, err := b.mapper.MapPointKey(key, value)
	if err != nil {
		return err
	}
	b.blockInterval.UnionWith(interval)
	return nil
}

// AddRangeKeys is part of the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) AddRangeKeys(span Span) error {
	if span.Empty() {
		return nil
	}
	interval, err := b.mapper.MapRangeKeys(span)
	if err != nil {
		return err
	}
	// Range keys are not included in block or index intervals; they just apply
	// directly to the table interval.
	b.tableInterval.UnionWith(interval)
	return nil
}

// AddCollectedWithSuffixReplacement is part of the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) AddCollectedWithSuffixReplacement(
	oldProp []byte, oldSuffix, newSuffix []byte,
) error {
	i, err := decodeBlockInterval(oldProp)
	if err != nil {
		return err
	}
	i, err = b.suffixReplacer.ApplySuffixReplacement(i, newSuffix)
	if err != nil {
		return err
	}
	b.blockInterval.UnionWith(i)
	return nil
}

// SupportsSuffixReplacement is part of the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) SupportsSuffixReplacement() bool {
	return b.suffixReplacer != nil
}

// FinishDataBlock is part of the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	buf = encodeBlockInterval(b.blockInterval, buf)
	b.tableInterval.UnionWith(b.blockInterval)
	return buf, nil
}

// AddPrevDataBlockToIndexBlock implements the BlockPropertyCollector
// interface.
func (b *BlockIntervalCollector) AddPrevDataBlockToIndexBlock() {
	b.indexInterval.UnionWith(b.blockInterval)
	b.blockInterval = BlockInterval{}
}

// FinishIndexBlock implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	buf = encodeBlockInterval(b.indexInterval, buf)
	b.indexInterval = BlockInterval{}
	return buf, nil
}

// FinishTable implements the BlockPropertyCollector interface.
func (b *BlockIntervalCollector) FinishTable(buf []byte) ([]byte, error) {
	return encodeBlockInterval(b.tableInterval, buf), nil
}

// BlockInterval represents the [Lower, Upper) interval of 64-bit values
// corresponding to a set of keys. The meaning of the values themselves is
// opaque to the BlockIntervalCollector.
//
// If Lower >= Upper, the interval is the empty set.
type BlockInterval struct {
	Lower uint64
	Upper uint64
}

// IsEmpty returns true if the interval is empty.
func (i BlockInterval) IsEmpty() bool {
	return i.Lower >= i.Upper
}

// Intersects returns true if the two intervals intersect.
func (i BlockInterval) Intersects(other BlockInterval) bool {
	return !i.IsEmpty() && !other.IsEmpty() && i.Upper > other.Lower && i.Lower < other.Upper
}

// UnionWith extends the receiver to include another interval.
func (i *BlockInterval) UnionWith(other BlockInterval) {
	switch {
	case other.IsEmpty():
	case i.IsEmpty():
		*i = other
	default:
		i.Lower = min(i.Lower, other.Lower)
		i.Upper = max(i.Upper, other.Upper)
	}
}

func encodeBlockInterval(i BlockInterval, buf []byte) []byte {
	if i.IsEmpty() {
		return buf
	}

	var encoded [binary.MaxVarintLen64 * 2]byte
	n := binary.PutUvarint(encoded[:], i.Lower)
	n += binary.PutUvarint(encoded[n:], i.Upper-i.Lower)
	return append(buf, encoded[:n]...)
}

func decodeBlockInterval(buf []byte) (BlockInterval, error) {
	if len(buf) == 0 {
		return BlockInterval{}, nil
	}
	var i BlockInterval
	var n int
	i.Lower, n = binary.Uvarint(buf)
	if n <= 0 || n >= len(buf) {
		return BlockInterval{}, base.CorruptionErrorf("cannot decode interval from buf %x", buf)
	}
	pos := n
	i.Upper, n = binary.Uvarint(buf[pos:])
	pos += n
	if pos != len(buf) || n <= 0 {
		return BlockInterval{}, base.CorruptionErrorf("cannot decode interval from buf %x", buf)
	}
	// Delta decode.
	i.Upper += i.Lower
	if i.Upper < i.Lower {
		return BlockInterval{}, base.CorruptionErrorf("unexpected overflow, upper %d < lower %d", i.Upper, i.Lower)
	}
	return i, nil
}

// BlockIntervalSuffixReplacer provides methods to conduct just in time
// adjustments of a passed in block prop interval before filtering.
type BlockIntervalSuffixReplacer interface {
	// ApplySuffixReplacement recalculates a previously calculated interval (which
	// corresponds to an arbitrary collection of keys) under the assumption
	// that those keys are rewritten with a new prefix.
	//
	// Such a transformation is possible when the intervals depend only on the
	// suffixes.
	ApplySuffixReplacement(interval BlockInterval, newSuffix []byte) (BlockInterval, error)
}

// BlockIntervalFilter is an implementation of BlockPropertyFilter when the
// corresponding collector is a BlockIntervalCollector. That is, the set is of
// the form [lower, upper).
type BlockIntervalFilter struct {
	name           string
	filterInterval BlockInterval
	suffixReplacer BlockIntervalSuffixReplacer
}

var _ BlockPropertyFilter = (*BlockIntervalFilter)(nil)

// NewBlockIntervalFilter constructs a BlockPropertyFilter that filters blocks
// based on an interval property collected by BlockIntervalCollector and the
// given [lower, upper) bounds. The given name specifies the
// BlockIntervalCollector's properties to read.
func NewBlockIntervalFilter(
	name string, lower uint64, upper uint64, suffixReplacer BlockIntervalSuffixReplacer,
) *BlockIntervalFilter {
	b := new(BlockIntervalFilter)
	b.Init(name, lower, upper, suffixReplacer)
	return b
}

// Init initializes (or re-initializes, clearing previous state) an existing
// BLockPropertyFilter to filter blocks based on an interval property collected
// by BlockIntervalCollector and the given [lower, upper) bounds. The given name
// specifies the BlockIntervalCollector's properties to read.
func (b *BlockIntervalFilter) Init(
	name string, lower, upper uint64, suffixReplacer BlockIntervalSuffixReplacer,
) {
	*b = BlockIntervalFilter{
		name:           name,
		filterInterval: BlockInterval{Lower: lower, Upper: upper},
		suffixReplacer: suffixReplacer,
	}
}

// Name implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) Name() string {
	return b.name
}

// Intersects implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) Intersects(prop []byte) (bool, error) {
	i, err := decodeBlockInterval(prop)
	if err != nil {
		return false, err
	}
	return i.Intersects(b.filterInterval), nil
}

// SyntheticSuffixIntersects implements the BlockPropertyFilter interface.
func (b *BlockIntervalFilter) SyntheticSuffixIntersects(prop []byte, suffix []byte) (bool, error) {
	if b.suffixReplacer == nil {
		return false, base.AssertionFailedf("missing SuffixReplacer for SyntheticSuffixIntersects()")
	}
	i, err := decodeBlockInterval(prop)
	if err != nil {
		return false, err
	}

	newInterval, err := b.suffixReplacer.ApplySuffixReplacement(i, suffix)
	if err != nil {
		return false, err
	}
	return newInterval.Intersects(b.filterInterval), nil
}

// SetInterval adjusts the [lower, upper) bounds used by the filter. It is not
// generally safe to alter the filter while it's in use, except as part of the
// implementation of BlockPropertyFilterMask.SetSuffix used for range-key
// masking.
func (b *BlockIntervalFilter) SetInterval(lower, upper uint64) {
	b.filterInterval = BlockInterval{Lower: lower, Upper: upper}
}

// When encoding block properties for each block, we cannot afford to encode the
// name. Instead, the name is mapped to a shortID, in the scope of that sstable,
// and the shortID is encoded as a single byte (which imposes a limit of of 256
// block property collectors per sstable).
// Note that the in-memory type is int16 to avoid overflows (e.g. in loops) and
// to allow special values like -1 in code.
type shortID int16

const invalidShortID shortID = -1
const maxShortID shortID = math.MaxUint8
const maxPropertyCollectors = int(maxShortID) + 1

func (id shortID) IsValid() bool {
	return id >= 0 && id <= maxShortID
}

func (id shortID) ToByte() byte {
	if invariants.Enabled && !id.IsValid() {
		panic(fmt.Sprintf("inavlid id %d", id))
	}
	return byte(id)
}

type blockPropertiesEncoder struct {
	propsBuf []byte
	scratch  []byte
}

func (e *blockPropertiesEncoder) getScratchForProp() []byte {
	return e.scratch[:0]
}

func (e *blockPropertiesEncoder) resetProps() {
	e.propsBuf = e.propsBuf[:0]
}

func (e *blockPropertiesEncoder) addProp(id shortID, scratch []byte) {
	if len(scratch) == 0 {
		// We omit empty properties. The decoder will know that any missing IDs had
		// empty values.
		return
	}
	const lenID = 1
	lenProp := uvarintLen(uint32(len(scratch)))
	n := lenID + lenProp + len(scratch)
	if cap(e.propsBuf)-len(e.propsBuf) < n {
		size := len(e.propsBuf) + 2*n
		if size < 2*cap(e.propsBuf) {
			size = 2 * cap(e.propsBuf)
		}
		buf := make([]byte, len(e.propsBuf), size)
		copy(buf, e.propsBuf)
		e.propsBuf = buf
	}
	pos := len(e.propsBuf)
	b := e.propsBuf[pos : pos+lenID]
	b[0] = id.ToByte()
	pos += lenID
	b = e.propsBuf[pos : pos+lenProp]
	n = binary.PutUvarint(b, uint64(len(scratch)))
	pos += n
	b = e.propsBuf[pos : pos+len(scratch)]
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

	// numCollectedProps is the number of collectors that were used when writing
	// these properties. The encoded properties contain values for shortIDs 0
	// through numCollectedProps-1, in order (with empty properties omitted).
	numCollectedProps int
	nextID            shortID
}

func makeBlockPropertiesDecoder(numCollectedProps int, propsBuf []byte) blockPropertiesDecoder {
	return blockPropertiesDecoder{
		props:             propsBuf,
		numCollectedProps: numCollectedProps,
	}
}

func (d *blockPropertiesDecoder) Done() bool {
	return int(d.nextID) >= d.numCollectedProps
}

// Next returns the property for each shortID between 0 and numCollectedProps-1, in order.
// Note that some properties might be empty.
// REQUIRES: !Done()
func (d *blockPropertiesDecoder) Next() (id shortID, prop []byte, err error) {
	id = d.nextID
	d.nextID++

	if len(d.props) == 0 || shortID(d.props[0]) != id {
		if invariants.Enabled && len(d.props) > 0 && shortID(d.props[0]) < id {
			panic("shortIDs are not in order")
		}
		// This property was omitted because it was empty.
		return id, nil, nil
	}

	const lenID = 1
	propLen, m := binary.Uvarint(d.props[lenID:])
	n := lenID + m
	if m <= 0 || propLen == 0 || (n+int(propLen)) > len(d.props) {
		return 0, nil, base.CorruptionErrorf("corrupt block property length")
	}
	prop = d.props[n : n+int(propLen)]
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

	// boundLimitedFilter, if non-nil, holds a single block-property filter with
	// additional constraints on its filtering. A boundLimitedFilter may only
	// filter blocks that are wholly contained within its bounds. During forward
	// iteration the lower bound (and during backward iteration the upper bound)
	// must be externally guaranteed, with Intersects only returning false if
	// that bound is met. The opposite bound is verified during iteration by the
	// sstable iterator.
	//
	// boundLimitedFilter is permitted to be defined on a property (`Name()`)
	// for which another filter exists in filters. In this case both filters
	// will be consulted, and either filter may exclude block(s). Only a single
	// bound-limited block-property filter may be set.
	//
	// The boundLimitedShortID field contains the shortID of the filter's
	// property within the sstable. It's set to -1 if the property was not
	// collected when the table was built.
	boundLimitedFilter  BoundLimitedBlockPropertyFilter
	boundLimitedShortID int

	syntheticSuffix SyntheticSuffix
}

var blockPropertiesFiltererPool = sync.Pool{
	New: func() interface{} {
		return &BlockPropertiesFilterer{}
	},
}

// newBlockPropertiesFilterer returns a partially initialized filterer. To complete
// initialization, call IntersectsUserPropsAndFinishInit.
func newBlockPropertiesFilterer(
	filters []BlockPropertyFilter,
	limited BoundLimitedBlockPropertyFilter,
	syntheticSuffix SyntheticSuffix,
) *BlockPropertiesFilterer {
	filterer := blockPropertiesFiltererPool.Get().(*BlockPropertiesFilterer)
	*filterer = BlockPropertiesFilterer{
		filters:               filters,
		shortIDToFiltersIndex: filterer.shortIDToFiltersIndex[:0],
		boundLimitedFilter:    limited,
		boundLimitedShortID:   -1,
		syntheticSuffix:       syntheticSuffix,
	}
	return filterer
}

func releaseBlockPropertiesFilterer(filterer *BlockPropertiesFilterer) {
	*filterer = BlockPropertiesFilterer{
		shortIDToFiltersIndex: filterer.shortIDToFiltersIndex[:0],
	}
	blockPropertiesFiltererPool.Put(filterer)
}

// IntersectsTable evaluates the provided block-property filter against the
// provided set of table-level properties. If there is no intersection between
// the filters and the table or an error is encountered, IntersectsTable returns
// a nil filterer (and possibly an error). If there is an intersection,
// IntersectsTable returns a non-nil filterer that may be used by an iterator
// reading the table.
func IntersectsTable(
	filters []BlockPropertyFilter,
	limited BoundLimitedBlockPropertyFilter,
	userProperties map[string]string,
	syntheticSuffix SyntheticSuffix,
) (*BlockPropertiesFilterer, error) {
	f := newBlockPropertiesFilterer(filters, limited, syntheticSuffix)
	ok, err := f.intersectsUserPropsAndFinishInit(userProperties)
	if !ok || err != nil {
		releaseBlockPropertiesFilterer(f)
		return nil, err
	}
	return f, nil
}

// intersectsUserPropsAndFinishInit is called with the user properties map for
// the sstable and returns whether the sstable intersects the filters. It
// additionally initializes the shortIDToFiltersIndex for the filters that are
// relevant to this sstable.
func (f *BlockPropertiesFilterer) intersectsUserPropsAndFinishInit(
	userProperties map[string]string,
) (bool, error) {
	for i := range f.filters {
		props, ok := userProperties[f.filters[i].Name()]
		if !ok {
			// Collector was not used when writing this file, so it is
			// considered intersecting.
			continue
		}
		if len(props) < 1 {
			return false, base.CorruptionErrorf(
				"block properties for %s is corrupted", f.filters[i].Name())
		}
		shortID := shortID(props[0])
		{
			// Use an unsafe conversion to avoid allocating. Intersects() is not
			// supposed to modify the given slice.
			// Note that unsafe.StringData only works if the string is not empty
			// (which we already checked).
			byteProps := unsafe.Slice(unsafe.StringData(props), len(props))
			var intersects bool
			var err error
			if len(f.syntheticSuffix) == 0 {
				intersects, err = f.filters[i].Intersects(byteProps[1:])
			} else {
				intersects, err = f.filters[i].SyntheticSuffixIntersects(byteProps[1:], f.syntheticSuffix)
			}
			if err != nil || !intersects {
				return false, err
			}
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
	if f.boundLimitedFilter == nil {
		return true, nil
	}

	// There's a bound-limited filter. Find its shortID. It's possible that
	// there's an existing filter in f.filters on the same property. That's
	// okay. Both filters will be consulted whenever a relevant prop is decoded.
	props, ok := userProperties[f.boundLimitedFilter.Name()]
	if !ok {
		// The collector was not used when writing this file, so it's
		// intersecting. We leave f.boundLimitedShortID=-1, so the filter will
		// be unused within this file.
		return true, nil
	}
	if len(props) < 1 {
		return false, base.CorruptionErrorf(
			"block properties for %s is corrupted", f.boundLimitedFilter.Name())
	}
	f.boundLimitedShortID = int(props[0])

	// We don't check for table-level intersection for the bound-limited filter.
	// The bound-limited filter is treated as vacuously intersecting.
	//
	// NB: If a block-property filter needs to be toggled inactive/active, it
	// should be implemented within the Intersects implementation.
	//
	// TODO(jackson): We could filter at the table-level by threading the table
	// smallest and largest bounds here.

	// The bound-limited filter isn't included in shortIDToFiltersIndex.
	//
	// When determining intersection, we decode props only up to the shortID
	// len(shortIDToFiltersIndex). If f.limitedShortID is greater than any of
	// the existing filters' shortIDs, we need to grow shortIDToFiltersIndex.
	// Growing the index with -1s ensures we're able to consult the index
	// without length checks.
	if n := len(f.shortIDToFiltersIndex); n <= f.boundLimitedShortID {
		if cap(f.shortIDToFiltersIndex) <= f.boundLimitedShortID {
			index := make([]int, f.boundLimitedShortID+1)
			copy(index, f.shortIDToFiltersIndex)
			f.shortIDToFiltersIndex = index
		} else {
			f.shortIDToFiltersIndex = f.shortIDToFiltersIndex[:f.boundLimitedShortID+1]
		}
		for j := n; j <= f.boundLimitedShortID; j++ {
			f.shortIDToFiltersIndex[j] = -1
		}
	}
	return true, nil
}

type intersectsResult int8

const (
	blockIntersects intersectsResult = iota
	blockExcluded
	// blockMaybeExcluded is returned by BlockPropertiesFilterer.intersects when
	// no filters unconditionally exclude the block, but the bound-limited block
	// property filter will exclude it if the block's bounds fall within the
	// filter's current bounds. See the reader's
	// {single,two}LevelIterator.resolveMaybeExcluded methods.
	blockMaybeExcluded
)

func (f *BlockPropertiesFilterer) intersects(props []byte) (intersectsResult, error) {
	decoder := makeBlockPropertiesDecoder(len(f.shortIDToFiltersIndex), props)
	ret := blockIntersects
	for !decoder.Done() {
		id, prop, err := decoder.Next()
		if err != nil {
			return ret, err
		}
		intersects, err := f.intersectsFilter(id, prop)
		if err != nil {
			return ret, err
		}
		if intersects == blockExcluded {
			return blockExcluded, nil
		}
		if intersects == blockMaybeExcluded {
			ret = blockMaybeExcluded
		}
	}
	// ret is either blockIntersects or blockMaybeExcluded.
	return ret, nil
}

func (f *BlockPropertiesFilterer) intersectsFilter(
	id shortID, prop []byte,
) (intersectsResult, error) {
	var intersects bool
	var err error
	if filterIdx := f.shortIDToFiltersIndex[id]; filterIdx >= 0 {
		if !f.syntheticSuffix.IsSet() {
			intersects, err = f.filters[filterIdx].Intersects(prop)
		} else {
			intersects, err = f.filters[filterIdx].SyntheticSuffixIntersects(prop, f.syntheticSuffix)
		}
		if err != nil {
			return blockIntersects, err
		}
		if !intersects {
			return blockExcluded, nil
		}
	}
	if int(id) == f.boundLimitedShortID {
		// The bound-limited filter uses this id.
		//
		// The bound-limited filter only applies within a keyspan interval. We
		// expect the Intersects call to be cheaper than bounds checks. If
		// Intersects determines that there is no intersection, we return
		// `blockMaybeExcluded` if no other bpf unconditionally excludes the
		// block.
		if !f.syntheticSuffix.IsSet() {
			intersects, err = f.boundLimitedFilter.Intersects(prop)
		} else {
			intersects, err = f.boundLimitedFilter.SyntheticSuffixIntersects(prop, f.syntheticSuffix)
		}
		if err != nil {
			return blockIntersects, err
		} else if !intersects {
			return blockMaybeExcluded, nil
		}
	}
	return blockIntersects, nil
}

func uvarintLen(v uint32) int {
	i := 0
	for v >= 0x80 {
		v >>= 7
		i++
	}
	return i + 1
}

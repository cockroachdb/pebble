// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/batchskl"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rawalloc"
)

const (
	batchHeaderLen       = 12
	batchInitialSize     = 1 << 10 // 1 KB
	batchMaxRetainedSize = 1 << 20 // 1 MB
	invalidBatchCount    = 1<<32 - 1
	maxVarintLen32       = 5
)

// ErrNotIndexed means that a read operation on a batch failed because the
// batch is not indexed and thus doesn't support reads.
var ErrNotIndexed = errors.New("pebble: batch not indexed")

// ErrInvalidBatch indicates that a batch is invalid or otherwise corrupted.
var ErrInvalidBatch = errors.New("pebble: invalid batch")

type batchStorage struct {
	// Data is the wire format of a batch's log entry:
	//   - 8 bytes for a sequence number of the first batch element,
	//     or zeroes if the batch has not yet been applied,
	//   - 4 bytes for the count: the number of elements in the batch,
	//     or "\xff\xff\xff\xff" if the batch is invalid,
	//   - count elements, being:
	//     - one byte for the kind
	//     - the varint-string user key,
	//     - the varint-string value (if kind != delete).
	// The sequence number and count are stored in little-endian order.
	data           []byte
	cmp            Compare
	abbreviatedKey AbbreviatedKey
}

// Get implements Storage.Get, as documented in the pebble/batchskl package.
func (s *batchStorage) Get(offset uint32) InternalKey {
	kind := InternalKeyKind(s.data[offset])
	_, key, ok := batchDecodeStr(s.data[offset+1:])
	if !ok {
		panic(fmt.Sprintf("corrupted batch entry: %d", offset))
	}
	return base.MakeInternalKey(key, uint64(offset)|InternalKeySeqNumBatch, kind)
}

// AbbreviatedKey implements Storage.AbbreviatedKey, as documented in the
// pebble/batchskl package.
func (s *batchStorage) AbbreviatedKey(key []byte) uint64 {
	return s.abbreviatedKey(key)
}

// Compare implements Storage.Compare, as documented in the pebble/batchskl
// package.
func (s *batchStorage) Compare(a []byte, b uint32) int {
	// The key "a" is always the search key or the newer key being inserted. If
	// it is equal to the existing key consider it smaller so that it sorts
	// first.
	if s.cmp(a, s.Get(b).UserKey) <= 0 {
		return -1
	}
	return 1
}

// DeferredBatchOp represents a batch operation (eg. set, merge, delete) that is
// being inserted into the batch. Indexing is not performed on the specified key
// until Finish is called, hence the name deferred. This struct lets the caller
// copy or encode keys/values directly into the batch representation instead of
// copying into an intermediary buffer then having pebble.Batch copy off of it.
type DeferredBatchOp struct {
	index *batchskl.Skiplist

	// Key and Value point to parts of the binary batch representation where
	// keys and values should be encoded/copied into. len(Key) and len(Value)
	// bytes must be copied into these slices respectively before calling
	// Finish(). Changing where these slices point to is not allowed.
	Key, Value []byte
	offset     uint32
}

// Finish completes the addition of this batch operation, and adds it to the
// index if necessary. Must be called once (and exactly once) keys/values
// have been filled into Key and Value. Not calling Finish or not
// copying/encoding keys will result in an incomplete index, and calling Finish
// twice may result in a panic.
func (d DeferredBatchOp) Finish() {
	if d.index != nil {
		if err := d.index.Add(d.offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
}

// A Batch is a sequence of Sets, Merges, Deletes, and/or DeleteRanges that are
// applied atomically. Batch implements the Reader interface, but only an
// indexed batch supports reading (without error) via Get or NewIter. A
// non-indexed batch will return ErrNotIndexed when read from .
//
// Indexing
//
// Batches can be optionally indexed (see DB.NewIndexedBatch). An indexed batch
// allows iteration via an Iterator (see Batch.NewIter). The iterator provides
// a merged view of the operations in the batch and the underlying
// database. This is implemented by treating the batch as an additional layer
// in the LSM where every entry in the batch is considered newer than any entry
// in the underlying database (batch entries have the InternalKeySeqNumBatch
// bit set). By treating the batch as an additional layer in the LSM, iteration
// supports all batch operations (i.e. Set, Merge, Delete, and DeleteRange)
// with minimal effort.
//
// The same key can be operated on multiple times in a batch, though only the
// latest operation will be visible. For example, Put("a", "b"), Delete("a")
// will cause the key "a" to not be visible in the batch. Put("a", "b"),
// Put("a", "c") will cause a read of "a" to return the value "c".
//
// The batch index is implemented via an skiplist (internal/batchskl). While
// the skiplist implementation is very fast, inserting into an indexed batch is
// significantly slower than inserting into a non-indexed batch. Only use an
// indexed batch if you require reading from it.
//
// Atomic commit
//
// The operations in a batch are persisted by calling Batch.Commit which is
// equivalent to calling DB.Apply(batch). A batch is committed atomically by
// writing the internal batch representation to the WAL, adding all of the
// batch operations to the memtable associated with the WAL, and then
// incrementing the visible sequence number so that subsequent reads can see
// the effects of the batch operations. If WriteOptions.Sync is true, a call to
// Batch.Commit will guarantee that the batch is persisted to disk before
// returning. See commitPipeline for more on the implementation details.
//
// Large batches
//
// The size of a batch is limited only by available memory (be aware that
// indexed batches require considerably additional memory for the skiplist
// structure). A given WAL file has a single memtable associated with it (this
// restriction could be removed, but doing so is onerous and complex). And a
// memtable has a fixed size due to the underlying fixed size arena. Note that
// this differs from RocksDB where a memtable can grow arbitrarily large using
// a list of arena chunks. In RocksDB this is accomplished by storing pointers
// in the arena memory, but that isn't possible in Go.
//
// During Batch.Commit, a batch which is larger than a threshold (>
// MemTableSize/2) is wrapped in a flushableBatch and inserted into the queue
// of memtables. A flushableBatch forces WAL to be rotated, but that happens
// anyways when the memtable becomes full so this does not cause significant
// WAL churn. Because the flushableBatch is readable as another layer in the
// LSM, Batch.Commit returns as soon as the flushableBatch has been added to
// the queue of memtables.
//
// Internally, a flushableBatch provides Iterator support by sorting the batch
// contents (the batch is sorted once, when it is added to the memtable
// queue). Sorting the batch contents and insertion of the contents into a
// memtable have the same big-O time, but the constant factor dominates
// here. Sorting is significantly faster and uses significantly less memory.
//
// Internal representation
//
// The internal batch representation is a contiguous byte buffer with a fixed
// 12-byte header, followed by a series of records.
//
//   +-------------+------------+--- ... ---+
//   | SeqNum (8B) | Count (4B) |  Entries  |
//   +-------------+------------+--- ... ---+
//
// Each record has a 1-byte kind tag prefix, followed by 1 or 2 length prefixed
// strings (varstring):
//
//   +-----------+-----------------+-------------------+
//   | Kind (1B) | Key (varstring) | Value (varstring) |
//   +-----------+-----------------+-------------------+
//
// A varstring is a varint32 followed by N bytes of data. The Kind tags are
// exactly those specified by InternalKeyKind. The following table shows the
// format for records of each kind:
//
//   InternalKeyKindDelete       varstring
//   InternalKeyKindLogData      varstring
//   InternalKeyKindSet          varstring varstring
//   InternalKeyKindMerge        varstring varstring
//   InternalKeyKindRangeDelete  varstring varstring
//
// The intuitive understanding here are that the arguments to Delete(), Set(),
// Merge(), and DeleteRange() are encoded into the batch.
//
// The internal batch representation is the on disk format for a batch in the
// WAL, and thus stable. New record kinds may be added, but the existing ones
// will not be modified.
type Batch struct {
	storage batchStorage

	memTableSize uint32

	// The db to which the batch will be committed.
	db *DB

	// The count of records in the batch. This count will be stored in the batch
	// data whenever Repr() is called.
	count uint32

	// A deferredOp struct, stored in the Batch so that a pointer can be returned
	// from the *Deferred() methods rather than a value.
	deferredOp DeferredBatchOp

	// An optional skiplist keyed by offset into data of the entry.
	index         *batchskl.Skiplist
	rangeDelIndex *batchskl.Skiplist

	// Fragmented range deletion tombstones. Cached the first time a range
	// deletion iterator is requested. The cache is invalidated whenever a new
	// range deletion is added to the batch.
	tombstones []rangedel.Tombstone

	// The flushableBatch wrapper if the batch is too large to fit in the
	// memtable.
	flushable *flushableBatch

	commit    sync.WaitGroup
	commitErr error
	applied   uint32 // updated atomically
}

var _ Reader = (*Batch)(nil)
var _ Writer = (*Batch)(nil)

var batchPool = sync.Pool{
	New: func() interface{} {
		return &Batch{}
	},
}

type indexedBatch struct {
	batch Batch
	index batchskl.Skiplist
}

var indexedBatchPool = sync.Pool{
	New: func() interface{} {
		return &indexedBatch{}
	},
}

func newBatch(db *DB) *Batch {
	b := batchPool.Get().(*Batch)
	b.db = db
	return b
}

func newIndexedBatch(db *DB, comparer *Comparer) *Batch {
	i := indexedBatchPool.Get().(*indexedBatch)
	i.batch.storage.cmp = comparer.Compare
	i.batch.storage.abbreviatedKey = comparer.AbbreviatedKey
	i.batch.db = db
	i.batch.index = &i.index
	i.batch.index.Reset(&i.batch.storage, 0)
	return &i.batch
}

func (b *Batch) release() {
	// NB: This is ugly, but necessary so that we can use atomic.StoreUint32 for
	// the Batch.applied field. Without using an atomic to clear that field the
	// Go race detector complains.
	b.Reset()
	b.storage.cmp = nil
	b.storage.abbreviatedKey = nil
	b.memTableSize = 0

	b.flushable = nil
	b.commit = sync.WaitGroup{}
	b.commitErr = nil
	atomic.StoreUint32(&b.applied, 0)

	if b.db == nil {
		// Batch not created using newBatch or newIndexedBatch, so don't put it
		// back in the pool.
		return
	}
	b.db = nil

	if b.index == nil {
		batchPool.Put(b)
	} else {
		*b.index = batchskl.Skiplist{}
		b.index, b.rangeDelIndex = nil, nil
		indexedBatchPool.Put((*indexedBatch)(unsafe.Pointer(b)))
	}
}

func (b *Batch) refreshMemTableSize() {
	b.memTableSize = 0
	for r := b.Reader(); ; {
		_, key, value, ok := r.Next()
		if !ok {
			break
		}
		b.memTableSize += memTableEntrySize(len(key), len(value))
	}
}

// Apply the operations contained in the batch to the receiver batch.
//
// It is safe to modify the contents of the arguments after Apply returns.
func (b *Batch) Apply(batch *Batch, _ *WriteOptions) error {
	if len(batch.storage.data) == 0 {
		return nil
	}
	if len(batch.storage.data) < batchHeaderLen {
		return errors.New("pebble: invalid batch")
	}

	offset := len(b.storage.data)
	if offset == 0 {
		b.init(offset)
		offset = batchHeaderLen
	}
	b.storage.data = append(b.storage.data, batch.storage.data[batchHeaderLen:]...)

	b.setCount(b.Count() + batch.Count())

	for iter := BatchReader(b.storage.data[offset:]); len(iter) > 0; {
		offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&b.storage.data[0]))
		kind, key, value, ok := iter.Next()
		if !ok {
			break
		}
		if b.index != nil {
			var err error
			if kind == InternalKeyKindRangeDelete {
				if b.rangeDelIndex == nil {
					b.rangeDelIndex = batchskl.NewSkiplist(&b.storage, 0)
				}
				err = b.rangeDelIndex.Add(uint32(offset))
			} else {
				err = b.index.Add(uint32(offset))
			}
			if err != nil {
				// We never add duplicate entries, so an error should never occur.
				panic(err)
			}
		}
		b.memTableSize += memTableEntrySize(len(key), len(value))
	}
	return nil
}

// Get gets the value for the given key. It returns ErrNotFound if the DB
// does not contain the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (b *Batch) Get(key []byte) (value []byte, err error) {
	if b.index == nil {
		return nil, ErrNotIndexed
	}
	return b.db.getInternal(key, b, nil /* snapshot */)
}

func (b *Batch) prepareDeferredKeyValueRecord(
	keyLen, valueLen int, kind InternalKeyKind) error {
	if len(b.storage.data) == 0 {
		b.init(keyLen + valueLen + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}
	b.memTableSize += memTableEntrySize(keyLen, valueLen)

	pos := len(b.storage.data)
	b.deferredOp.offset = uint32(pos)
	b.grow(1 + 2*maxVarintLen32 + keyLen + valueLen)
	b.storage.data[pos] = byte(kind)
	pos++

	varlen1 := putUvarint32(b.storage.data[pos:], uint32(keyLen))
	pos += varlen1
	b.deferredOp.Key = b.storage.data[pos : pos+keyLen]
	pos += keyLen

	varlen2 := putUvarint32(b.storage.data[pos:], uint32(valueLen))
	pos += varlen2
	b.deferredOp.Value = b.storage.data[pos : pos+valueLen]
	pos += valueLen
	// Shrink data since varints may be shorter than the upper bound.
	b.storage.data =
		b.storage.data[:len(b.storage.data)-(2*maxVarintLen32-varlen1-varlen2)]
	return nil
}

func (b *Batch) prepareDeferredKeyRecord(
	keyLen int, kind InternalKeyKind) error {
	if len(b.storage.data) == 0 {
		b.init(keyLen + binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}
	b.memTableSize += memTableEntrySize(keyLen, 0)

	pos := len(b.storage.data)
	b.deferredOp.offset = uint32(pos)
	b.grow(1 + maxVarintLen32 + keyLen)
	b.storage.data[pos] = byte(kind)
	pos++

	varlen1 := putUvarint32(b.storage.data[pos:], uint32(keyLen))
	pos += varlen1
	b.deferredOp.Key = b.storage.data[pos : pos+keyLen]
	b.deferredOp.Value = nil

	// Shrink data since varint may be shorter than the upper bound.
	b.storage.data = b.storage.data[:len(b.storage.data)-(maxVarintLen32-varlen1)]
	return nil
}

// Set adds an action to the batch that sets the key to map to the value.
//
// It is safe to modify the contents of the arguments after Set returns.
func (b *Batch) Set(key, value []byte, _ *WriteOptions) error {
	deferredOp, err := b.SetDeferred(len(key), len(value), nil)
	if err != nil {
		return err
	}
	copy(deferredOp.Key, key)
	copy(deferredOp.Value, value)
	// TODO(peter): Manually inline DeferredBatchOp.Finish(). Mid-stack inlining
	// in go1.13 will remove the need for this.
	if b.index != nil {
		if err := b.index.Add(deferredOp.offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	return nil
}

// SetDeferred is similar to Set in that it adds a set operation to the batch,
// except it only takes in key/value lengths instead of complete slices,
// letting the caller encode into those objects and then call Finish() on the
// returned object.
func (b *Batch) SetDeferred(keyLen, valueLen int, _ *WriteOptions) (*DeferredBatchOp, error) {
	err := b.prepareDeferredKeyValueRecord(keyLen, valueLen, InternalKeyKindSet)
	if err != nil {
		return nil, err
	}
	b.deferredOp.index = b.index
	return &b.deferredOp, nil
}

// Merge adds an action to the batch that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (b *Batch) Merge(key, value []byte, _ *WriteOptions) error {
	deferredOp, err := b.MergeDeferred(len(key), len(value), nil)
	if err != nil {
		return err
	}
	copy(deferredOp.Key, key)
	copy(deferredOp.Value, value)
	// TODO(peter): Manually inline DeferredBatchOp.Finish(). Mid-stack inlining
	// in go1.13 will remove the need for this.
	if b.index != nil {
		if err := b.index.Add(deferredOp.offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	return nil
}

// MergeDeferred is similar to Merge in that it adds a merge operation to the
// batch, except it only takes in key/value lengths instead of complete slices,
// letting the caller encode into those objects and then call Finish() on the
// returned object.
func (b *Batch) MergeDeferred(keyLen, valueLen int, _ *WriteOptions) (*DeferredBatchOp, error) {
	err := b.prepareDeferredKeyValueRecord(keyLen, valueLen, InternalKeyKindMerge)
	if err != nil {
		return nil, err
	}
	b.deferredOp.index = b.index
	return &b.deferredOp, nil
}

// Delete adds an action to the batch that deletes the entry for key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (b *Batch) Delete(key []byte, _ *WriteOptions) error {
	deferredOp, err := b.DeleteDeferred(len(key), nil)
	if err != nil {
		return err
	}
	copy(deferredOp.Key, key)
	// TODO(peter): Manually inline DeferredBatchOp.Finish(). Mid-stack inlining
	// in go1.13 will remove the need for this.
	if b.index != nil {
		if err := b.index.Add(deferredOp.offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	return nil
}

// DeleteDeferred is similar to Delete in that it adds a delete operation to
// the batch, except it only takes in key/value lengths instead of complete
// slices, letting the caller encode into those objects and then call Finish()
// on the returned object.
func (b *Batch) DeleteDeferred(keyLen int, _ *WriteOptions) (*DeferredBatchOp, error) {
	err := b.prepareDeferredKeyRecord(keyLen, InternalKeyKindDelete)
	if err != nil {
		return nil, err
	}
	b.deferredOp.index = b.index
	return &b.deferredOp, nil
}

// SingleDelete adds an action to the batch that single deletes the entry for key.
// See Writer.SingleDelete for more details on the semantics of SingleDelete.
//
// It is safe to modify the contents of the arguments after SingleDelete returns.
func (b *Batch) SingleDelete(key []byte, _ *WriteOptions) error {
	deferredOp, err := b.SingleDeleteDeferred(len(key), nil)
	if err != nil {
		return err
	}
	copy(deferredOp.Key, key)
	// TODO(peter): Manually inline DeferredBatchOp.Finish(). Mid-stack inlining
	// in go1.13 will remove the need for this.
	if b.index != nil {
		if err := b.index.Add(deferredOp.offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	return nil
}

// SingleDeleteDeferred is similar to SingleDelete in that it adds a single delete
// operation to the batch, except it only takes in key/value lengths instead of
// complete slices, letting the caller encode into those objects and then call
// Finish() on the returned object.
func (b *Batch) SingleDeleteDeferred(keyLen int, _ *WriteOptions) (*DeferredBatchOp, error) {
	err := b.prepareDeferredKeyRecord(keyLen, InternalKeyKindSingleDelete)
	if err != nil {
		return nil, err
	}
	b.deferredOp.index = b.index
	return &b.deferredOp, nil
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
//
// It is safe to modify the contents of the arguments after DeleteRange
// returns.
func (b *Batch) DeleteRange(start, end []byte, _ *WriteOptions) error {
	deferredOp, err := b.DeleteRangeDeferred(len(start), len(end), nil)
	if err != nil {
		return err
	}
	copy(deferredOp.Key, start)
	copy(deferredOp.Value, end)
	// TODO(peter): Manually inline DeferredBatchOp.Finish(). Mid-stack inlining
	// in go1.13 will remove the need for this.
	if deferredOp.index != nil {
		if err := deferredOp.index.Add(deferredOp.offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	return nil
}

// DeleteRangeDeferred is similar to DeleteRange in that it adds a delete range
// operation to the batch, except it only takes in key lengths instead of
// complete slices, letting the caller encode into those objects and then call
// Finish() on the returned object. Note that DeferredBatchOp.Key should be
// populated with the start key, and DeferredBatchOp.Value should be populated
// with the end key.
func (b *Batch) DeleteRangeDeferred(startLen, endLen int, _ *WriteOptions) (*DeferredBatchOp, error) {
	err := b.prepareDeferredKeyValueRecord(startLen, endLen, InternalKeyKindRangeDelete)
	if err != nil {
		return nil, err
	}
	if b.index != nil {
		b.tombstones = nil
		// Range deletions are rare, so we lazily allocate the index for them.
		if b.rangeDelIndex == nil {
			b.rangeDelIndex = batchskl.NewSkiplist(&b.storage, 0)
		}
		b.deferredOp.index = b.rangeDelIndex
	}
	return &b.deferredOp, nil
}

// LogData adds the specified to the batch. The data will be written to the
// WAL, but not added to memtables or sstables. Log data is never indexed,
// which makes it useful for testing WAL performance.
//
// It is safe to modify the contents of the argument after LogData returns.
func (b *Batch) LogData(data []byte, _ *WriteOptions) error {
	if len(b.storage.data) == 0 {
		b.init(len(data) + binary.MaxVarintLen64 + batchHeaderLen)
	}
	// Since LogData only writes to the WAL and does not affect the memtable,
	// we don't increment b.count here. b.count only tracks operations that
	// are applied to the memtable.

	pos := len(b.storage.data)
	b.grow(1 + maxVarintLen32 + len(data))
	b.storage.data[pos] = byte(InternalKeyKindLogData)
	_, varlen1 := b.copyStr(pos+1, data)
	b.storage.data = b.storage.data[:len(b.storage.data)-(maxVarintLen32-varlen1)]
	return nil
}

// Empty returns true if the batch is empty, and false otherwise.
func (b *Batch) Empty() bool {
	return len(b.storage.data) <= batchHeaderLen
}

// Repr returns the underlying batch representation. It is not safe to modify
// the contents. Reset() will not change the contents of the returned value,
// though any other mutation operation may do so.
func (b *Batch) Repr() []byte {
	if len(b.storage.data) == 0 {
		b.init(batchHeaderLen)
	}
	binary.LittleEndian.PutUint32(b.countData(), b.count)
	return b.storage.data
}

// SetRepr sets the underlying batch representation. The batch takes ownership
// of the supplied slice. It is not safe to modify it afterwards until the
// Batch is no longer in use.
func (b *Batch) SetRepr(data []byte) error {
	if len(data) < batchHeaderLen {
		return fmt.Errorf("invalid batch")
	}
	b.storage.data = data
	b.count = binary.LittleEndian.Uint32(b.countData())
	b.refreshMemTableSize()
	return nil
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekPrefixGE, SeekLT, First or Last. Only indexed batches support iterators.
func (b *Batch) NewIter(o *IterOptions) *Iterator {
	if b.index == nil {
		return &Iterator{err: ErrNotIndexed}
	}
	return b.db.newIterInternal(b.newInternalIter(o),
		b.newRangeDelIter(o), nil /* snapshot */, o)
}

// newInternalIter creates a new internalIterator that iterates over the
// contents of the batch.
func (b *Batch) newInternalIter(o *IterOptions) internalIterator {
	if b.index == nil {
		return newErrorIter(ErrNotIndexed)
	}
	return &batchIter{
		cmp:   b.storage.cmp,
		batch: b,
		iter:  b.index.NewIter(o.GetLowerBound(), o.GetUpperBound()),
	}
}

func (b *Batch) newRangeDelIter(o *IterOptions) internalIterator {
	if b.index == nil {
		return newErrorIter(ErrNotIndexed)
	}
	if b.rangeDelIndex == nil {
		return nil
	}

	// Fragment the range tombstones the first time a range deletion iterator is
	// requested. The cached tombstones are invalidated if another range deletion
	// tombstone is added to the batch.
	if b.tombstones == nil {
		frag := &rangedel.Fragmenter{
			Cmp: b.storage.cmp,
			Emit: func(fragmented []rangedel.Tombstone) {
				b.tombstones = append(b.tombstones, fragmented...)
			},
		}
		it := &batchIter{
			cmp:   b.storage.cmp,
			batch: b,
			iter:  b.rangeDelIndex.NewIter(nil, nil),
		}
		for {
			key, val := it.Next()
			if key == nil {
				break
			}
			frag.Add(*key, val)
		}
		frag.Finish()
	}

	return rangedel.NewIter(b.storage.cmp, b.tombstones)
}

// Commit applies the batch to its parent writer.
func (b *Batch) Commit(o *WriteOptions) error {
	return b.db.Apply(b, o)
}

// Close closes the batch without committing it.
func (b *Batch) Close() error {
	b.release()
	return nil
}

// Indexed returns true if the batch is indexed (i.e. supports read
// operations).
func (b *Batch) Indexed() bool {
	return b.index != nil
}

func (b *Batch) init(cap int) {
	n := batchInitialSize
	for n < cap {
		n *= 2
	}
	b.storage.data = rawalloc.New(batchHeaderLen, n)
	b.setCount(0)
	b.setSeqNum(0)
	b.storage.data = b.storage.data[:batchHeaderLen]
}

// Reset clears the underlying byte slice and effectively empties the batch for
// reuse. Used in cases where Batch is only being used to build a batch, and
// where the end result is a Repr() call, not a Commit call or a Close call.
// Commits and Closes take care of releasing resources when appropriate.
func (b *Batch) Reset() {
	if b.storage.data != nil {
		if cap(b.storage.data) > batchMaxRetainedSize {
			// If the capacity of the buffer is larger than our maximum
			// retention size, don't re-use it. Let it be GC-ed instead.
			// This prevents the memory from an unusually large batch from
			// being held on to indefinitely.
			b.storage.data = nil
		} else {
			// Otherwise, reset the buffer for re-use.
			b.storage.data = b.storage.data[:batchHeaderLen]
		}
		b.count = 0
	}
}

// seqNumData returns the 8 byte little-endian sequence number. Zero means that
// the batch has not yet been applied.
func (b *Batch) seqNumData() []byte {
	return b.storage.data[:8]
}

// countData returns the 4 byte little-endian count data. "\xff\xff\xff\xff"
// means that the batch is invalid.
func (b *Batch) countData() []byte {
	return b.storage.data[8:12]
}

func (b *Batch) increment() (ok bool) {
	if b.count == math.MaxUint32 {
		return false
	}
	b.count++
	return true
}

func (b *Batch) grow(n int) {
	newSize := len(b.storage.data) + n
	if newSize > cap(b.storage.data) {
		newCap := 2 * cap(b.storage.data)
		for newCap < newSize {
			newCap *= 2
		}
		newData := rawalloc.New(len(b.storage.data), newCap)
		copy(newData, b.storage.data)
		b.storage.data = newData
	}
	b.storage.data = b.storage.data[:newSize]
}

func putUvarint32(buf []byte, x uint32) int {
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

func (b *Batch) copyStr(pos int, s []byte) (int, int) {
	n := putUvarint32(b.storage.data[pos:], uint32(len(s)))
	return pos + n + copy(b.storage.data[pos+n:], s), n
}

func (b *Batch) setSeqNum(seqNum uint64) {
	binary.LittleEndian.PutUint64(b.seqNumData(), seqNum)
}

// SeqNum returns the batch sequence number which is applied to the first
// record in the batch. The sequence number is incremented for each subsequent
// record.
func (b *Batch) SeqNum() uint64 {
	return binary.LittleEndian.Uint64(b.seqNumData())
}

func (b *Batch) setCount(v uint32) {
	b.count = v
}

// Count returns the count of memtable-modifying operations in this batch. All
// operations with the except of LogData increment this count.
func (b *Batch) Count() uint32 {
	return b.count
}

// Reader returns a BatchReader for the current batch contents. If the batch is
// mutated, the new entries will not be visible to the reader.
func (b *Batch) Reader() BatchReader {
	return b.storage.data[batchHeaderLen:]
}

func batchDecode(data []byte, offset uint32) (kind InternalKeyKind, ukey []byte, value []byte, ok bool) {
	p := data[offset:]
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, p = InternalKeyKind(p[0]), p[1:]
	if kind > InternalKeyKindMax {
		return 0, nil, nil, false
	}
	p, ukey, ok = batchDecodeStr(p)
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindRangeDelete:
		_, value, ok = batchDecodeStr(p)
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, ukey, value, true
}

func batchDecodeStr(data []byte) (odata []byte, s []byte, ok bool) {
	v, n := binary.Uvarint(data)
	if n <= 0 {
		return nil, nil, false
	}
	data = data[n:]
	if v > uint64(len(data)) {
		return nil, nil, false
	}
	return data[v:], data[:v], true
}

// BatchReader iterates over the entries contained in a batch.
type BatchReader []byte

// MakeBatchReader constructs a BatchReader from a batch representation. The
// header (containing the batch count and seqnum) is ignored.
func MakeBatchReader(repr []byte) BatchReader {
	return repr[batchHeaderLen:]
}

// Next returns the next entry in this batch. The final return value is false
// if the batch is corrupt. The end of batch is reached when len(r)==0.
func (r *BatchReader) Next() (kind InternalKeyKind, ukey []byte, value []byte, ok bool) {
	p := *r
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, *r = InternalKeyKind(p[0]), p[1:]
	if kind > InternalKeyKindMax {
		return 0, nil, nil, false
	}
	ukey, ok = r.nextStr()
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindRangeDelete:
		value, ok = r.nextStr()
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, ukey, value, true
}

func (r *BatchReader) nextStr() (s []byte, ok bool) {
	p := *r
	u, numBytes := binary.Uvarint(p)
	if numBytes <= 0 {
		return nil, false
	}
	p = p[numBytes:]
	if u > uint64(len(p)) {
		return nil, false
	}
	s, *r = p[:u], p[u:]
	return s, true
}

// Note: batchIter mirrors the implementation of flushableBatchIter. Keep the
// two in sync.
type batchIter struct {
	cmp   Compare
	batch *Batch
	iter  batchskl.Iterator
	err   error
}

// batchIter implements the internalIterator interface.
var _ internalIterator = (*batchIter)(nil)

func (i *batchIter) SeekGE(key []byte) (*InternalKey, []byte) {
	ikey := i.iter.SeekGE(key)
	if ikey == nil {
		return nil, nil
	}
	return ikey, i.Value()
}

func (i *batchIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	return i.SeekGE(key)
}

func (i *batchIter) SeekLT(key []byte) (*InternalKey, []byte) {
	ikey := i.iter.SeekLT(key)
	if ikey == nil {
		return nil, nil
	}
	return ikey, i.Value()
}

func (i *batchIter) First() (*InternalKey, []byte) {
	ikey := i.iter.First()
	if ikey == nil {
		return nil, nil
	}
	return ikey, i.Value()
}

func (i *batchIter) Last() (*InternalKey, []byte) {
	ikey := i.iter.Last()
	if ikey == nil {
		return nil, nil
	}
	return ikey, i.Value()
}

func (i *batchIter) Next() (*InternalKey, []byte) {
	ikey := i.iter.Next()
	if ikey == nil {
		return nil, nil
	}
	return ikey, i.Value()
}

func (i *batchIter) Prev() (*InternalKey, []byte) {
	ikey := i.iter.Prev()
	if ikey == nil {
		return nil, nil
	}
	return ikey, i.Value()
}

func (i *batchIter) Key() *InternalKey {
	return i.iter.Key()
}

func (i *batchIter) Value() []byte {
	_, _, value, ok := batchDecode(i.batch.storage.data, i.iter.KeyOffset())
	if !ok {
		i.err = fmt.Errorf("corrupted batch")
	}
	return value
}

func (i *batchIter) Valid() bool {
	return i.iter.Valid()
}

func (i *batchIter) Error() error {
	return i.err
}

func (i *batchIter) Close() error {
	_ = i.iter.Close()
	return i.err
}

func (i *batchIter) SetBounds(lower, upper []byte) {
	i.iter.SetBounds(lower, upper)
}

type flushableBatchEntry struct {
	// offset is the byte offset of the record within the batch repr.
	offset uint32
	// index is the 0-based ordinal number of the record within the batch. Used
	// to compute the seqnum for the record.
	index uint32
	// key{Start,End} are the start and end byte offsets of the key within the
	// batch repr. Cached to avoid decoding the key length on every
	// comparison. The value is stored starting at keyEnd.
	keyStart uint32
	keyEnd   uint32
}

// flushableBatch wraps an existing batch and provides the interfaces needed
// for making the batch flushable (i.e. able to mimic a memtable).
type flushableBatch struct {
	cmp  Compare
	data []byte

	// The base sequence number for the entries in the batch. This is the same
	// value as Batch.seqNum() and is cached here for performance.
	seqNum uint64

	// A slice of offsets and indices for the entries in the batch. Used to
	// implement flushableBatchIter. Unlike the indexing on a normal batch, a
	// flushable batch is indexed such that batch entry i will be given the
	// sequence number flushableBatch.seqNum+i.
	//
	// Sorted in increasing order of key and decreasing order of offset (since
	// higher offsets correspond to higher sequence numbers).
	//
	// Does not include range deletion entries.
	offsets []flushableBatchEntry

	// Fragmented range deletion tombstones.
	tombstones []rangedel.Tombstone

	flushedCh chan struct{}

	logNum uint64
}

var _ flushable = (*flushableBatch)(nil)

// newFlushableBatch creates a new batch that implements the flushable
// interface. This allows the batch to act like a memtable and be placed in the
// queue of flushable memtables. Note that the flushable batch takes ownership
// of the batch data.
func newFlushableBatch(batch *Batch, comparer *Comparer) *flushableBatch {
	b := &flushableBatch{
		data:      batch.storage.data,
		cmp:       comparer.Compare,
		offsets:   make([]flushableBatchEntry, 0, batch.Count()),
		flushedCh: make(chan struct{}),
	}

	var rangeDelOffsets []flushableBatchEntry
	if len(b.data) > batchHeaderLen {
		// Non-empty batch.
		var index uint32
		for iter := BatchReader(b.data[batchHeaderLen:]); len(iter) > 0; index++ {
			offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&b.data[0]))
			kind, key, _, ok := iter.Next()
			if !ok {
				break
			}
			entry := flushableBatchEntry{
				offset: uint32(offset),
				index:  uint32(index),
			}
			if keySize := uint32(len(key)); keySize == 0 {
				// Must add 2 to the offset. One byte encodes `kind` and the next
				// byte encodes `0`, which is the length of the key.
				entry.keyStart = uint32(offset) + 2
				entry.keyEnd = entry.keyStart
			} else {
				entry.keyStart = uint32(uintptr(unsafe.Pointer(&key[0])) -
					uintptr(unsafe.Pointer(&b.data[0])))
				entry.keyEnd = entry.keyStart + keySize
			}
			if kind == InternalKeyKindRangeDelete {
				rangeDelOffsets = append(rangeDelOffsets, entry)
			} else {
				b.offsets = append(b.offsets, entry)
			}
		}
	}

	// Sort both offsets and rangeDelOffsets.
	sort.Sort(b)
	rangeDelOffsets, b.offsets = b.offsets, rangeDelOffsets
	sort.Sort(b)
	rangeDelOffsets, b.offsets = b.offsets, rangeDelOffsets

	if len(rangeDelOffsets) > 0 {
		frag := &rangedel.Fragmenter{
			Cmp: b.cmp,
			Emit: func(fragmented []rangedel.Tombstone) {
				b.tombstones = append(b.tombstones, fragmented...)
			},
		}
		it := &flushableBatchIter{
			batch:   b,
			data:    b.data,
			offsets: rangeDelOffsets,
			cmp:     b.cmp,
			index:   -1,
		}
		for {
			key, val := it.Next()
			if key == nil {
				break
			}
			frag.Add(*key, val)
		}
		frag.Finish()
	}
	return b
}

func (b *flushableBatch) Len() int {
	return len(b.offsets)
}

func (b *flushableBatch) Less(i, j int) bool {
	ei := &b.offsets[i]
	ej := &b.offsets[j]
	ki := b.data[ei.keyStart:ei.keyEnd]
	kj := b.data[ej.keyStart:ej.keyEnd]
	switch c := b.cmp(ki, kj); {
	case c < 0:
		return true
	case c > 0:
		return false
	default:
		return ei.offset > ej.offset
	}
}

func (b *flushableBatch) Swap(i, j int) {
	b.offsets[i], b.offsets[j] = b.offsets[j], b.offsets[i]
}

func (b *flushableBatch) newIter(o *IterOptions) internalIterator {
	return &flushableBatchIter{
		batch:   b,
		data:    b.data,
		offsets: b.offsets,
		cmp:     b.cmp,
		index:   -1,
		lower:   o.GetLowerBound(),
		upper:   o.GetUpperBound(),
	}
}

func (b *flushableBatch) newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator {
	return &flushFlushableBatchIter{
		flushableBatchIter: flushableBatchIter{
			batch:   b,
			data:    b.data,
			offsets: b.offsets,
			cmp:     b.cmp,
			index:   -1,
		},
		bytesIterated: bytesFlushed,
	}
}

func (b *flushableBatch) newRangeDelIter(o *IterOptions) internalIterator {
	if len(b.tombstones) == 0 {
		return nil
	}
	return rangedel.NewIter(b.cmp, b.tombstones)
}

func (b *flushableBatch) totalBytes() uint64 {
	return uint64(len(b.data) - batchHeaderLen)
}

func (b *flushableBatch) flushed() chan struct{} {
	return b.flushedCh
}

func (b *flushableBatch) readyForFlush() bool {
	return true
}

func (b *flushableBatch) logInfo() (uint64, uint64) {
	return b.logNum, 0 /* logSize */
}

// Note: flushableBatchIter mirrors the implementation of batchIter. Keep the
// two in sync.
type flushableBatchIter struct {
	// Members to be initialized by creator.
	batch *flushableBatch
	// The bytes backing the batch. Always the same as batch.data?
	data []byte
	// The sorted entries. This is not always equal to batch.offsets.
	offsets []flushableBatchEntry
	cmp     Compare
	// Must be initialized to -1. It is the index into offsets that represents
	// the current iterator position.
	index int

	// For internal use by the implementation.
	key InternalKey
	err error

	// Optionally initialize to bounds of iteration, if any.
	lower []byte
	upper []byte
}

// flushableBatchIter implements the internalIterator interface.
var _ internalIterator = (*flushableBatchIter)(nil)

// SeekGE implements internalIterator.SeekGE, as documented in the pebble
// package.
func (i *flushableBatchIter) SeekGE(key []byte) (*InternalKey, []byte) {
	ikey := base.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return base.InternalCompare(i.cmp, ikey, i.getKey(j)) <= 0
	})
	if i.index >= len(i.offsets) {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.upper != nil && i.cmp(i.key.UserKey, i.upper) >= 0 {
		i.index = len(i.offsets)
		return nil, nil
	}
	return &i.key, i.Value()
}

// SeekPrefixGE implements internalIterator.SeekPrefixGE, as documented in the
// pebble package.
func (i *flushableBatchIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	return i.SeekGE(key)
}

// SeekLT implements internalIterator.SeekLT, as documented in the pebble
// package.
func (i *flushableBatchIter) SeekLT(key []byte) (*InternalKey, []byte) {
	ikey := base.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return base.InternalCompare(i.cmp, ikey, i.getKey(j)) <= 0
	})
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.lower != nil && i.cmp(i.key.UserKey, i.lower) < 0 {
		i.index = -1
		return nil, nil
	}
	return &i.key, i.Value()
}

// First implements internalIterator.First, as documented in the pebble
// package.
func (i *flushableBatchIter) First() (*InternalKey, []byte) {
	if len(i.offsets) == 0 {
		return nil, nil
	}
	i.index = 0
	i.key = i.getKey(i.index)
	if i.upper != nil && i.cmp(i.key.UserKey, i.upper) >= 0 {
		i.index = len(i.offsets)
		return nil, nil
	}
	return &i.key, i.Value()
}

// Last implements internalIterator.Last, as documented in the pebble
// package.
func (i *flushableBatchIter) Last() (*InternalKey, []byte) {
	if len(i.offsets) == 0 {
		return nil, nil
	}
	i.index = len(i.offsets) - 1
	i.key = i.getKey(i.index)
	if i.lower != nil && i.cmp(i.key.UserKey, i.lower) < 0 {
		i.index = -1
		return nil, nil
	}
	return &i.key, i.Value()
}

// Note: flushFlushableBatchIter.Next mirrors the implementation of
// flushableBatchIter.Next due to performance. Keep the two in sync.
func (i *flushableBatchIter) Next() (*InternalKey, []byte) {
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.index++
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.upper != nil && i.cmp(i.key.UserKey, i.upper) >= 0 {
		i.index = len(i.offsets)
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) Prev() (*InternalKey, []byte) {
	if i.index < 0 {
		return nil, nil
	}
	i.index--
	if i.index < 0 {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	if i.lower != nil && i.cmp(i.key.UserKey, i.lower) < 0 {
		i.index = -1
		return nil, nil
	}
	return &i.key, i.Value()
}

func (i *flushableBatchIter) getKey(index int) InternalKey {
	e := &i.offsets[index]
	kind := InternalKeyKind(i.data[e.offset])
	key := i.data[e.keyStart:e.keyEnd]
	return base.MakeInternalKey(key, i.batch.seqNum+uint64(e.index), kind)
}

func (i *flushableBatchIter) Key() *InternalKey {
	return &i.key
}

func (i *flushableBatchIter) Value() []byte {
	p := i.data[i.offsets[i.index].offset:]
	if len(p) == 0 {
		i.err = fmt.Errorf("corrupted batch")
		return nil
	}
	kind := InternalKeyKind(p[0])
	if kind > InternalKeyKindMax {
		i.err = fmt.Errorf("corrupted batch")
		return nil
	}
	var value []byte
	var ok bool
	switch kind {
	case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindRangeDelete:
		keyEnd := i.offsets[i.index].keyEnd
		_, value, ok = batchDecodeStr(i.data[keyEnd:])
		if !ok {
			i.err = fmt.Errorf("corrupted batch")
			return nil
		}
	}
	return value
}

func (i *flushableBatchIter) Valid() bool {
	return i.index >= 0 && i.index < len(i.offsets)
}

func (i *flushableBatchIter) Error() error {
	return i.err
}

func (i *flushableBatchIter) Close() error {
	return i.err
}

func (i *flushableBatchIter) SetBounds(lower, upper []byte) {
	i.lower = lower
	i.upper = upper
}

// flushFlushableBatchIter is similar to flushableBatchIter but it keeps track
// of number of bytes iterated.
type flushFlushableBatchIter struct {
	flushableBatchIter
	bytesIterated *uint64
}

// flushFlushableBatchIter implements the internalIterator interface.
var _ internalIterator = (*flushFlushableBatchIter)(nil)

func (i *flushFlushableBatchIter) SeekGE(key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekGE unimplemented")
}

func (i *flushFlushableBatchIter) SeekPrefixGE(prefix, key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekPrefixGE unimplemented")
}

func (i *flushFlushableBatchIter) SeekLT(key []byte) (*InternalKey, []byte) {
	panic("pebble: SeekLT unimplemented")
}

func (i *flushFlushableBatchIter) First() (*InternalKey, []byte) {
	key, val := i.flushableBatchIter.First()
	if key == nil {
		return nil, nil
	}
	entryBytes := i.offsets[i.index].keyEnd - i.offsets[i.index].offset
	*i.bytesIterated += uint64(entryBytes) + i.valueSize()
	return key, val
}

// Note: flushFlushableBatchIter.Next mirrors the implementation of
// flushableBatchIter.Next due to performance. Keep the two in sync.
func (i *flushFlushableBatchIter) Next() (*InternalKey, []byte) {
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.index++
	if i.index == len(i.offsets) {
		return nil, nil
	}
	i.key = i.getKey(i.index)
	entryBytes := i.offsets[i.index].keyEnd - i.offsets[i.index].offset
	*i.bytesIterated += uint64(entryBytes) + i.valueSize()
	return &i.key, i.Value()
}

func (i flushFlushableBatchIter) Prev() (*InternalKey, []byte) {
	panic("pebble: Prev unimplemented")
}

func (i flushFlushableBatchIter) valueSize() uint64 {
	p := i.data[i.offsets[i.index].offset:]
	if len(p) == 0 {
		i.err = fmt.Errorf("corrupted batch")
		return 0
	}
	kind := InternalKeyKind(p[0])
	if kind > InternalKeyKindMax {
		i.err = fmt.Errorf("corrupted batch")
		return 0
	}
	var length uint64
	switch kind {
	case InternalKeyKindSet, InternalKeyKindMerge, InternalKeyKindRangeDelete:
		keyEnd := i.offsets[i.index].keyEnd
		v, n := binary.Uvarint(i.data[keyEnd:])
		if n <= 0 {
			i.err = fmt.Errorf("corrupted batch")
			return 0
		}
		length = v + uint64(n)
	}
	return length
}

// batchSort returns iterators for the sorted contents of the batch. It is
// intended for testing use only. The batch.Sort dance is done to prevent
// exposing this method in the public pebble interface.
func batchSort(i interface{}) (internalIterator, internalIterator) {
	b := i.(*Batch)
	if b.Indexed() {
		return b.newInternalIter(nil), b.newRangeDelIter(nil)
	}
	f := newFlushableBatch(b, b.db.opts.Comparer)
	return f.newIter(nil), f.newRangeDelIter(nil)
}

func init() {
	private.BatchSort = batchSort
}

// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/batchskl"
	"github.com/petermattis/pebble/internal/rangedel"
	"github.com/petermattis/pebble/internal/rawalloc"
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
	cmp            db.Compare
	abbreviatedKey db.AbbreviatedKey
}

// Get implements Storage.Get, as documented in the pebble/batchskl package.
func (s *batchStorage) Get(offset uint32) db.InternalKey {
	kind := db.InternalKeyKind(s.data[offset])
	_, key, ok := batchDecodeStr(s.data[offset+1:])
	if !ok {
		panic(fmt.Sprintf("corrupted batch entry: %d", offset))
	}
	return db.MakeInternalKey(key, uint64(offset)|db.InternalKeySeqNumBatch, kind)
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
// exactly those specified by db.InternalKeyKind. The following table shows the
// format for records of each kind:
//
//   InternalKeyKindDelete       varstring
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

	commit  sync.WaitGroup
	applied uint32 // updated atomically
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

func newIndexedBatch(db *DB, comparer *db.Comparer) *Batch {
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
	b.reset()
	b.storage.cmp = nil
	b.storage.abbreviatedKey = nil
	b.memTableSize = 0
	b.db = nil
	b.flushable = nil
	b.commit = sync.WaitGroup{}
	atomic.StoreUint32(&b.applied, 0)

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
	for iter := b.iter(); ; {
		_, key, value, ok := iter.next()
		if !ok {
			break
		}
		b.memTableSize += memTableEntrySize(len(key), len(value))
	}
}

// Apply the operations contained in the batch to the receiver batch.
//
// It is safe to modify the contents of the arguments after Apply returns.
func (b *Batch) Apply(batch *Batch, _ *db.WriteOptions) error {
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

	count := binary.LittleEndian.Uint32(batch.storage.data[8:12])
	b.setCount(b.count() + count)

	for iter := batchReader(b.storage.data[offset:]); len(iter) > 0; {
		offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&b.storage.data[0]))
		kind, key, value, ok := iter.next()
		if !ok {
			break
		}
		if b.index != nil {
			var err error
			if kind == db.InternalKeyKindRangeDelete {
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

func (b *Batch) encodeKeyValue(key, value []byte, kind db.InternalKeyKind) uint32 {
	pos := len(b.storage.data)
	offset := uint32(pos)
	b.grow(1 + 2*maxVarintLen32 + len(key) + len(value))
	b.storage.data[pos] = byte(kind)
	pos, varlen1 := b.copyStr(pos+1, key)
	_, varlen2 := b.copyStr(pos, value)
	b.storage.data = b.storage.data[:len(b.storage.data)-(2*maxVarintLen32-varlen1-varlen2)]
	return offset
}

// Set adds an action to the batch that sets the key to map to the value.
//
// It is safe to modify the contents of the arguments after Set returns.
func (b *Batch) Set(key, value []byte, _ *db.WriteOptions) error {
	if len(b.storage.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}

	offset := b.encodeKeyValue(key, value, db.InternalKeyKindSet)

	if b.index != nil {
		if err := b.index.Add(offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	b.memTableSize += memTableEntrySize(len(key), len(value))
	return nil
}

// Merge adds an action to the batch that merges the value at key with the new
// value. The details of the merge are dependent upon the configured merge
// operator.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (b *Batch) Merge(key, value []byte, _ *db.WriteOptions) error {
	if len(b.storage.data) == 0 {
		b.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}

	offset := b.encodeKeyValue(key, value, db.InternalKeyKindMerge)

	if b.index != nil {
		if err := b.index.Add(offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	b.memTableSize += memTableEntrySize(len(key), len(value))
	return nil
}

// Delete adds an action to the batch that deletes the entry for key.
//
// It is safe to modify the contents of the arguments after Delete returns.
func (b *Batch) Delete(key []byte, _ *db.WriteOptions) error {
	if len(b.storage.data) == 0 {
		b.init(len(key) + binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}

	pos := len(b.storage.data)
	offset := uint32(pos)
	b.grow(1 + maxVarintLen32 + len(key))
	b.storage.data[pos] = byte(db.InternalKeyKindDelete)
	pos, varlen1 := b.copyStr(pos+1, key)
	b.storage.data = b.storage.data[:len(b.storage.data)-(maxVarintLen32-varlen1)]

	if b.index != nil {
		if err := b.index.Add(offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
	}
	b.memTableSize += memTableEntrySize(len(key), 0)
	return nil
}

// DeleteRange deletes all of the keys (and values) in the range [start,end)
// (inclusive on start, exclusive on end).
//
// It is safe to modify the contents of the arguments after DeleteRange
// returns.
func (b *Batch) DeleteRange(start, end []byte, _ *db.WriteOptions) error {
	if len(b.storage.data) == 0 {
		b.init(len(start) + len(end) + 2*binary.MaxVarintLen64 + batchHeaderLen)
	}
	if !b.increment() {
		return ErrInvalidBatch
	}

	offset := b.encodeKeyValue(start, end, db.InternalKeyKindRangeDelete)

	if b.index != nil {
		// Range deletions are rare, so we lazily allocate the index for them.
		if b.rangeDelIndex == nil {
			b.rangeDelIndex = batchskl.NewSkiplist(&b.storage, 0)
		}
		if err := b.rangeDelIndex.Add(offset); err != nil {
			// We never add duplicate entries, so an error should never occur.
			panic(err)
		}
		b.tombstones = nil
	}
	b.memTableSize += memTableEntrySize(len(start), len(end))
	return nil
}

// Repr returns the underlying batch representation. It is not safe to modify
// the contents.
func (b *Batch) Repr() []byte {
	return b.storage.data
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE, SeekLT,
// First or Last. Only indexed batches support iterators.
func (b *Batch) NewIter(o *db.IterOptions) *Iterator {
	if b.index == nil {
		return &Iterator{err: ErrNotIndexed}
	}
	return b.db.newIterInternal(b.newInternalIter(o),
		b.newRangeDelIter(o), nil /* snapshot */, o)
}

// newInternalIter creates a new internalIterator that iterates over the
// contents of the batch.
func (b *Batch) newInternalIter(o *db.IterOptions) internalIterator {
	if b.index == nil {
		return newErrorIter(ErrNotIndexed)
	}
	return &batchIter{
		cmp:   b.storage.cmp,
		batch: b,
		iter:  b.index.NewIter(),
	}
}

func (b *Batch) newRangeDelIter(o *db.IterOptions) internalIterator {
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
			iter:  b.rangeDelIndex.NewIter(),
		}
		for it.Next() {
			frag.Add(it.Key(), it.Value())
		}
		frag.Finish()
	}

	return rangedel.NewIter(b.storage.cmp, b.tombstones)
}

// Commit applies the batch to its parent writer.
func (b *Batch) Commit(o *db.WriteOptions) error {
	return b.db.Apply(b, o)
}

// Close implements DB.Close, as documented in the pebble/db package.
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

func (b *Batch) reset() {
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
			b.setCount(0)
			b.setSeqNum(0)
		}
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
	p := b.countData()
	for i := range p {
		p[i]++
		if p[i] != 0x00 {
			return true
		}
	}
	// The countData was "\xff\xff\xff\xff". Leave it as it was.
	p[0] = 0xff
	p[1] = 0xff
	p[2] = 0xff
	p[3] = 0xff
	return false
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

func (b *Batch) seqNum() uint64 {
	return binary.LittleEndian.Uint64(b.seqNumData())
}

func (b *Batch) setCount(v uint32) {
	binary.LittleEndian.PutUint32(b.countData(), v)
}

func (b *Batch) count() uint32 {
	return binary.LittleEndian.Uint32(b.countData())
}

func (b *Batch) iter() batchReader {
	return b.storage.data[batchHeaderLen:]
}

func batchDecode(data []byte, offset uint32) (kind db.InternalKeyKind, ukey []byte, value []byte, ok bool) {
	p := data[offset:]
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, p = db.InternalKeyKind(p[0]), p[1:]
	if kind > db.InternalKeyKindMax {
		return 0, nil, nil, false
	}
	p, ukey, ok = batchDecodeStr(p)
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case db.InternalKeyKindSet, db.InternalKeyKindMerge, db.InternalKeyKindRangeDelete:
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

type batchReader []byte

// next returns the next operation in this batch.
// The final return value is false if the batch is corrupi.
func (r *batchReader) next() (kind db.InternalKeyKind, ukey []byte, value []byte, ok bool) {
	p := *r
	if len(p) == 0 {
		return 0, nil, nil, false
	}
	kind, *r = db.InternalKeyKind(p[0]), p[1:]
	if kind > db.InternalKeyKindMax {
		return 0, nil, nil, false
	}
	ukey, ok = r.nextStr()
	if !ok {
		return 0, nil, nil, false
	}
	switch kind {
	case db.InternalKeyKindSet, db.InternalKeyKindMerge, db.InternalKeyKindRangeDelete:
		value, ok = r.nextStr()
		if !ok {
			return 0, nil, nil, false
		}
	}
	return kind, ukey, value, true
}

func (r *batchReader) nextStr() (s []byte, ok bool) {
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
	cmp     db.Compare
	batch   *Batch
	reverse bool
	iter    batchskl.Iterator
	err     error
}

// batchIter implements the internalIterator interface.
var _ internalIterator = (*batchIter)(nil)

func (i *batchIter) SeekGE(key []byte) bool {
	return i.iter.SeekGE(key)
}

func (i *batchIter) SeekLT(key []byte) bool {
	return i.iter.SeekLT(key)
}

func (i *batchIter) First() bool {
	return i.iter.First()
}

func (i *batchIter) Last() bool {
	return i.iter.Last()
}

func (i *batchIter) Next() bool {
	return i.iter.Next()
}

func (i *batchIter) Prev() bool {
	return i.iter.Prev()
}

func (i *batchIter) Key() db.InternalKey {
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

type flushableBatchEntry struct {
	offset   uint32
	index    uint32
	keyStart uint32
	keyEnd   uint32
}

// flushableBatch wraps an existing batch and provides the interfaces needed
// for making the batch flushable (i.e. able to mimic a memtable).
type flushableBatch struct {
	cmp  db.Compare
	data []byte

	// The base sequence number for the entries in the batch. This is the same
	// value as Batch.seqNum() and is cached here for performance.
	seqNum uint64

	// A slice of offsets and indices for the entries in the batch. Used to
	// implement flushableBatchIter. Unlike the indexing on a normal batch, a
	// flushable batch is indexed such that batch entry i will be given the
	// sequence number flushableBatch.seqNum+i.
	offsets         []flushableBatchEntry
	rangeDelOffsets []flushableBatchEntry

	// Fragmented range deletion tombstones. Created and cached the first time a
	// range deletion iterator is requested.
	tombstones []rangedel.Tombstone

	flushedCh chan struct{}
}

var _ flushable = (*flushableBatch)(nil)

// newFlushableBatch creates a new batch that implements the flushable
// interface. This allows the batch to act like a memtable and be placed in the
// queue of flushable memtables. Note that the flushable batch takes ownership
// of the batch data.
func newFlushableBatch(batch *Batch, comparer *db.Comparer) *flushableBatch {
	b := &flushableBatch{
		data:            batch.storage.data,
		cmp:             comparer.Compare,
		offsets:         make([]flushableBatchEntry, 0, batch.count()),
		rangeDelOffsets: nil, // NB: assume no range deletions need indexing
		flushedCh:       make(chan struct{}),
	}

	var index uint32
	for iter := batchReader(b.data[batchHeaderLen:]); len(iter) > 0; index++ {
		offset := uintptr(unsafe.Pointer(&iter[0])) - uintptr(unsafe.Pointer(&b.data[0]))
		kind, key, _, ok := iter.next()
		if !ok {
			break
		}
		entry := flushableBatchEntry{
			offset: uint32(offset),
			index:  uint32(index),
		}
		if keySize := uint32(len(key)); keySize == 0 {
			entry.keyStart = uint32(offset)
			entry.keyEnd = uint32(offset)
		} else {
			entry.keyStart = uint32(uintptr(unsafe.Pointer(&key[0])) -
				uintptr(unsafe.Pointer(&b.data[0])))
			entry.keyEnd = entry.keyStart + keySize
		}
		if kind == db.InternalKeyKindRangeDelete {
			b.rangeDelOffsets = append(b.rangeDelOffsets, entry)
		} else {
			b.offsets = append(b.offsets, entry)
		}
	}

	// Sort both offsets and rangeDelOffsets.
	sort.Sort(b)
	b.rangeDelOffsets, b.offsets = b.offsets, b.rangeDelOffsets
	sort.Sort(b)
	b.rangeDelOffsets, b.offsets = b.offsets, b.rangeDelOffsets
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

func (b *flushableBatch) newIter(o *db.IterOptions) internalIterator {
	return &flushableBatchIter{
		batch:   b,
		data:    b.data,
		offsets: b.offsets,
		cmp:     b.cmp,
		index:   -1,
	}
}

func (b *flushableBatch) newRangeDelIter(o *db.IterOptions) internalIterator {
	if len(b.rangeDelOffsets) == 0 {
		return nil
	}

	// Fragment the range tombstones the first time a range deletion iterator is
	// requested.
	if b.tombstones == nil {
		frag := &rangedel.Fragmenter{
			Cmp: b.cmp,
			Emit: func(fragmented []rangedel.Tombstone) {
				b.tombstones = append(b.tombstones, fragmented...)
			},
		}
		it := &flushableBatchIter{
			batch:   b,
			data:    b.data,
			offsets: b.rangeDelOffsets,
			cmp:     b.cmp,
			index:   -1,
		}
		for it.Next() {
			frag.Add(it.Key(), it.Value())
		}
		frag.Finish()
	}

	return rangedel.NewIter(b.cmp, b.tombstones)
}

func (b *flushableBatch) flushed() chan struct{} {
	return b.flushedCh
}

func (b *flushableBatch) readyForFlush() bool {
	return true
}

// Note: flushableBatchIter mirrors the implementation of batchIter. Keep the
// two in sync.
type flushableBatchIter struct {
	batch   *flushableBatch
	data    []byte
	offsets []flushableBatchEntry
	cmp     db.Compare
	reverse bool
	index   int
	err     error
}

// flushableBatchIter implements the internalIterator interface.
var _ internalIterator = (*flushableBatchIter)(nil)

func (i *flushableBatchIter) SeekGE(key []byte) bool {
	ikey := db.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return db.InternalCompare(i.cmp, ikey, i.getKey(j)) < 0
	})
	return i.index < len(i.offsets)
}

func (i *flushableBatchIter) SeekLT(key []byte) bool {
	ikey := db.MakeSearchKey(key)
	i.index = sort.Search(len(i.offsets), func(j int) bool {
		return db.InternalCompare(i.cmp, ikey, i.getKey(j)) <= 0
	})
	i.index--
	return i.index >= 0
}

func (i *flushableBatchIter) First() bool {
	if len(i.offsets) == 0 {
		return false
	}
	i.index = 0
	return true
}

func (i *flushableBatchIter) Last() bool {
	if len(i.offsets) == 0 {
		return false
	}
	i.index = len(i.offsets) - 1
	return true
}

func (i *flushableBatchIter) Next() bool {
	if i.index == len(i.offsets) {
		return false
	}
	i.index++
	return i.index < len(i.offsets)
}

func (i *flushableBatchIter) Prev() bool {
	if i.index < 0 {
		return false
	}
	i.index--
	return i.index >= 0
}

func (i *flushableBatchIter) getKey(index int) db.InternalKey {
	e := &i.offsets[index]
	kind := db.InternalKeyKind(i.data[e.offset])
	key := i.data[e.keyStart:e.keyEnd]
	return db.MakeInternalKey(key, i.batch.seqNum+uint64(e.index), kind)
}

func (i *flushableBatchIter) Key() db.InternalKey {
	return i.getKey(i.index)
}

func (i *flushableBatchIter) Value() []byte {
	offset := i.offsets[i.index].offset
	_, _, value, ok := batchDecode(i.batch.data, offset)
	if !ok {
		i.err = fmt.Errorf("corrupted batch")
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

package table

import (
	"encoding/binary"
	"errors"
	"sort"

	"github.com/petermattis/pebble/db"
)

type blockWriter struct {
	coder           coder
	restartInterval int
	nEntries        int
	buf             []byte
	restarts        []uint32
	curKey          []byte
	prevKey         []byte
	tmp             [50]byte
}

func (w *blockWriter) add(key *db.InternalKey, value []byte) {
	w.curKey, w.prevKey = w.prevKey, w.curKey

	size := w.coder.Size(key)
	if cap(w.curKey) < size {
		w.curKey = make([]byte, 0, size*2)
	}
	w.curKey = w.curKey[:size]
	w.coder.Encode(key, w.curKey)

	shared := 0
	if w.nEntries%w.restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	} else {
		shared = db.SharedPrefixLen(w.curKey, w.prevKey)
	}

	n := binary.PutUvarint(w.tmp[0:], uint64(shared))
	n += binary.PutUvarint(w.tmp[n:], uint64(size-shared))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(value)))
	w.buf = append(w.buf, w.tmp[:n]...)
	w.buf = append(w.buf, w.curKey[shared:]...)
	w.buf = append(w.buf, value...)

	w.nEntries++
}

func (w *blockWriter) finish() []byte {
	// Write the restart points to the buffer.
	if w.nEntries == 0 {
		// Every block must have at least one restart point.
		w.restarts = w.restarts[:1]
		w.restarts[0] = 0
	}
	tmp4 := w.tmp[:4]
	for _, x := range w.restarts {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp4...)
	return w.buf
}

func (w *blockWriter) size() int {
	return len(w.buf) + 4*(len(w.restarts)+1)
}

type blockEntry struct {
	offset int
	key    []byte
	val    []byte
}

// blockIter2 is an iterator over a single block of data.
type blockIter2 struct {
	cmp         db.Compare
	coder       coder
	offset      int
	nextOffset  int
	restarts    int
	numRestarts int
	data        []byte
	key, val    []byte
	ikey        db.InternalKey
	cached      []blockEntry
	cachedBuf   []byte
}

// blockIter2 implements the db.InternalIterator interface.
var _ db.InternalIterator = (*blockIter2)(nil)

func decodeVarint(src []byte) (uint32, int) {
	dst := uint32(src[0]) & 0x7f
	if src[0] < 128 {
		return dst, 1
	}
	dst |= (uint32(src[1]&0x7f) << 7)
	if src[1] < 128 {
		return dst, 2
	}
	dst |= (uint32(src[2]&0x7f) << 14)
	if src[2] < 128 {
		return dst, 3
	}
	dst |= (uint32(src[3]&0x7f) << 21)
	if src[3] < 128 {
		return dst, 4
	}
	dst |= (uint32(src[4]&0x7f) << 28)
	return dst, 5
}

func newBlockIter2(cmp db.Compare, coder coder, block block) (*blockIter2, error) {
	i := &blockIter2{}
	return i, i.init(cmp, coder, block)
}

func (i *blockIter2) init(cmp db.Compare, coder coder, block block) error {
	numRestarts := int(binary.LittleEndian.Uint32(block[len(block)-4:]))
	if numRestarts == 0 {
		return errors.New("pebble/table: invalid table (block has no restart points)")
	}
	*i = blockIter2{
		cmp:         cmp,
		coder:       coder,
		restarts:    len(block) - 4*(1+numRestarts),
		numRestarts: numRestarts,
		data:        block,
		key:         make([]byte, 0, 256),
	}
	return nil
}

func (i *blockIter2) readEntry() {
	shared, n := decodeVarint(i.data[i.offset:])
	i.nextOffset = i.offset + n
	unshared, n := decodeVarint(i.data[i.nextOffset:])
	i.nextOffset += n
	value, n := decodeVarint(i.data[i.nextOffset:])
	i.nextOffset += n
	i.key = append(i.key[:shared], i.data[i.nextOffset:i.nextOffset+int(unshared)]...)
	i.key = i.key[:len(i.key):len(i.key)]
	i.nextOffset += int(unshared)
	i.val = i.data[i.nextOffset : i.nextOffset+int(value) : i.nextOffset+int(value)]
	i.nextOffset += int(value)
}

func (i *blockIter2) loadEntry() {
	i.readEntry()
	i.ikey = i.coder.Decode(i.key)
}

func (i *blockIter2) clearCache() {
	i.cached = i.cached[:0]
	i.cachedBuf = i.cachedBuf[:0]
}

func (i *blockIter2) cacheEntry() {
	i.cachedBuf = append(i.cachedBuf, i.key...)
	i.cached = append(i.cached, blockEntry{
		offset: i.offset,
		key:    i.cachedBuf[len(i.cachedBuf)-len(i.key) : len(i.cachedBuf) : len(i.cachedBuf)],
		val:    i.val,
	})
}

// SeekGE implements Iterator.SeekGE, as documented in the pebble/db package.
func (i *blockIter2) SeekGE(key *db.InternalKey) {
	// Find the index of the smallest restart point whose key is > the key
	// sought; index will be numRestarts if there is no such restart point.
	i.offset = 0
	if key == nil {
		i.loadEntry()
		return
	}

	index := sort.Search(i.numRestarts, func(j int) bool {
		offset := int(binary.LittleEndian.Uint32(i.data[i.restarts+4*j:]))
		// For a restart point, there are 0 bytes shared with the previous key.
		// The varint encoding of 0 occupies 1 byte.
		offset++
		// Decode the key at that restart point, and compare it to the key sought.
		v1, n1 := decodeVarint(i.data[offset:])
		_, n2 := decodeVarint(i.data[offset+n1:])
		m := offset + n1 + n2
		s := i.data[m : m+int(v1)]
		return db.InternalCompare(i.cmp, *key, i.coder.Decode(s)) < 0
	})

	// Since keys are strictly increasing, if index > 0 then the restart point at
	// index-1 will be the largest whose key is <= the key sought.  If index ==
	// 0, then all keys in this block are larger than the key sought, and offset
	// remains at zero.
	if index > 0 {
		i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}
	i.loadEntry()

	// Iterate from that restart point to somewhere >= the key sought.
	for ; i.Valid(); i.Next() {
		if db.InternalCompare(i.cmp, *key, i.ikey) <= 0 {
			break
		}
	}
}

// SeekLE implements Iterator.SeekLE, as documented in the pebble/db package.
func (i *blockIter2) SeekLE(key *db.InternalKey) {
	panic("pebble/table: SeekLE unimplemented")
}

// First implements Iterator.First, as documented in the pebble/db package.
func (i *blockIter2) First() {
	i.offset = 0
	i.loadEntry()
}

// Last implements Iterator.Last, as documented in the pebble/db package.
func (i *blockIter2) Last() {
	// Seek forward from the last restart point.
	i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(i.numRestarts-1):]))

	i.readEntry()
	i.clearCache()
	i.cacheEntry()

	for i.nextOffset < i.restarts {
		i.offset = i.nextOffset
		i.readEntry()
		i.cacheEntry()
	}

	i.ikey = i.coder.Decode(i.key)
}

// Next implements Iterator.Next, as documented in the pebble/db package.
func (i *blockIter2) Next() bool {
	i.offset = i.nextOffset
	if !i.Valid() {
		return false
	}
	i.loadEntry()
	return true
}

// Prev implements Iterator.Prev, as documented in the pebble/db package.
func (i *blockIter2) Prev() bool {
	if n := len(i.cached) - 1; n > 0 && i.cached[n].offset == i.offset {
		i.nextOffset = i.offset
		e := &i.cached[n-1]
		i.offset = e.offset
		i.val = e.val
		i.ikey = i.coder.Decode(e.key)
		i.cached = i.cached[:n]
		return true
	}

	if i.offset == 0 {
		i.offset = -1
		i.nextOffset = 0
		return false
	}

	targetOffset := i.offset
	index := sort.Search(i.numRestarts, func(j int) bool {
		offset := int(binary.LittleEndian.Uint32(i.data[i.restarts+4*j:]))
		return offset >= targetOffset
	})
	i.offset = 0
	if index > 0 {
		i.offset = int(binary.LittleEndian.Uint32(i.data[i.restarts+4*(index-1):]))
	}

	i.readEntry()
	i.clearCache()
	i.cacheEntry()

	for i.nextOffset < targetOffset {
		i.offset = i.nextOffset
		i.readEntry()
		i.cacheEntry()
	}

	i.ikey = i.coder.Decode(i.key)
	return true
}

// Key implements Iterator.Key, as documented in the pebble/db package.
func (i *blockIter2) Key() *db.InternalKey {
	return &i.ikey
}

// Value implements Iterator.Value, as documented in the pebble/db package.
func (i *blockIter2) Value() []byte {
	return i.val
}

// Valid implements Iterator.Valid, as documented in the pebble/db package.
func (i *blockIter2) Valid() bool {
	return i.offset >= 0 && i.offset < i.restarts
}

// Close implements Iterator.Close, as documented in the pebble/db package.
func (i *blockIter2) Close() error {
	i.val = nil
	return nil
}

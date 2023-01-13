// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"encoding/binary"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/vfs"
)

type blobFileFormat uint8

const (
	blobFileFormatV1 blobFileFormat = 1
)

const (
	blobFileFooterLength = 40
	blobFileMagic        = "\xf0\x9f\xaa\xb3\xf0\x9f\xa6\x80" // 🪳🦀
)

// Blob file footer format:
//   - value blocks index handle: maximum length is valueBlocksIndexHandleMaxLen
//     = 23. We use varint encoding for the above not to save space (since the
//     footer needs to be fixed size) but to share encoding decoding code.
//   - padding: to make the total length equal to blobFooterLength.
//   - checksum type: 1 byte
//   - format: 1 byte
//   - blob file magic: 8 bytes
type blobFileFooter struct {
	format             blobFileFormat
	checksum           ChecksumType
	valueBlocksIndexBH valueBlocksIndexHandle
}

// REQUIRES: len(buf) >= blobFileFooterLength
func (f *blobFileFooter) encode(buf []byte) []byte {
	buf = buf[:blobFileFooterLength]
	for i := range buf {
		buf[i] = 0
	}
	n := encodeValueBlocksIndexHandle(buf, f.valueBlocksIndexBH)
	if len(buf)-n < 2+len(blobFileMagic) {
		panic("footer is too short")
	}
	buf[len(buf)-2-len(blobFileMagic)] = byte(f.checksum)
	buf[len(buf)-1-len(blobFileMagic)] = byte(f.format)
	copy(buf[len(buf)-len(blobFileMagic):], blobFileMagic)
	return buf
}

func (f *blobFileFooter) read(file ReadableFile) error {
	stat, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, "pebble/blob_file: invalid (could not stat file)")
	}
	blobFileSize := stat.Size()
	if blobFileSize < blobFileFooterLength {
		return base.CorruptionErrorf("pebble/blob_file: invalid (file size is too small)")
	}
	buf := make([]byte, blobFileFooterLength)
	off := blobFileSize - blobFileFooterLength
	n, err := file.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		return errors.Wrap(err, "pebble/blob_file: invalid (could not read footer)")
	}
	if n < blobFileFooterLength {
		return base.CorruptionErrorf("pebble/blob_file: invalid (read partial footer %d bytes)", n)
	}
	if string(buf[len(buf)-len(blobFileMagic):]) != blobFileMagic {
		return base.CorruptionErrorf("pebble/blob_file: invalid (bad magic number)")
	}
	f.checksum = ChecksumType(buf[len(buf)-2-len(blobFileMagic)])
	f.format = blobFileFormat(buf[len(buf)-1-len(blobFileMagic)])
	f.valueBlocksIndexBH, _, err = decodeValueBlocksIndexHandle(buf, false)
	if err != nil {
		return errors.Wrap(err, "pebble/blob_file: invalid (unable to decode value blocks index handle")
	}
	return nil
}

// Blob file format:
// [value block 0]
// ...
// [value block M-1] (optional)
// [value blocks index]
// [footer]
// <end_of_file>
//
// There must be at least one value in a blob file. The value blocks are
// identical to the value blocks in sstables.
//
// TODO(sumeer): for BlobFileWriter:
// - don't buffer all value blocks in-memory, unlike the sstable case, since
//   we can compress and write a value block as soon as it is complete.
// - when not buffering in-memory, do the writes on a separate goroutine, like
//   sstable writer does with write_queue.go.
//
// Since the blob file start offset of the value block is known when the key
// is being written to the sst, we could store it in the value handle instead
// of writing and retrieving the blockOffset from the value blocks index. This
// is probably not a good choice since (a) it can't be done if we start doing
// parallel compression, (b) we anyway need to read from the value blocks
// index for the value block length, (c) decoding the fixed width encodings in
// the value blocks index is cheaper than varint decoding in the value handle,
// (d) we would pay the cost of storing the block offset with each value
// handle instead of once in the value blocks index.
//
// Using value blocks is not optimal in terms of retrieval cost for huge
// values (say > 32KB), that can be compressed and stored without being part
// of a value block. Storing them without the value block wrapping would
// eliminate the indirection via the value block index. We expect high cache
// hit rates for the value block index, so the improvement is likely only the
// CPU cost of this indirection.

// BlobFileWriter writes a single blob file.
type BlobFileWriter struct {
	fileNum       base.FileNum
	w             writeCloseSyncer
	opts          BlobFileWriterOptions
	vbw           *valueBlockWriter
	buf           [blobFileFooterLength + blobValueHandleMaxLen]byte
	blobValueSize uint64
	fileSize      uint64
}

var blobFileWriterPool = sync.Pool{
	New: func() interface{} {
		return &BlobFileWriter{}
	},
}

// BlobFileWriterOptions are used to configure the BlobFileWriter.
type BlobFileWriterOptions struct {
	BlockSize          int
	BlockSizeThreshold int
	Compression
	ChecksumType
}

// NewBlobFileWriter constructs a new writer for a blob file.
func NewBlobFileWriter(
	fileNum base.FileNum, writer writeCloseSyncer, opts BlobFileWriterOptions,
) *BlobFileWriter {
	w := blobFileWriterPool.Get().(*BlobFileWriter)
	*w = BlobFileWriter{
		fileNum: fileNum,
		w:       writer,
		opts:    opts,
		vbw: newValueBlockWriter(
			opts.BlockSize, opts.BlockSizeThreshold, opts.Compression, opts.ChecksumType, func(int) {}),
	}
	return w
}

// AddValue stores the value v in the blob file. The long attribute la, and
// the location in the file is used to construct a BlobValueHandle, which is
// then encoded and returned.
func (w *BlobFileWriter) AddValue(v []byte, la base.LongAttribute) ([]byte, error) {
	w.blobValueSize += uint64(len(v))
	vh, err := w.vbw.addValue(v)
	if err != nil {
		return nil, err
	}
	bvh := BlobValueHandle{
		ValueLen:      uint32(len(v)),
		LongAttribute: la,
		FileNum:       w.fileNum,
		BlockNum:      vh.blockNum,
		OffsetInBlock: vh.offsetInBlock,
	}
	n := encodeBlobValueHandle(w.buf[:], bvh)
	return w.buf[:n], nil
}

// EstimatedSize is the total size of the value blocks written to the blob
// file. It can be used in deciding when to rollover to a new blob file or a
// new sst.
func (w *BlobFileWriter) EstimatedSize() uint64 {
	return w.vbw.totalBlockBytes
}

// BlobValueSize is the total uncompressed size of the values written to the
// blob file.
func (w *BlobFileWriter) BlobValueSize() uint64 {
	return w.blobValueSize
}

// Flush is called to finish writing the blob file.
func (w *BlobFileWriter) Flush() error {
	vbiBH, stats, err := w.vbw.finish(w.w, 0)
	if err != nil {
		return err
	}
	footer := blobFileFooter{
		format:             blobFileFormatV1,
		checksum:           w.vbw.checksummer.checksumType,
		valueBlocksIndexBH: vbiBH,
	}
	encodedFooter := footer.encode(w.buf[:])
	w.fileSize = stats.valueBlocksAndIndexSize + uint64(len(encodedFooter))
	_, err = w.w.Write(encodedFooter)
	if err == nil {
		err = w.w.Sync()
	}
	if err == nil {
		err = w.w.Close()
		w.w = nil
	}
	releaseValueBlockWriter(w.vbw)
	return err
}

// FileSize must be called after Flush and returns the size of the file.
func (w *BlobFileWriter) FileSize() uint64 {
	return w.fileSize
}

// Close is used to close the BlobFileWriter. Must be called to ensure file
// descriptors etc. are closed.
func (w *BlobFileWriter) Close() {
	if w.w != nil {
		w.w.Close()
	}
	*w = BlobFileWriter{}
	blobFileWriterPool.Put(w)

}

// blobFileReader is analogous to the sstable Reader, but for blob files.
type blobFileReader struct {
	opts    BlobFileReaderOptions
	file    ReadableFile
	fileNum base.FileNum
	footer  blobFileFooter
	err     error
}

var _ AbstractReaderForVBR = &blobFileReader{}

// BlobFileReaderOptions ...
type BlobFileReaderOptions struct {
	Cache   *cache.Cache
	CacheID uint64
}

func newBlobFileReader(
	f ReadableFile, fileNum base.FileNum, o BlobFileReaderOptions,
) (*blobFileReader, error) {
	r := &blobFileReader{
		opts:    o,
		file:    f,
		fileNum: fileNum,
	}
	if r.opts.Cache == nil {
		panic("nil cache")
	}
	r.opts.Cache.Ref()
	if f == nil {
		r.err = errors.New("pebble/table: nil file")
		return nil, r.close()
	}
	if err := r.footer.read(f); err != nil {
		r.err = err
		return nil, r.close()
	}
	return r, nil
}

func (r *blobFileReader) close() error {
	r.opts.Cache.Unref()
	if r.file != nil {
		err := r.file.Close()
		r.file = nil
		r.err = firstError(r.err, err)
	}
	if r.err != nil {
		return r.err
	}
	// Make any future calls to blobFileReader methods return an error.
	r.err = errReaderClosed
	return nil
}

// readBlockFromVBR is used to read blocks from a blob file.
func (r *blobFileReader) readBlockForVBR(
	bh BlockHandle, stats *base.InternalIteratorStats,
) (cache.Handle, error) {
	if h := r.opts.Cache.Get(r.opts.CacheID, r.fileNum, bh.Offset); h.Get() != nil {
		if stats != nil {
			stats.BlockBytes += bh.Length
			stats.BlockBytesInCache += bh.Length
		}
		return h, nil
	}
	file := r.file
	v := r.opts.Cache.Alloc(int(bh.Length + blockTrailerLen))
	b := v.Buf()
	if _, err := file.ReadAt(b, int64(bh.Offset)); err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, err
	}

	if err := checkChecksum(r.footer.checksum, b, bh, r.fileNum); err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, err
	}

	typ := blockType(b[bh.Length])
	b = b[:bh.Length]
	v.Truncate(len(b))

	decoded, err := decompressBlock(r.opts.Cache, typ, b)
	if decoded != nil {
		r.opts.Cache.Free(v)
		v = decoded
	} else if err != nil {
		r.opts.Cache.Free(v)
		return cache.Handle{}, err
	}
	if stats != nil {
		stats.BlockBytes += bh.Length
	}

	h := r.opts.Cache.Set(r.opts.CacheID, r.fileNum, bh.Offset, v)
	return h, nil
}

func (r *blobFileReader) getValueBlocksIndexHandle() valueBlocksIndexHandle {
	return r.footer.valueBlocksIndexBH
}

var _ blobFileReaderInterface = &blobFileReader{}

// blobFileReaderInterface abstracts blobFileReader. A cache provided
// implementation would wrap close() and only call blobFileReader.close() when
// the reader has been evicted from the cache and the last reference is
// calling close().
type blobFileReaderInterface interface {
	AbstractReaderForVBR
	getValueBlocksIndexHandle() valueBlocksIndexHandle
	close() error
}

// ProviderOfReaderForBlobFiles returns a blobFileReaderInterface for any blob
// FileNum. It is thread-safe. This is expected to be implemented by a cache
// of blobFileReaders.
type ProviderOfReaderForBlobFiles interface {
	getBlobFileReader(fileNum base.FileNum) (blobFileReaderInterface, error)
}

// TODO(sumeer): clock-pro cache for blobFileReader, analogous to TableCache.
// Make tableCacheShard generic and move specialized sstable logic into the
// specialization.

// BlobFileReaderCache is an LRU cache of blobFileReaders.
type BlobFileReaderCache struct {
	opts BlobFileReaderCacheOptions
	mu   struct {
		sync.Mutex
		readers   map[base.FileNum]*cacheValue
		valueList cacheValue
	}
}

var _ ProviderOfReaderForBlobFiles = &BlobFileReaderCache{}

// BlobFileReaderCacheOptions ...
type BlobFileReaderCacheOptions struct {
	// Dirname contains the blob files.
	Dirname       string
	// FS is used to open the blob files.
	FS            vfs.FS
	// ReaderOptions is used when creating blobFileReaders.
	ReaderOptions BlobFileReaderOptions
	// MaxReaders is the maximum cache size.
	MaxReaders    int
}

// NewBlobFileReaderCache ...
func NewBlobFileReaderCache(opts BlobFileReaderCacheOptions) *BlobFileReaderCache {
	// TODO(sumeer): require Cache to be non-nil since this path is only being
	// supported for tests and it is safer to not support it.
	if opts.ReaderOptions.Cache == nil {
		opts.ReaderOptions.Cache = cache.New(0)
		opts.ReaderOptions.CacheID = opts.ReaderOptions.Cache.NewID()
	} else {
		opts.ReaderOptions.Cache.Ref()
	}
	c := &BlobFileReaderCache{
		opts: opts,
	}
	c.mu.readers = map[base.FileNum]*cacheValue{}
	c.mu.valueList.links.next = &c.mu.valueList
	c.mu.valueList.links.prev = &c.mu.valueList
	return c
}

// Close closes the cache.
func (c *BlobFileReaderCache) Close() {
	c.opts.ReaderOptions.Cache.Unref()
	c.mu.Lock()
	for fileNum, cv := range c.mu.readers {
		delete(c.mu.readers, fileNum)
		c.mu.valueList.remove(cv)
		cv.close()
	}
}

// getBlobFileReader implements ProviderOfReaderForBlobFiles.
func (c *BlobFileReaderCache) getBlobFileReader(
	fileNum base.FileNum,
) (blobFileReaderInterface, error) {
	c.mu.Lock()
	cv := c.mu.readers[fileNum]
	if cv != nil {
		c.mu.valueList.moveToEnd(cv)
		cv.refs.Add(1)
		c.mu.Unlock()
		return cv, nil
	}
	c.mu.Unlock()

	fileName := base.MakeFilepath(c.opts.FS, c.opts.Dirname, base.FileTypeBlob, fileNum)
	f, err := c.opts.FS.Open(fileName, vfs.RandomReadsOption)
	if err != nil {
		return nil, err
	}
	bfr, err := newBlobFileReader(f, fileNum, c.opts.ReaderOptions)
	if err != nil {
		return nil, err
	}
	newCV := &cacheValue{
		reader: bfr,
		cache:  c,
	}
	newCV.refs.Add(1)
	c.mu.Lock()
	cv = c.mu.readers[fileNum]
	if cv != nil {
		cv.refs.Add(1)
		c.mu.Unlock()
		newCV.reader.close()
		return cv, nil
	}
	c.mu.readers[fileNum] = newCV
	c.mu.valueList.insertEnd(newCV)
	newCV.refs.Add(1)
	var evictCV *cacheValue
	if len(c.mu.readers) > c.opts.MaxReaders {
		evictCV = c.mu.valueList.popFront()
		delete(c.mu.readers, evictCV.reader.fileNum)
	}
	c.mu.Unlock()
	if evictCV != nil {
		evictCV.close()
	}
	return newCV, nil
}

// cacheValue is the type of the values stored in the BlobFileReaderCache. It
// implements blobFileReaderInterface.
type cacheValue struct {
	reader *blobFileReader
	refs   atomic.Int64
	cache  *BlobFileReaderCache
	links  struct {
		next *cacheValue
		prev *cacheValue
	}
}

var _ blobFileReaderInterface = &cacheValue{}

func (l *cacheValue) readBlockForVBR(
	bh BlockHandle, stats *base.InternalIteratorStats,
) (_ cache.Handle, _ error) {
	return l.reader.readBlockForVBR(bh, stats)
}

func (l *cacheValue) getValueBlocksIndexHandle() valueBlocksIndexHandle {
	return l.reader.getValueBlocksIndexHandle()
}

func (l *cacheValue) close() error {
	var err error
	if l.refs.Add(-1) == 0 {
		err = l.reader.close()
	}
	return err
}

func (l *cacheValue) insertEnd(cv *cacheValue) {
	cv.links.next = l
	cv.links.prev = l.links.prev
	l.links.prev.links.next = cv
	l.links.prev = cv
}

func (l *cacheValue) moveToEnd(cv *cacheValue) {
	l.remove(cv)
	l.insertEnd(cv)
}

func (l *cacheValue) empty() bool {
	return l.links.prev == l
}

func (l *cacheValue) popFront() *cacheValue {
	if l.empty() {
		panic("nothing in cache")
	}
	cv := l.links.next
	l.remove(cv)
	return cv
}

func (l *cacheValue) remove(cv *cacheValue) {
	if l == cv {
		panic("cannot remove")
	}
	prev := cv.links.prev
	next := cv.links.next
	prev.links.next = next
	next.links.prev = prev
}

// blobFileReaderProvider implements ReaderProvider for a particular FileNum.
// It is needed by valueBlockReader, which has no awareness of what file it is
// operating on (or whether it is working with a blob file or the value blocks
// in a sstable).
type blobFileReaderProvider struct {
	// Implemented by BlobFileReaderCache.
	readersProvider ProviderOfReaderForBlobFiles
	fileNum         base.FileNum
	// Implemented by cacheValue.
	reader          blobFileReaderInterface
}

var _ ReaderProvider = &blobFileReaderProvider{}

// GetReader implements ReaderProvider.
func (rp *blobFileReaderProvider) GetReader() (r AbstractReaderForVBR, err error) {
	return rp.getReader()
}

// getReader is like GetReader, but returns a blobFileReaderInterface.
func (rp *blobFileReaderProvider) getReader() (r blobFileReaderInterface, err error) {
	if rp.reader != nil {
		panic("blobFileReaderProvider.reader is non-nil")
	}
	rp.reader, err = rp.readersProvider.getBlobFileReader(rp.fileNum)
	return rp.reader, err
}

// Close implements ReaderProvider.
func (rp *blobFileReaderProvider) Close() {
	if rp.reader == nil {
		panic("blobFileReaderProvider.reader is nil")
	}
	_ = rp.reader.close()
	rp.reader = nil
}

// blobValueReader is created by an iterator that has one or more references
// to blobs in blob files. This is the equivalent to the valueBlockReader in
// the iterator for reading from value blocks. It implements the
// base.ValueFetcher interface, and can fetch from any blob file. The reason
// it is scoped to the lifetime of a sstable iterator is that calling close()
// on this transitions it to a less efficient mode for fetching (see the
// memory management discussion in lazy_value.go and P2).
type blobValueReader struct {
	provider ProviderOfReaderForBlobFiles
	stats    *base.InternalIteratorStats

	// fetchCount is used for assigning cachedValueBlockReader.lastFetchCount,
	// for LRU eviction.
	fetchCount uint64
	// Up to 3 valueBlockReaders are cached. We expect that the fanout from a
	// single sstable to blob files is low, and more importantly there will be
	// high locality in that sequences of keys will typically access <= 3 value
	// blocks. We measured cache hit rates when running TestWriteAmpWithBlobs,
	// with initCompactionBlobFileState using a depthThresholdOverall = 10. The
	// cache hit rates were:
	//
	// - up to 3 vbrs: 96.6%
	// - up to 2 vbrs: 90.6%
	// - up to 1 vbr: 73.7%
	//
	// So 3 seems sufficient.
	//
	// Caching is especially beneficial when the corresponding sstable iterator
	// has not closed the blobValueReader since we can return values from Fetch
	// that are backed by the corresponding block, and the valueBlockReaders
	// retain the latest decompressed cache block for efficient reads when there
	// is locality of access. Once the blobValueReader is closed, the cached
	// valueBlockReaders are also in closed state and the benefit of this cache
	// is limited to avoiding repeated memory allocations in constructing a
	// valueBlockReader for each Fetch.
	//
	// If there is a cachedValueBlockReader.fileNum == 0, that and subsequent
	// elements in this array are not populated. That is, we fill the cache
	// starting from index 0.
	vbrs        [3]cachedValueBlockReader
	// NB: we don't use the LazyFetcher embedded in the valueBlockReaders.
	lazyFetcher base.LazyFetcher
	closed      bool
}

func newBlobValueReader(
	provider ProviderOfReaderForBlobFiles, stats *base.InternalIteratorStats,
) *blobValueReader {
	return &blobValueReader{
		provider: provider,
		stats:    stats,
	}
}

type cachedValueBlockReader struct {
	// The cache key. It is > 0 for a valid cache entry.
	fileNum base.FileNum
	// INVARIANT: fileNum > 0 <=> vbr != nil.
	vbr     *valueBlockReader
	// When vbr is not closed (i.e., !blobValueReader.closed), the
	// initialization of valueBlockReader requires an AbstractReaderForVBR,
	// valueBlocksIndexHandle, and a ReaderProvider. The ReaderProvider is
	// implemented by blobFileReaderProvider, which will be called after the vbr
	// is closed and Fetch is called. Since we know that the ReaderProvider
	// methods will not be used until after vbr is closed, we use the
	// ReaderProvider.getReader to get us a blobFileReaderInterface that can be
	// used as an AbstractReaderForVBR. The blobFileReaderInterface also is used
	// to get the valueBlocksIndexHandle. But we need to remember to close it
	// before the valueBlockReader starts to use the ReaderProvider. So we stash
	// it here.
	//
	// TODO(sumeer): the above life-cycle is brittle. Clean it up.
	readerProviderToClose ReaderProvider
	// For LRU eviction.
	lastFetchCount        uint64
}

// Called by the sstable iterator when it finds a value that points to a blob,
// i.e., isBlobValueHandle() returns true. handle includes the 1 byte prefix.
func (r *blobValueReader) getLazyValueForPrefixAndValueHandle(handle []byte) base.LazyValue {
	fetcher := &r.lazyFetcher
	valLen, h := decodeLenFromValueHandle(handle[1:])
	longAttr, h := decodeLongAttributeFromValueHandle(h)
	*fetcher = base.LazyFetcher{
		Fetcher: r,
		Attribute: base.AttributeAndLen{
			ValueLen:               int32(valLen),
			ShortAttribute:         getShortAttribute(valuePrefix(handle[0])),
			LongAttributeExtracted: true,
			LongAttribute:          longAttr,
		},
	}
	if r.stats != nil {
		r.stats.BlobPointValue.Count++
		r.stats.BlobPointValue.ValueBytes += uint64(valLen)
	}
	return base.LazyValue{
		ValueOrHandle: h,
		Fetcher:       fetcher,
	}
}

// Fetch implements base.ValueFetcher.
func (r *blobValueReader) Fetch(
	handle []byte, valLen int32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	fn, n := binary.Uvarint(handle)
	if n <= 0 {
		panic("")
	}
	fileNum := base.FileNum(fn)
	handle = handle[n:]
	vbr, err := r.getCachedValueBlockReader(fileNum)
	if err != nil {
		return nil, false, err
	}
	r.fetchCount++
	vbr.lastFetchCount = r.fetchCount
	val, callerOwned, err = vbr.vbr.Fetch(handle, valLen, buf)
	if r.stats != nil {
		r.stats.BlobPointValue.ValueBytesFetched += uint64(len(val))
	}
	return
}

func (r *blobValueReader) getCachedValueBlockReader(
	fileNum base.FileNum,
) (*cachedValueBlockReader, error) {
	oldestFetchIndex := -1
	oldestFetchCount := uint64(math.MaxUint64)
	i := 0
	n := len(r.vbrs)
	for ; i < n; i++ {
		fn := r.vbrs[i].fileNum
		if fn == 0 {
			break
		}
		if fn == fileNum {
			if r.stats != nil {
				r.stats.BlobPointValue.CachedVBRHit++
			}
			return &r.vbrs[i], nil
		}
		if r.vbrs[i].lastFetchCount < oldestFetchCount {
			oldestFetchIndex = i
			oldestFetchCount = r.vbrs[i].lastFetchCount
		}
	}
	if i >= n {
		if r.stats != nil {
			r.stats.BlobPointValue.CacheVBREvictCount++
			r.stats.BlobPointValue.CachedVBRMissNotInit++
		}
		// Replace the cached index i.
		i = oldestFetchIndex
		if !r.closed {
			r.vbrs[i].readerProviderToClose.Close()
			r.vbrs[i].vbr.close()
		}
		r.vbrs[i] = cachedValueBlockReader{}
	} else {
		if r.stats != nil {
			r.stats.BlobPointValue.CachedVBRMissInit++
		}
	}
	rp := &blobFileReaderProvider{
		readersProvider: r.provider,
		fileNum:         fileNum,
	}
	reader, err := rp.getReader()
	if err != nil {
		return nil, err
	}
	vbih := reader.getValueBlocksIndexHandle()
	readerProviderToClose := rp
	if r.closed {
		reader = nil
		rp.Close()
		readerProviderToClose = nil
	}
	var vbr *valueBlockReader
	if r.closed {
		vbr = newClosedValueBlockReader(rp, vbih, r.stats)
	} else {
		vbr = &valueBlockReader{
			bpOpen: reader,
			rp:     rp,
			vbih:   vbih,
			stats:  r.stats,
		}
	}
	r.vbrs[i] = cachedValueBlockReader{
		fileNum:               fileNum,
		vbr:                   vbr,
		readerProviderToClose: readerProviderToClose,
		lastFetchCount:        0,
	}
	return &r.vbrs[i], nil
}

func (r *blobValueReader) close() {
	if r.closed {
		return
	}
	r.closed = true
	for i := range r.vbrs {
		if r.vbrs[i].fileNum > 0 {
			r.vbrs[i].readerProviderToClose.Close()
			r.vbrs[i].vbr.close()
			r.vbrs[i].readerProviderToClose = nil
		} else {
			break
		}
	}
}

const blobValueHandleMaxLen = 5*3 + 10 + base.LongAttributeMaxLen + 1

type BlobValueHandle struct {
	ValueLen      uint32
	LongAttribute base.LongAttribute
	FileNum       base.FileNum
	BlockNum      uint32
	OffsetInBlock uint32
}

// encoding is partially compatible with valueHandle in that valueLen is
// encoded first, so decodeLenFromValueHandle can be used. Then
// decodeLongAttributeFromValueHandle should be used. Followed by
// decodeRemainingBlobValueHandleForTesting.
func encodeBlobValueHandle(dst []byte, v BlobValueHandle) int {
	n := 0
	n += binary.PutUvarint(dst[n:], uint64(v.ValueLen))
	dst[n] = byte(len(v.LongAttribute))
	n++
	n += copy(dst[n:], v.LongAttribute)
	n += binary.PutUvarint(dst[n:], uint64(v.FileNum))
	n += binary.PutUvarint(dst[n:], uint64(v.BlockNum))
	n += binary.PutUvarint(dst[n:], uint64(v.OffsetInBlock))
	return n
}

func decodeLongAttributeFromValueHandle(src []byte) ([]byte, []byte) {
	attrLen := int(src[0])
	return src[1 : 1+attrLen], src[1+attrLen:]
}

// Only populates the suffix of (FileNum, BlockNum, OffsetInBlock).
func decodeRemainingBlobValueHandleForTesting(src []byte) BlobValueHandle {
	var bvh BlobValueHandle
	v, n := binary.Uvarint(src)
	if n <= 0 {
		panic("")
	}
	bvh.FileNum = base.FileNum(v)
	src = src[n:]
	vh := decodeRemainingValueHandle(src)
	bvh.BlockNum = vh.blockNum
	bvh.OffsetInBlock = vh.offsetInBlock
	return bvh
}

// IsValueReferenceToBlob returns true iff the LazyValue references a blob in
// a blob file.
func IsValueReferenceToBlob(lv base.LazyValue) bool {
	if lv.Fetcher == nil {
		return false
	}
	_, ok := lv.Fetcher.Fetcher.(*blobValueReader)
	return ok
}

// GetFileNumFromValueReferenceToBlob is used in compactions to decide whether to rewrite
// the reference or not.
// REQUIRES: IsValueReferenceToBlob
func GetFileNumFromValueReferenceToBlob(lv base.LazyValue) (base.FileNum, error) {
	fn, n := binary.Uvarint(lv.ValueOrHandle)
	if n <= 0 {
		return 0, errors.Errorf("could not parse filenum")
	}
	return base.FileNum(fn), nil
}

// ConstructValueReferenceAndShortAttribute is used in compactions when
// reusing an existing reference. It returns the reference to use and the
// short attribute.
func ConstructValueReferenceAndShortAttribute(
	lv base.LazyValue, buf []byte,
) ([]byte, base.ShortAttribute) {
	if lv.Fetcher == nil || !lv.Fetcher.Attribute.LongAttributeExtracted {
		panic("called with non value reference")
	}
	sa := lv.Fetcher.Attribute.ShortAttribute
	if cap(buf) < blobValueHandleMaxLen {
		buf = make([]byte, blobValueHandleMaxLen)
	} else {
		buf = buf[:cap(buf)]
	}
	n := 0
	n += binary.PutUvarint(buf[n:], uint64(lv.Fetcher.Attribute.ValueLen))
	buf[n] = byte(len(lv.Fetcher.Attribute.LongAttribute))
	n++
	n += copy(buf[n:], lv.Fetcher.Attribute.LongAttribute)
	n += copy(buf[n:], lv.ValueOrHandle)
	return buf[:n], sa
}

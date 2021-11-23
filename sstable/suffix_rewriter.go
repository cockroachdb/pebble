package sstable

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/crc"
)

// RewriteKeySuffixes copies the sstable in the passed reader to out, but with
// each key updated to have the byte suffix `from` replaced with `to`. It is
// required that the SST consist of only SETs, and that every key have `from` as
// its suffix, and furthermore the length of from must match the suffix length
// designated by the Split() function if one is set in the WriterOptions.
//
// If concurrency is zero, a new Writer configured with the passed options is
// simply passed each key from the Reader, and its result is returned, with the
// only constraints being that the input SST is entirely SETs and that each has
// the suffix from. In this implementation, all filters and properties are
// re-derived just as if the new SST had been constructed from arbitrary
// external keys.
//
// However if concurrency is 1 or more, a specialized implementation is used, in
// which data blocks are rewritten in parallel by `concurrency` workers and then
// assembled into a final SST, with filters copied over from the original SST
// unmodified, and block and table properties are only minimally computed. This
// is a much cheaper and faster implementation, but its use comes with some
// limitations:
//
// 1) to be able to copy unmodified filters, it is required that the writer's
// comparer have a Split function and that the replaced suffix of each key not
// overlap with the prefix determined by it designates (i.e. no part of the key
// added to the filter is being modified).
//
// 2) any block and table property collectors configured in the WriterOptions
// must implement SuffixReplaceableTableCollector/SuffixReplaceableBlockCollector.
func RewriteKeySuffixes(
	r *Reader,
	out writeCloseSyncer, o WriterOptions,
	from, to []byte,
	concurrency int,
) (*WriterMetadata, error) {
	if concurrency == 0 {
		return replaceSuffixWithReaderWriterLoop(r, out, o, from, to)
	}
	return replaceSuffixInBlocks(r, out, o, from, to, concurrency)
}

func replaceSuffixWithReaderWriterLoop(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte,
) (*WriterMetadata, error) {
	w := NewWriter(out, o)
	for _, c := range w.propCollectors {
		if _, ok := c.(SuffixReplaceableTableCollector); !ok {
			return nil, errors.Errorf("property collector %s does not support suffix replacement", c.Name())
		}
	}
	for _, c := range w.blockPropCollectors {
		if _, ok := c.(SuffixReplaceableBlockCollector); !ok {
			return nil, errors.Errorf("block property collector %s does not support suffix replacement", c.Name())
		}
	}

	i, err := r.NewIter(nil, nil)
	if err != nil {
		return nil, err
	}
	defer i.Close()

	k, v := i.First()
	var scratch InternalKey
	for k != nil {
		if k.Kind() != InternalKeyKindSet {
			return nil, errors.New("invalid key type")
		}
		if !bytes.HasSuffix(k.UserKey, from) {
			return nil, errors.New("mismatched key suffix")
		}
		if w.split != nil {
			if s := len(k.UserKey) - w.split(k.UserKey); s != len(from) {
				return nil, errors.Errorf("mismatched replacement (%d) vs Split (%d) suffix", len(from), s)
			}
		}
		scratch.UserKey = append(scratch.UserKey[:0], k.UserKey[:len(k.UserKey)-len(from)]...)
		scratch.UserKey = append(scratch.UserKey, to...)
		scratch.Trailer = k.Trailer

		if w.addPoint(scratch, v); err != nil {
			return nil, err
		}
		k, v = i.Next()
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return &w.meta, nil
}

// replaceSuffixInBlocks is described in the comment on RewriteKeySuffixes.
//
// TODO(dt): rework block reading to avoid cache.Set contention.
func replaceSuffixInBlocks(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte, concurrency int,
) (*WriterMetadata, error) {
	w := NewWriter(out, o)

	for _, c := range w.propCollectors {
		if _, ok := c.(SuffixReplaceableTableCollector); !ok {
			return nil, errors.Errorf("property collector %s does not support suffix replacement", c.Name())
		}
	}
	for _, c := range w.blockPropCollectors {
		if _, ok := c.(SuffixReplaceableBlockCollector); !ok {
			return nil, errors.Errorf("block property collector %s does not support suffix replacement", c.Name())
		}
	}

	l, err := r.Layout()
	if err != nil {
		return nil, errors.Wrap(err, "reading layout")
	}

	if err := rewriteDataBlocksToWriter(r, w, l.Data, from, to, concurrency); err != nil {
		return nil, errors.Wrap(err, "rewriting data blocks")
	}

	// Copy over the filter block if it exists (if it does, copyDataBlocks will
	// already have ensured this is valid).
	if w.filter != nil {
		if l.Filter.Length == 0 {
			return nil, errors.New("input table has no filter")
		}
		filterBlock, _, err := readBlockBuf(r, l.Filter, nil)
		if err != nil {
			return nil, errors.Wrap(err, "reading filter")
		}
		w.filter = copyFilterWriter{
			origPolicyName: w.filter.policyName(), origMetaName: w.filter.metaName(), data: filterBlock,
		}
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return w.Metadata()
}

var errBadKind = errors.New("key does not have expected kind (set)")

type blockWithSpan struct {
	start, end InternalKey
	data       []byte
}

func asyncRewriteBlocks(
	r *Reader, restartInterval int, checksumType ChecksumType, compression Compression,
	input []BlockHandleWithProperties, output []blockWithSpan,
	totalWorkers, worker int,
	from, to []byte,
	errCh chan error,
	g *sync.WaitGroup,
) {
	err := rewriteBlocks(
		r, restartInterval, checksumType, compression, input, output, totalWorkers, worker, from, to,
	)
	if err != nil {
		errCh <- err
	}
	g.Done()
}

func readBlockBuf(r *Reader, bh BlockHandle, buf []byte) ([]byte, []byte, error) {
	raw := r.file.(memReader).b[bh.Offset : bh.Offset+bh.Length+blockTrailerLen]
	if err := checkChecksum(r.checksumType, raw, bh, 0); err != nil {
		return nil, buf, err
	}
	typ := blockType(raw[bh.Length])
	raw = raw[:bh.Length]
	if typ == noCompressionBlockType {
		return raw, buf, nil
	}
	decompressedLen, prefix, err := decompressedLen(typ, raw)
	if err != nil {
		return nil, buf, err
	}
	if cap(buf) < decompressedLen {
		buf = make([]byte, decompressedLen)
	}
	res, err := decompressInto(typ, raw[prefix:], buf[:decompressedLen])
	return res, buf, err
}

func rewriteBlocks(
	r *Reader, restartInterval int, checksumType ChecksumType, compression Compression,
	input []BlockHandleWithProperties, output []blockWithSpan,
	totalWorkers, worker int,
	from, to []byte,
) error {
	bw := blockWriter{
		restartInterval: restartInterval,
	}
	var compressionBuf []byte
	var xxHasher *xxhash.Digest
	if checksumType == ChecksumTypeXXHash {
		xxHasher = xxhash.New()
	}

	var blockAlloc []byte
	var keySlab []byte
	var scratch InternalKey

	var inputBlock, inputBlockBuf []byte

	iter := &blockIter{}

	for i := worker; i < len(input); i += totalWorkers {
		bh := input[i]

		var err error
		inputBlock, inputBlockBuf, err = readBlockBuf(r, bh.BlockHandle, inputBlockBuf)
		if err != nil {
			return err
		}
		if err := iter.init(r.Compare, inputBlock, r.Properties.GlobalSeqNum); err != nil {
			return err
		}

		if cap(bw.restarts) < int(iter.restarts) {
			bw.restarts = make([]uint32, 0, iter.restarts)
		}
		if cap(bw.buf) == 0 {
			bw.buf = make([]byte, 0, len(inputBlock))
		}
		if cap(bw.restarts) < int(iter.numRestarts) {
			bw.restarts = make([]uint32, 0, iter.numRestarts)
		}

		for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
			if key.Kind() != InternalKeyKindSet {
				return errBadKind
			}
			if !bytes.HasSuffix(iter.Key().UserKey, from) {
				return errors.Errorf("key %q does not have expected suffix %q", iter.Key().UserKey, from)
			}
			prefixLen := len(key.UserKey) - len(from)
			newLen := prefixLen + len(to)
			if cap(scratch.UserKey) < newLen {
				scratch.UserKey = make([]byte, 0, len(key.UserKey)*2+len(to)-len(from))
			}

			scratch.Trailer = key.Trailer
			scratch.UserKey = scratch.UserKey[:newLen]
			copy(scratch.UserKey, key.UserKey[:prefixLen])
			copy(scratch.UserKey[prefixLen:], to)

			bw.add(scratch, iter.Value())
			if output[i].start.UserKey == nil {
				keySlab, output[i].start = cloneKeyWithBuf(scratch, keySlab)
			}
		}
		*iter = iter.resetForReuse()

		keySlab, output[i].end = cloneKeyWithBuf(scratch, keySlab)
		b := bw.finish()

		blockType, compressed := compressBlock(compression, b, compressionBuf)
		if blockType != noCompressionBlockType && cap(compressed) > cap(compressionBuf) {
			compressionBuf = compressed[:cap(compressed)]
		}

		if len(compressed) < len(b)-len(b)/8 {
			b = compressed
		} else {
			blockType = noCompressionBlockType
		}

		sz := len(b) + blockTrailerLen
		if cap(blockAlloc) < sz {
			blockAlloc = make([]byte, sz*512)
		}
		output[i].data = blockAlloc[:sz:sz]
		blockAlloc = blockAlloc[sz:]
		copy(output[i].data, b)

		trailer := output[i].data[len(b):]
		trailer[0] = byte(blockType)

		// Calculate the checksum.
		var checksum uint32
		switch checksumType {
		case ChecksumTypeCRC32c:
			checksum = crc.New(b).Update(trailer[:1]).Value()
		case ChecksumTypeXXHash64:
			xxHasher.Reset()
			xxHasher.Write(b)
			xxHasher.Write(trailer[:1])
			checksum = uint32(xxHasher.Sum64())
		default:
			return errors.Newf("unsupported checksum type: %d", checksumType)
		}
		binary.LittleEndian.PutUint32(trailer[1:5], checksum)
	}
	return nil
}

func rewriteDataBlocksToWriter(r *Reader, w *Writer, data []BlockHandleWithProperties, from, to []byte, concurrency int) error {
	blocks := make([]blockWithSpan, len(data))

	if w.filter != nil {
		if r.Properties.FilterPolicyName != w.filter.policyName() {
			return errors.New("mismatched filters")
		}
		if w.split == nil {
			return errors.New("no splitter, cannot copy filters")
		}
		if was, is := r.Properties.ComparerName, w.props.ComparerName; was != is {
			return errors.Errorf("mismatched Comparer %s vs %s, replacement requires same splitter to copy filters", was, is)
		}
	}

	g := &sync.WaitGroup{}
	g.Add(concurrency)
	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		worker := i
		go asyncRewriteBlocks(
			r, w.block.restartInterval, w.checksumType, w.compression, data, blocks, concurrency, worker, from, to, errCh, g,
		)
	}
	g.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
	}

	if w.split != nil {
		if len(blocks[0].start.UserKey)-w.split(blocks[0].start.UserKey) != len(from) {
			return errors.New("cannot replace outside split suffix")
		}
	}

	for _, p := range w.propCollectors {
		if err := p.(SuffixReplaceableTableCollector).UpdateKeySuffixes(r.Properties.UserProperties, from, to); err != nil {
			return err
		}
	}

	var decoder blockPropertiesDecoder
	var oldShortIDs []shortID
	var oldProps [][]byte
	if len(w.blockPropCollectors) > 0 {
		oldProps = make([][]byte, len(w.blockPropCollectors))
		oldShortIDs = make([]shortID, math.MaxUint8)
		for i, p := range w.blockPropCollectors {
			if prop, ok := r.Properties.UserProperties[p.Name()]; ok {
				was, is := shortID(byte(prop[0])), shortID(i)
				oldShortIDs[was] = is
			}
		}
	}

	for i := range blocks {
		// Write the rewritten block to the file.
		n, err := w.writer.Write(blocks[i].data)
		if err != nil {
			return err
		}

		bh := BlockHandle{Offset: w.meta.Size, Length: uint64(n) - blockTrailerLen}
		// Update the overall size.
		w.meta.Size += uint64(n)

		// Load any previous values for our prop collectors into oldProps.
		for i := range oldProps {
			oldProps[i] = nil
		}
		decoder.props = data[i].Props
		for !decoder.done() {
			id, val, err := decoder.next()
			if err != nil {
				return err
			}
			oldProps[oldShortIDs[id]] = val
		}

		for i, p := range w.blockPropCollectors {
			if err := p.(SuffixReplaceableBlockCollector).UpdateKeySuffixes(oldProps[i], from, to); err != nil {
				return err
			}
		}

		var bhp BlockHandleWithProperties
		if bhp, err = w.maybeAddBlockPropertiesToBlockHandle(bh); err != nil {
			return err
		}
		var nextKey InternalKey
		if i+1 < len(blocks) {
			nextKey = blocks[i+1].start
		}
		if err = w.addIndexEntry(blocks[i].end, nextKey, bhp); err != nil {
			return err
		}
	}

	w.meta.updateSeqNum(blocks[0].start.SeqNum())
	w.props.NumEntries = r.Properties.NumEntries
	w.props.RawKeySize = r.Properties.RawKeySize
	w.props.RawValueSize = r.Properties.RawValueSize
	w.meta.SmallestPoint = blocks[0].start
	w.meta.LargestPoint = blocks[len(blocks)-1].end
	return nil
}

type copyFilterWriter struct {
	origMetaName   string
	origPolicyName string
	data           []byte
}

func (copyFilterWriter) addKey(key []byte)         { panic("unimplemented") }
func (c copyFilterWriter) finish() ([]byte, error) { return c.data, nil }
func (c copyFilterWriter) metaName() string        { return c.origMetaName }
func (c copyFilterWriter) policyName() string      { return c.origPolicyName }

// NewMemReader opens a reader over the SST stored in the passed []byte.
func NewMemReader(sst []byte, o ReaderOptions) (*Reader, error) {
	return NewReader(memReader{sst, bytes.NewReader(sst), sizeOnlyStat(int64(len(sst)))}, o)
}

// memReader is a thin wrapper around a []byte such that it can be passed to an
// sstable.Reader. It supports concurrent use, and does so without locking in
// contrast to the heavier read/write vfs.MemFile.
type memReader struct {
	b []byte
	r *bytes.Reader
	s sizeOnlyStat
}

var _ ReadableFile = memReader{}

// ReadAt implements io.ReaderAt.
func (m memReader) ReadAt(p []byte, off int64) (n int, err error) { return m.r.ReadAt(p, off) }

// Close implements io.Closer.
func (memReader) Close() error { return nil }

// Stat implements ReadableFile.
func (m memReader) Stat() (os.FileInfo, error) { return m.s, nil }

type sizeOnlyStat int64

func (s sizeOnlyStat) Size() int64      { return int64(s) }
func (sizeOnlyStat) IsDir() bool        { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) ModTime() time.Time { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Mode() os.FileMode  { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Name() string       { panic(errors.AssertionFailedf("unimplemented")) }
func (sizeOnlyStat) Sys() interface{}   { panic(errors.AssertionFailedf("unimplemented")) }

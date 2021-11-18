package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
)

// RewriteKeySuffixes copies the content of the passed SSTable Reader to a new
// sstable written to `out` in which the suffix of each key has been replaced.
// The input sstable must consist of only sets and every key must have `from` as
// its suffix -- as determined by the Split function of the Comparer in the
// passed WriterOptions -- the   an sstable to `out` containing  the sstable in
// the passed reader to out, but with each key updated to have its suffix.
//
// If concurrency is zero, a new Writer, configured with the passed options, is
// simply passed each key from the Reader, and its result is returned. In this
// implementation, all filters and properties are re-derived, just as if the new
// SST had been constructed from arbitrary external keys.
//
// However if concurrency is 1 or more, a specialized implementation is used, in
// which data blocks are rewritten in parallel by `concurrency` workers and then
// assembled into a final SST. In this mode, filters copied from the original
// SST unmodified, and block and table properties are only minimally recomputed.
//
// TODO(dt): Currently table and block properties are re-derived by passing each
// property collector just one example key from each block. While this may be
// sufficient for some implementations, it is not correct in the general case.
// Instead, a new API should be added to let existing collectors update their
// value. Until that follow-up change however, this method should only be called
// with >0 concurrency iff the collectors configured are correct when presented
// with just one key per block.
func RewriteKeySuffixes(
	r *Reader,
	out writeCloseSyncer, o WriterOptions,
	from, to []byte,
	concurrency int,
) (*WriterMetadata, error) {
	if o.Comparer == nil || o.Comparer.Split == nil {
		return nil, errors.New("a valid splitter is required to define suffix to replace replace suffix")
	}
	if concurrency == 0 {
		return replaceSuffixWithReaderWriterLoop(r, out, o, from, to)
	}
	return replaceSuffixInBlocks(r, out, o, from, to, concurrency)
}

func replaceSuffixWithReaderWriterLoop(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte,
) (*WriterMetadata, error) {
	w := NewWriter(out, o)
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
		oldSuffix := k.UserKey[r.Split(k.UserKey):]
		if !bytes.Equal(oldSuffix, from) {
			return nil, errors.Errorf("key has suffix %q, expected %q", oldSuffix, from)
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

// replaceSuffixInBlocks is described in the comment on RewriteKeySuffixes.
//
// TODO(dt): rework block reading to avoid cache.Set contention.
func replaceSuffixInBlocks(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte, concurrency int,
) (*WriterMetadata, error) {
	l, err := r.Layout()
	if err != nil {
		return nil, errors.Wrap(err, "reading layout")
	}

	w := NewWriter(out, o)

	if err := rewriteDataBlocksToWriter(r, w, l.Data, from, to, w.split, concurrency); err != nil {
		return nil, errors.Wrap(err, "rewriting data blocks")
	}

	// Copy over the filter block if it exists (if it does, copyDataBlocks will
	// already have ensured this is valid).
	var filterBlock cache.Handle
	if w.filter != nil {
		if l.Filter.Length == 0 {
			return nil, errors.New("input table has no filter")
		}
		filterBlock, err = r.readBlock(l.Filter, nil, nil)
		if err != nil {
			return nil, errors.Wrap(err, "reading filter")
		}
		w.filter = copyFilterWriter{
			origPolicyName: w.filter.policyName(), origMetaName: w.filter.metaName(), data: filterBlock.Get(),
		}
	}

	if err := w.Close(); err != nil {
		filterBlock.Release()
		return nil, err
	}
	filterBlock.Release()

	return w.Metadata()
}

var errBadKind = errors.New("key does not have expected kind (set)")

type blockWithSpan struct {
	start, end InternalKey
	data       []byte
}

func asyncRewriteBlocks(
	r *Reader, restartInterval int, checksumType ChecksumType, compression Compression,
	input []BlockHandle, output []blockWithSpan,
	totalWorkers, worker int,
	from, to []byte,
	split Split,
	errCh chan error,
	g *sync.WaitGroup,
) {
	err := rewriteBlocks(
		r, restartInterval, checksumType, compression, input, output, totalWorkers, worker, from, to, split,
	)
	if err != nil {
		errCh <- err
	}
	g.Done()
}

func rewriteBlocks(
	r *Reader, restartInterval int, checksumType ChecksumType, compression Compression,
	input []BlockHandle, output []blockWithSpan,
	totalWorkers, worker int,
	from, to []byte,
	split Split,
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
	iter := &blockIter{}

	// We'll assume all blocks are _roughly_ equal so round-robin static partition
	// of each worker doing every ith block is probably enough.
	for i := worker; i < len(input); i += totalWorkers {
		bh := input[i]

		h, err := r.readBlock(bh, nil /* transform */, nil /*rs*/)
		if err != nil {
			return err
		}
		inputBlock := h.Get()
		if err := iter.init(r.Compare, inputBlock, r.Properties.GlobalSeqNum); err != nil {
			h.Release()
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

		for key, val := iter.First(); key != nil; key, val = iter.Next() {
			if key.Kind() != InternalKeyKindSet {
				h.Release()
				return errBadKind
			}
			si := split(key.UserKey)
			oldSuffix := key.UserKey[si:]
			if !bytes.Equal(oldSuffix, from) {
				err := errors.Errorf("key has suffix %q, expected %q", oldSuffix, from)
				h.Release()
				return err
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

			bw.add(scratch, val)
			if output[i].start.UserKey == nil {
				keySlab, output[i].start = cloneKeyWithBuf(scratch, keySlab)
			}
		}
		*iter = iter.resetForReuse()

		keySlab, output[i].end = cloneKeyWithBuf(scratch, keySlab)
		h.Release()
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

func rewriteDataBlocksToWriter(r *Reader, w *Writer, data []BlockHandle, from, to []byte, split Split, concurrency int) error {
	blocks := make([]blockWithSpan, len(data))

	if w.filter != nil {
		if r.Properties.FilterPolicyName != w.filter.policyName() {
			return errors.New("mismatched filters")
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
			r, w.dataBlockBuf.dataBlock.restartInterval, w.blockBuf.checksummer.checksumType, w.compression, data, blocks, concurrency, worker, from, to, split, errCh, g,
		)
	}
	g.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
	}

	for _, p := range w.propCollectors {
		// TODO(dt): we're not passing value here. Perhaps we should have a separate
		// method in the interface like AddSingleExampleKeyForTable(key) that an
		// impl could choose to implement or not?
		if err := p.Add(blocks[0].start, nil); err != nil {
			return err
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

		// Pass each block property collector the first key from the block, so that
		// it can collect any properties for the block. This assumes that it'd
		// collect the same property from this one key that it'd collect if passed
		// all keys in the block.
		// TODO(dt): perhaps we should add a method to the collector interface like
		// AddExampleForBlock so that implementations can decide if they are okay
		// with this? also ditto above, no value here.
		for _, p := range w.blockPropCollectors {
			if err := p.Add(blocks[i].start, nil); err != nil {
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
		if err = w.addIndexEntry(blocks[i].end, nextKey, bhp, w.dataBlockBuf.tmp[:]); err != nil {
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

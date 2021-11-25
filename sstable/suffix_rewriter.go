package sstable

import (
	"bytes"
	"math"
	"os"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/cache"
)

// RewriteKeySuffixes copies the content of the passed SSTable Reader to a new
// sstable, written to `out`, in which the suffix `from` has is replaced with
// `to` in every key. The input sstable must consist of only Sets and every key
// must have `from` as its suffix as determined by the Split function of the
// Comparer in the passed WriterOptions.
//
// Data blocks are rewritten in parallel by `concurrency` workers and then
// assembled into a final SST. Filters are copied from the original SST without
// modification as they are not affected by the suffix, while block and table
// properties are only minimally recomputed.
//
// Any block and table property collectors configured in the WriterOptions must
// implement SuffixReplaceableTableCollector/SuffixReplaceableBlockCollector.
//
// TODO(dt): rework block reading to avoid cache.Set contention.
func RewriteKeySuffixes(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte, concurrency int,
) (*WriterMetadata, error) {
	if o.Comparer == nil || o.Comparer.Split == nil {
		return nil, errors.New("a valid splitter is required to define suffix to replace replace suffix")
	}
	if concurrency < 1 {
		return nil, errors.New("concurrency must be >= 1")
	}

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

	if err := rewriteDataBlocksToWriter(r, w, l.Data, from, to, w.split, concurrency); err != nil {
		return nil, errors.Wrap(err, "rewriting data blocks")
	}

	// Copy over the filter block if it exists (rewriteDataBlocksToWriter will
	// already have ensured this is valid if it exists).
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

func rewriteBlocks(
	r *Reader, restartInterval int, checksumType ChecksumType, compression Compression,
	input []BlockHandleWithProperties, output []blockWithSpan,
	totalWorkers, worker int,
	from, to []byte,
	split Split,
) error {
	bw := blockWriter{
		restartInterval: restartInterval,
	}
	buf := blockBuf{checksummer: checksummer{checksumType: checksumType}}
	if checksumType == ChecksumTypeXXHash {
		buf.checksummer.xxHasher = xxhash.New()
	}

	var blockAlloc []byte
	var keyAlloc []byte
	var scratch InternalKey
	iter := &blockIter{}

	// We'll assume all blocks are _roughly_ equal so round-robin static partition
	// of each worker doing every ith block is probably enough.
	for i := worker; i < len(input); i += totalWorkers {
		bh := input[i]

		h, err := r.readBlock(bh.BlockHandle, nil /* transform */, nil /*rs*/)
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
			newLen := si + len(to)
			if cap(scratch.UserKey) < newLen {
				scratch.UserKey = make([]byte, 0, len(key.UserKey)*2+len(to)-len(from))
			}

			scratch.Trailer = key.Trailer
			scratch.UserKey = scratch.UserKey[:newLen]
			copy(scratch.UserKey, key.UserKey[:si])
			copy(scratch.UserKey[si:], to)

			bw.add(scratch, val)
			if output[i].start.UserKey == nil {
				keyAlloc, output[i].start = cloneKeyWithBuf(scratch, keyAlloc)
			}
		}
		*iter = iter.resetForReuse()

		keyAlloc, output[i].end = cloneKeyWithBuf(scratch, keyAlloc)
		h.Release()

		finished := compressAndChecksum(bw.finish(), compression, &buf)

		// copy our finished block into the output buffer.
		sz := len(finished) + blockTrailerLen
		if cap(blockAlloc) < sz {
			blockAlloc = make([]byte, sz*128)
		}
		output[i].data = blockAlloc[:sz:sz]
		blockAlloc = blockAlloc[sz:]
		copy(output[i].data, finished)
		copy(output[i].data[len(finished):], buf.tmp[:blockTrailerLen])
	}
	return nil
}

func rewriteDataBlocksToWriter(
	r *Reader, w *Writer,
	data []BlockHandleWithProperties,
	from, to []byte,
	split Split, concurrency int,
) error {
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
		go func() {
			defer g.Done()
			err := rewriteBlocks(
				r,
				w.dataBlockBuf.dataBlock.restartInterval,
				w.blockBuf.checksummer.checksumType,
				w.compression,
				data,
				blocks,
				concurrency,
				worker,
				from, to,
				split,
			)
			if err != nil {
				errCh <- err
			}
		}()
	}
	g.Wait()
	close(errCh)
	if err, ok := <-errCh; ok {
		return err
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

// RewriteKeySuffixesViaWriter is similar to RewriteKeySuffixes but uses just a
// single loop over the Reader that writes each key to the Writer with the new
// suffix. The is significantly slower than the parallelized rewriter, and does
// more work to rederive filters, props, etc, however re-doing that work makes
// it less restrictive -- props no longer need to
func RewriteKeySuffixesViaWriter(
	r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte,
) (*WriterMetadata, error) {
	if o.Comparer == nil || o.Comparer.Split == nil {
		return nil, errors.New("a valid splitter is required to define suffix to replace replace suffix")
	}

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

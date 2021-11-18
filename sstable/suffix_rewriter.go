package sstable

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/crc"
)

// RewriteKeySuffixes writes an sstable to out containing the content of r but
// with the suffix from replaced with to.
func RewriteKeySuffixes(r *Reader, out writeCloseSyncer, o WriterOptions, from, to []byte, concurrency int) (*WriterMetadata, error) {
	l, err := r.Layout()
	if err != nil {
		return nil, errors.Wrap(err, "reading layout")
	}

	w := NewWriter(out, o)

	if err := rewriteDataBlocksToWriter(r, w, l.Data, from, to, concurrency); err != nil {
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

func cloneKeyWithBuf(k InternalKey, buf []byte) ([]byte, InternalKey) {
	if len(buf) < len(k.UserKey) {
		buf = make([]byte, len(k.UserKey)+512<<10)
	}
	n := copy(buf, k.UserKey)
	return buf[n:], InternalKey{UserKey: buf[:n:n], Trailer: k.Trailer}
}

func rewriteBlocks(
	r *Reader, restartInterval int, checksumType ChecksumType, compression Compression,
	input []BlockHandle, output []blockWithSpan,
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

	var blockSlab []byte
	var keySlab []byte
	var scratch InternalKey

	for i := worker; i < len(input); i += totalWorkers {
		bh := input[i]

		h, err := r.readBlock(bh, nil /* transform */, nil /*rs*/)
		if err != nil {
			return err
		}
		inputBlock := h.Get()
		iter := &blockIter{}
		if err := iter.init(r.Compare, inputBlock, r.Properties.GlobalSeqNum); err != nil {
			h.Release()
			return err
		}

		if cap(bw.buf) == 0 {
			bw.buf = make([]byte, 0, len(inputBlock))
		}

		for key, _ := iter.First(); key != nil; key, _ = iter.Next() {
			if key.Kind() != InternalKeyKindSet {
				h.Release()
				return errBadKind
			}
			if !bytes.HasSuffix(iter.Key().UserKey, from) {
				h.Release()
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
		output[i].end = scratch.Clone()
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
		if cap(blockSlab) < sz {
			blockSlab = make([]byte, sz*512)
		}
		output[i].data = blockSlab[:sz:sz]
		blockSlab = blockSlab[sz:]
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

func rewriteDataBlocksToWriter(r *Reader, w *Writer, data []BlockHandle, from, to []byte, concurrency int) error {
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

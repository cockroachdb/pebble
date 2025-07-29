// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"cmp"
	"context"
	"math"
	"slices"
	"sync"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/block/blockkind"
)

// RewriteKeySuffixesAndReturnFormat copies the content of the passed SSTable
// bytes to a new sstable, written to `out`, in which the suffix `from` has is
// replaced with `to` in every key. The input sstable must consist of only
// Sets or RangeKeySets and every key must have `from` as its suffix as
// determined by the Split function of the Comparer in the passed
// WriterOptions. Range deletes must not exist in this sstable, as they will
// be ignored.
//
// Data blocks are rewritten in parallel by `concurrency` workers and then
// assembled into a final SST. Filters are copied from the original SST without
// modification as they are not affected by the suffix, while block and table
// properties are only minimally recomputed.
//
// TODO(sumeer): document limitations, if any, due to this limited
// re-computation of properties (is there any loss of fidelity?).
//
// Any block property collectors configured in the WriterOptions must implement
// AddCollectedWithSuffixChange.
//
// The WriterOptions.TableFormat is ignored, and the output sstable has the
// same TableFormat as the input, which is returned in case the caller wants
// to do some error checking. Suffix rewriting is meant to be efficient, and
// allowing changes in the TableFormat detracts from that efficiency.
//
// Any obsolete bits that key-value pairs may be annotated with are ignored
// and lost during the rewrite. Additionally, the output sstable has the
// pebble.obsolete.is_strict property set to false. These limitations could be
// removed if needed. The current use case for
// RewriteKeySuffixesAndReturnFormat in CockroachDB is for MVCC-compliant file
// ingestion, where these files do not contain RANGEDELs and have one
// key-value pair per userkey -- so they trivially satisfy the strict
// criteria, and we don't need the obsolete bit as a performance optimization.
// For disaggregated storage, strict obsolete sstables are needed for L5 and
// L6, but at the time of writing, we expect such MVCC-compliant file
// ingestion to only ingest into levels L4 and higher. If this changes, we can
// do one of two things to get rid of this limitation:
//   - Validate that there are no duplicate userkeys and no RANGEDELs/MERGEs
//     in the sstable to be rewritten. Validating no duplicate userkeys is
//     non-trivial when rewriting blocks in parallel, so we could encode the
//     pre-existing condition in the (existing) SnapshotPinnedKeys property --
//     we need to update the external sst writer to calculate and encode this
//     property.
//   - Preserve the obsolete bit (with changes to the blockIter).
func RewriteKeySuffixesAndReturnFormat(
	sst []byte,
	rOpts ReaderOptions,
	out objstorage.Writable,
	o WriterOptions,
	from, to []byte,
	concurrency int,
) (*WriterMetadata, TableFormat, error) {
	r, err := NewMemReader(sst, rOpts)
	if err != nil {
		return nil, TableFormatUnspecified, err
	}
	defer func() { _ = r.Close() }()
	return rewriteKeySuffixesInBlocks(r, sst, out, o, from, to, concurrency)
}

func rewriteKeySuffixesInBlocks(
	r *Reader, sst []byte, out objstorage.Writable, o WriterOptions, from, to []byte, concurrency int,
) (*WriterMetadata, TableFormat, error) {
	o = o.ensureDefaults()
	props, err := r.ReadPropertiesBlock(context.TODO(), nil /* buffer pool */)
	if err != nil {
		return nil, TableFormatUnspecified, err
	}

	switch {
	case concurrency < 1:
		return nil, TableFormatUnspecified, errors.New("concurrency must be >= 1")
	case r.Attributes.Has(AttributeValueBlocks):
		return nil, TableFormatUnspecified,
			errors.New("sstable with a single suffix should not have value blocks")
	case props.ComparerName != o.Comparer.Name:
		return nil, TableFormatUnspecified, errors.Errorf("mismatched Comparer %s vs %s, replacement requires same splitter to copy filters",
			props.ComparerName, o.Comparer.Name)
	case o.FilterPolicy != base.NoFilterPolicy && props.FilterPolicyName != o.FilterPolicy.Name():
		return nil, TableFormatUnspecified, errors.Errorf("mismatched filters %q vs %q", props.FilterPolicyName, o.FilterPolicy.Name())
	}

	o.TableFormat = r.tableFormat
	w := NewRawWriter(out, o)
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()

	if err := w.rewriteSuffixes(r, sst, o, from, to, concurrency); err != nil {
		return nil, TableFormatUnspecified, err
	}

	if err := w.Close(); err != nil {
		w = nil
		return nil, TableFormatUnspecified, err
	}
	writerMeta, err := w.Metadata()
	w = nil
	return writerMeta, r.tableFormat, err
}

var errBadKind = errors.New("key does not have expected kind (set)")

type blockWithSpan struct {
	start, end base.InternalKey
	physical   block.PhysicalBlock
}

type blockRewriter interface {
	RewriteSuffixes(
		input []byte, from []byte, to []byte,
	) (start, end base.InternalKey, rewritten []byte, err error)
}

func rewriteDataBlocksInParallel(
	r *Reader,
	sstBytes []byte,
	opts WriterOptions,
	input []block.HandleWithProperties,
	from, to []byte,
	concurrency int,
	compressionStats *block.CompressionStats,
	newDataBlockRewriter func() blockRewriter,
) ([]blockWithSpan, error) {
	if !r.Attributes.Has(AttributePointKeys) {
		// No point keys.
		return nil, nil
	}
	output := make([]blockWithSpan, len(input))

	var compressionStatsMu sync.Mutex

	g := &sync.WaitGroup{}
	g.Add(concurrency)
	type workerErr struct {
		worker int
		err    error
	}
	errCh := make(chan workerErr, concurrency)
	for j := 0; j < concurrency; j++ {
		worker := j
		go func() {
			defer g.Done()
			rw := newDataBlockRewriter()
			var inputBlock, inputBlockBuf []byte
			var physBlockMaker block.PhysicalBlockMaker
			physBlockMaker.Init(opts.Compression, opts.Checksum)
			defer physBlockMaker.Close()
			// We'll assume all blocks are _roughly_ equal so round-robin static partition
			// of each worker doing every ith block is probably enough.
			err := func() error {
				for i := worker; i < len(input); i += concurrency {
					bh := input[i]
					var err error
					inputBlock, inputBlockBuf, err = readBlockBuf(sstBytes, bh.Handle, r.blockReader.ChecksumType(), inputBlockBuf)
					if err != nil {
						return err
					}
					var outputBlock []byte
					output[i].start, output[i].end, outputBlock, err =
						rw.RewriteSuffixes(inputBlock, from, to)
					if err != nil {
						return err
					}
					if err := r.Comparer.ValidateKey.Validate(output[i].start.UserKey); err != nil {
						return err
					}
					if err := r.Comparer.ValidateKey.Validate(output[i].end.UserKey); err != nil {
						return err
					}
					output[i].physical = physBlockMaker.Make(outputBlock, blockkind.SSTableData, block.NoFlags)
				}
				return nil
			}()
			if err != nil {
				errCh <- workerErr{worker: worker, err: err}
			}
			compressionStatsMu.Lock()
			compressionStats.MergeWith(physBlockMaker.Compressor.Stats())
			defer compressionStatsMu.Unlock()
		}()
	}
	g.Wait()
	close(errCh)
	if werr, ok := <-errCh; ok {
		// Collect errors from all workers and sort them by worker for determinism.
		werrs := []workerErr{werr}
		for werr := range errCh {
			werrs = append(werrs, werr)
		}
		slices.SortFunc(werrs, func(a, b workerErr) int { return cmp.Compare(a.worker, b.worker) })
		return nil, werrs[0].err
	}
	return output, nil
}

func rewriteRangeKeyBlockToWriter(r *Reader, w RawWriter, from, to []byte) error {
	iter, err := r.NewRawRangeKeyIter(context.TODO(), NoFragmentTransforms, NoReadEnv)
	if err != nil {
		return err
	}
	if iter == nil {
		// No range keys.
		return nil
	}
	defer iter.Close()

	s, err := iter.First()
	for ; s != nil; s, err = iter.Next() {
		if !s.Valid() {
			break
		}
		if err := r.Comparer.ValidateKey.Validate(s.Start); err != nil {
			return err
		}
		if err := r.Comparer.ValidateKey.Validate(s.End); err != nil {
			return err
		}
		for i := range s.Keys {
			if s.Keys[i].Kind() != base.InternalKeyKindRangeKeySet {
				return errBadKind
			}
			if !bytes.Equal(s.Keys[i].Suffix, from) {
				return errors.Errorf("key has suffix %q, expected %q", s.Keys[i].Suffix, from)
			}
			s.Keys[i].Suffix = to
		}

		if err := w.EncodeSpan(*s); err != nil {
			return err
		}
	}
	return err
}

// getShortIDs returns a slice keyed by the shortIDs of the block property
// collector in r, with the values containing a new shortID corresponding to the
// index of the corresponding block property collector in collectors.
//
// Additionally, getShortIDs returns n—n is the number of encoded properties
// that a reader needs to process in order to get values for all the given
// collectors.  This can be different than len(collectors) when the latter is
// different from the number of block property collectors used when the sstable
// was written.  Note that shortIDs[n:] are all invalid.
//
// getShortIDs errors if any of the collectors are not found in the sstable.
func getShortIDs(
	r *Reader, collectors []BlockPropertyCollector,
) (shortIDs []shortID, n int, err error) {
	if len(collectors) == 0 {
		return nil, 0, nil
	}
	shortIDs = make([]shortID, math.MaxUint8)
	for i := range shortIDs {
		shortIDs[i] = invalidShortID
	}
	for i, p := range collectors {
		prop, ok := r.UserProperties[p.Name()]
		if !ok {
			return nil, 0, errors.Errorf("sstable does not contain property %s", p.Name())
		}
		oldShortID := shortID(prop[0])
		shortIDs[oldShortID] = shortID(i)
		n = max(n, int(oldShortID)+1)
	}
	return shortIDs, n, nil
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
// more work to rederive filters, props, etc.
//
// Any obsolete bits that key-value pairs may be annotated with are ignored
// and lost during the rewrite. Some of the obsolete bits may be recreated --
// specifically when there are multiple keys with the same user key.
// Additionally, the output sstable has the pebble.obsolete.is_strict property
// set to false. See the longer comment at RewriteKeySuffixesAndReturnFormat.
func RewriteKeySuffixesViaWriter(
	r *Reader, out objstorage.Writable, o WriterOptions, from, to []byte,
) (*WriterMetadata, error) {
	if o.Comparer == nil || o.Comparer.Split == nil {
		return nil, errors.New("a valid splitter is required to rewrite suffixes")
	}
	if r.Attributes.Has(AttributeBlobValues) {
		return nil, errors.New("cannot rewrite suffixes of sstable with blob values")
	}

	o.IsStrictObsolete = false
	w := NewRawWriter(out, o)
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	i, err := r.NewIter(NoTransforms, nil, nil, AssertNoBlobHandles)
	if err != nil {
		return nil, err
	}
	defer func() { _ = i.Close() }()

	kv := i.First()
	var scratch InternalKey
	for kv != nil {
		if kv.Kind() != InternalKeyKindSet {
			return nil, errors.New("invalid key type")
		}
		oldSuffix := kv.K.UserKey[r.Comparer.Split(kv.K.UserKey):]
		if !bytes.Equal(oldSuffix, from) {
			return nil, errors.Errorf("key has suffix %q, expected %q", oldSuffix, from)
		}
		scratch.UserKey = append(scratch.UserKey[:0], kv.K.UserKey[:len(kv.K.UserKey)-len(from)]...)
		scratch.UserKey = append(scratch.UserKey, to...)
		scratch.Trailer = kv.K.Trailer

		if invariants.Enabled && invariants.Sometimes(10) {
			r.Comparer.ValidateKey.MustValidate(scratch.UserKey)
		}

		val, _, err := kv.Value(nil)
		if err != nil {
			return nil, err
		}
		if err := w.Add(scratch, val, false); err != nil {
			return nil, err
		}
		kv = i.Next()
	}
	if err := rewriteRangeKeyBlockToWriter(r, w, from, to); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		w = nil
		return nil, err
	}
	writerMeta, err := w.Metadata()
	w = nil
	return writerMeta, err
}

// NewMemReader opens a reader over the SST stored in the passed []byte.
func NewMemReader(sst []byte, o ReaderOptions) (*Reader, error) {
	// Since all operations are from memory, plumbing a context here is not useful.
	return NewReader(context.Background(), newMemReader(sst), o)
}

// readBlockBuf may return a byte slice that points directly into sstBytes. If
// the caller is going to expect that sstBytes remain stable, it should copy the
// returned slice before writing it out to a objstorage.Writable which may
// mangle it.
func readBlockBuf(
	sstBytes []byte, bh block.Handle, checksumType block.ChecksumType, buf []byte,
) ([]byte, []byte, error) {
	raw := sstBytes[bh.Offset : bh.Offset+bh.Length+block.TrailerLen]
	if err := block.ValidateChecksum(checksumType, raw, bh); err != nil {
		return nil, buf, err
	}
	algo := block.CompressionIndicator(raw[bh.Length])
	raw = raw[:bh.Length]

	if algo == block.NoCompressionIndicator {
		// Return the raw buffer if possible.
		if uintptr(unsafe.Pointer(unsafe.SliceData(raw)))&7 == 0 {
			return raw, buf, nil
		}
	}

	decompressedLen, err := block.DecompressedLen(algo, raw)
	if err != nil {
		return nil, buf, err
	}
	if cap(buf) < decompressedLen {
		// We want the buffer to be 8 byte aligned.
		buf = slices.Grow(buf[:0], decompressedLen+8)
		if n := uintptr(unsafe.Pointer(unsafe.SliceData(buf))) & 7; n != 0 {
			buf = buf[8-n:]
		}
	}
	dst := buf[:decompressedLen]
	err = block.DecompressInto(algo, raw, dst)
	return dst, buf, err
}

// memReader is a thin wrapper around a []byte such that it can be passed to
// sstable.Reader. It supports concurrent use, and does so without locking in
// contrast to the heavier read/write vfs.MemFile.
type memReader struct {
	b  []byte
	r  *bytes.Reader
	rh objstorage.NoopReadHandle
}

var _ objstorage.Readable = (*memReader)(nil)

func newMemReader(b []byte) *memReader {
	r := &memReader{
		b: b,
		r: bytes.NewReader(b),
	}
	r.rh = objstorage.MakeNoopReadHandle(r)
	return r
}

// ReadAt is part of objstorage.Readable.
func (m *memReader) ReadAt(_ context.Context, p []byte, off int64) error {
	n, err := m.r.ReadAt(p, off)
	if invariants.Enabled && err == nil && n != len(p) {
		panic("short read")
	}
	return err
}

// Close is part of objstorage.Readable.
func (*memReader) Close() error {
	return nil
}

// Stat is part of objstorage.Readable.
func (m *memReader) Size() int64 {
	return int64(len(m.b))
}

// NewReadHandle is part of objstorage.Readable.
func (m *memReader) NewReadHandle(readBeforeSize objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &m.rh
}

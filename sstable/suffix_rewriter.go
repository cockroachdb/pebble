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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/block"
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
	defer r.Close()
	return rewriteKeySuffixesInBlocks(r, out, o, from, to, concurrency)
}

func rewriteKeySuffixesInBlocks(
	r *Reader, out objstorage.Writable, o WriterOptions, from, to []byte, concurrency int,
) (*WriterMetadata, TableFormat, error) {
	o = o.ensureDefaults()
	switch {
	case concurrency < 1:
		return nil, TableFormatUnspecified, errors.New("concurrency must be >= 1")
	case r.Properties.NumValueBlocks > 0 || r.Properties.NumValuesInValueBlocks > 0:
		return nil, TableFormatUnspecified,
			errors.New("sstable with a single suffix should not have value blocks")
	case r.Properties.ComparerName != o.Comparer.Name:
		return nil, TableFormatUnspecified, errors.Errorf("mismatched Comparer %s vs %s, replacement requires same splitter to copy filters",
			r.Properties.ComparerName, o.Comparer.Name)
	case o.FilterPolicy != nil && r.Properties.FilterPolicyName != o.FilterPolicy.Name():
		return nil, TableFormatUnspecified, errors.New("mismatched filters")
	}

	o.TableFormat = r.tableFormat
	w := NewRawWriter(out, o)
	defer func() {
		if w != nil {
			w.Close()
		}
	}()

	if err := w.rewriteSuffixes(r, o, from, to, concurrency); err != nil {
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
	opts WriterOptions,
	input []block.HandleWithProperties,
	from, to []byte,
	concurrency int,
	newDataBlockRewriter func() blockRewriter,
) ([]blockWithSpan, error) {
	if r.Properties.NumEntries == 0 {
		// No point keys.
		return nil, nil
	}
	output := make([]blockWithSpan, len(input))

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
			var blockAlloc bytealloc.A
			var compressedBuf []byte
			var inputBlock, inputBlockBuf []byte
			checksummer := block.Checksummer{Type: opts.Checksum}
			// We'll assume all blocks are _roughly_ equal so round-robin static partition
			// of each worker doing every ith block is probably enough.
			err := func() error {
				for i := worker; i < len(input); i += concurrency {
					bh := input[i]
					var err error
					inputBlock, inputBlockBuf, err = readBlockBuf(r, bh.Handle, inputBlockBuf)
					if err != nil {
						return err
					}
					var outputBlock []byte
					output[i].start, output[i].end, outputBlock, err =
						rw.RewriteSuffixes(inputBlock, from, to)
					if err != nil {
						return err
					}
					compressedBuf = compressedBuf[:cap(compressedBuf)]
					finished := block.CompressAndChecksum(&compressedBuf, outputBlock, opts.Compression, &checksummer)
					output[i].physical = finished.CloneWithByteAlloc(&blockAlloc)
				}
				return nil
			}()
			if err != nil {
				errCh <- workerErr{worker: worker, err: err}
			}
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
	iter, err := r.NewRawRangeKeyIter(context.TODO(), NoFragmentTransforms)
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
		prop, ok := r.Properties.UserProperties[p.Name()]
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

	o.IsStrictObsolete = false
	w := newRowWriter(out, o)
	defer func() {
		if w != nil {
			w.Close()
		}
	}()
	i, err := r.NewIter(NoTransforms, nil, nil)
	if err != nil {
		return nil, err
	}
	defer i.Close()

	kv := i.First()
	var scratch InternalKey
	for kv != nil {
		if kv.Kind() != InternalKeyKindSet {
			return nil, errors.New("invalid key type")
		}
		oldSuffix := kv.K.UserKey[r.Split(kv.K.UserKey):]
		if !bytes.Equal(oldSuffix, from) {
			return nil, errors.Errorf("key has suffix %q, expected %q", oldSuffix, from)
		}
		scratch.UserKey = append(scratch.UserKey[:0], kv.K.UserKey[:len(kv.K.UserKey)-len(from)]...)
		scratch.UserKey = append(scratch.UserKey, to...)
		scratch.Trailer = kv.K.Trailer

		val, _, err := kv.Value(nil)
		if err != nil {
			return nil, err
		}
		w.addPoint(scratch, val, false)
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

func readBlockBuf(r *Reader, bh block.Handle, buf []byte) ([]byte, []byte, error) {
	raw := r.readable.(*memReader).b[bh.Offset : bh.Offset+bh.Length+block.TrailerLen]
	if err := checkChecksum(r.checksumType, raw, bh, 0); err != nil {
		return nil, buf, err
	}
	algo := block.CompressionIndicator(raw[bh.Length])
	raw = raw[:bh.Length]
	if algo == block.NoCompressionIndicator {
		return raw, buf, nil
	}
	decompressedLen, prefix, err := block.DecompressedLen(algo, raw)
	if err != nil {
		return nil, buf, err
	}
	if cap(buf) < decompressedLen {
		buf = make([]byte, decompressedLen)
	}
	dst := buf[:decompressedLen]
	err = block.DecompressInto(algo, raw[prefix:], dst)
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

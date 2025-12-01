// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valsep

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
)

// SSTBlobWriter writes an sstable and 0 or more blob value files
// for ingesting into pebble. Values are extracted depending on the
// value separation strategy defined. If ValueSeparation is
// neverSeparateValues, this behaves like sstable.Writer and no blob
// files are written.
type SSTBlobWriter struct {
	SSTWriter *sstable.Writer
	valSep    ValueSeparation
	err       error

	blobFileNum base.DiskFileNum
	closed      bool
	// isStrictObsolete is true if the writer is configured to write and enforce
	// a 'strict obsolete' sstable. This includes prohibiting the addition of
	// MERGE keys. See the documentation in format.go for more details.
	isStrictObsolete bool

	kvScratch base.InternalKV
	// Metadata on the blob files written.
	blobFilesMeta []blob.FileWriterStats
}

type NewBlobFile func() (objstorage.Writable, error)

var neverSeparateValues = &NeverSeparateValues{}

type SSTBlobWriterOptions struct {
	SSTWriterOpts  sstable.WriterOptions
	BlobWriterOpts blob.FileWriterOptions
	// BlobFilesDisabled is true if value separation into blob files
	// is disabled and the writer will behave like an sstable.Writer
	// in this case. Note that values may still be separated into a
	// value block in the same sstable.
	BlobFilesDisabled bool
	// The minimum size required for a value to be separated into a
	// blob file. This value may be overridden by the span policy.
	ValueSeparationMinSize int
	// The minimum size required for a value to be separated into a
	// blob file if it is classified to be MVCC garbage. A value
	// of 0 means all MVCC garbage values are eligible for separation.
	// To disable MVCC garbage value separation, set
	// DisableSeparationBySuffix in the SpanPolicy's ValueStoragePolicy.
	MVCCGarbageValueSeparationMinSize int
	// SpanPolicy specifies the specific policies applied to the table span.
	// When using the external writer, there should be 1 span policy
	// applied to the entire sstable.
	SpanPolicy    base.SpanPolicy
	NewBlobFileFn NewBlobFile
}

// NewSSTBlobWriter returns a new SSTBlobWriter that writes to the provided
// sstHandle. The writer uses the provided options to configure both the sstable
// writer and the blob file writer.
func NewSSTBlobWriter(sstHandle objstorage.Writable, opts SSTBlobWriterOptions) *SSTBlobWriter {
	writer := &SSTBlobWriter{
		isStrictObsolete: opts.SSTWriterOpts.IsStrictObsolete,
	}

	if opts.SpanPolicy.PreferFastCompression && opts.SSTWriterOpts.Compression != block.NoCompression {
		opts.SSTWriterOpts.Compression = block.FastestCompression
	}

	writer.SSTWriter = sstable.NewWriter(sstHandle, opts.SSTWriterOpts)

	// Create the value separator.
	minimumValueSize := opts.ValueSeparationMinSize
	if opts.SpanPolicy.ValueStoragePolicy.OverrideBlobSeparationMinimumSize > 0 {
		minimumValueSize = opts.SpanPolicy.ValueStoragePolicy.OverrideBlobSeparationMinimumSize
	}
	if opts.BlobFilesDisabled || minimumValueSize == 0 || opts.SpanPolicy.ValueStoragePolicy.DisableBlobSeparation {
		writer.valSep = neverSeparateValues
	} else {
		newBlobObject := func() (objstorage.Writable, objstorage.ObjectMetadata, error) {
			// The ObjectMetadata collected by the value separator will not be
			// exposed by this writer, since this store does not yet know about
			// these objects. However, we must provide a unique file number for
			// each new blob file because the value separator uses file ids to
			// retrieve the index of the blob file within the sst's tracked blob
			// references array. The reference id (array index) is what is then
			// written to the inline blob handle.
			newHandle, err := opts.NewBlobFileFn()
			if err != nil {
				return nil, objstorage.ObjectMetadata{}, err
			}
			nextFileNum := writer.blobFileNum
			writer.blobFileNum++
			return newHandle, objstorage.ObjectMetadata{DiskFileNum: nextFileNum}, nil
		}
		writer.valSep = NewWriteNewBlobFiles(
			opts.SSTWriterOpts.Comparer,
			newBlobObject,
			opts.BlobWriterOpts,
			minimumValueSize,
			opts.MVCCGarbageValueSeparationMinSize,
			WriteNewBlobFilesOptions{
				DisableValueSeparationBySuffix: opts.SpanPolicy.ValueStoragePolicy.DisableSeparationBySuffix,
				ShortAttrExtractor:             opts.SSTWriterOpts.ShortAttributeExtractor,
				InvalidValueCallback: func(userKey []byte, value []byte, err error) {
					writer.err = errors.CombineErrors(writer.err, err)
				},
			},
		)
	}

	return writer
}

// Error returns the current accumulated error if any.
func (w *SSTBlobWriter) Error() error {
	return errors.CombineErrors(w.err, w.SSTWriter.Error())
}

// Set sets the value for the given key. The sequence number is set to 0.
// Values may be separated into blob files depending on the value separation
// strategy configured for the writer. Intended for use to externally construct
// an sstable with its blob files before ingestion into a DB. For a given
// SSTBlobWriter, the keys passed to Set must be in strictly increasing order.
func (w *SSTBlobWriter) Set(key, value []byte) error {
	if err := w.Error(); err != nil {
		return err
	}

	if w.isStrictObsolete {
		return errors.Errorf("use raw writer Add in strict obsolete mode")
	}

	w.kvScratch.K = base.MakeInternalKey(key, 0, sstable.InternalKeyKindSet)
	w.kvScratch.V = base.MakeInPlaceValue(value)
	isLikelyMVCCGarbage := w.SSTWriter.Raw().IsLikelyMVCCGarbage(w.kvScratch.K.UserKey, w.kvScratch.Kind())
	return w.valSep.Add(w.SSTWriter.Raw(), &w.kvScratch, false, isLikelyMVCCGarbage)
}

// BlobWriterMetas returns a slice of blob.FileWriterStats describing the
// blob files written by this SSTBlobWriter. The ordering of the returned
// slice matches the ordering of blob files as they should appear in the
// sstable's manifest.BlobReferences. Close must be called before calling this
// method.
func (w *SSTBlobWriter) BlobWriterMetas() ([]blob.FileWriterStats, error) {
	if !w.closed {
		return nil, errors.New("blob writer not closed")
	}
	return w.blobFilesMeta, nil
}

// Close closes both the sstable writer and the blob file writer if any.
func (w *SSTBlobWriter) Close() error {
	w.err = errors.CombineErrors(w.err, w.SSTWriter.Close())
	meta, err := w.valSep.FinishOutput()
	if err != nil {
		w.err = errors.CombineErrors(w.err, err)
	}
	for _, blobFile := range meta.NewBlobFiles {
		w.blobFilesMeta = append(w.blobFilesMeta, blobFile.FileStats)
	}
	w.closed = true
	return w.err
}

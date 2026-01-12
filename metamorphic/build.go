// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/valsep"
	"github.com/cockroachdb/pebble/vfs"
)

// writeSSTForIngestion writes an SST that is to be ingested, either directly or
// as an external file. Returns the sstable metadata.
//
// Closes the iterators in all cases.
func writeSSTForIngestion(
	t *Test,
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
	uniquePrefixes bool,
	syntheticSuffix sstable.SyntheticSuffix,
	syntheticPrefix sstable.SyntheticPrefix,
	writable objstorage.Writable,
	targetFMV pebble.FormatMajorVersion,
) (*sstable.WriterMetadata, error) {
	writerOpts := t.opts.MakeWriterOptions(0, targetFMV.MaxTableFormat())
	if t.testOpts.disableValueBlocksForIngestSSTables {
		writerOpts.DisableValueBlocks = true
	}
	w := sstable.NewWriter(writable, writerOpts)
	pointIterCloser := base.CloseHelper(pointIter)
	defer func() {
		_ = pointIterCloser.Close()
	}()

	outputKey := func(key []byte) []byte {
		if !syntheticPrefix.IsSet() {
			return slices.Clone(key)
		}
		return syntheticPrefix.Apply(key)
	}

	outputKeyWithSuffix := func(key []byte) []byte {
		if !syntheticPrefix.IsSet() && !syntheticSuffix.IsSet() {
			return slices.Clone(key)
		}
		if syntheticPrefix.IsSet() {
			key = syntheticPrefix.Apply(key)
		}
		if syntheticSuffix.IsSet() {
			n := t.opts.Comparer.Split(key)
			key = append(key[:n:n], syntheticSuffix...)
		}
		return key
	}

	var lastUserKey []byte
	for kv := pointIter.First(); kv != nil; kv = pointIter.Next() {
		// Ignore duplicate keys.
		if lastUserKey != nil {
			last := lastUserKey
			this := kv.K.UserKey
			if uniquePrefixes {
				last = last[:t.opts.Comparer.Split(last)]
				this = this[:t.opts.Comparer.Split(this)]
			}
			if t.opts.Comparer.Equal(last, this) {
				continue
			}
		}
		lastUserKey = append(lastUserKey[:0], kv.K.UserKey...)

		k := *kv
		k.K.SetSeqNum(base.SeqNumZero)
		k.K.UserKey = outputKeyWithSuffix(k.K.UserKey)
		value := kv.LazyValue()
		// It's possible that we wrote the key on a batch from a db that supported
		// DeleteSized, but will be ingesting into a db that does not. Detect this
		// case and translate the key to an InternalKeyKindDelete.
		if targetFMV < pebble.FormatDeleteSizedAndObsolete && kv.Kind() == pebble.InternalKeyKindDeleteSized {
			value = pebble.LazyValue{}
			k.K.SetKind(pebble.InternalKeyKindDelete)
		}
		valBytes, _, err := value.Value(nil)
		if err != nil {
			return nil, err
		}
		t.opts.Comparer.ValidateKey.MustValidate(k.K.UserKey)
		if err := w.Raw().Add(k.K, valBytes, false, base.KVMeta{}); err != nil {
			return nil, err
		}
	}
	if err := pointIterCloser.Close(); err != nil {
		return nil, err
	}

	if syntheticSuffix.IsSet() && rangeDelIter != nil {
		panic("synthetic suffix with RangeDel")
	}
	if err := writeRangeDeletes(w, rangeDelIter, t.opts.Comparer, outputKey); err != nil {
		return nil, err
	}

	if err := writeRangeKeys(w, rangeKeyIter, t.opts.Comparer, syntheticSuffix, outputKey); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	sstMeta, err := w.Raw().Metadata()
	if err != nil {
		return nil, err
	}
	return sstMeta, nil
}

// buildForIngest builds a local SST file containing the keys in the given batch
// and returns its path, blob paths (if any), and metadata.
func buildForIngest(
	t *Test, dbID objID, b *pebble.Batch, i int,
) (path string, blobPaths []string, _ *sstable.WriterMetadata, _ error) {
	path = t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d", dbID.slot(), i))
	f, err := t.opts.FS.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return "", nil, nil, err
	}
	db := t.getDB(dbID)

	// If value separation is enabled and the format major version supports
	// ingesting blob files, use SSTBlobWriter to potentially create blob files.
	if t.opts.Experimental.ValueSeparationPolicy().Enabled &&
		db.FormatMajorVersion() >= pebble.FormatIngestBlobFiles {
		return buildForIngestWithBlobs(t, dbID, b, i, path, f, db)
	}

	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)

	writable := objstorageprovider.NewFileWritable(f)
	meta, err := writeSSTForIngestion(
		t,
		iter, rangeDelIter, rangeKeyIter,
		false, /* uniquePrefixes */
		nil,   /* syntheticSuffix */
		nil,   /* syntheticPrefix */
		writable,
		db.FormatMajorVersion(),
	)
	return path, nil, meta, err
}

// buildForIngestWithBlobs builds a local SST file and associated blob files
// containing the keys in the given batch. Returns the SST path, blob paths,
// and metadata.
func buildForIngestWithBlobs(
	t *Test, dbID objID, b *pebble.Batch, sstIndex int, path string, f vfs.File, db *pebble.DB,
) (string, []string, *sstable.WriterMetadata, error) {
	var blobPaths []string
	blobFileCount := 0

	// Get the value separation policy to determine the minimum value size.
	policy := t.opts.Experimental.ValueSeparationPolicy()

	writerOpts := valsep.SSTBlobWriterOptions{
		SSTWriterOpts:                     t.opts.MakeWriterOptions(0, db.FormatMajorVersion().MaxTableFormat()),
		ValueSeparationMinSize:            policy.MinimumSize,
		MVCCGarbageValueSeparationMinSize: policy.MinimumMVCCGarbageSize,
		NewBlobFileFn: func() (objstorage.Writable, error) {
			// Include sstIndex in the blob file path to ensure unique names when
			// building multiple SSTs for the same ingest operation.
			blobPath := t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d-blob%d", dbID.slot(), sstIndex, blobFileCount))
			blobFile, err := t.opts.FS.Create(blobPath, vfs.WriteCategoryUnspecified)
			if err != nil {
				return nil, err
			}
			blobPaths = append(blobPaths, blobPath)
			blobFileCount++
			return objstorageprovider.NewFileWritable(blobFile), nil
		},
	}

	if t.testOpts.disableValueBlocksForIngestSSTables {
		writerOpts.SSTWriterOpts.DisableValueBlocks = true
	}

	writable := objstorageprovider.NewFileWritable(f)
	writer := valsep.NewSSTBlobWriter(writable, writerOpts)
	writerCloser := base.CloseHelper(writer)

	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)
	iterCloser := base.CloseHelper(iter)
	defer func() {
		_ = writerCloser.Close()
		_ = iterCloser.Close()
		if rangeDelIter != nil {
			rangeDelIter.Close()
		}
		if rangeKeyIter != nil {
			rangeKeyIter.Close()
		}
	}()

	// Write point keys.
	var lastUserKey []byte
	for kv := iter.First(); kv != nil; kv = iter.Next() {
		// Ignore duplicate keys.
		if lastUserKey != nil && t.opts.Comparer.Equal(lastUserKey, kv.K.UserKey) {
			continue
		}
		lastUserKey = append(lastUserKey[:0], kv.K.UserKey...)

		value := kv.LazyValue()
		valBytes, _, err := value.Value(nil)
		if err != nil {
			return "", nil, nil, err
		}

		t.opts.Comparer.ValidateKey.MustValidate(kv.K.UserKey)

		switch kv.Kind() {
		case pebble.InternalKeyKindSet, pebble.InternalKeyKindSetWithDelete:
			if err := writer.Set(kv.K.UserKey, valBytes); err != nil {
				return "", nil, nil, err
			}
		case pebble.InternalKeyKindDelete:
			if err := writer.SSTWriter.Delete(kv.K.UserKey); err != nil {
				return "", nil, nil, err
			}
		case pebble.InternalKeyKindDeleteSized:
			k := base.MakeInternalKey(kv.K.UserKey, base.SeqNumZero, pebble.InternalKeyKindDeleteSized)
			if err := writer.SSTWriter.Raw().Add(k, valBytes, false, base.KVMeta{}); err != nil {
				return "", nil, nil, err
			}
		case pebble.InternalKeyKindSingleDelete:
			// SingleDelete doesn't have a dedicated method, use Raw().Add().
			k := base.MakeInternalKey(kv.K.UserKey, base.SeqNumZero, pebble.InternalKeyKindSingleDelete)
			if err := writer.SSTWriter.Raw().Add(k, nil, false, base.KVMeta{}); err != nil {
				return "", nil, nil, err
			}
		case pebble.InternalKeyKindMerge:
			if err := writer.SSTWriter.Merge(kv.K.UserKey, valBytes); err != nil {
				return "", nil, nil, err
			}
		}
	}
	if err := iterCloser.Close(); err != nil {
		return "", nil, nil, err
	}

	// Write range deletions using the underlying SST writer.
	// writeRangeDeletes closes rangeDelIter. Set to nil before calling to
	// avoid double-close in deferred cleanup.
	rdi := rangeDelIter
	rangeDelIter = nil
	if err := writeRangeDeletes(writer.SSTWriter, rdi, t.opts.Comparer, slices.Clone); err != nil {
		return "", nil, nil, err
	}

	// Write range keys using the underlying SST writer.
	// writeRangeKeys closes rangeKeyIter. Set to nil before calling to
	// avoid double-close in deferred cleanup.
	rki := rangeKeyIter
	rangeKeyIter = nil
	if err := writeRangeKeys(writer.SSTWriter, rki, t.opts.Comparer, nil, slices.Clone); err != nil {
		return "", nil, nil, err
	}

	if err := writerCloser.Close(); err != nil {
		return "", nil, nil, err
	}

	meta, err := writer.SSTWriter.Raw().Metadata()
	if err != nil {
		return "", nil, nil, err
	}

	return path, blobPaths, meta, nil
}

// writeRangeDeletes writes range deletions from the iterator to the SST writer.
// The iterator is closed after writing.
func writeRangeDeletes(
	w *sstable.Writer,
	rangeDelIter keyspan.FragmentIterator,
	comparer *base.Comparer,
	outputKey func(key []byte) []byte,
) error {
	if rangeDelIter == nil {
		return nil
	}
	defer rangeDelIter.Close()

	span, err := rangeDelIter.First()
	for ; span != nil; span, err = rangeDelIter.Next() {
		start := outputKey(span.Start)
		end := outputKey(span.End)
		comparer.ValidateKey.MustValidate(start)
		comparer.ValidateKey.MustValidate(end)
		if err := w.DeleteRange(start, end); err != nil {
			return err
		}
	}
	return err
}

// writeRangeKeys writes range keys from the iterator to the SST writer.
// The iterator is closed after writing.
func writeRangeKeys(
	w *sstable.Writer,
	rangeKeyIter keyspan.FragmentIterator,
	comparer *base.Comparer,
	syntheticSuffix sstable.SyntheticSuffix,
	outputKey func(key []byte) []byte,
) error {
	if rangeKeyIter == nil {
		return nil
	}
	defer rangeKeyIter.Close()

	span, err := rangeKeyIter.First()
	for ; span != nil; span, err = rangeKeyIter.Next() {
		// Coalesce the keys of this span and then zero the sequence numbers.
		collapsed := keyspan.Span{
			Start: outputKey(span.Start),
			End:   outputKey(span.End),
			Keys:  make([]keyspan.Key, 0, len(span.Keys)),
		}
		comparer.ValidateKey.MustValidate(collapsed.Start)
		comparer.ValidateKey.MustValidate(collapsed.End)

		keys := span.Keys
		if syntheticSuffix.IsSet() {
			keys = slices.Clone(span.Keys)
			for i := range keys {
				if keys[i].Kind() == base.InternalKeyKindRangeKeyUnset {
					panic("RangeKeyUnset with synthetic suffix")
				}
				if len(keys[i].Suffix) > 0 {
					keys[i].Suffix = syntheticSuffix
				}
			}
		}
		rangekey.Coalesce(comparer.CompareRangeSuffixes, keys, &collapsed.Keys)
		for i := range collapsed.Keys {
			collapsed.Keys[i].Trailer = base.MakeTrailer(0, collapsed.Keys[i].Kind())
		}
		keyspan.SortKeysByTrailer(collapsed.Keys)
		if err := w.Raw().EncodeSpan(collapsed); err != nil {
			return err
		}
	}
	return err
}

// buildForIngestExternalEmulation builds a local SST file containing the keys in the given
// external object (truncated to the given bounds) and returns its path and
// metadata.
func buildForIngestExternalEmulation(
	t *Test,
	dbID objID,
	externalObjID objID,
	bounds pebble.KeyRange,
	syntheticSuffix sstable.SyntheticSuffix,
	syntheticPrefix sstable.SyntheticPrefix,
	i int,
) (path string, _ *sstable.WriterMetadata) {
	path = t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d", dbID.slot(), i))
	f, err := t.opts.FS.Create(path, vfs.WriteCategoryUnspecified)
	panicIfErr(err)

	reader, pointIter, rangeDelIter, rangeKeyIter := openExternalObj(t, externalObjID, bounds, syntheticPrefix)
	defer func() { _ = reader.Close() }()

	writable := objstorageprovider.NewFileWritable(f)
	// The underlying file should already have unique prefixes. Plus we are
	// emulating the external ingestion path which won't remove duplicate prefixes
	// if they exist.
	const uniquePrefixes = false
	meta, err := writeSSTForIngestion(
		t,
		pointIter, rangeDelIter, rangeKeyIter,
		uniquePrefixes,
		syntheticSuffix,
		syntheticPrefix,
		writable,
		t.minFMV(),
	)
	if err != nil {
		panic(err)
	}
	return path, meta
}

func openExternalObj(
	t *Test, externalObjID objID, bounds pebble.KeyRange, syntheticPrefix sstable.SyntheticPrefix,
) (
	reader *sstable.Reader,
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
) {
	objMeta := t.getExternalObj(externalObjID)
	objReader, objSize, err := t.externalStorage.ReadObject(context.Background(), objMeta.objName)
	panicIfErr(err)
	opts := t.opts.MakeReaderOptions()
	reader, err = sstable.NewReader(
		context.Background(),
		objstorageprovider.NewRemoteReadable(objReader, objSize, t.externalStorage.IsNotExistError),
		opts,
	)
	if err != nil {
		panic(errors.CombineErrors(err, objReader.Close()))
	}

	start := bounds.Start
	end := bounds.End
	if syntheticPrefix.IsSet() {
		start = syntheticPrefix.Invert(start)
		end = syntheticPrefix.Invert(end)
	}
	pointIter, err = reader.NewIter(sstable.NoTransforms, start, end, sstable.AssertNoBlobHandles)
	panicIfErr(err)

	rangeDelIter, err = reader.NewRawRangeDelIter(context.Background(), sstable.NoFragmentTransforms, sstable.NoReadEnv)
	panicIfErr(err)
	if rangeDelIter != nil {
		rangeDelIter = keyspan.Truncate(
			t.opts.Comparer.Compare,
			rangeDelIter,
			base.UserKeyBoundsEndExclusive(start, end),
		)
	}

	rangeKeyIter, err = reader.NewRawRangeKeyIter(context.Background(), sstable.NoFragmentTransforms, sstable.NoReadEnv)
	panicIfErr(err)
	if rangeKeyIter != nil {
		rangeKeyIter = keyspan.Truncate(
			t.opts.Comparer.Compare,
			rangeKeyIter,
			base.UserKeyBoundsEndExclusive(start, end),
		)
	}
	return reader, pointIter, rangeDelIter, rangeKeyIter
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

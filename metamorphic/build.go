// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
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
		if rangeDelIter != nil {
			rangeDelIter.Close()
		}
		if rangeKeyIter != nil {
			rangeKeyIter.Close()
		}
	}()

	outputKey := func(key []byte, syntheticSuffix sstable.SyntheticSuffix) []byte {
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
		k.K.UserKey = outputKey(k.K.UserKey, syntheticSuffix)
		value := kv.V
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
		if err := w.Raw().AddWithForceObsolete(k.K, valBytes, false); err != nil {
			return nil, err
		}
	}
	if err := pointIterCloser.Close(); err != nil {
		return nil, err
	}

	if rangeDelIter != nil {
		span, err := rangeDelIter.First()
		for ; span != nil; span, err = rangeDelIter.Next() {
			if syntheticSuffix.IsSet() {
				panic("synthetic suffix with RangeDel")
			}
			start := outputKey(span.Start, nil)
			end := outputKey(span.End, nil)
			t.opts.Comparer.ValidateKey.MustValidate(start)
			t.opts.Comparer.ValidateKey.MustValidate(end)
			if err := w.DeleteRange(start, end); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		rangeDelIter.Close()
		rangeDelIter = nil
	}

	if rangeKeyIter != nil {
		span, err := rangeKeyIter.First()
		for ; span != nil; span, err = rangeKeyIter.Next() {
			// Coalesce the keys of this span and then zero the sequence
			// numbers. This is necessary in order to make the range keys within
			// the ingested sstable internally consistent at the sequence number
			// it's ingested at. The individual keys within a batch are
			// committed at unique sequence numbers, whereas all the keys of an
			// ingested sstable are given the same sequence number. A span
			// containing keys that both set and unset the same suffix at the
			// same sequence number is nonsensical, so we "coalesce" or collapse
			// the keys.
			collapsed := keyspan.Span{
				Start: outputKey(span.Start, nil),
				End:   outputKey(span.End, nil),
				Keys:  make([]keyspan.Key, 0, len(span.Keys)),
			}
			t.opts.Comparer.ValidateKey.MustValidate(collapsed.Start)
			t.opts.Comparer.ValidateKey.MustValidate(collapsed.End)
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
			rangekey.Coalesce(t.opts.Comparer.CompareRangeSuffixes, keys, &collapsed.Keys)
			for i := range collapsed.Keys {
				collapsed.Keys[i].Trailer = base.MakeTrailer(0, collapsed.Keys[i].Kind())
			}
			keyspan.SortKeysByTrailer(collapsed.Keys)
			if err := w.Raw().EncodeSpan(collapsed); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		rangeKeyIter.Close()
		rangeKeyIter = nil
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
// and returns its path and metadata.
func buildForIngest(
	t *Test, dbID objID, b *pebble.Batch, i int,
) (path string, _ *sstable.WriterMetadata, _ error) {
	path = t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d", dbID.slot(), i))
	f, err := t.opts.FS.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return "", nil, err
	}
	db := t.getDB(dbID)

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
	return path, meta, err
}

// buildForIngest builds a local SST file containing the keys in the given
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
	defer reader.Close()

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
		objstorageprovider.NewRemoteReadable(objReader, objSize),
		opts,
	)
	panicIfErr(err)

	start := bounds.Start
	end := bounds.End
	if syntheticPrefix.IsSet() {
		start = syntheticPrefix.Invert(start)
		end = syntheticPrefix.Invert(end)
	}
	pointIter, err = reader.NewIter(sstable.NoTransforms, start, end)
	panicIfErr(err)

	rangeDelIter, err = reader.NewRawRangeDelIter(context.Background(), sstable.NoFragmentTransforms, block.NoReadEnv)
	panicIfErr(err)
	if rangeDelIter != nil {
		rangeDelIter = keyspan.Truncate(
			t.opts.Comparer.Compare,
			rangeDelIter,
			base.UserKeyBoundsEndExclusive(start, end),
		)
	}

	rangeKeyIter, err = reader.NewRawRangeKeyIter(context.Background(), sstable.NoFragmentTransforms, block.NoReadEnv)
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

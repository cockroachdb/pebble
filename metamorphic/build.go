// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
)

// writeSSTForIngestion writes an SST that is to be ingested, either directly or
// as an external file.
//
// If convertDelSizedToDel is set, then any DeleteSized keys are converted to
// Delete keys; this is useful when the database that will ingest the file is at
// a format that doesn't support DeleteSized.
//
// If bounds are specified, all points and ranges are truncated to the
// [Start, End) range.
//
// Closes the iterators in all cases.
func writeSSTForIngestion(
	t *Test,
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
	writable objstorage.Writable,
	targetFMV pebble.FormatMajorVersion,
	bounds pebble.KeyRange,
) (*sstable.WriterMetadata, error) {
	writerOpts := t.opts.MakeWriterOptions(0, targetFMV.MaxTableFormat())
	if t.testOpts.disableValueBlocksForIngestSSTables {
		writerOpts.DisableValueBlocks = true
	}
	w := sstable.NewWriter(writable, writerOpts)
	// TODO(radu): this seems like it should work.
	defer closeIters(pointIter, rangeDelIter, rangeKeyIter)

	var lastUserKey []byte
	var key *base.InternalKey
	var value base.LazyValue
	if bounds.Start == nil {
		key, value = pointIter.First()
	} else {
		key, value = pointIter.SeekGE(bounds.Start, base.SeekGEFlagsNone)
	}

	for ; key != nil; key, value = pointIter.Next() {
		if bounds.End != nil && t.opts.Comparer.Compare(key.UserKey, bounds.End) >= 0 {
			break
		}
		// Ignore duplicate keys.
		if t.opts.Comparer.Equal(lastUserKey, key.UserKey) {
			continue
		}
		lastUserKey = append(lastUserKey[:0], key.UserKey...)

		key.SetSeqNum(base.SeqNumZero)
		// It's possible that we wrote the key on a batch from a db that supported
		// DeleteSized, but will be ingesting into a db that does not. Detect this
		// case and translate the key to an InternalKeyKindDelete.
		if targetFMV < pebble.FormatDeleteSizedAndObsolete && key.Kind() == pebble.InternalKeyKindDeleteSized {
			value = pebble.LazyValue{}
			key.SetKind(pebble.InternalKeyKindDelete)
		}
		fmt.Printf("Add %v\n", *key)
		if err := w.Add(*key, value.InPlaceValue()); err != nil {
			return nil, err
		}
	}
	if err := pointIter.Close(); err != nil {
		return nil, err
	}

	if rangeDelIter != nil {
		if bounds.Start != nil || bounds.End != nil {
			rangeDelIter = keyspan.Truncate(
				t.opts.Comparer.Compare,
				rangeDelIter,
				bounds.Start, bounds.End,
				nil /* start */, nil /* end */, false, /* panicOnUpperTruncate */
			)
		}
		span, err := rangeDelIter.First()
		for ; span != nil; span, err = rangeDelIter.Next() {
			startCopy := append([]byte(nil), span.Start...)
			endCopy := append([]byte(nil), span.End...)
			fmt.Printf("DeleteRange %q %q\n", startCopy, endCopy)
			if err := w.DeleteRange(startCopy, endCopy); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		if err := rangeDelIter.Close(); err != nil {
			return nil, err
		}
	}

	if rangeKeyIter != nil {
		if bounds.Start != nil || bounds.End != nil {
			rangeKeyIter = keyspan.Truncate(
				t.opts.Comparer.Compare,
				rangeKeyIter,
				bounds.Start, bounds.End,
				nil /* start */, nil /* end */, false, /* panicOnUpperTruncate */
			)
		}
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
			startCopy := append([]byte(nil), span.Start...)
			endCopy := append([]byte(nil), span.End...)
			collapsed := keyspan.Span{
				Start: startCopy,
				End:   endCopy,
				Keys:  make([]keyspan.Key, 0, len(span.Keys)),
			}
			if err := rangekey.Coalesce(
				t.opts.Comparer.Compare, t.opts.Comparer.Equal, span.Keys, &collapsed.Keys,
			); err != nil {
				return nil, err
			}
			for i := range collapsed.Keys {
				collapsed.Keys[i].Trailer = base.MakeTrailer(0, collapsed.Keys[i].Kind())
			}
			keyspan.SortKeysByTrailer(&collapsed.Keys)
			fmt.Printf("AddRangeKey %q %q\n", startCopy, endCopy)
			if err := rangekey.Encode(&collapsed, w.AddRangeKey); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		if err := rangeKeyIter.Close(); err != nil {
			return nil, err
		}
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	return w.Metadata()
}

// buildForIngest builds a local SST file containing the keys in the given batch
// and returns its path and metadata.
func buildForIngest(
	t *Test, dbID objID, b *pebble.Batch, i int,
) (path string, _ *sstable.WriterMetadata, _ error) {
	path = t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d", dbID.slot(), i))
	f, err := t.opts.FS.Create(path)
	if err != nil {
		return "", nil, err
	}
	db := t.getDB(dbID)

	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)

	writable := objstorageprovider.NewFileWritable(f)
	meta, err := writeSSTForIngestion(
		t,
		iter, rangeDelIter, rangeKeyIter,
		writable,
		db.FormatMajorVersion(),
		pebble.KeyRange{},
	)
	return path, meta, err
}

// buildForIngest builds a local SST file containing the keys in the given
// external object (truncated to the given bounds) and returns its path and
// metadata.
func buildForIngestExternalEmulation(
	t *Test, dbID objID, externalObjID objID, bounds pebble.KeyRange, i int,
) (path string, _ *sstable.WriterMetadata, _ error) {
	path = t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d", dbID.slot(), i))
	fmt.Printf("buildForIngestExternalEmulation %v\n", path)
	f, err := t.opts.FS.Create(path)
	if err != nil {
		return "", nil, err
	}

	objReader, objSize, err := t.externalStorage.ReadObject(context.Background(), externalObjName(externalObjID))
	if err != nil {
		return "", nil, err
	}
	opts := sstable.ReaderOptions{
		Comparer: t.opts.Comparer,
	}
	reader, err := sstable.NewReader(objstorageprovider.NewRemoteReadable(objReader, objSize), opts)
	if err != nil {
		return "", nil, err
	}
	defer reader.Close()
	pointIter, err := reader.NewIter(bounds.Start, bounds.End)
	if err != nil {
		return "", nil, err
	}
	rangeDelIter, err := reader.NewRawRangeDelIter()
	if err != nil {
		return "", nil, err
	}
	rangeKeyIter, err := reader.NewRawRangeKeyIter()
	if err != nil {
		return "", nil, err
	}

	writable := objstorageprovider.NewFileWritable(f)
	meta, err := writeSSTForIngestion(
		t,
		pointIter, rangeDelIter, rangeKeyIter,
		writable,
		t.minFMV(),
		bounds,
	)
	fmt.Printf("buildForIngestExternalEmulation %v done\n", path)
	return path, meta, err
}

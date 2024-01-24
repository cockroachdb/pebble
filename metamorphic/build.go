// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
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
// Closes the iterators in all cases.
func writeSSTForIngestion(
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
	equal base.Equal,
	compare base.Compare,
	writerOpts sstable.WriterOptions,
	writable objstorage.Writable,
	convertDelSizedToDel bool,
) (*sstable.WriterMetadata, error) {
	w := sstable.NewWriter(writable, writerOpts)
	defer closeIters(pointIter, rangeDelIter, rangeKeyIter)

	var lastUserKey []byte
	for key, value := pointIter.First(); key != nil; key, value = pointIter.Next() {
		// Ignore duplicate keys.
		if equal(lastUserKey, key.UserKey) {
			continue
		}
		lastUserKey = append(lastUserKey[:0], key.UserKey...)

		key.SetSeqNum(base.SeqNumZero)
		// It's possible that we wrote the key on a batch from a db that supported
		// DeleteSized, but are now ingesting into a db that does not. Detect
		// this case and translate the key to an InternalKeyKindDelete.
		if convertDelSizedToDel && key.Kind() == pebble.InternalKeyKindDeleteSized {
			value = pebble.LazyValue{}
			key.SetKind(pebble.InternalKeyKindDelete)
		}
		if err := w.Add(*key, value.InPlaceValue()); err != nil {
			return nil, err
		}
	}
	if err := pointIter.Close(); err != nil {
		return nil, err
	}

	if rangeDelIter != nil {
		// NB: The range tombstones have already been fragmented by the Batch.
		var startCopy, endCopy []byte
		t, err := rangeDelIter.First()
		for ; t != nil; t, err = rangeDelIter.Next() {
			startCopy = append(startCopy[:0], t.Start...)
			endCopy = append(endCopy[:0], t.End...)
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
		var startCopy, endCopy []byte
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
			startCopy = append(startCopy[:0], span.Start...)
			endCopy = append(endCopy[:0], span.End...)
			collapsed := keyspan.Span{
				Start: startCopy,
				End:   endCopy,
				Keys:  make([]keyspan.Key, 0, len(span.Keys)),
			}
			if err := rangekey.Coalesce(compare, equal, span.Keys, &collapsed.Keys); err != nil {
				return nil, err
			}
			for i := range collapsed.Keys {
				collapsed.Keys[i].Trailer = base.MakeTrailer(0, collapsed.Keys[i].Kind())
			}
			keyspan.SortKeysByTrailer(&collapsed.Keys)
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

func buildForIngest(
	t *Test, dbID objID, b *pebble.Batch, i int,
) (string, *sstable.WriterMetadata, error) {
	path := t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d-%d", dbID.slot(), i))
	f, err := t.opts.FS.Create(path)
	if err != nil {
		return "", nil, err
	}
	db := t.getDB(dbID)

	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)

	tableFormat := db.FormatMajorVersion().MaxTableFormat()
	wOpts := t.opts.MakeWriterOptions(0, tableFormat)
	if t.testOpts.disableValueBlocksForIngestSSTables {
		wOpts.DisableValueBlocks = true
	}
	writable := objstorageprovider.NewFileWritable(f)
	// It's possible that we wrote the key on a batch from a db that supported
	// DeleteSized, but are now ingesting into a db that does not. Detect
	// this case and translate the key to an InternalKeyKindDelete.
	convertDelSizedToDel := !t.isFMV(dbID, pebble.FormatDeleteSizedAndObsolete)

	meta, err := writeSSTForIngestion(
		iter, rangeDelIter, rangeKeyIter,
		t.opts.Comparer.Equal,
		t.opts.Comparer.Compare,
		wOpts,
		writable,
		convertDelSizedToDel,
	)
	return path, meta, err
}

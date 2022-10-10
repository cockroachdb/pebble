// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// op defines the interface for a single operation, such as creating a batch,
// or advancing an iterator.
type op interface {
	String() string
	run(t *test, h historyRecorder)

	// receiver returns the object ID of the object the operation is performed
	// on. Every operation has a receiver (eg, batch0.Set(...) has `batch0` as
	// its receiver). Receivers are used for synchronization when running with
	// concurrency.
	receiver() objID

	// syncObjs returns an additional set of object IDs—excluding the
	// receiver—that the operation must synchronize with. At execution time,
	// the operation will run serially with respect to all other operations
	// that return these objects from their own syncObjs or receiver methods.
	syncObjs() objIDSlice
}

// initOp performs test initialization
type initOp struct {
	batchSlots    uint32
	iterSlots     uint32
	snapshotSlots uint32
}

func (o *initOp) run(t *test, h historyRecorder) {
	t.batches = make([]*pebble.Batch, o.batchSlots)
	t.iters = make([]*retryableIter, o.iterSlots)
	t.snapshots = make([]*pebble.Snapshot, o.snapshotSlots)
	h.Recordf("%s", o)
}

func (o *initOp) String() string {
	return fmt.Sprintf("Init(%d /* batches */, %d /* iters */, %d /* snapshots */)",
		o.batchSlots, o.iterSlots, o.snapshotSlots)
}

func (o *initOp) receiver() objID      { return dbObjID }
func (o *initOp) syncObjs() objIDSlice { return nil }

// applyOp models a Writer.Apply operation.
type applyOp struct {
	writerID objID
	batchID  objID
}

func (o *applyOp) run(t *test, h historyRecorder) {
	b := t.getBatch(o.batchID)
	w := t.getWriter(o.writerID)
	err := w.Apply(b, t.writeOpts)
	h.Recordf("%s // %v", o, err)
	_ = b.Close()
	t.clearObj(o.batchID)
}

func (o *applyOp) String() string  { return fmt.Sprintf("%s.Apply(%s)", o.writerID, o.batchID) }
func (o *applyOp) receiver() objID { return o.writerID }
func (o *applyOp) syncObjs() objIDSlice {
	// Apply should not be concurrent with operations that are mutating the
	// batch.
	return []objID{o.batchID}
}

// checkpointOp models a DB.Checkpoint operation.
type checkpointOp struct{}

func (o *checkpointOp) run(t *test, h historyRecorder) {
	err := withRetries(func() error {
		return t.db.Checkpoint(o.dir(t.dir, h.op))
	})
	h.Recordf("%s // %v", o, err)
}

func (o *checkpointOp) dir(dataDir string, idx int) string {
	return filepath.Join(dataDir, "checkpoints", fmt.Sprintf("op-%06d", idx))
}

func (o *checkpointOp) String() string       { return "db.Checkpoint()" }
func (o *checkpointOp) receiver() objID      { return dbObjID }
func (o *checkpointOp) syncObjs() objIDSlice { return nil }

// closeOp models a {Batch,Iterator,Snapshot}.Close operation.
type closeOp struct {
	objID objID
}

func (o *closeOp) run(t *test, h historyRecorder) {
	c := t.getCloser(o.objID)
	if o.objID.tag() == dbTag && t.opts.DisableWAL {
		// Special case: If WAL is disabled, do a flush right before DB Close. This
		// allows us to reuse this run's data directory as initial state for
		// future runs without losing any mutations.
		_ = t.db.Flush()
	}
	t.clearObj(o.objID)
	err := c.Close()
	h.Recordf("%s // %v", o, err)
}

func (o *closeOp) String() string  { return fmt.Sprintf("%s.Close()", o.objID) }
func (o *closeOp) receiver() objID { return o.objID }
func (o *closeOp) syncObjs() objIDSlice {
	// Synchronize on the database so that we don't close the database before
	// all its iterators, snapshots and batches are closed.
	// TODO(jackson): It would be nice to relax this so that Close calls can
	// execute in parallel.
	if o.objID == dbObjID {
		return nil
	}
	return []objID{dbObjID}
}

// compactOp models a DB.Compact operation.
type compactOp struct {
	start       []byte
	end         []byte
	parallelize bool
}

func (o *compactOp) run(t *test, h historyRecorder) {
	err := withRetries(func() error {
		return t.db.Compact(o.start, o.end, o.parallelize)
	})
	h.Recordf("%s // %v", o, err)
}

func (o *compactOp) String() string {
	return fmt.Sprintf("db.Compact(%q, %q, %t /* parallelize */)", o.start, o.end, o.parallelize)
}

func (o *compactOp) receiver() objID      { return dbObjID }
func (o *compactOp) syncObjs() objIDSlice { return nil }

// deleteOp models a Write.Delete operation.
type deleteOp struct {
	writerID objID
	key      []byte
}

func (o *deleteOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.Delete(o.key, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *deleteOp) String() string       { return fmt.Sprintf("%s.Delete(%q)", o.writerID, o.key) }
func (o *deleteOp) receiver() objID      { return o.writerID }
func (o *deleteOp) syncObjs() objIDSlice { return nil }

// singleDeleteOp models a Write.SingleDelete operation.
type singleDeleteOp struct {
	writerID           objID
	key                []byte
	maybeReplaceDelete bool
}

func (o *singleDeleteOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	var err error
	if t.testOpts.replaceSingleDelete && o.maybeReplaceDelete {
		err = w.Delete(o.key, t.writeOpts)
	} else {
		err = w.SingleDelete(o.key, t.writeOpts)
	}
	// NOTE: even if the SINGLEDEL was replaced with a DELETE, we must still
	// write the former to the history log. The log line will indicate whether
	// or not the delete *could* have been replaced. The OPTIONS file should
	// also be consulted to determine what happened at runtime (i.e. by taking
	// the logical AND).
	h.Recordf("%s // %v", o, err)
}

func (o *singleDeleteOp) String() string {
	return fmt.Sprintf("%s.SingleDelete(%q, %v /* maybeReplaceDelete */)", o.writerID, o.key, o.maybeReplaceDelete)
}

func (o *singleDeleteOp) receiver() objID      { return o.writerID }
func (o *singleDeleteOp) syncObjs() objIDSlice { return nil }

// deleteRangeOp models a Write.DeleteRange operation.
type deleteRangeOp struct {
	writerID objID
	start    []byte
	end      []byte
}

func (o *deleteRangeOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.DeleteRange(o.start, o.end, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *deleteRangeOp) String() string {
	return fmt.Sprintf("%s.DeleteRange(%q, %q)", o.writerID, o.start, o.end)
}

func (o *deleteRangeOp) receiver() objID      { return o.writerID }
func (o *deleteRangeOp) syncObjs() objIDSlice { return nil }

// flushOp models a DB.Flush operation.
type flushOp struct {
}

func (o *flushOp) run(t *test, h historyRecorder) {
	err := t.db.Flush()
	h.Recordf("%s // %v", o, err)
}

func (o *flushOp) String() string       { return "db.Flush()" }
func (o *flushOp) receiver() objID      { return dbObjID }
func (o *flushOp) syncObjs() objIDSlice { return nil }

// mergeOp models a Write.Merge operation.
type mergeOp struct {
	writerID objID
	key      []byte
	value    []byte
}

func (o *mergeOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.Merge(o.key, o.value, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *mergeOp) String() string       { return fmt.Sprintf("%s.Merge(%q, %q)", o.writerID, o.key, o.value) }
func (o *mergeOp) receiver() objID      { return o.writerID }
func (o *mergeOp) syncObjs() objIDSlice { return nil }

// setOp models a Write.Set operation.
type setOp struct {
	writerID objID
	key      []byte
	value    []byte
}

func (o *setOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.Set(o.key, o.value, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *setOp) String() string       { return fmt.Sprintf("%s.Set(%q, %q)", o.writerID, o.key, o.value) }
func (o *setOp) receiver() objID      { return o.writerID }
func (o *setOp) syncObjs() objIDSlice { return nil }

// rangeKeyDeleteOp models a Write.RangeKeyDelete operation.
type rangeKeyDeleteOp struct {
	writerID objID
	start    []byte
	end      []byte
}

func (o *rangeKeyDeleteOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.RangeKeyDelete(o.start, o.end, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *rangeKeyDeleteOp) String() string {
	return fmt.Sprintf("%s.RangeKeyDelete(%q, %q)", o.writerID, o.start, o.end)
}

func (o *rangeKeyDeleteOp) receiver() objID      { return o.writerID }
func (o *rangeKeyDeleteOp) syncObjs() objIDSlice { return nil }

// rangeKeySetOp models a Write.RangeKeySet operation.
type rangeKeySetOp struct {
	writerID objID
	start    []byte
	end      []byte
	suffix   []byte
	value    []byte
}

func (o *rangeKeySetOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.RangeKeySet(o.start, o.end, o.suffix, o.value, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *rangeKeySetOp) String() string {
	return fmt.Sprintf("%s.RangeKeySet(%q, %q, %q, %q)",
		o.writerID, o.start, o.end, o.suffix, o.value)
}

func (o *rangeKeySetOp) receiver() objID      { return o.writerID }
func (o *rangeKeySetOp) syncObjs() objIDSlice { return nil }

// rangeKeyUnsetOp models a Write.RangeKeyUnset operation.
type rangeKeyUnsetOp struct {
	writerID objID
	start    []byte
	end      []byte
	suffix   []byte
}

func (o *rangeKeyUnsetOp) run(t *test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.RangeKeyUnset(o.start, o.end, o.suffix, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *rangeKeyUnsetOp) String() string {
	return fmt.Sprintf("%s.RangeKeyUnset(%q, %q, %q)",
		o.writerID, o.start, o.end, o.suffix)
}

func (o *rangeKeyUnsetOp) receiver() objID      { return o.writerID }
func (o *rangeKeyUnsetOp) syncObjs() objIDSlice { return nil }

// newBatchOp models a Write.NewBatch operation.
type newBatchOp struct {
	batchID objID
}

func (o *newBatchOp) run(t *test, h historyRecorder) {
	b := t.db.NewBatch()
	t.setBatch(o.batchID, b)
	h.Recordf("%s", o)
}

func (o *newBatchOp) String() string  { return fmt.Sprintf("%s = db.NewBatch()", o.batchID) }
func (o *newBatchOp) receiver() objID { return dbObjID }
func (o *newBatchOp) syncObjs() objIDSlice {
	// NewBatch should not be concurrent with operations that interact with that
	// same batch.
	return []objID{o.batchID}
}

// newIndexedBatchOp models a Write.NewIndexedBatch operation.
type newIndexedBatchOp struct {
	batchID objID
}

func (o *newIndexedBatchOp) run(t *test, h historyRecorder) {
	b := t.db.NewIndexedBatch()
	t.setBatch(o.batchID, b)
	h.Recordf("%s", o)
}

func (o *newIndexedBatchOp) String() string {
	return fmt.Sprintf("%s = db.NewIndexedBatch()", o.batchID)
}
func (o *newIndexedBatchOp) receiver() objID { return dbObjID }
func (o *newIndexedBatchOp) syncObjs() objIDSlice {
	// NewIndexedBatch should not be concurrent with operations that interact
	// with that same batch.
	return []objID{o.batchID}
}

// batchCommitOp models a Batch.Commit operation.
type batchCommitOp struct {
	batchID objID
}

func (o *batchCommitOp) run(t *test, h historyRecorder) {
	b := t.getBatch(o.batchID)
	t.clearObj(o.batchID)
	err := b.Commit(t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *batchCommitOp) String() string  { return fmt.Sprintf("%s.Commit()", o.batchID) }
func (o *batchCommitOp) receiver() objID { return o.batchID }
func (o *batchCommitOp) syncObjs() objIDSlice {
	// Synchronize on the database so that NewIters wait for the commit.
	return []objID{dbObjID}
}

// ingestOp models a DB.Ingest operation.
type ingestOp struct {
	batchIDs []objID
}

func (o *ingestOp) run(t *test, h historyRecorder) {
	// We can only use apply as an alternative for ingestion if we are ingesting
	// a single batch. If we are ingesting multiple batches, the batches may
	// overlap which would cause ingestion to fail but apply would succeed.
	if t.testOpts.ingestUsingApply && len(o.batchIDs) == 1 {
		id := o.batchIDs[0]
		b := t.getBatch(id)
		iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)
		c, err := o.collapseBatch(t, iter, rangeDelIter, rangeKeyIter)
		if err == nil {
			w := t.getWriter(makeObjID(dbTag, 0))
			err = w.Apply(c, t.writeOpts)
		}
		_ = b.Close()
		_ = c.Close()
		t.clearObj(id)
		h.Recordf("%s // %v", o, err)
		return
	}

	var paths []string
	var err error
	for i, id := range o.batchIDs {
		b := t.getBatch(id)
		t.clearObj(id)
		path, err2 := o.build(t, h, b, i)
		if err2 != nil {
			h.Recordf("Build(%s) // %v", id, err2)
		}
		err = firstError(err, err2)
		if err2 == nil {
			paths = append(paths, path)
		}
		err = firstError(err, b.Close())
	}

	err = firstError(err, withRetries(func() error {
		return t.db.Ingest(paths)
	}))

	h.Recordf("%s // %v", o, err)
}

func (o *ingestOp) build(t *test, h historyRecorder, b *pebble.Batch, i int) (string, error) {
	rootFS := vfs.Root(t.opts.FS)
	path := rootFS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d", i))
	f, err := rootFS.Create(path)
	if err != nil {
		return "", err
	}

	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)
	defer closeIters(iter, rangeDelIter, rangeKeyIter)

	equal := t.opts.Comparer.Equal
	tableFormat := t.db.FormatMajorVersion().MaxTableFormat()
	w := sstable.NewWriter(f, t.opts.MakeWriterOptions(0, tableFormat))

	var lastUserKey []byte
	for key, value := iter.First(); key != nil; key, value = iter.Next() {
		// Ignore duplicate keys.
		if equal(lastUserKey, key.UserKey) {
			continue
		}
		// NB: We don't have to copy the key or value since we're reading from a
		// batch which doesn't do prefix compression.
		lastUserKey = key.UserKey

		key.SetSeqNum(0)
		if err := w.Add(*key, value.InPlaceValue()); err != nil {
			return "", err
		}
	}
	if err := iter.Close(); err != nil {
		return "", err
	}
	iter = nil

	if rangeDelIter != nil {
		// NB: The range tombstones have already been fragmented by the Batch.
		for t := rangeDelIter.First(); t != nil; t = rangeDelIter.Next() {
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			if err := w.DeleteRange(t.Start, t.End); err != nil {
				return "", err
			}
		}
		if err := rangeDelIter.Close(); err != nil {
			return "", err
		}
		rangeDelIter = nil
	}

	if err := w.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func (o *ingestOp) receiver() objID { return dbObjID }
func (o *ingestOp) syncObjs() objIDSlice {
	// Ingest should not be concurrent with mutating the batches that will be
	// ingested as sstables.
	return o.batchIDs
}

func closeIters(
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
) {
	if pointIter != nil {
		pointIter.Close()
	}
	if rangeDelIter != nil {
		rangeDelIter.Close()
	}
	if rangeKeyIter != nil {
		rangeKeyIter.Close()
	}
}

// collapseBatch collapses the mutations in a batch to be equivalent to an
// sstable ingesting those mutations. Duplicate updates to a key are collapsed
// so that only the latest update is performed. All range deletions are
// performed first in the batch to match the semantics of ingestion where a
// range deletion does not delete a point record contained in the sstable.
func (o *ingestOp) collapseBatch(
	t *test, pointIter base.InternalIterator, rangeDelIter, rangeKeyIter keyspan.FragmentIterator,
) (*pebble.Batch, error) {
	defer closeIters(pointIter, rangeDelIter, rangeKeyIter)
	equal := t.opts.Comparer.Equal
	collapsed := t.db.NewBatch()

	if rangeDelIter != nil {
		// NB: The range tombstones have already been fragmented by the Batch.
		for t := rangeDelIter.First(); t != nil; t = rangeDelIter.Next() {
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			if err := collapsed.DeleteRange(t.Start, t.End, nil); err != nil {
				return nil, err
			}
		}
		if err := rangeDelIter.Close(); err != nil {
			return nil, err
		}
		rangeDelIter = nil
	}

	if pointIter != nil {
		var lastUserKey []byte
		for key, value := pointIter.First(); key != nil; key, value = pointIter.Next() {
			// Ignore duplicate keys.
			if equal(lastUserKey, key.UserKey) {
				continue
			}
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			lastUserKey = key.UserKey

			var err error
			switch key.Kind() {
			case pebble.InternalKeyKindDelete:
				err = collapsed.Delete(key.UserKey, nil)
			case pebble.InternalKeyKindSingleDelete:
				err = collapsed.SingleDelete(key.UserKey, nil)
			case pebble.InternalKeyKindSet:
				err = collapsed.Set(key.UserKey, value.InPlaceValue(), nil)
			case pebble.InternalKeyKindMerge:
				err = collapsed.Merge(key.UserKey, value.InPlaceValue(), nil)
			case pebble.InternalKeyKindLogData:
				err = collapsed.LogData(key.UserKey, nil)
			default:
				err = errors.Errorf("unknown batch record kind: %d", key.Kind())
			}
			if err != nil {
				return nil, err
			}
		}
		if err := pointIter.Close(); err != nil {
			return nil, err
		}
		pointIter = nil
	}

	return collapsed, nil
}

func (o *ingestOp) String() string {
	var buf strings.Builder
	buf.WriteString("db.Ingest(")
	for i, id := range o.batchIDs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(id.String())
	}
	buf.WriteString(")")
	return buf.String()
}

// getOp models a Reader.Get operation.
type getOp struct {
	readerID objID
	key      []byte
}

func (o *getOp) run(t *test, h historyRecorder) {
	r := t.getReader(o.readerID)
	var val []byte
	var closer io.Closer
	err := withRetries(func() (err error) {
		val, closer, err = r.Get(o.key)
		return err
	})
	h.Recordf("%s // [%q] %v", o, val, err)
	if closer != nil {
		closer.Close()
	}
}

func (o *getOp) String() string  { return fmt.Sprintf("%s.Get(%q)", o.readerID, o.key) }
func (o *getOp) receiver() objID { return o.readerID }
func (o *getOp) syncObjs() objIDSlice {
	if o.readerID == dbObjID {
		return nil
	}
	// batch.Get reads through to the current database state.
	return []objID{dbObjID}
}

// newIterOp models a Reader.NewIter operation.
type newIterOp struct {
	readerID objID
	iterID   objID
	iterOpts
}

func (o *newIterOp) run(t *test, h historyRecorder) {
	r := t.getReader(o.readerID)
	opts := iterOptions(o.iterOpts)

	var i *pebble.Iterator
	for {
		i = r.NewIter(opts)
		if err := i.Error(); !errors.Is(err, errorfs.ErrInjected) {
			break
		}
		// close this iter and retry NewIter
		_ = i.Close()
	}
	t.setIter(o.iterID, i, o.filterMin, o.filterMax)

	// Trash the bounds to ensure that Pebble doesn't rely on the stability of
	// the user-provided bounds.
	if opts != nil {
		rand.Read(opts.LowerBound[:])
		rand.Read(opts.UpperBound[:])
	}
	h.Recordf("%s // %v", o, i.Error())
}

func (o *newIterOp) String() string {
	return fmt.Sprintf("%s = %s.NewIter(%q, %q, %d /* key types */, %d, %d, %q /* masking suffix */)",
		o.iterID, o.readerID, o.lower, o.upper, o.keyTypes, o.filterMin, o.filterMax, o.maskSuffix)
}

func (o *newIterOp) receiver() objID { return o.readerID }
func (o *newIterOp) syncObjs() objIDSlice {
	// Prevent o.iterID ops from running before it exists.
	objs := []objID{o.iterID}
	// If reading through a batch, the new iterator will also observe database
	// state, and we must synchronize on the database state for a consistent
	// view.
	if o.readerID.tag() == batchTag {
		objs = append(objs, dbObjID)
	}
	return objs
}

// newIterUsingCloneOp models a Iterator.Clone operation.
type newIterUsingCloneOp struct {
	existingIterID objID
	iterID         objID
	refreshBatch   bool
	iterOpts

	// derivedReaderID is the ID of the underlying reader that backs both the
	// existing iterator and the new iterator. The derivedReaderID is NOT
	// serialized by String and is derived from other operations during parse.
	derivedReaderID objID
}

func (o *newIterUsingCloneOp) run(t *test, h historyRecorder) {
	iter := t.getIter(o.existingIterID)
	cloneOpts := pebble.CloneOptions{
		IterOptions:      iterOptions(o.iterOpts),
		RefreshBatchView: o.refreshBatch,
	}
	i, err := iter.iter.Clone(cloneOpts)
	if err != nil {
		panic(err)
	}
	filterMin, filterMax := o.filterMin, o.filterMax
	if cloneOpts.IterOptions == nil {
		// We're adopting the same block property filters as iter, so we need to
		// adopt the same run-time filters to ensure determinism.
		filterMin, filterMax = iter.filterMin, iter.filterMax
	}
	t.setIter(o.iterID, i, filterMin, filterMax)
	h.Recordf("%s // %v", o, i.Error())
}

func (o *newIterUsingCloneOp) String() string {
	return fmt.Sprintf("%s = %s.Clone(%t, %q, %q, %d /* key types */, %d, %d, %q /* masking suffix */)",
		o.iterID, o.existingIterID, o.refreshBatch, o.lower, o.upper,
		o.keyTypes, o.filterMin, o.filterMax, o.maskSuffix)
}

func (o *newIterUsingCloneOp) receiver() objID { return o.existingIterID }

func (o *newIterUsingCloneOp) syncObjs() objIDSlice {
	objIDs := []objID{o.iterID}
	// If the underlying reader is a batch, we must synchronize with the batch.
	// If refreshBatch=true, synchronizing is necessary to observe all the
	// mutations up to until this op and no more. Even when refreshBatch=false,
	// we must synchronize because iterator construction may access state cached
	// on the indexed batch to avoid refragmenting range tombstones or range
	// keys.
	if o.derivedReaderID.tag() == batchTag {
		objIDs = append(objIDs, o.derivedReaderID)
	}
	return objIDs
}

// iterSetBoundsOp models an Iterator.SetBounds operation.
type iterSetBoundsOp struct {
	iterID objID
	lower  []byte
	upper  []byte
}

func (o *iterSetBoundsOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	var lower, upper []byte
	if o.lower != nil {
		lower = append(lower, o.lower...)
	}
	if o.upper != nil {
		upper = append(upper, o.upper...)
	}
	i.SetBounds(lower, upper)

	// Trash the bounds to ensure that Pebble doesn't rely on the stability of
	// the user-provided bounds.
	rand.Read(lower[:])
	rand.Read(upper[:])

	h.Recordf("%s // %v", o, i.Error())
}

func (o *iterSetBoundsOp) String() string {
	return fmt.Sprintf("%s.SetBounds(%q, %q)", o.iterID, o.lower, o.upper)
}

func (o *iterSetBoundsOp) receiver() objID      { return o.iterID }
func (o *iterSetBoundsOp) syncObjs() objIDSlice { return nil }

// iterSetOptionsOp models an Iterator.SetOptions operation.
type iterSetOptionsOp struct {
	iterID objID
	iterOpts

	// derivedReaderID is the ID of the underlying reader that backs the
	// iterator. The derivedReaderID is NOT serialized by String and is derived
	// from other operations during parse.
	derivedReaderID objID
}

func (o *iterSetOptionsOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)

	opts := iterOptions(o.iterOpts)
	if opts == nil {
		opts = &pebble.IterOptions{}
	}
	i.SetOptions(opts)

	// Trash the bounds to ensure that Pebble doesn't rely on the stability of
	// the user-provided bounds.
	rand.Read(opts.LowerBound[:])
	rand.Read(opts.UpperBound[:])

	// Adjust the iterator's filters.
	i.filterMin, i.filterMax = o.filterMin, o.filterMax

	h.Recordf("%s // %v", o, i.Error())
}

func (o *iterSetOptionsOp) String() string {
	return fmt.Sprintf("%s.SetOptions(%q, %q, %d /* key types */, %d, %d, %q /* masking suffix */)",
		o.iterID, o.lower, o.upper, o.keyTypes, o.filterMin, o.filterMax, o.maskSuffix)
}

func iterOptions(o iterOpts) *pebble.IterOptions {
	if o.IsZero() {
		return nil
	}
	var lower, upper []byte
	if o.lower != nil {
		lower = append(lower, o.lower...)
	}
	if o.upper != nil {
		upper = append(upper, o.upper...)
	}
	opts := &pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
		KeyTypes:   pebble.IterKeyType(o.keyTypes),
		RangeKeyMasking: pebble.RangeKeyMasking{
			Suffix: o.maskSuffix,
		},
	}
	if opts.RangeKeyMasking.Suffix != nil {
		opts.RangeKeyMasking.Filter = func() pebble.BlockPropertyFilterMask {
			return sstable.NewTestKeysMaskingFilter()
		}
	}
	if o.filterMax > 0 {
		opts.PointKeyFilters = []pebble.BlockPropertyFilter{
			sstable.NewTestKeysBlockPropertyFilter(o.filterMin, o.filterMax),
		}
	}
	return opts
}

func (o *iterSetOptionsOp) receiver() objID { return o.iterID }

func (o *iterSetOptionsOp) syncObjs() objIDSlice {
	if o.derivedReaderID.tag() == batchTag {
		// If the underlying reader is a batch, we must synchronize with the
		// batch so that we observe all the mutations up until this operation
		// and no more.
		return []objID{o.derivedReaderID}
	}
	return nil
}

// iterSeekGEOp models an Iterator.SeekGE[WithLimit] operation.
type iterSeekGEOp struct {
	iterID objID
	key    []byte
	limit  []byte

	derivedReaderID objID
}

func iteratorPos(i *retryableIter) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%q", i.Key())
	hasPoint, hasRange := i.HasPointAndRange()
	if hasPoint {
		fmt.Fprintf(&buf, ",%q", i.Value())
	} else {
		fmt.Fprint(&buf, ",<no point>")
	}
	if hasRange {
		start, end := i.RangeBounds()
		fmt.Fprintf(&buf, ",[%q,%q)=>{", start, end)
		for i, rk := range i.RangeKeys() {
			if i > 0 {
				fmt.Fprint(&buf, ",")
			}
			fmt.Fprintf(&buf, "%q=%q", rk.Suffix, rk.Value)
		}
		fmt.Fprint(&buf, "}")
	} else {
		fmt.Fprint(&buf, ",<no range>")
	}
	if i.RangeKeyChanged() {
		fmt.Fprint(&buf, "*")
	}
	return buf.String()
}

func validBoolToStr(valid bool) string {
	return fmt.Sprintf("%t", valid)
}

func validityStateToStr(validity pebble.IterValidityState) (bool, string) {
	// We can't distinguish between IterExhausted and IterAtLimit in a
	// deterministic manner.
	switch validity {
	case pebble.IterExhausted, pebble.IterAtLimit:
		return false, "invalid"
	case pebble.IterValid:
		return true, "valid"
	default:
		panic("unknown validity")
	}
}

func (o *iterSeekGEOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	var valid bool
	var validStr string
	if o.limit == nil {
		valid = i.SeekGE(o.key)
		validStr = validBoolToStr(valid)
	} else {
		valid, validStr = validityStateToStr(i.SeekGEWithLimit(o.key, o.limit))
	}
	if valid {
		h.Recordf("%s // [%s,%s] %v", o, validStr, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterSeekGEOp) String() string {
	return fmt.Sprintf("%s.SeekGE(%q, %q)", o.iterID, o.key, o.limit)
}
func (o *iterSeekGEOp) receiver() objID      { return o.iterID }
func (o *iterSeekGEOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func onlyBatchIDs(ids ...objID) objIDSlice {
	var ret objIDSlice
	for _, id := range ids {
		if id.tag() == batchTag {
			ret = append(ret, id)
		}
	}
	return ret
}

// iterSeekPrefixGEOp models an Iterator.SeekPrefixGE operation.
type iterSeekPrefixGEOp struct {
	iterID objID
	key    []byte

	derivedReaderID objID
}

func (o *iterSeekPrefixGEOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.SeekPrefixGE(o.key)
	if valid {
		h.Recordf("%s // [%t,%s] %v", o, valid, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterSeekPrefixGEOp) String() string {
	return fmt.Sprintf("%s.SeekPrefixGE(%q)", o.iterID, o.key)
}
func (o *iterSeekPrefixGEOp) receiver() objID      { return o.iterID }
func (o *iterSeekPrefixGEOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

// iterSeekLTOp models an Iterator.SeekLT[WithLimit] operation.
type iterSeekLTOp struct {
	iterID objID
	key    []byte
	limit  []byte

	derivedReaderID objID
}

func (o *iterSeekLTOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	var valid bool
	var validStr string
	if o.limit == nil {
		valid = i.SeekLT(o.key)
		validStr = validBoolToStr(valid)
	} else {
		valid, validStr = validityStateToStr(i.SeekLTWithLimit(o.key, o.limit))
	}
	if valid {
		h.Recordf("%s // [%s,%s] %v", o, validStr, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterSeekLTOp) String() string {
	return fmt.Sprintf("%s.SeekLT(%q, %q)", o.iterID, o.key, o.limit)
}

func (o *iterSeekLTOp) receiver() objID      { return o.iterID }
func (o *iterSeekLTOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

// iterFirstOp models an Iterator.First operation.
type iterFirstOp struct {
	iterID objID

	derivedReaderID objID
}

func (o *iterFirstOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.First()
	if valid {
		h.Recordf("%s // [%t,%s] %v", o, valid, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterFirstOp) String() string       { return fmt.Sprintf("%s.First()", o.iterID) }
func (o *iterFirstOp) receiver() objID      { return o.iterID }
func (o *iterFirstOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

// iterLastOp models an Iterator.Last operation.
type iterLastOp struct {
	iterID objID

	derivedReaderID objID
}

func (o *iterLastOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.Last()
	if valid {
		h.Recordf("%s // [%t,%s] %v", o, valid, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterLastOp) String() string       { return fmt.Sprintf("%s.Last()", o.iterID) }
func (o *iterLastOp) receiver() objID      { return o.iterID }
func (o *iterLastOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

// iterNextOp models an Iterator.Next[WithLimit] operation.
type iterNextOp struct {
	iterID objID
	limit  []byte

	derivedReaderID objID
}

func (o *iterNextOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	var valid bool
	var validStr string
	if o.limit == nil {
		valid = i.Next()
		validStr = validBoolToStr(valid)
	} else {
		valid, validStr = validityStateToStr(i.NextWithLimit(o.limit))
	}
	if valid {
		h.Recordf("%s // [%s,%s] %v", o, validStr, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterNextOp) String() string       { return fmt.Sprintf("%s.Next(%q)", o.iterID, o.limit) }
func (o *iterNextOp) receiver() objID      { return o.iterID }
func (o *iterNextOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

// iterPrevOp models an Iterator.Prev[WithLimit] operation.
type iterPrevOp struct {
	iterID objID
	limit  []byte

	derivedReaderID objID
}

func (o *iterPrevOp) run(t *test, h historyRecorder) {
	i := t.getIter(o.iterID)
	var valid bool
	var validStr string
	if o.limit == nil {
		valid = i.Prev()
		validStr = validBoolToStr(valid)
	} else {
		valid, validStr = validityStateToStr(i.PrevWithLimit(o.limit))
	}
	if valid {
		h.Recordf("%s // [%s,%s] %v", o, validStr, iteratorPos(i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterPrevOp) String() string       { return fmt.Sprintf("%s.Prev(%q)", o.iterID, o.limit) }
func (o *iterPrevOp) receiver() objID      { return o.iterID }
func (o *iterPrevOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

// newSnapshotOp models a DB.NewSnapshot operation.
type newSnapshotOp struct {
	snapID objID
}

func (o *newSnapshotOp) run(t *test, h historyRecorder) {
	s := t.db.NewSnapshot()
	t.setSnapshot(o.snapID, s)
	h.Recordf("%s", o)
}

func (o *newSnapshotOp) String() string       { return fmt.Sprintf("%s = db.NewSnapshot()", o.snapID) }
func (o *newSnapshotOp) receiver() objID      { return dbObjID }
func (o *newSnapshotOp) syncObjs() objIDSlice { return []objID{o.snapID} }

type dbRestartOp struct{}

func (o *dbRestartOp) run(t *test, h historyRecorder) {
	if err := t.restartDB(); err != nil {
		h.Recordf("%s // %v", o, err)
		h.history.err.Store(errors.Wrap(err, "dbRestartOp"))
	} else {
		h.Recordf("%s", o)
	}
}

func (o *dbRestartOp) String() string       { return "db.Restart()" }
func (o *dbRestartOp) receiver() objID      { return dbObjID }
func (o *dbRestartOp) syncObjs() objIDSlice { return nil }

func formatOps(ops []op) string {
	var buf strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	return buf.String()
}

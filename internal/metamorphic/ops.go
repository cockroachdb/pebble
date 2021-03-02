// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/errorfs"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// op defines the interface for a single operation, such as creating a batch,
// or advancing an iterator.
type op interface {
	run(t *test, h *history)
	String() string
}

// initOp performs test initialization
type initOp struct {
	batchSlots    uint32
	iterSlots     uint32
	snapshotSlots uint32
}

func (o *initOp) run(t *test, h *history) {
	t.batches = make([]*pebble.Batch, o.batchSlots)
	t.iters = make([]*retryableIter, o.iterSlots)
	t.snapshots = make([]*pebble.Snapshot, o.snapshotSlots)
	h.Recordf("%s", o)
}

func (o *initOp) String() string {
	return fmt.Sprintf("Init(%d /* batches */, %d /* iters */, %d /* snapshots */)",
		o.batchSlots, o.iterSlots, o.snapshotSlots)
}

// applyOp models a Writer.Apply operation.
type applyOp struct {
	writerID objID
	batchID  objID
}

func (o *applyOp) run(t *test, h *history) {
	b := t.getBatch(o.batchID)
	w := t.getWriter(o.writerID)
	err := w.Apply(b, t.writeOpts)
	h.Recordf("%s // %v", o, err)
	_ = b.Close()
	t.clearObj(o.batchID)
}

func (o *applyOp) String() string {
	return fmt.Sprintf("%s.Apply(%s)", o.writerID, o.batchID)
}

// checkpointOp models a DB.Checkpoint operation.
type checkpointOp struct{}

func (o *checkpointOp) run(t *test, h *history) {
	err := withRetries(func() error {
		return t.db.Checkpoint(o.dir(t.dir, t.idx))
	})
	h.Recordf("%s // %v", o, err)
}

func (o *checkpointOp) dir(dataDir string, idx int) string {
	return filepath.Join(dataDir, "checkpoints", fmt.Sprintf("op-%06d", idx))
}

func (o *checkpointOp) String() string {
	return "db.Checkpoint()"
}

// closeOp models a {Batch,Iterator,Snapshot}.Close operation.
type closeOp struct {
	objID objID
}

func (o *closeOp) run(t *test, h *history) {
	c := t.getCloser(o.objID)
	t.clearObj(o.objID)
	err := c.Close()
	h.Recordf("%s // %v", o, err)
}

func (o *closeOp) String() string {
	return fmt.Sprintf("%s.Close()", o.objID)
}

// compactOp models a DB.Compact operation.
type compactOp struct {
	start []byte
	end   []byte
}

func (o *compactOp) run(t *test, h *history) {
	err := withRetries(func() error {
		return t.db.Compact(o.start, o.end)
	})
	h.Recordf("%s // %v", o, err)
}

func (o *compactOp) String() string {
	return fmt.Sprintf("db.Compact(%q, %q)", o.start, o.end)
}

// deleteOp models a Write.Delete operation.
type deleteOp struct {
	writerID objID
	key      []byte
}

func (o *deleteOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.Delete(o.key, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *deleteOp) String() string {
	return fmt.Sprintf("%s.Delete(%q)", o.writerID, o.key)
}

// singleDeleteOp models a Write.SingleDelete operation.
type singleDeleteOp struct {
	writerID objID
	key      []byte
}

func (o *singleDeleteOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.SingleDelete(o.key, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *singleDeleteOp) String() string {
	return fmt.Sprintf("%s.SingleDelete(%q)", o.writerID, o.key)
}

// deleteRangeOp models a Write.DeleteRange operation.
type deleteRangeOp struct {
	writerID objID
	start    []byte
	end      []byte
}

func (o *deleteRangeOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.DeleteRange(o.start, o.end, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *deleteRangeOp) String() string {
	return fmt.Sprintf("%s.DeleteRange(%q, %q)", o.writerID, o.start, o.end)
}

// flushOp models a DB.Flush operation.
type flushOp struct {
}

func (o *flushOp) run(t *test, h *history) {
	err := t.db.Flush()
	h.Recordf("%s // %v", o, err)
}

func (o *flushOp) String() string {
	return "db.Flush()"
}

// mergeOp models a Write.Merge operation.
type mergeOp struct {
	writerID objID
	key      []byte
	value    []byte
}

func (o *mergeOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.Merge(o.key, o.value, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *mergeOp) String() string {
	return fmt.Sprintf("%s.Merge(%q, %q)", o.writerID, o.key, o.value)
}

// setOp models a Write.Set operation.
type setOp struct {
	writerID objID
	key      []byte
	value    []byte
}

func (o *setOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.Set(o.key, o.value, t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *setOp) String() string {
	return fmt.Sprintf("%s.Set(%q, %q)", o.writerID, o.key, o.value)
}

// newBatchOp models a Write.NewBatch operation.
type newBatchOp struct {
	batchID objID
}

func (o *newBatchOp) run(t *test, h *history) {
	b := t.db.NewBatch()
	t.setBatch(o.batchID, b)
	h.Recordf("%s", o)
}

func (o *newBatchOp) String() string {
	return fmt.Sprintf("%s = db.NewBatch()", o.batchID)
}

// newIndexedBatchOp models a Write.NewIndexedBatch operation.
type newIndexedBatchOp struct {
	batchID objID
}

func (o *newIndexedBatchOp) run(t *test, h *history) {
	b := t.db.NewIndexedBatch()
	t.setBatch(o.batchID, b)
	h.Recordf("%s", o)
}

func (o *newIndexedBatchOp) String() string {
	return fmt.Sprintf("%s = db.NewIndexedBatch()", o.batchID)
}

// batchCommitOp models a Batch.Commit operation.
type batchCommitOp struct {
	batchID objID
}

func (o *batchCommitOp) run(t *test, h *history) {
	b := t.getBatch(o.batchID)
	t.clearObj(o.batchID)
	err := b.Commit(t.writeOpts)
	h.Recordf("%s // %v", o, err)
}

func (o *batchCommitOp) String() string {
	return fmt.Sprintf("%s.Commit()", o.batchID)
}

// ingestOp models a DB.Ingest operation.
type ingestOp struct {
	batchIDs []objID
}

func (o *ingestOp) run(t *test, h *history) {
	// We can only use apply as an alternative for ingestion if we are ingesting
	// a single batch. If we are ingesting multiple batches, the batches may
	// overlap which would cause ingestion to fail but apply would succeed.
	if t.testOpts.ingestUsingApply && len(o.batchIDs) == 1 {
		id := o.batchIDs[0]
		b := t.getBatch(id)
		c, err := o.collapseBatch(t, b)
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

func (o *ingestOp) build(t *test, h *history, b *pebble.Batch, i int) (string, error) {
	rootFS := vfs.Root(t.opts.FS)
	path := rootFS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d", i))
	f, err := rootFS.Create(path)
	if err != nil {
		return "", err
	}

	iter, rangeDelIter := private.BatchSort(b)
	defer func() {
		if iter != nil {
			iter.Close()
		}
		if rangeDelIter != nil {
			rangeDelIter.Close()
		}
	}()

	equal := t.opts.Comparer.Equal
	w := sstable.NewWriter(f, t.opts.MakeWriterOptions(0))

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
		if err := w.Add(*key, value); err != nil {
			return "", err
		}
	}
	if err := iter.Close(); err != nil {
		return "", err
	}
	iter = nil

	if rangeDelIter != nil {
		// NB: The range tombstones have already been fragmented by the Batch.
		var lastUserKey []byte
		var lastValue []byte
		for key, value := rangeDelIter.First(); key != nil; key, value = rangeDelIter.Next() {
			// Ignore duplicate tombstones.
			if equal(lastUserKey, key.UserKey) && equal(lastValue, value) {
				continue
			}
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			lastUserKey = key.UserKey
			lastValue = value

			if err := w.DeleteRange(key.UserKey, value); err != nil {
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

// collapseBatch collapses the mutations in a batch to be equivalent to an
// sstable ingesting those mutations. Duplicate updates to a key are collapsed
// so that only the latest update is performed. All range deletions are
// performed first in the batch to match the semantics of ingestion where a
// range deletion does not delete a point record contained in the sstable.
func (o *ingestOp) collapseBatch(t *test, b *pebble.Batch) (*pebble.Batch, error) {
	iter, rangeDelIter := private.BatchSort(b)
	defer func() {
		if iter != nil {
			iter.Close()
		}
		if rangeDelIter != nil {
			rangeDelIter.Close()
		}
	}()

	equal := t.opts.Comparer.Equal
	collapsed := t.db.NewBatch()

	if rangeDelIter != nil {
		// NB: The range tombstones have already been fragmented by the Batch.
		var lastUserKey []byte
		var lastValue []byte
		for key, value := rangeDelIter.First(); key != nil; key, value = rangeDelIter.Next() {
			// Ignore duplicate tombstones.
			if equal(lastUserKey, key.UserKey) && equal(lastValue, value) {
				continue
			}
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			lastUserKey = key.UserKey
			lastValue = value

			if err := collapsed.DeleteRange(key.UserKey, value, nil); err != nil {
				return nil, err
			}
		}
		if err := rangeDelIter.Close(); err != nil {
			return nil, err
		}
		rangeDelIter = nil
	}

	if iter != nil {
		var lastUserKey []byte
		for key, value := iter.First(); key != nil; key, value = iter.Next() {
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
				err = collapsed.Set(key.UserKey, value, nil)
			case pebble.InternalKeyKindMerge:
				err = collapsed.Merge(key.UserKey, value, nil)
			case pebble.InternalKeyKindLogData:
				err = collapsed.LogData(key.UserKey, nil)
			default:
				err = errors.Errorf("unknown batch record kind: %d", key.Kind())
			}
			if err != nil {
				return nil, err
			}
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		iter = nil
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

func (o *getOp) run(t *test, h *history) {
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

func (o *getOp) String() string {
	return fmt.Sprintf("%s.Get(%q)", o.readerID, o.key)
}

// newIterOp models a Reader.NewIter operation.
type newIterOp struct {
	readerID objID
	iterID   objID
	lower    []byte
	upper    []byte
}

func (o *newIterOp) run(t *test, h *history) {
	r := t.getReader(o.readerID)
	var i *pebble.Iterator
	for {
		i = r.NewIter(&pebble.IterOptions{
			LowerBound: o.lower,
			UpperBound: o.upper,
		})
		if err := i.Error(); !errors.Is(err, errorfs.ErrInjected) {
			break
		}
		// close this iter and retry NewIter
		_ = i.Close()
	}
	t.setIter(o.iterID, i)
	h.Recordf("%s // %v", o, i.Error())
}

func (o *newIterOp) String() string {
	return fmt.Sprintf("%s = %s.NewIter(%q, %q)",
		o.iterID, o.readerID, o.lower, o.upper)
}

// newIterUsingCloneOp models a Iterator.Clone operation.
type newIterUsingCloneOp struct {
	existingIterID objID
	iterID         objID
}

func (o *newIterUsingCloneOp) run(t *test, h *history) {
	iter := t.getIter(o.existingIterID)
	i, err := iter.iter.Clone()
	if err != nil {
		panic(err)
	}
	t.setIter(o.iterID, i)
	h.Recordf("%s // %v", o, i.Error())
}

func (o *newIterUsingCloneOp) String() string {
	return fmt.Sprintf("%s = %s.Clone()", o.iterID, o.existingIterID)
}

// iterSetBoundsOp models an Iterator.SetBounds operation.
type iterSetBoundsOp struct {
	iterID objID
	lower  []byte
	upper  []byte
}

func (o *iterSetBoundsOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	i.SetBounds(o.lower, o.upper)
	h.Recordf("%s // %v", o, i.Error())
}

func (o *iterSetBoundsOp) String() string {
	return fmt.Sprintf("%s.SetBounds(%q, %q)", o.iterID, o.lower, o.upper)
}

// iterSeekGEOp models an Iterator.SeekGE[WithLimit] operation.
type iterSeekGEOp struct {
	iterID objID
	key    []byte
	limit  []byte
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

func (o *iterSeekGEOp) run(t *test, h *history) {
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
		h.Recordf("%s // [%s,%q,%q] %v", o, validStr, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterSeekGEOp) String() string {
	return fmt.Sprintf("%s.SeekGE(%q, %q)", o.iterID, o.key, o.limit)
}

// iterSeekPrefixGEOp models an Iterator.SeekPrefixGE operation.
type iterSeekPrefixGEOp struct {
	iterID objID
	key    []byte
}

func (o *iterSeekPrefixGEOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.SeekPrefixGE(o.key)
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterSeekPrefixGEOp) String() string {
	return fmt.Sprintf("%s.SeekPrefixGE(%q)", o.iterID, o.key)
}

// iterSeekLTOp models an Iterator.SeekLT[WithLimit] operation.
type iterSeekLTOp struct {
	iterID objID
	key    []byte
	limit  []byte
}

func (o *iterSeekLTOp) run(t *test, h *history) {
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
		h.Recordf("%s // [%s,%q,%q] %v", o, validStr, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterSeekLTOp) String() string {
	return fmt.Sprintf("%s.SeekLT(%q, %q)", o.iterID, o.key, o.limit)
}

// iterFirstOp models an Iterator.First operation.
type iterFirstOp struct {
	iterID objID
}

func (o *iterFirstOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.First()
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterFirstOp) String() string {
	return fmt.Sprintf("%s.First()", o.iterID)
}

// iterLastOp models an Iterator.Last operation.
type iterLastOp struct {
	iterID objID
}

func (o *iterLastOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.Last()
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterLastOp) String() string {
	return fmt.Sprintf("%s.Last()", o.iterID)
}

// iterNextOp models an Iterator.Next[WithLimit] operation.
type iterNextOp struct {
	iterID objID
	limit  []byte
}

func (o *iterNextOp) run(t *test, h *history) {
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
		h.Recordf("%s // [%s,%q,%q] %v", o, validStr, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterNextOp) String() string {
	return fmt.Sprintf("%s.Next(%q)", o.iterID, o.limit)
}

// iterPrevOp models an Iterator.Prev[WithLimit] operation.
type iterPrevOp struct {
	iterID objID
	limit  []byte
}

func (o *iterPrevOp) run(t *test, h *history) {
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
		h.Recordf("%s // [%s,%q,%q] %v", o, validStr, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o, validStr, i.Error())
	}
}

func (o *iterPrevOp) String() string {
	return fmt.Sprintf("%s.Prev(%q)", o.iterID, o.limit)
}

// newSnapshotOp models a DB.NewSnapshot operation.
type newSnapshotOp struct {
	snapID objID
}

func (o *newSnapshotOp) run(t *test, h *history) {
	s := t.db.NewSnapshot()
	t.setSnapshot(o.snapID, s)
	h.Recordf("%s", o)
}

func (o *newSnapshotOp) String() string {
	return fmt.Sprintf("%s = db.NewSnapshot()", o.snapID)
}

type dbRestartOp struct {
}

func (o *dbRestartOp) run(t *test, h *history) {
	if err := t.restartDB(); err != nil {
		h.Recordf("%s // %v", o, err)
		h.err.Store(errors.Wrap(err, "dbRestartOp"))
	} else {
		h.Recordf("%s", o)
	}
}

func (o *dbRestartOp) String() string {
	return "db.Restart()"
}

func formatOps(ops []op) string {
	var buf strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	return buf.String()
}

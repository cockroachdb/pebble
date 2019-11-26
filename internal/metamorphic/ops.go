// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/sstable"
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
	t.iters = make([]*pebble.Iterator, o.iterSlots)
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
	err := w.Apply(b, pebble.NoSync)
	h.Recordf("%s // %v", o, err)
	_ = b.Close()
	t.clearObj(o.batchID)
}

func (o *applyOp) String() string {
	return fmt.Sprintf("%s.Apply(%s)", o.writerID, o.batchID)
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

// deleteOp models a Write.Delete operation.
type deleteOp struct {
	writerID objID
	key      []byte
}

func (o *deleteOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.Delete(o.key, pebble.NoSync)
	h.Recordf("%s // %v", o, err)
}

func (o *deleteOp) String() string {
	return fmt.Sprintf("%s.Delete(%q)", o.writerID, o.key)
}

// deleteRangeOp models a Write.DeleteRange operation.
type deleteRangeOp struct {
	writerID objID
	start    []byte
	end      []byte
}

func (o *deleteRangeOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.DeleteRange(o.start, o.end, pebble.NoSync)
	h.Recordf("%s // %v", o, err)
}

func (o *deleteRangeOp) String() string {
	return fmt.Sprintf("%s.DeleteRange(%q, %q)", o.writerID, o.start, o.end)
}

// mergeOp models a Write.Merge operation.
type mergeOp struct {
	writerID objID
	key      []byte
	value    []byte
}

func (o *mergeOp) run(t *test, h *history) {
	w := t.getWriter(o.writerID)
	err := w.Merge(o.key, o.value, pebble.NoSync)
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
	err := w.Set(o.key, o.value, pebble.NoSync)
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
	err := b.Commit(pebble.NoSync)
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

	err = firstError(err, t.db.Ingest(paths))
	for _, path := range paths {
		err = firstError(err, t.opts.FS.Remove(path))
	}

	h.Recordf("%s // %v", o, err)
}

func (o *ingestOp) build(t *test, h *history, b *pebble.Batch, i int) (string, error) {
	path := t.opts.FS.PathJoin(t.tmpDir, fmt.Sprintf("ext%d", i))
	f, err := t.opts.FS.Create(path)
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
	val, err := r.Get(o.key)
	h.Recordf("%s // [%q] %v", o, val, err)
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
	i := r.NewIter(&pebble.IterOptions{
		LowerBound: o.lower,
		UpperBound: o.upper,
	})
	t.setIter(o.iterID, i)
	h.Recordf("%s", o)
}

func (o *newIterOp) String() string {
	return fmt.Sprintf("%s = %s.NewIter(%q, %q)",
		o.iterID, o.readerID, o.lower, o.upper)
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

// iterSeekGEOp models an Iterator.SeekGE operation.
type iterSeekGEOp struct {
	iterID objID
	key    []byte
}

func (o *iterSeekGEOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.SeekGE(o.key)
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterSeekGEOp) String() string {
	return fmt.Sprintf("%s.SeekGE(%q)", o.iterID, o.key)
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
	return fmt.Sprintf("%s.SetPrefixGE(%q)", o.iterID, o.key)
}

// iterSeekLTOp models an Iterator.SeekLT operation.
type iterSeekLTOp struct {
	iterID objID
	key    []byte
}

func (o *iterSeekLTOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.SeekLT(o.key)
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterSeekLTOp) String() string {
	return fmt.Sprintf("%s.SeekLT(%q)", o.iterID, o.key)
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

// iterNextOp models an Iterator.Next operation.
type iterNextOp struct {
	iterID objID
}

func (o *iterNextOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.Next()
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterNextOp) String() string {
	return fmt.Sprintf("%s.Next()", o.iterID)
}

// iterPrevOp models an Iterator.Prev operation.
type iterPrevOp struct {
	iterID objID
}

func (o *iterPrevOp) run(t *test, h *history) {
	i := t.getIter(o.iterID)
	valid := i.Prev()
	if valid {
		h.Recordf("%s // [%t,%q,%q] %v", o, valid, i.Key(), i.Value(), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o, valid, i.Error())
	}
}

func (o *iterPrevOp) String() string {
	return fmt.Sprintf("%s.Prev()", o.iterID)
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

func formatOps(ops []op) string {
	var buf strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	return buf.String()
}

// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/private"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// Ops holds a sequence of operations to be executed by the metamorphic tests.
type Ops []op

// op defines the interface for a single operation, such as creating a batch,
// or advancing an iterator.
type op interface {
	formattedString(KeyFormat) string

	run(t *Test, h historyRecorder)

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

	// rewriteKeys invokes fn for every user key used by the operation. The key
	// is updated to be the result of fn(key).
	//
	// Used for simplification of operations for easier investigations.
	rewriteKeys(fn func(UserKey) UserKey)

	// diagramKeyRanges() returns key spans associated with this operation, to be
	// shown on an ASCII diagram of operations.
	diagramKeyRanges() []pebble.KeyRange
}

// A UserKey is a user key used by the metamorphic test. The format is
// determined by the KeyFormat used by the test.
type UserKey []byte

// A UserKeySuffix is the suffix of a user key used by the metamorphic test. The
// format is determined by the KeyFormat used by the test.
type UserKeySuffix []byte

// initOp performs test initialization
type initOp struct {
	dbSlots          uint32
	batchSlots       uint32
	iterSlots        uint32
	snapshotSlots    uint32
	externalObjSlots uint32
}

func (o *initOp) run(t *Test, h historyRecorder) {
	t.batches = make([]*pebble.Batch, o.batchSlots)
	t.iters = make([]*retryableIter, o.iterSlots)
	t.snapshots = make([]readerCloser, o.snapshotSlots)
	t.externalObjs = make([]externalObjMeta, o.externalObjSlots)
	h.Recordf("%s", o.formattedString(t.testOpts.KeyFormat))
}

func (o *initOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("Init(%d /* dbs */, %d /* batches */, %d /* iters */, %d /* snapshots */, %d /* externalObjs */)",
		o.dbSlots, o.batchSlots, o.iterSlots, o.snapshotSlots, o.externalObjSlots)
}

func (o *initOp) receiver() objID { return makeObjID(dbTag, 1) }
func (o *initOp) syncObjs() objIDSlice {
	syncObjs := make([]objID, 0)
	// Add any additional DBs to syncObjs.
	for i := uint32(2); i < o.dbSlots+1; i++ {
		syncObjs = append(syncObjs, makeObjID(dbTag, i))
	}
	return syncObjs
}

func (o *initOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *initOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// applyOp models a Writer.Apply operation.
type applyOp struct {
	writerID objID
	batchID  objID
}

func (o *applyOp) run(t *Test, h historyRecorder) {
	b := t.getBatch(o.batchID)
	w := t.getWriter(o.writerID)
	var err error
	if o.writerID.tag() == dbTag && t.testOpts.asyncApplyToDB && t.writeOpts.Sync {
		err = w.(*pebble.DB).ApplyNoSyncWait(b, t.writeOpts)
		if err == nil {
			err = b.SyncWait()
		}
	} else {
		err = w.Apply(b, t.writeOpts)
	}
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
	// batch will be closed by a closeOp which is guaranteed to be generated
}

func (o *applyOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s.Apply(%s)", o.writerID, o.batchID)
}
func (o *applyOp) receiver() objID { return o.writerID }
func (o *applyOp) syncObjs() objIDSlice {
	// Apply should not be concurrent with operations that are mutating the
	// batch.
	return []objID{o.batchID}
}

func (o *applyOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *applyOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// checkpointOp models a DB.Checkpoint operation.
type checkpointOp struct {
	dbID objID
	// If non-empty, the checkpoint is restricted to these spans.
	spans []pebble.CheckpointSpan
}

func (o *checkpointOp) run(t *Test, h historyRecorder) {
	// TODO(josh): db.Checkpoint does not work with shared storage yet.
	// It would be better to filter out ahead of calling run on the op,
	// by setting the weight that generator.go uses to zero, or similar.
	// But IIUC the ops are shared for ALL the metamorphic test runs, so
	// not sure how to do that easily:
	// https://github.com/cockroachdb/pebble/blob/master/metamorphic/meta.go#L177
	if t.testOpts.sharedStorageEnabled || t.testOpts.externalStorageEnabled {
		h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), nil)
		return
	}
	var opts []pebble.CheckpointOption
	if len(o.spans) > 0 {
		opts = append(opts, pebble.WithRestrictToSpans(o.spans))
	}
	db := t.getDB(o.dbID)
	err := t.withRetries(func() error {
		return db.Checkpoint(o.dir(t.dir, h.op), opts...)
	})
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *checkpointOp) dir(dataDir string, idx int) string {
	return filepath.Join(dataDir, "checkpoints", fmt.Sprintf("op-%06d", idx))
}

func (o *checkpointOp) formattedString(kf KeyFormat) string {
	var spanStr bytes.Buffer
	for i, span := range o.spans {
		if i > 0 {
			spanStr.WriteString(",")
		}
		fmt.Fprintf(&spanStr, "%q,%q", kf.FormatKey(span.Start), kf.FormatKey(span.End))
	}
	return fmt.Sprintf("%s.Checkpoint(%s)", o.dbID, spanStr.String())
}

func (o *checkpointOp) receiver() objID      { return o.dbID }
func (o *checkpointOp) syncObjs() objIDSlice { return nil }
func (o *checkpointOp) rewriteKeys(fn func(UserKey) UserKey) {
	for i := range o.spans {
		o.spans[i].Start = fn(o.spans[i].Start)
		o.spans[i].End = fn(o.spans[i].End)
	}
}

func (o *checkpointOp) diagramKeyRanges() []pebble.KeyRange {
	var res []pebble.KeyRange
	for i := range o.spans {
		res = append(res, pebble.KeyRange{
			Start: o.spans[i].Start,
			End:   o.spans[i].End,
		})
	}
	return res
}

// downloadOp models a DB.Download operation.
type downloadOp struct {
	dbID  objID
	spans []pebble.DownloadSpan
}

func (o *downloadOp) run(t *Test, h historyRecorder) {
	if t.testOpts.disableDownloads {
		h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), nil)
		return
	}
	db := t.getDB(o.dbID)
	err := t.withRetries(func() error {
		return db.Download(context.Background(), o.spans)
	})
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *downloadOp) formattedString(kf KeyFormat) string {
	var spanStr bytes.Buffer
	for i, span := range o.spans {
		if i > 0 {
			spanStr.WriteString(", ")
		}
		fmt.Fprintf(&spanStr, "%q /* start */, %q /* end */, %v /* viaBackingFileDownload */",
			kf.FormatKey(span.StartKey),
			kf.FormatKey(span.EndKey),
			span.ViaBackingFileDownload)
	}
	return fmt.Sprintf("%s.Download(%s)", o.dbID, spanStr.String())
}

func (o *downloadOp) receiver() objID     { return o.dbID }
func (o downloadOp) syncObjs() objIDSlice { return nil }

func (o *downloadOp) rewriteKeys(fn func(UserKey) UserKey) {
	for i := range o.spans {
		o.spans[i].StartKey = fn(o.spans[i].StartKey)
		o.spans[i].EndKey = fn(o.spans[i].EndKey)
	}
}

func (o *downloadOp) diagramKeyRanges() []pebble.KeyRange {
	var res []pebble.KeyRange
	for i := range o.spans {
		res = append(res, pebble.KeyRange{
			Start: o.spans[i].StartKey,
			End:   o.spans[i].EndKey,
		})
	}
	return res
}

// closeOp models a {Batch,Iterator,Snapshot}.Close operation.
type closeOp struct {
	objID objID

	// affectedObjects is the list of additional objects that are affected by this
	// operation, and which syncObjs() must return so that we don't perform the
	// close in parallel with other operations to affected objects.
	affectedObjects []objID
}

func (o *closeOp) run(t *Test, h historyRecorder) {
	c := t.getCloser(o.objID)
	if o.objID.tag() == dbTag && t.opts.DisableWAL {
		// Special case: If WAL is disabled, do a flush right before DB Close. This
		// allows us to reuse this run's data directory as initial state for
		// future runs without losing any mutations.
		_ = t.getDB(o.objID).Flush()
	}
	t.clearObj(o.objID)
	err := c.Close()
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *closeOp) formattedString(KeyFormat) string { return fmt.Sprintf("%s.Close()", o.objID) }
func (o *closeOp) receiver() objID                  { return o.objID }
func (o *closeOp) syncObjs() objIDSlice {
	return o.affectedObjects
}

func (o *closeOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *closeOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// compactOp models a DB.Compact operation.
type compactOp struct {
	dbID        objID
	start       UserKey
	end         UserKey
	parallelize bool
}

func (o *compactOp) run(t *Test, h historyRecorder) {
	err := t.withRetries(func() error {
		return t.getDB(o.dbID).Compact(o.start, o.end, o.parallelize)
	})
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *compactOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Compact(%q, %q, %t /* parallelize */)",
		o.dbID, kf.FormatKey(o.start), kf.FormatKey(o.end), o.parallelize)
}

func (o *compactOp) receiver() objID      { return o.dbID }
func (o *compactOp) syncObjs() objIDSlice { return nil }

func (o *compactOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.start = fn(o.start)
	o.end = fn(o.end)
}

func (o *compactOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.start, End: o.end}}
}

// deleteOp models a Write.Delete operation.
type deleteOp struct {
	writerID objID
	key      UserKey

	derivedDBID objID
}

func (o *deleteOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	var err error
	if t.testOpts.deleteSized && t.isFMV(o.derivedDBID, pebble.FormatDeleteSizedAndObsolete) {
		// Call DeleteSized with a deterministic size derived from the index.
		// The size does not need to be accurate for correctness.
		err = w.DeleteSized(o.key, hashSize(t.idx), t.writeOpts)
	} else {
		err = w.Delete(o.key, t.writeOpts)
	}
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func hashSize(index int) uint32 {
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	return uint32((11400714819323198485 * uint64(index)) % maxValueSize)
}

func (o *deleteOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Delete(%q)", o.writerID, kf.FormatKey(o.key))
}
func (o *deleteOp) receiver() objID      { return o.writerID }
func (o *deleteOp) syncObjs() objIDSlice { return nil }

func (o *deleteOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *deleteOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// singleDeleteOp models a Write.SingleDelete operation.
type singleDeleteOp struct {
	writerID           objID
	key                UserKey
	maybeReplaceDelete bool
}

func (o *singleDeleteOp) run(t *Test, h historyRecorder) {
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
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *singleDeleteOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.SingleDelete(%q, %v /* maybeReplaceDelete */)",
		o.writerID, kf.FormatKey(o.key), o.maybeReplaceDelete)
}

func (o *singleDeleteOp) receiver() objID      { return o.writerID }
func (o *singleDeleteOp) syncObjs() objIDSlice { return nil }

func (o *singleDeleteOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *singleDeleteOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// deleteRangeOp models a Write.DeleteRange operation.
type deleteRangeOp struct {
	writerID objID
	start    UserKey
	end      UserKey
}

func (o *deleteRangeOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.DeleteRange(o.start, o.end, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *deleteRangeOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.DeleteRange(%q, %q)",
		o.writerID, kf.FormatKey(o.start), kf.FormatKey(o.end))
}

func (o *deleteRangeOp) receiver() objID      { return o.writerID }
func (o *deleteRangeOp) syncObjs() objIDSlice { return nil }

func (o *deleteRangeOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.start = fn(o.start)
	o.end = fn(o.end)
}

func (o *deleteRangeOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.start, End: o.end}}
}

// flushOp models a DB.Flush operation.
type flushOp struct {
	db objID
}

func (o *flushOp) run(t *Test, h historyRecorder) {
	db := t.getDB(o.db)
	err := db.Flush()
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *flushOp) formattedString(KeyFormat) string    { return fmt.Sprintf("%s.Flush()", o.db) }
func (o *flushOp) receiver() objID                     { return o.db }
func (o *flushOp) syncObjs() objIDSlice                { return nil }
func (o *flushOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *flushOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// mergeOp models a Write.Merge operation.
type mergeOp struct {
	writerID objID
	key      UserKey
	value    []byte
}

func (o *mergeOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.Merge(o.key, o.value, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *mergeOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Merge(%q, %q)", o.writerID, kf.FormatKey(o.key), o.value)
}
func (o *mergeOp) receiver() objID      { return o.writerID }
func (o *mergeOp) syncObjs() objIDSlice { return nil }

func (o *mergeOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *mergeOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// setOp models a Write.Set operation.
type setOp struct {
	writerID objID
	key      UserKey
	value    []byte
}

func (o *setOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.Set(o.key, o.value, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *setOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Set(%q, %q)", o.writerID, kf.FormatKey(o.key), o.value)
}
func (o *setOp) receiver() objID      { return o.writerID }
func (o *setOp) syncObjs() objIDSlice { return nil }

func (o *setOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *setOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// rangeKeyDeleteOp models a Write.RangeKeyDelete operation.
type rangeKeyDeleteOp struct {
	writerID objID
	start    UserKey
	end      UserKey
}

func (o *rangeKeyDeleteOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.RangeKeyDelete(o.start, o.end, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *rangeKeyDeleteOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.RangeKeyDelete(%q, %q)",
		o.writerID, kf.FormatKey(o.start), kf.FormatKey(o.end))
}

func (o *rangeKeyDeleteOp) receiver() objID      { return o.writerID }
func (o *rangeKeyDeleteOp) syncObjs() objIDSlice { return nil }

func (o *rangeKeyDeleteOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.start = fn(o.start)
	o.end = fn(o.end)
}

func (o *rangeKeyDeleteOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.start, End: o.end}}
}

// rangeKeySetOp models a Write.RangeKeySet operation.
type rangeKeySetOp struct {
	writerID objID
	start    UserKey
	end      UserKey
	suffix   UserKeySuffix
	value    []byte
}

func (o *rangeKeySetOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.RangeKeySet(o.start, o.end, o.suffix, o.value, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *rangeKeySetOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.RangeKeySet(%q, %q, %q, %q)",
		o.writerID, kf.FormatKey(o.start), kf.FormatKey(o.end),
		kf.FormatKeySuffix(o.suffix), o.value)
}

func (o *rangeKeySetOp) receiver() objID      { return o.writerID }
func (o *rangeKeySetOp) syncObjs() objIDSlice { return nil }

func (o *rangeKeySetOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.start = fn(o.start)
	o.end = fn(o.end)
}

func (o *rangeKeySetOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.start, End: o.end}}
}

// rangeKeyUnsetOp models a Write.RangeKeyUnset operation.
type rangeKeyUnsetOp struct {
	writerID objID
	start    UserKey
	end      UserKey
	suffix   UserKeySuffix
}

func (o *rangeKeyUnsetOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.RangeKeyUnset(o.start, o.end, o.suffix, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *rangeKeyUnsetOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.RangeKeyUnset(%q, %q, %q)",
		o.writerID, kf.FormatKey(o.start), kf.FormatKey(o.end),
		kf.FormatKeySuffix(o.suffix))
}

func (o *rangeKeyUnsetOp) receiver() objID      { return o.writerID }
func (o *rangeKeyUnsetOp) syncObjs() objIDSlice { return nil }

func (o *rangeKeyUnsetOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.start = fn(o.start)
	o.end = fn(o.end)
}

func (o *rangeKeyUnsetOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.start, End: o.end}}
}

// logDataOp models a Writer.LogData operation.
type logDataOp struct {
	writerID objID
	data     []byte
}

func (o *logDataOp) run(t *Test, h historyRecorder) {
	w := t.getWriter(o.writerID)
	err := w.LogData(o.data, t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *logDataOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s.LogData(%q)", o.writerID, o.data)
}

func (o *logDataOp) receiver() objID                     { return o.writerID }
func (o *logDataOp) syncObjs() objIDSlice                { return nil }
func (o *logDataOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *logDataOp) diagramKeyRanges() []pebble.KeyRange { return []pebble.KeyRange{} }

// newBatchOp models a Write.NewBatch operation.
type newBatchOp struct {
	dbID    objID
	batchID objID
}

func (o *newBatchOp) run(t *Test, h historyRecorder) {
	b := t.getDB(o.dbID).NewBatch()
	t.setBatch(o.batchID, b)
	h.Recordf("%s", o.formattedString(t.testOpts.KeyFormat))
}

func (o *newBatchOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s = %s.NewBatch()", o.batchID, o.dbID)
}
func (o *newBatchOp) receiver() objID { return o.dbID }
func (o *newBatchOp) syncObjs() objIDSlice {
	// NewBatch should not be concurrent with operations that interact with that
	// same batch.
	return []objID{o.batchID}
}

func (o *newBatchOp) rewriteKeys(fn func(UserKey) UserKey) {}
func (o *newBatchOp) diagramKeyRanges() []pebble.KeyRange  { return nil }

// newIndexedBatchOp models a Write.NewIndexedBatch operation.
type newIndexedBatchOp struct {
	dbID    objID
	batchID objID
}

func (o *newIndexedBatchOp) run(t *Test, h historyRecorder) {
	b := t.getDB(o.dbID).NewIndexedBatch()
	t.setBatch(o.batchID, b)
	h.Recordf("%s", o.formattedString(t.testOpts.KeyFormat))
}

func (o *newIndexedBatchOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s = %s.NewIndexedBatch()", o.batchID, o.dbID)
}
func (o *newIndexedBatchOp) receiver() objID { return o.dbID }
func (o *newIndexedBatchOp) syncObjs() objIDSlice {
	// NewIndexedBatch should not be concurrent with operations that interact
	// with that same batch.
	return []objID{o.batchID}
}

func (o *newIndexedBatchOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *newIndexedBatchOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// batchCommitOp models a Batch.Commit operation.
type batchCommitOp struct {
	dbID    objID
	batchID objID
}

func (o *batchCommitOp) run(t *Test, h historyRecorder) {
	b := t.getBatch(o.batchID)
	err := b.Commit(t.writeOpts)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *batchCommitOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s.Commit()", o.batchID)
}
func (o *batchCommitOp) receiver() objID { return o.batchID }
func (o *batchCommitOp) syncObjs() objIDSlice {
	// Synchronize on the database so that NewIters wait for the commit.
	return []objID{o.dbID}
}

func (o *batchCommitOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *batchCommitOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// ingestOp models a DB.Ingest operation.
type ingestOp struct {
	dbID     objID
	batchIDs []objID

	derivedDBIDs []objID
}

func (o *ingestOp) run(t *Test, h historyRecorder) {
	// We can only use apply as an alternative for ingestion if we are ingesting
	// a single batch. If we are ingesting multiple batches, the batches may
	// overlap which would cause ingestion to fail but apply would succeed.
	if t.testOpts.ingestUsingApply && len(o.batchIDs) == 1 && o.derivedDBIDs[0] == o.dbID {
		id := o.batchIDs[0]
		b := t.getBatch(id)
		iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)
		db := t.getDB(o.dbID)
		c, err := o.collapseBatch(t, db, iter, rangeDelIter, rangeKeyIter, b)
		if err == nil {
			err = db.Apply(c, t.writeOpts)
		}
		_ = b.Close()
		_ = c.Close()
		t.clearObj(id)
		h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
		return
	}

	var paths []string
	var err error
	for i, id := range o.batchIDs {
		b := t.getBatch(id)
		t.clearObj(id)
		path, _, err2 := buildForIngest(t, o.dbID, b, i)
		if err2 != nil {
			h.Recordf("Build(%s) // %v", id, err2)
		}
		err = firstError(err, err2)
		if err2 == nil {
			paths = append(paths, path)
		}
		err = firstError(err, b.Close())
	}

	err = firstError(err, t.withRetries(func() error {
		return t.getDB(o.dbID).Ingest(context.Background(), paths)
	}))

	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *ingestOp) receiver() objID { return o.dbID }
func (o *ingestOp) syncObjs() objIDSlice {
	// Ingest should not be concurrent with mutating the batches that will be
	// ingested as sstables.
	objs := make([]objID, 0, len(o.batchIDs)+1)
	objs = append(objs, o.batchIDs...)
	addedDBs := make(map[objID]struct{})
	for i := range o.derivedDBIDs {
		_, ok := addedDBs[o.derivedDBIDs[i]]
		if !ok && o.derivedDBIDs[i] != o.dbID {
			objs = append(objs, o.derivedDBIDs[i])
			addedDBs[o.derivedDBIDs[i]] = struct{}{}
		}
	}
	return objs
}

func closeIters(
	pointIter base.InternalIterator,
	rangeDelIter keyspan.FragmentIterator,
	rangeKeyIter keyspan.FragmentIterator,
) {
	if pointIter != nil {
		_ = pointIter.Close()
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
	t *Test,
	db *pebble.DB,
	pointIter base.InternalIterator,
	rangeDelIter, rangeKeyIter keyspan.FragmentIterator,
	b *pebble.Batch,
) (*pebble.Batch, error) {
	defer closeIters(pointIter, rangeDelIter, rangeKeyIter)
	equal := t.opts.Comparer.Equal
	collapsed := db.NewBatch()

	if rangeDelIter != nil {
		// NB: The range tombstones have already been fragmented by the Batch.
		t, err := rangeDelIter.First()
		for ; t != nil; t, err = rangeDelIter.Next() {
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			if err := collapsed.DeleteRange(t.Start, t.End, nil); err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		rangeDelIter.Close()
		rangeDelIter = nil
	}

	if pointIter != nil {
		var lastUserKey []byte
		for kv := pointIter.First(); kv != nil; kv = pointIter.Next() {
			t.opts.Comparer.ValidateKey.MustValidate(kv.K.UserKey)
			// Ignore duplicate keys.
			//
			// Note: this is necessary due to MERGE keys, otherwise it would be
			// fine to include all the keys in the batch and let the normal
			// sequence number precedence determine which of the keys "wins".
			// But the code to build the ingested sstable will only keep the
			// most recent internal key and will not merge across internal keys.
			if equal(lastUserKey, kv.K.UserKey) {
				continue
			}
			// NB: We don't have to copy the key or value since we're reading from a
			// batch which doesn't do prefix compression.
			lastUserKey = kv.K.UserKey

			var err error
			switch kv.Kind() {
			case pebble.InternalKeyKindDelete:
				err = collapsed.Delete(kv.K.UserKey, nil)
			case pebble.InternalKeyKindDeleteSized:
				v, _ := binary.Uvarint(kv.InPlaceValue())
				// Batch.DeleteSized takes just the length of the value being
				// deleted and adds the key's length to derive the overall entry
				// size of the value being deleted. This has already been done
				// to the key we're reading from the batch, so we must subtract
				// the key length from the encoded value before calling
				// collapsed.DeleteSized, which will again add the key length
				// before encoding.
				err = collapsed.DeleteSized(kv.K.UserKey, uint32(v-uint64(len(kv.K.UserKey))), nil)
			case pebble.InternalKeyKindSingleDelete:
				err = collapsed.SingleDelete(kv.K.UserKey, nil)
			case pebble.InternalKeyKindSet:
				err = collapsed.Set(kv.K.UserKey, kv.InPlaceValue(), nil)
			case pebble.InternalKeyKindMerge:
				err = collapsed.Merge(kv.K.UserKey, kv.InPlaceValue(), nil)
			case pebble.InternalKeyKindLogData:
				err = collapsed.LogData(kv.K.UserKey, nil)
			default:
				err = errors.Errorf("unknown batch record kind: %d", kv.Kind())
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

	// There's no equivalent of a MERGE operator for range keys, so there's no
	// need to collapse the range keys here. Rather than reading the range keys
	// from `rangeKeyIter`, which will already be fragmented, read the range
	// keys from the batch and copy them verbatim. This marginally improves our
	// test coverage over the alternative approach of pre-fragmenting and
	// pre-coalescing before writing to the batch.
	//
	// The `rangeKeyIter` is used only to determine if there are any range keys
	// in the batch at all, and only because we already have it handy from
	// private.BatchSort.
	if rangeKeyIter != nil {
		for r := b.Reader(); ; {
			kind, key, value, ok, err := r.Next()
			if !ok {
				if err != nil {
					return nil, err
				}
				break
			} else if !rangekey.IsRangeKey(kind) {
				continue
			}
			ik := base.MakeInternalKey(key, 0, kind)
			t.opts.Comparer.ValidateKey.MustValidate(ik.UserKey)
			if err := collapsed.AddInternalKey(&ik, value, nil); err != nil {
				return nil, err
			}
		}
		rangeKeyIter.Close()
		rangeKeyIter = nil
	}

	return collapsed, nil
}

func (o *ingestOp) formattedString(KeyFormat) string {
	var buf strings.Builder
	buf.WriteString(o.dbID.String())
	buf.WriteString(".Ingest(")
	for i, id := range o.batchIDs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(id.String())
	}
	buf.WriteString(")")
	return buf.String()
}

func (o *ingestOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *ingestOp) diagramKeyRanges() []pebble.KeyRange { return nil }

type ingestAndExciseOp struct {
	dbID                   objID
	batchID                objID
	derivedDBID            objID
	exciseStart, exciseEnd UserKey
}

func (o *ingestAndExciseOp) run(t *Test, h historyRecorder) {
	var err error
	b := t.getBatch(o.batchID)
	t.clearObj(o.batchID)
	if t.testOpts.Opts.Comparer.Compare(o.exciseEnd, o.exciseStart) <= 0 {
		panic("non-well-formed excise span")
	}
	db := t.getDB(o.dbID)
	if b.Empty() {
		h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat),
			o.simulateExcise(db, t))
		return
	}

	path, writerMeta, err2 := buildForIngest(t, o.dbID, b, 0 /* i */)
	if err2 != nil {
		h.Recordf("Build(%s) // %v", o.batchID, err2)
		return
	}
	err = firstError(err, b.Close())

	if writerMeta.Properties.NumEntries == 0 && writerMeta.Properties.NumRangeKeys() == 0 {
		h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), o.simulateExcise(db, t))
		return
	}

	t.opts.Comparer.ValidateKey.MustValidate(o.exciseStart)
	t.opts.Comparer.ValidateKey.MustValidate(o.exciseEnd)
	if t.testOpts.useExcise {
		err = firstError(err, t.withRetries(func() error {
			_, err := db.IngestAndExcise(context.Background(), []string{path}, nil /* shared */, nil /* external */, pebble.KeyRange{
				Start: o.exciseStart,
				End:   o.exciseEnd,
			})
			return err
		}))
	} else {
		err = firstError(err, o.simulateExcise(db, t))
		err = firstError(err, t.withRetries(func() error {
			return db.Ingest(context.Background(), []string{path})
		}))
	}

	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *ingestAndExciseOp) simulateExcise(db *pebble.DB, t *Test) error {
	// Simulate the excise using a DeleteRange and RangeKeyDelete.
	return errors.CombineErrors(
		db.DeleteRange(o.exciseStart, o.exciseEnd, t.writeOpts),
		db.RangeKeyDelete(o.exciseStart, o.exciseEnd, t.writeOpts),
	)
}

func (o *ingestAndExciseOp) receiver() objID { return o.dbID }
func (o *ingestAndExciseOp) syncObjs() objIDSlice {
	// Ingest should not be concurrent with mutating the batches that will be
	// ingested as sstables.
	objs := []objID{o.batchID}
	if o.derivedDBID != o.dbID {
		objs = append(objs, o.derivedDBID)
	}
	return objs
}

func (o *ingestAndExciseOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.IngestAndExcise(%s, %q, %q)",
		o.dbID, o.batchID, kf.FormatKey(o.exciseStart), kf.FormatKey(o.exciseEnd))
}

func (o *ingestAndExciseOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.exciseStart = fn(o.exciseStart)
	o.exciseEnd = fn(o.exciseEnd)
}

func (o *ingestAndExciseOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.exciseStart, End: o.exciseEnd}}
}

// ingestExternalFilesOp models a DB.IngestExternalFiles operation.
//
// When remote storage is not enabled, the operation is emulated using the
// regular DB.Ingest; this serves as a cross-check of the result.
type ingestExternalFilesOp struct {
	dbID objID
	// The bounds of the objects cannot overlap.
	objs []externalObjWithBounds
}

type externalObjWithBounds struct {
	externalObjID objID

	// bounds for the external object. These bounds apply after keys undergo
	// any prefix or suffix transforms.
	bounds pebble.KeyRange

	syntheticPrefix sstable.SyntheticPrefix
	syntheticSuffix sstable.SyntheticSuffix
}

func (o *ingestExternalFilesOp) run(t *Test, h historyRecorder) {
	db := t.getDB(o.dbID)

	// Verify the objects exist (useful for --try-to-reduce).
	for i := range o.objs {
		t.getExternalObj(o.objs[i].externalObjID)
	}

	var err error
	if !t.testOpts.externalStorageEnabled {
		// Emulate the operation by crating local, truncated SST files and ingesting
		// them.
		var paths []string
		for i, obj := range o.objs {
			// Make sure the object exists and is not empty.
			path, sstMeta := buildForIngestExternalEmulation(
				t, o.dbID, obj.externalObjID, obj.bounds, obj.syntheticSuffix, obj.syntheticPrefix, i,
			)
			if sstMeta.HasPointKeys || sstMeta.HasRangeKeys || sstMeta.HasRangeDelKeys {
				paths = append(paths, path)
			}
		}
		if len(paths) > 0 {
			err = db.Ingest(context.Background(), paths)
		}
	} else {
		external := make([]pebble.ExternalFile, len(o.objs))
		for i, obj := range o.objs {
			t.opts.Comparer.ValidateKey.MustValidate(obj.bounds.Start)
			t.opts.Comparer.ValidateKey.MustValidate(obj.bounds.End)

			meta := t.getExternalObj(obj.externalObjID)
			external[i] = pebble.ExternalFile{
				Locator:           "external",
				ObjName:           externalObjName(obj.externalObjID),
				Size:              meta.sstMeta.Size,
				StartKey:          obj.bounds.Start,
				EndKey:            obj.bounds.End,
				EndKeyIsInclusive: false,
				// Note: if the table has point/range keys, we don't know for sure whether
				// this particular range has any, but that's acceptable.
				HasPointKey:     meta.sstMeta.HasPointKeys || meta.sstMeta.HasRangeDelKeys,
				HasRangeKey:     meta.sstMeta.HasRangeKeys,
				SyntheticSuffix: obj.syntheticSuffix,
			}
			if obj.syntheticPrefix.IsSet() {
				external[i].SyntheticPrefix = obj.syntheticPrefix
			}
		}
		_, err = db.IngestExternalFiles(context.Background(), external)
	}

	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *ingestExternalFilesOp) receiver() objID { return o.dbID }
func (o *ingestExternalFilesOp) syncObjs() objIDSlice {
	res := make(objIDSlice, len(o.objs))
	for i := range res {
		res[i] = o.objs[i].externalObjID
	}
	// Deduplicate the IDs.
	slices.Sort(res)
	return slices.Compact(res)
}

func (o *ingestExternalFilesOp) formattedString(kf KeyFormat) string {
	strs := make([]string, len(o.objs))
	for i, obj := range o.objs {
		// NB: The syntheticPrefix is intentionally not formatted, because it's
		// not a valid key itself. It is printed as a simple quoted Go string.
		strs[i] = fmt.Sprintf("%s, %q /* start */, %q /* end */, %q /* syntheticSuffix */, %q /* syntheticPrefix */",
			obj.externalObjID, kf.FormatKey(obj.bounds.Start), kf.FormatKey(obj.bounds.End),
			kf.FormatKeySuffix(UserKeySuffix(obj.syntheticSuffix)), obj.syntheticPrefix,
		)
	}
	return fmt.Sprintf("%s.IngestExternalFiles(%s)", o.dbID, strings.Join(strs, ", "))
}

func (o *ingestExternalFilesOp) rewriteKeys(fn func(UserKey) UserKey) {
	// If any of the objects have synthetic prefixes, we can't allow
	// modification of external object bounds.
	allowModification := true
	for i := range o.objs {
		allowModification = allowModification && !o.objs[i].syntheticPrefix.IsSet()
	}

	for i := range o.objs {
		s, e := fn(o.objs[i].bounds.Start), fn(o.objs[i].bounds.End)
		if allowModification {
			o.objs[i].bounds.Start = s
			o.objs[i].bounds.End = e
		}
	}
}

func (o *ingestExternalFilesOp) diagramKeyRanges() []pebble.KeyRange {
	ranges := make([]pebble.KeyRange, len(o.objs))
	for i, obj := range o.objs {
		ranges[i] = obj.bounds
	}
	return ranges
}

// getOp models a Reader.Get operation.
type getOp struct {
	readerID    objID
	key         UserKey
	derivedDBID objID
}

func (o *getOp) run(t *Test, h historyRecorder) {
	r := t.getReader(o.readerID)
	var val []byte
	var closer io.Closer
	err := t.withRetries(func() (err error) {
		val, closer, err = r.Get(o.key)
		return err
	})
	h.Recordf("%s // [%q] %v", o.formattedString(t.testOpts.KeyFormat), val, err)
	if closer != nil {
		closer.Close()
	}
}

func (o *getOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Get(%q)", o.readerID, kf.FormatKey(o.key))
}
func (o *getOp) receiver() objID { return o.readerID }
func (o *getOp) syncObjs() objIDSlice {
	if o.readerID.tag() == dbTag {
		return nil
	}
	// batch.Get reads through to the current database state.
	if o.derivedDBID != 0 {
		return []objID{o.derivedDBID}
	}
	return nil
}

func (o *getOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *getOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// newIterOp models a Reader.NewIter operation.
type newIterOp struct {
	readerID objID
	iterID   objID
	iterOpts
	derivedDBID objID
}

// Enable this to enable debug logging of range key iterator operations.
const debugIterators = false

func (o *newIterOp) run(t *Test, h historyRecorder) {
	r := t.getReader(o.readerID)
	opts := iterOptions(t.testOpts.KeyFormat, o.iterOpts)
	if debugIterators {
		opts.DebugRangeKeyStack = true
	}

	var i *pebble.Iterator
	for {
		i, _ = r.NewIter(opts)
		if err := i.Error(); !errors.Is(err, errorfs.ErrInjected) {
			break
		}
		// close this iter and retry NewIter
		_ = i.Close()
	}
	t.setIter(o.iterID, i)

	// Trash the bounds to ensure that Pebble doesn't rely on the stability of
	// the user-provided bounds.
	if opts != nil {
		rand.Read(opts.LowerBound[:])
		rand.Read(opts.UpperBound[:])
	}
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), i.Error())
}

func (o *newIterOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s = %s.NewIter(%q, %q, %d /* key types */, %q, %q, %t /* use L6 filters */, %q /* masking suffix */)",
		o.iterID, o.readerID, kf.FormatKey(o.lower), kf.FormatKey(o.upper),
		o.keyTypes, kf.FormatKeySuffix(o.filterMax), kf.FormatKeySuffix(o.filterMin),
		o.useL6Filters, kf.FormatKeySuffix(o.maskSuffix))
}

func (o *newIterOp) receiver() objID { return o.readerID }
func (o *newIterOp) syncObjs() objIDSlice {
	// Prevent o.iterID ops from running before it exists.
	objs := []objID{o.iterID}
	// If reading through a batch or snapshot, the new iterator will also observe database
	// state, and we must synchronize on the database state for a consistent
	// view.
	if o.readerID.tag() == batchTag || o.readerID.tag() == snapTag {
		objs = append(objs, o.derivedDBID)
	}
	return objs
}

func (o *newIterOp) rewriteKeys(fn func(UserKey) UserKey) {
	if o.lower != nil {
		o.lower = fn(o.lower)
	}
	if o.upper != nil {
		o.upper = fn(o.upper)
	}
}

func (o *newIterOp) diagramKeyRanges() []pebble.KeyRange {
	var res []pebble.KeyRange
	if o.lower != nil {
		res = append(res, pebble.KeyRange{Start: o.lower, End: o.lower})
	}
	if o.upper != nil {
		res = append(res, pebble.KeyRange{Start: o.upper, End: o.upper})
	}
	return res
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

func (o *newIterUsingCloneOp) run(t *Test, h historyRecorder) {
	iter := t.getIter(o.existingIterID)
	cloneOpts := pebble.CloneOptions{
		IterOptions:      iterOptions(t.testOpts.KeyFormat, o.iterOpts),
		RefreshBatchView: o.refreshBatch,
	}
	i, err := iter.iter.Clone(cloneOpts)
	if err != nil {
		panic(err)
	}
	t.setIter(o.iterID, i)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), i.Error())
}

func (o *newIterUsingCloneOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s = %s.Clone(%t, %q, %q, %d /* key types */, %q, %q, %t /* use L6 filters */, %q /* masking suffix */)",
		o.iterID, o.existingIterID, o.refreshBatch, kf.FormatKey(o.lower),
		kf.FormatKey(o.upper), o.keyTypes, kf.FormatKeySuffix(o.filterMax),
		kf.FormatKeySuffix(o.filterMin), o.useL6Filters, kf.FormatKeySuffix(o.maskSuffix))
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

func (o *newIterUsingCloneOp) rewriteKeys(fn func(UserKey) UserKey) {}
func (o *newIterUsingCloneOp) diagramKeyRanges() []pebble.KeyRange  { return nil }

// iterSetBoundsOp models an Iterator.SetBounds operation.
type iterSetBoundsOp struct {
	iterID objID
	lower  UserKey
	upper  UserKey
}

func (o *iterSetBoundsOp) run(t *Test, h historyRecorder) {
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

	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), i.Error())
}

func (o *iterSetBoundsOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.SetBounds(%q, %q)", o.iterID,
		kf.FormatKey(o.lower), kf.FormatKey(o.upper))
}

func (o *iterSetBoundsOp) receiver() objID      { return o.iterID }
func (o *iterSetBoundsOp) syncObjs() objIDSlice { return nil }

func (o *iterSetBoundsOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.lower = fn(o.lower)
	o.upper = fn(o.upper)
}

func (o *iterSetBoundsOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.lower, End: o.upper}}
}

// iterSetOptionsOp models an Iterator.SetOptions operation.
type iterSetOptionsOp struct {
	iterID objID
	iterOpts

	// derivedReaderID is the ID of the underlying reader that backs the
	// iterator. The derivedReaderID is NOT serialized by String and is derived
	// from other operations during parse.
	derivedReaderID objID
}

func (o *iterSetOptionsOp) run(t *Test, h historyRecorder) {
	i := t.getIter(o.iterID)

	opts := iterOptions(t.testOpts.KeyFormat, o.iterOpts)
	if opts == nil {
		opts = &pebble.IterOptions{}
	}
	i.SetOptions(opts)

	// Trash the bounds to ensure that Pebble doesn't rely on the stability of
	// the user-provided bounds.
	rand.Read(opts.LowerBound[:])
	rand.Read(opts.UpperBound[:])

	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), i.Error())
}

func (o *iterSetOptionsOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.SetOptions(%q, %q, %d /* key types */, %q, %q, %t /* use L6 filters */, %q /* masking suffix */)",
		o.iterID, kf.FormatKey(o.lower), kf.FormatKey(o.upper),
		o.keyTypes, kf.FormatKeySuffix(o.filterMax), kf.FormatKeySuffix(o.filterMin),
		o.useL6Filters, kf.FormatKeySuffix(o.maskSuffix))
}

func iterOptions(kf KeyFormat, o iterOpts) *pebble.IterOptions {
	if o.IsZero() && !debugIterators {
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
		UseL6Filters:       o.useL6Filters,
		DebugRangeKeyStack: debugIterators,
	}
	if opts.RangeKeyMasking.Suffix != nil {
		opts.RangeKeyMasking.Filter = kf.NewSuffixFilterMask
	}
	if o.filterMin != nil {
		opts.PointKeyFilters = []pebble.BlockPropertyFilter{
			kf.NewSuffixBlockPropertyFilter(o.filterMin, o.filterMax),
		}
		if o.filterMax != nil && kf.Comparer.ComparePointSuffixes(o.filterMin, o.filterMax) >= 0 {
			panic(errors.AssertionFailedf("filterMin >= filterMax: %q >= %q", o.filterMin, o.filterMax))
		}
		// Enforce the timestamp bounds in SkipPoint, so that the iterator never
		// returns a key outside the filterMin, filterMax bounds. This provides
		// deterministic iteration.
		opts.SkipPoint = func(k []byte) (skip bool) {
			n := kf.Comparer.Split(k)
			if n == len(k) {
				// No suffix, skip it. We adopt these semantics because the MVCC
				// timestamp block-property collector used by CockroachDB just
				// ignores keys without suffixes, meaning that filtering on the
				// property results in nondeterministic presence of
				// non-timestamped keys.
				return true
			}
			if kf.Comparer.ComparePointSuffixes(k[n:], o.filterMin) <= 0 {
				return true
			}
			if o.filterMax != nil && kf.Comparer.ComparePointSuffixes(k[n:], o.filterMax) > 0 {
				return true
			}
			return false
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

func (o *iterSetOptionsOp) rewriteKeys(fn func(UserKey) UserKey) {}
func (o *iterSetOptionsOp) diagramKeyRanges() []pebble.KeyRange  { return nil }

// iterSeekGEOp models an Iterator.SeekGE[WithLimit] operation.
type iterSeekGEOp struct {
	iterID objID
	key    UserKey
	limit  UserKey

	derivedReaderID objID
}

func iteratorPos(kf KeyFormat, i *retryableIter) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%q", kf.FormatKey(i.Key()))
	hasPoint, hasRange := i.HasPointAndRange()
	if hasPoint {
		fmt.Fprintf(&buf, ",%q", i.Value())
	} else {
		fmt.Fprint(&buf, ",<no point>")
	}
	if hasRange {
		start, end := i.RangeBounds()
		fmt.Fprintf(&buf, ",[%q,%q)=>{", kf.FormatKey(start), kf.FormatKey(end))
		for i, rk := range i.RangeKeys() {
			if i > 0 {
				fmt.Fprint(&buf, ",")
			}
			fmt.Fprintf(&buf, "%q=%q", kf.FormatKeySuffix(rk.Suffix), rk.Value)
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

func (o *iterSeekGEOp) run(t *Test, h historyRecorder) {
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
		h.Recordf("%s // [%s,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v",
			o.formattedString(t.testOpts.KeyFormat), validStr, i.Error())
	}
}

func (o *iterSeekGEOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.SeekGE(%q, %q)",
		o.iterID, kf.FormatKey(o.key), kf.FormatKey(o.limit))
}
func (o *iterSeekGEOp) receiver() objID      { return o.iterID }
func (o *iterSeekGEOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterSeekGEOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *iterSeekGEOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

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
	key    UserKey

	derivedReaderID objID
}

func (o *iterSeekPrefixGEOp) run(t *Test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.SeekPrefixGE(o.key)
	if valid {
		h.Recordf("%s // [%t,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			valid, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o.formattedString(t.testOpts.KeyFormat), valid, i.Error())
	}
}

func (o *iterSeekPrefixGEOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.SeekPrefixGE(%q)", o.iterID, kf.FormatKey(o.key))
}
func (o *iterSeekPrefixGEOp) receiver() objID      { return o.iterID }
func (o *iterSeekPrefixGEOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterSeekPrefixGEOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *iterSeekPrefixGEOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// iterSeekLTOp models an Iterator.SeekLT[WithLimit] operation.
type iterSeekLTOp struct {
	iterID objID
	key    UserKey
	limit  UserKey

	derivedReaderID objID
}

func (o *iterSeekLTOp) run(t *Test, h historyRecorder) {
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
		h.Recordf("%s // [%s,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, i.Error())
	}
}

func (o *iterSeekLTOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.SeekLT(%q, %q)", o.iterID,
		kf.FormatKey(o.key), kf.FormatKey(o.limit))
}

func (o *iterSeekLTOp) receiver() objID      { return o.iterID }
func (o *iterSeekLTOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterSeekLTOp) rewriteKeys(fn func(UserKey) UserKey) {
	o.key = fn(o.key)
}

func (o *iterSeekLTOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: o.key, End: o.key}}
}

// iterFirstOp models an Iterator.First operation.
type iterFirstOp struct {
	iterID objID

	derivedReaderID objID
}

func (o *iterFirstOp) run(t *Test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.First()
	if valid {
		h.Recordf("%s // [%t,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			valid, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o.formattedString(t.testOpts.KeyFormat),
			valid, i.Error())
	}
}

func (o *iterFirstOp) formattedString(KeyFormat) string { return fmt.Sprintf("%s.First()", o.iterID) }
func (o *iterFirstOp) receiver() objID                  { return o.iterID }
func (o *iterFirstOp) syncObjs() objIDSlice             { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterFirstOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *iterFirstOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// iterLastOp models an Iterator.Last operation.
type iterLastOp struct {
	iterID objID

	derivedReaderID objID
}

func (o *iterLastOp) run(t *Test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.Last()
	if valid {
		h.Recordf("%s // [%t,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			valid, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%t] %v", o.formattedString(t.testOpts.KeyFormat),
			valid, i.Error())
	}
}

func (o *iterLastOp) formattedString(KeyFormat) string { return fmt.Sprintf("%s.Last()", o.iterID) }
func (o *iterLastOp) receiver() objID                  { return o.iterID }
func (o *iterLastOp) syncObjs() objIDSlice             { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterLastOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *iterLastOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// iterNextOp models an Iterator.Next[WithLimit] operation.
type iterNextOp struct {
	iterID objID
	limit  UserKey

	derivedReaderID objID
}

func (o *iterNextOp) run(t *Test, h historyRecorder) {
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
		h.Recordf("%s // [%s,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, i.Error())
	}
}

func (o *iterNextOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Next(%q)", o.iterID, kf.FormatKey(o.limit))
}
func (o *iterNextOp) receiver() objID      { return o.iterID }
func (o *iterNextOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterNextOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *iterNextOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// iterNextPrefixOp models an Iterator.NextPrefix operation.
type iterNextPrefixOp struct {
	iterID objID

	derivedReaderID objID
}

func (o *iterNextPrefixOp) run(t *Test, h historyRecorder) {
	i := t.getIter(o.iterID)
	valid := i.NextPrefix()
	validStr := validBoolToStr(valid)
	if valid {
		h.Recordf("%s // [%s,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, i.Error())
	}
}

func (o *iterNextPrefixOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s.NextPrefix()", o.iterID)
}
func (o *iterNextPrefixOp) receiver() objID      { return o.iterID }
func (o *iterNextPrefixOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterNextPrefixOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *iterNextPrefixOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// iterCanSingleDelOp models a call to CanDeterministicallySingleDelete with an
// Iterator.
type iterCanSingleDelOp struct {
	iterID objID

	derivedReaderID objID
}

func (o *iterCanSingleDelOp) run(t *Test, h historyRecorder) {
	// TODO(jackson): When we perform error injection, we'll need to rethink
	// this.
	_, err := pebble.CanDeterministicallySingleDelete(t.getIter(o.iterID).iter)
	// The return value of CanDeterministicallySingleDelete is dependent on
	// internal LSM state and non-deterministic, so we don't record it.
	// Including the operation within the metamorphic test at all helps ensure
	// that it does not change the result of any other Iterator operation that
	// should be deterministic, regardless of its own outcome.
	//
	// We still record the value of the error because it's deterministic, at
	// least for now. The possible error cases are:
	//  - The iterator was already in an error state when the operation ran.
	//  - The operation is deterministically invalid (like using an InternalNext
	//    to change directions.)
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *iterCanSingleDelOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s.InternalNext()", o.iterID)
}
func (o *iterCanSingleDelOp) receiver() objID      { return o.iterID }
func (o *iterCanSingleDelOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterCanSingleDelOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *iterCanSingleDelOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// iterPrevOp models an Iterator.Prev[WithLimit] operation.
type iterPrevOp struct {
	iterID objID
	limit  UserKey

	derivedReaderID objID
}

func (o *iterPrevOp) run(t *Test, h historyRecorder) {
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
		h.Recordf("%s // [%s,%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, iteratorPos(t.testOpts.KeyFormat, i), i.Error())
	} else {
		h.Recordf("%s // [%s] %v", o.formattedString(t.testOpts.KeyFormat),
			validStr, i.Error())
	}
}

func (o *iterPrevOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Prev(%q)", o.iterID, kf.FormatKey(o.limit))
}
func (o *iterPrevOp) receiver() objID      { return o.iterID }
func (o *iterPrevOp) syncObjs() objIDSlice { return onlyBatchIDs(o.derivedReaderID) }

func (o *iterPrevOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *iterPrevOp) diagramKeyRanges() []pebble.KeyRange { return nil }

// newSnapshotOp models a DB.NewSnapshot operation.
type newSnapshotOp struct {
	dbID   objID
	snapID objID
	// If nonempty, this snapshot must not be used to read any keys outside of
	// the provided bounds. This allows some implementations to use 'Eventually
	// file-only snapshots,' which require bounds.
	bounds []pebble.KeyRange
}

func (o *newSnapshotOp) run(t *Test, h historyRecorder) {
	bounds := o.bounds
	if len(bounds) == 0 {
		panic("bounds unexpectedly unset for newSnapshotOp")
	}
	// Fibonacci hash https://probablydance.com/2018/06/16/fibonacci-hashing-the-optimization-that-the-world-forgot-or-a-better-alternative-to-integer-modulo/
	createEfos := ((11400714819323198485 * uint64(t.idx) * t.testOpts.seedEFOS) >> 63) == 1
	// If either of these options is true, an EFOS _must_ be created, regardless
	// of what the fibonacci hash returned.
	excisePossible := t.testOpts.useSharedReplicate || t.testOpts.useExternalReplicate || t.testOpts.useExcise
	if createEfos || excisePossible {
		s := t.getDB(o.dbID).NewEventuallyFileOnlySnapshot(bounds)
		t.setSnapshot(o.snapID, s)
	} else {
		s := t.getDB(o.dbID).NewSnapshot()
		t.setSnapshot(o.snapID, s)
	}
	h.Recordf("%s", o.formattedString(t.testOpts.KeyFormat))
}

func (o *newSnapshotOp) formattedString(kf KeyFormat) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s = %s.NewSnapshot(", o.snapID, o.dbID)
	for i := range o.bounds {
		if i > 0 {
			fmt.Fprint(&buf, ", ")
		}
		fmt.Fprintf(&buf, "%q, %q", kf.FormatKey(o.bounds[i].Start), kf.FormatKey(o.bounds[i].End))
	}
	fmt.Fprint(&buf, ")")
	return buf.String()
}
func (o *newSnapshotOp) receiver() objID      { return o.dbID }
func (o *newSnapshotOp) syncObjs() objIDSlice { return []objID{o.snapID} }

func (o *newSnapshotOp) rewriteKeys(fn func(UserKey) UserKey) {
	for i := range o.bounds {
		o.bounds[i].Start = fn(o.bounds[i].Start)
		o.bounds[i].End = fn(o.bounds[i].End)
	}
}

func (o *newSnapshotOp) diagramKeyRanges() []pebble.KeyRange {
	return o.bounds
}

// newExternalObjOp models a DB.NewExternalObj operation.
type newExternalObjOp struct {
	batchID       objID
	externalObjID objID
}

func externalObjName(externalObjID objID) string {
	if externalObjID.tag() != externalObjTag {
		panic(fmt.Sprintf("invalid externalObjID %s", externalObjID))
	}
	return fmt.Sprintf("external-for-ingest-%d.sst", externalObjID.slot())
}

func (o *newExternalObjOp) run(t *Test, h historyRecorder) {
	b := t.getBatch(o.batchID)
	t.clearObj(o.batchID)

	writeCloser, err := t.externalStorage.CreateObject(externalObjName(o.externalObjID))
	if err != nil {
		panic(err)
	}
	writable := objstorageprovider.NewRemoteWritable(writeCloser)

	iter, rangeDelIter, rangeKeyIter := private.BatchSort(b)

	sstMeta, err := writeSSTForIngestion(
		t,
		iter, rangeDelIter, rangeKeyIter,
		true, /* uniquePrefixes */
		nil,  /* syntheticSuffix */
		nil,  /* syntheticPrefix */
		writable,
		t.minFMV(),
	)
	if err != nil {
		panic(err)
	}
	if !sstMeta.HasPointKeys && !sstMeta.HasRangeDelKeys && !sstMeta.HasRangeKeys {
		// This can occur when using --try-to-reduce.
		panic("metamorphic test internal error: external object empty")
	}
	t.setExternalObj(o.externalObjID, externalObjMeta{
		sstMeta: sstMeta,
	})
	h.Recordf("%s", o.formattedString(t.testOpts.KeyFormat))
}

func (o *newExternalObjOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s = %s.NewExternalObj()", o.externalObjID, o.batchID)
}
func (o *newExternalObjOp) receiver() objID      { return o.batchID }
func (o *newExternalObjOp) syncObjs() objIDSlice { return []objID{o.externalObjID} }

func (o *newExternalObjOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *newExternalObjOp) diagramKeyRanges() []pebble.KeyRange { return nil }

type dbRatchetFormatMajorVersionOp struct {
	dbID objID
	vers pebble.FormatMajorVersion
}

func (o *dbRatchetFormatMajorVersionOp) run(t *Test, h historyRecorder) {
	var err error
	// NB: We no-op the operation if we're already at or above the provided
	// format major version. Different runs start at different format major
	// versions, making the presence of an error and the error message itself
	// non-deterministic if we attempt to upgrade to an older version.
	//
	// Regardless, subsequent operations should behave identically, which is
	// what we're really aiming to test by including this format major version
	// ratchet operation.
	if t.getDB(o.dbID).FormatMajorVersion() < o.vers {
		err = t.getDB(o.dbID).RatchetFormatMajorVersion(o.vers)
	}
	h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
}

func (o *dbRatchetFormatMajorVersionOp) formattedString(KeyFormat) string {
	return fmt.Sprintf("%s.RatchetFormatMajorVersion(%s)", o.dbID, o.vers)
}
func (o *dbRatchetFormatMajorVersionOp) receiver() objID      { return o.dbID }
func (o *dbRatchetFormatMajorVersionOp) syncObjs() objIDSlice { return nil }

func (o *dbRatchetFormatMajorVersionOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *dbRatchetFormatMajorVersionOp) diagramKeyRanges() []pebble.KeyRange { return nil }

type dbRestartOp struct {
	dbID objID

	// affectedObjects is the list of additional objects that are affected by this
	// operation, and which syncObjs() must return so that we don't perform the
	// restart in parallel with other operations to affected objects.
	affectedObjects []objID
}

func (o *dbRestartOp) run(t *Test, h historyRecorder) {
	if err := t.restartDB(o.dbID); err != nil {
		h.Recordf("%s // %v", o.formattedString(t.testOpts.KeyFormat), err)
		h.history.err.Store(errors.Wrap(err, "dbRestartOp"))
	} else {
		h.Recordf("%s", o.formattedString(t.testOpts.KeyFormat))
	}
}

func (o *dbRestartOp) formattedString(KeyFormat) string { return fmt.Sprintf("%s.Restart()", o.dbID) }
func (o *dbRestartOp) receiver() objID                  { return o.dbID }
func (o *dbRestartOp) syncObjs() objIDSlice             { return o.affectedObjects }

func (o *dbRestartOp) rewriteKeys(func(UserKey) UserKey)   {}
func (o *dbRestartOp) diagramKeyRanges() []pebble.KeyRange { return nil }

func formatOps(kf KeyFormat, ops []op) string {
	var buf strings.Builder
	for _, op := range ops {
		fmt.Fprintf(&buf, "%s\n", op.formattedString(kf))
	}
	return buf.String()
}

// replicateOp models an operation that could copy keys from one db to
// another through either an IngestAndExcise, or an Ingest.
type replicateOp struct {
	source, dest objID
	start, end   UserKey
}

func (r *replicateOp) runSharedReplicate(
	t *Test, h historyRecorder, source, dest *pebble.DB, w *sstable.Writer, sstPath string,
) {
	var sharedSSTs []pebble.SharedSSTMeta
	var err error
	err = source.ScanInternal(context.TODO(), block.CategoryUnknown, r.start, r.end,
		func(key *pebble.InternalKey, value pebble.LazyValue, _ pebble.IteratorLevel) error {
			val, _, err := value.Value(nil)
			if err != nil {
				panic(err)
			}
			return w.Raw().Add(base.MakeInternalKey(key.UserKey, 0, key.Kind()), val, false)
		},
		func(start, end []byte, seqNum base.SeqNum) error {
			return w.DeleteRange(start, end)
		},
		func(start, end []byte, keys []keyspan.Key) error {
			return w.Raw().EncodeSpan(keyspan.Span{
				Start: start,
				End:   end,
				Keys:  keys,
			})
		},
		func(sst *pebble.SharedSSTMeta) error {
			sharedSSTs = append(sharedSSTs, *sst)
			return nil
		},
		nil,
	)
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}

	err = w.Close()
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	meta, err := w.Raw().Metadata()
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	if len(sharedSSTs) == 0 && meta.Properties.NumEntries == 0 && meta.Properties.NumRangeKeys() == 0 {
		// IngestAndExcise below will be a no-op. We should do a
		// DeleteRange+RangeKeyDel to mimic the behaviour of the non-shared-replicate
		// case.
		//
		// TODO(bilal): Remove this when we support excises with no matching ingests.
		if err := dest.RangeKeyDelete(r.start, r.end, t.writeOpts); err != nil {
			h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
			return
		}
		err := dest.DeleteRange(r.start, r.end, t.writeOpts)
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}

	_, err = dest.IngestAndExcise(context.Background(), []string{sstPath}, sharedSSTs, nil /* external */, pebble.KeyRange{Start: r.start, End: r.end})
	h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
}

func (r *replicateOp) runExternalReplicate(
	t *Test, h historyRecorder, source, dest *pebble.DB, w *sstable.Writer, sstPath string,
) {
	var externalSSTs []pebble.ExternalFile
	var err error
	err = source.ScanInternal(context.TODO(), block.CategoryUnknown, r.start, r.end,
		func(key *pebble.InternalKey, value pebble.LazyValue, _ pebble.IteratorLevel) error {
			val, _, err := value.Value(nil)
			if err != nil {
				panic(err)
			}
			t.opts.Comparer.ValidateKey.MustValidate(key.UserKey)
			return w.Raw().Add(base.MakeInternalKey(key.UserKey, 0, key.Kind()), val, false)
		},
		func(start, end []byte, seqNum base.SeqNum) error {
			t.opts.Comparer.ValidateKey.MustValidate(start)
			t.opts.Comparer.ValidateKey.MustValidate(end)
			return w.DeleteRange(start, end)
		},
		func(start, end []byte, keys []keyspan.Key) error {
			t.opts.Comparer.ValidateKey.MustValidate(start)
			t.opts.Comparer.ValidateKey.MustValidate(end)
			return w.Raw().EncodeSpan(keyspan.Span{
				Start: start,
				End:   end,
				Keys:  keys,
			})
		},
		nil,
		func(sst *pebble.ExternalFile) error {
			externalSSTs = append(externalSSTs, *sst)
			return nil
		},
	)
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}

	err = w.Close()
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	meta, err := w.Raw().Metadata()
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	if len(externalSSTs) == 0 && meta.Properties.NumEntries == 0 && meta.Properties.NumRangeKeys() == 0 {
		// IngestAndExcise below will be a no-op. We should do a
		// DeleteRange+RangeKeyDel to mimic the behaviour of the non-external-replicate
		// case.
		//
		// TODO(bilal): Remove this when we support excises with no matching ingests.
		if err := dest.RangeKeyDelete(r.start, r.end, t.writeOpts); err != nil {
			h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
			return
		}
		err := dest.DeleteRange(r.start, r.end, t.writeOpts)
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}

	t.opts.Comparer.ValidateKey.MustValidate(r.start)
	t.opts.Comparer.ValidateKey.MustValidate(r.end)

	_, err = dest.IngestAndExcise(context.Background(), []string{sstPath}, nil, externalSSTs /* external */, pebble.KeyRange{Start: r.start, End: r.end})
	h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
}

func (r *replicateOp) run(t *Test, h historyRecorder) {
	// Shared replication only works if shared storage is enabled.
	useSharedIngest := t.testOpts.useSharedReplicate && t.testOpts.sharedStorageEnabled
	useExternalIngest := t.testOpts.useExternalReplicate && t.testOpts.externalStorageEnabled

	source := t.getDB(r.source)
	dest := t.getDB(r.dest)
	sstPath := path.Join(t.tmpDir, fmt.Sprintf("ext-replicate%d.sst", t.idx))
	f, err := t.opts.FS.Create(sstPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), t.opts.MakeWriterOptions(0, dest.TableFormat()))

	// NB: In practice we'll either do shared replicate or external replicate,
	// as ScanInternal does not support both. We arbitrarily choose to prioritize
	// external replication if both are enabled, as those are likely to hit
	// widespread usage first.
	if useExternalIngest {
		r.runExternalReplicate(t, h, source, dest, w, sstPath)
		return
	}
	if useSharedIngest {
		r.runSharedReplicate(t, h, source, dest, w, sstPath)
		return
	}

	t.opts.Comparer.ValidateKey.MustValidate(r.start)
	t.opts.Comparer.ValidateKey.MustValidate(r.end)
	// First, do a RangeKeyDelete and DeleteRange on the whole span.
	if err := dest.RangeKeyDelete(r.start, r.end, t.writeOpts); err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	if err := dest.DeleteRange(r.start, r.end, t.writeOpts); err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	iter, err := source.NewIter(&pebble.IterOptions{
		LowerBound: r.start,
		UpperBound: r.end,
		KeyTypes:   pebble.IterKeyTypePointsAndRanges,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = iter.Close() }()

	for ok := iter.SeekGE(r.start); ok && iter.Error() == nil; ok = iter.Next() {
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasPoint {
			val, err := iter.ValueAndErr()
			if err != nil {
				panic(err)
			}
			if err := w.Set(iter.Key(), val); err != nil {
				panic(err)
			}
		}
		if hasRange && iter.RangeKeyChanged() {
			rangeKeys := iter.RangeKeys()
			rkStart, rkEnd := iter.RangeBounds()

			t.opts.Comparer.ValidateKey.MustValidate(rkStart)
			t.opts.Comparer.ValidateKey.MustValidate(rkEnd)
			span := keyspan.Span{Start: rkStart, End: rkEnd, Keys: make([]keyspan.Key, len(rangeKeys))}
			for i := range rangeKeys {
				span.Keys[i] = keyspan.Key{
					Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeKeySet),
					Suffix:  rangeKeys[i].Suffix,
					Value:   rangeKeys[i].Value,
				}
			}
			keyspan.SortKeysByTrailer(span.Keys)
			if err := w.Raw().EncodeSpan(span); err != nil {
				panic(err)
			}
		}
	}
	if err := iter.Error(); err != nil {
		h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
		return
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	err = dest.Ingest(context.Background(), []string{sstPath})
	h.Recordf("%s // %v", r.formattedString(t.testOpts.KeyFormat), err)
}

func (r *replicateOp) formattedString(kf KeyFormat) string {
	return fmt.Sprintf("%s.Replicate(%s, %q, %q)", r.source, r.dest,
		kf.FormatKey(r.start), kf.FormatKey(r.end))
}

func (r *replicateOp) receiver() objID      { return r.source }
func (r *replicateOp) syncObjs() objIDSlice { return objIDSlice{r.dest} }

func (r *replicateOp) rewriteKeys(fn func(UserKey) UserKey) {
	r.start = fn(r.start)
	r.end = fn(r.end)
}

func (r *replicateOp) diagramKeyRanges() []pebble.KeyRange {
	return []pebble.KeyRange{{Start: r.start, End: r.end}}
}

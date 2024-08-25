// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
)

// New constructs a new metamorphic test that runs the provided operations
// against a database using the provided TestOptions and outputs the history of
// events to an io.Writer.
//
// dir specifies the path within opts.Opts.FS to open the database.
func New(ops Ops, opts *TestOptions, dir string, w io.Writer) (*Test, error) {
	t := newTest(ops)
	h := newHistory(nil /* failRegexp */, w)
	if err := t.init(h, dir, opts, 1 /* numInstances */, 0 /* opTimeout */); err != nil {
		return nil, err
	}
	return t, nil
}

// A Test configures an individual test run consisting of a set of operations,
// TestOptions configuring the target database to which the operations should be
// applied, and a sink for outputting test history.
type Test struct {
	// The list of ops to execute. The ops refer to slots in the batches, iters,
	// and snapshots slices.
	ops       []op
	opsWaitOn [][]int         // op index -> op indexes
	opsDone   []chan struct{} // op index -> done channel
	idx       int
	dir       string
	h         *history
	opTimeout time.Duration
	opts      *pebble.Options
	testOpts  *TestOptions
	writeOpts *pebble.WriteOptions
	tmpDir    string
	// The DBs the test is run on.
	dbs []*pebble.DB
	// The slots for the batches, iterators, and snapshots. These are read and
	// written by the ops to pass state from one op to another.
	batches      []*pebble.Batch
	iters        []*retryableIter
	snapshots    []readerCloser
	externalObjs []externalObjMeta

	// externalStorage is used to write external objects. If external storage is
	// enabled, this is the same with testOpts.externalStorageFS; otherwise, this
	// is an in-memory implementation used only by the test.
	externalStorage remote.Storage
}

type externalObjMeta struct {
	sstMeta *sstable.WriterMetadata
}

func newTest(ops []op) *Test {
	return &Test{
		ops: ops,
	}
}

func (t *Test) init(
	h *history, dir string, testOpts *TestOptions, numInstances int, opTimeout time.Duration,
) error {
	t.dir = dir
	t.h = h
	t.opTimeout = opTimeout
	t.testOpts = testOpts
	t.writeOpts = pebble.NoSync
	if testOpts.strictFS {
		t.writeOpts = pebble.Sync
	} else {
		t.writeOpts = pebble.NoSync
	}
	testOpts.Opts.WithFSDefaults()
	t.opts = testOpts.Opts.EnsureDefaults()
	t.opts.Logger = h
	lel := pebble.MakeLoggingEventListener(t.opts.Logger)
	t.opts.EventListener = &lel
	// If the test options set a DebugCheck func, wrap it with retrying of
	// retriable errors (according to the test's retry policy).
	if debugCheck := t.opts.DebugCheck; debugCheck != nil {
		t.opts.DebugCheck = func(db *pebble.DB) error {
			return t.withRetries(func() error { return debugCheck(db) })
		}
	}
	if numInstances < 1 {
		numInstances = 1
	}
	if t.testOpts.externalStorageEnabled {
		t.externalStorage = t.testOpts.externalStorageFS
	} else {
		t.externalStorage = remote.NewInMem()
	}

	t.opsWaitOn, t.opsDone = computeSynchronizationPoints(t.ops)

	defer t.opts.Cache.Unref()

	// If an error occurs and we were using an in-memory FS, attempt to clone to
	// on-disk in order to allow post-mortem debugging. Note that always using
	// the on-disk FS isn't desirable because there is a large performance
	// difference between in-memory and on-disk which causes different code paths
	// and timings to be exercised.
	maybeExit := func(err error) {
		if err == nil || errors.Is(err, errorfs.ErrInjected) || errors.Is(err, pebble.ErrCancelledCompaction) {
			return
		}
		t.saveInMemoryData()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Exit early on any error from a background operation.
	t.opts.EventListener.BackgroundError = func(err error) {
		t.opts.Logger.Infof("background error: %s", err)
		maybeExit(err)
	}
	t.opts.EventListener.CompactionEnd = func(info pebble.CompactionInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.FlushEnd = func(info pebble.FlushInfo) {
		t.opts.Logger.Infof("%s", info)
		if info.Err != nil && !strings.Contains(info.Err.Error(), "pebble: empty table") {
			maybeExit(info.Err)
		}
	}
	t.opts.EventListener.DownloadEnd = func(info pebble.DownloadInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.ManifestCreated = func(info pebble.ManifestCreateInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.ManifestDeleted = func(info pebble.ManifestDeleteInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.TableDeleted = func(info pebble.TableDeleteInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.TableIngested = func(info pebble.TableIngestInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.WALCreated = func(info pebble.WALCreateInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}
	t.opts.EventListener.WALDeleted = func(info pebble.WALDeleteInfo) {
		t.opts.Logger.Infof("%s", info)
		maybeExit(info.Err)
	}

	for i := range t.testOpts.CustomOpts {
		if err := t.testOpts.CustomOpts[i].Open(t.opts); err != nil {
			return err
		}
	}

	t.dbs = make([]*pebble.DB, numInstances)
	for i := range t.dbs {
		var db *pebble.DB
		var err error
		if len(t.dbs) > 1 {
			dir = path.Join(t.dir, fmt.Sprintf("db%d", i+1))
		}
		err = t.withRetries(func() error {
			db, err = pebble.Open(dir, t.opts)
			return err
		})
		if err != nil {
			return err
		}
		t.dbs[i] = db
		h.log.Printf("// db%d.Open() %v", i+1, err)

		if t.testOpts.sharedStorageEnabled {
			err = t.withRetries(func() error {
				return db.SetCreatorID(uint64(i + 1))
			})
			if err != nil {
				return err
			}
			h.log.Printf("// db%d.SetCreatorID() %v", i+1, err)
		}
	}

	var err error
	t.tmpDir = t.opts.FS.PathJoin(t.dir, "tmp")
	if err = t.opts.FS.MkdirAll(t.tmpDir, 0755); err != nil {
		return err
	}
	if t.testOpts.strictFS {
		// Sync the whole directory path for the tmpDir, since restartDB() is executed during
		// the test. That would reset MemFS to the synced state, which would make an unsynced
		// directory disappear in the middle of the test. It is the responsibility of the test
		// (not Pebble) to ensure that it can write the ssts that it will subsequently ingest
		// into Pebble.
		for {
			f, err := t.opts.FS.OpenDir(dir)
			if err != nil {
				return err
			}
			if err = f.Sync(); err != nil {
				return err
			}
			if err = f.Close(); err != nil {
				return err
			}
			if len(dir) == 1 {
				break
			}
			dir = t.opts.FS.PathDir(dir)
			// TODO(sbhola): PathDir returns ".", which OpenDir() complains about. Fix.
			if len(dir) == 1 {
				dir = "/"
			}
		}
	}

	return nil
}

func (t *Test) withRetries(fn func() error) error {
	return withRetries(fn, t.testOpts.RetryPolicy)
}

func (t *Test) isFMV(dbID objID, fmv pebble.FormatMajorVersion) bool {
	db := t.getDB(dbID)
	return db.FormatMajorVersion() >= fmv
}

// minFMV returns the minimum FormatMajorVersion between all databases.
func (t *Test) minFMV() pebble.FormatMajorVersion {
	minVersion := pebble.FormatNewest
	for _, db := range t.dbs {
		if db != nil {
			minVersion = min(minVersion, db.FormatMajorVersion())
		}
	}
	return minVersion
}

func (t *Test) restartDB(dbID objID) error {
	db := t.getDB(dbID)
	// If strictFS is not used, we use pebble.NoSync for writeOpts, so we can't
	// restart the database (even if we don't revert to synced data).
	if !t.testOpts.strictFS {
		return nil
	}
	if t.testOpts.sharedStorageEnabled {
		// We simulate a crash by essentially ignoring writes to disk after a
		// certain point. However, we cannot prevent the process (which didn't
		// actually crash) from deleting an external object before we call Close().
		// TODO(radu): perhaps we want all syncs to fail after the "crash" point?
		return nil
	}
	// We can't do this if we have more than one database since they share the
	// same FS (and we only close/reopen one of them).
	// TODO(radu): have each database use its own MemFS.
	if len(t.dbs) > 1 {
		return nil
	}
	t.opts.Cache.Ref()

	memFS := vfs.Root(t.opts.FS).(*vfs.MemFS)
	memFS.SetIgnoreSyncs(true)
	if err := db.Close(); err != nil {
		return err
	}
	// Release any resources held by custom options. This may be used, for
	// example, by the encryption-at-rest custom option (within the Cockroach
	// repository) to close the file registry.
	for i := range t.testOpts.CustomOpts {
		if err := t.testOpts.CustomOpts[i].Close(t.opts); err != nil {
			return err
		}
	}

	memFS.ResetToSyncedState()
	memFS.SetIgnoreSyncs(false)

	// TODO(jackson): Audit errorRate and ensure custom options' hooks semantics
	// are well defined within the context of retries.
	err := t.withRetries(func() (err error) {
		// Reacquire any resources required by custom options. This may be used, for
		// example, by the encryption-at-rest custom option (within the Cockroach
		// repository) to reopen the file registry.
		for i := range t.testOpts.CustomOpts {
			if err := t.testOpts.CustomOpts[i].Open(t.opts); err != nil {
				return err
			}
		}
		dir := t.dir
		if len(t.dbs) > 1 {
			dir = path.Join(dir, fmt.Sprintf("db%d", dbID.slot()))
		}
		t.dbs[dbID.slot()-1], err = pebble.Open(dir, t.opts)
		if err != nil {
			return err
		}
		return err
	})
	t.opts.Cache.Unref()
	return err
}

func (t *Test) saveInMemoryDataInternal() error {
	if rootFS := vfs.Root(t.opts.FS); rootFS != vfs.Default {
		// t.opts.FS is an in-memory system; copy it to disk.
		if err := os.RemoveAll(t.dir); err != nil {
			return err
		}
		if _, err := vfs.Clone(rootFS, vfs.Default, t.dir, t.dir); err != nil {
			return err
		}
	}
	if t.testOpts.sharedStorageEnabled {
		if err := copyRemoteStorage(t.testOpts.sharedStorageFS, filepath.Join(t.dir, "shared")); err != nil {
			return err
		}
	}
	if t.testOpts.externalStorageEnabled {
		if err := copyRemoteStorage(t.testOpts.externalStorageFS, filepath.Join(t.dir, "external")); err != nil {
			return err
		}
	}
	return nil
}

func copyRemoteStorage(fs remote.Storage, outputDir string) error {
	if err := vfs.Default.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	objs, err := fs.List("", "")
	if err != nil {
		return err
	}
	for i := range objs {
		reader, readSize, err := fs.ReadObject(context.TODO(), objs[i])
		if err != nil {
			return err
		}
		buf := make([]byte, readSize)
		if err := reader.ReadAt(context.TODO(), buf, 0); err != nil {
			return err
		}
		outputPath := vfs.Default.PathJoin(outputDir, objs[i])
		outputFile, err := vfs.Default.Create(outputPath, vfs.WriteCategoryUnspecified)
		if err != nil {
			return err
		}
		if _, err := outputFile.Write(buf); err != nil {
			outputFile.Close()
			return err
		}
		if err := outputFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// If an in-memory FS is being used, save the contents to disk.
func (t *Test) saveInMemoryData() {
	if err := t.saveInMemoryDataInternal(); err != nil {
		t.opts.Logger.Infof("unable to save data: %s: %v", t.dir, err)
	}
}

// Step runs one single operation, returning: whether there are additional
// operations remaining; the operation's output; and an error if any occurred
// while running the operation.
//
// Step may be used instead of Execute to advance a test one operation at a
// time.
func (t *Test) Step() (more bool, operationOutput string, err error) {
	more = t.step(t.h, func(format string, args ...interface{}) {
		operationOutput = fmt.Sprintf(format, args...)
	})
	err = t.h.Error()
	return more, operationOutput, err
}

func (t *Test) step(h *history, optionalRecordf func(string, ...interface{})) bool {
	if t.idx >= len(t.ops) {
		return false
	}
	t.runOp(t.idx, h.recorder(-1 /* thread */, t.idx, optionalRecordf))
	t.idx++
	return true
}

// runOp runs t.ops[idx] with t.opTimeout.
func (t *Test) runOp(idx int, h historyRecorder) {
	op := t.ops[idx]
	var timer *time.Timer
	if t.opTimeout > 0 {
		opTimeout := t.opTimeout
		switch op.(type) {
		case *compactOp, *downloadOp, *newSnapshotOp, *ingestOp, *ingestAndExciseOp, *ingestExternalFilesOp:
			// These ops can be very slow, especially if we end up with many tiny
			// tables. Bump up the timout by a factor.
			opTimeout *= 4
		}
		timer = time.AfterFunc(opTimeout, func() {
			panic(fmt.Sprintf("operation took longer than %s: %s", opTimeout, op.String()))
		})
	}
	op.run(t, h)
	if timer != nil {
		timer.Stop()
	}
}

func (t *Test) setBatch(id objID, b *pebble.Batch) {
	if id.tag() != batchTag {
		panic(fmt.Sprintf("invalid batch ID: %s", id))
	}
	t.batches[id.slot()] = b
}

func (t *Test) setIter(id objID, i *pebble.Iterator) {
	if id.tag() != iterTag {
		panic(fmt.Sprintf("invalid iter ID: %s", id))
	}
	t.iters[id.slot()] = &retryableIter{
		iter:      i,
		lastKey:   nil,
		needRetry: t.testOpts.RetryPolicy,
	}
}

type readerCloser interface {
	pebble.Reader
	io.Closer
}

func (t *Test) setSnapshot(id objID, s readerCloser) {
	if id.tag() != snapTag {
		panic(fmt.Sprintf("invalid snapshot ID: %s", id))
	}
	t.snapshots[id.slot()] = s
}

func (t *Test) setExternalObj(id objID, meta externalObjMeta) {
	if id.tag() != externalObjTag {
		panic(fmt.Sprintf("invalid external object ID: %s", id))
	}
	t.externalObjs[id.slot()] = meta
}

func (t *Test) getExternalObj(id objID) externalObjMeta {
	if id.tag() != externalObjTag || t.externalObjs[id.slot()].sstMeta == nil {
		panic(fmt.Sprintf("metamorphic test internal error: invalid external object ID: %s", id))
	}
	return t.externalObjs[id.slot()]
}

func (t *Test) clearObj(id objID) {
	switch id.tag() {
	case dbTag:
		t.dbs[id.slot()-1] = nil
	case batchTag:
		t.batches[id.slot()] = nil
	case iterTag:
		t.iters[id.slot()] = nil
	case snapTag:
		t.snapshots[id.slot()] = nil
	default:
		panic(fmt.Sprintf("cannot clear ID: %s", id))
	}
}

func (t *Test) getBatch(id objID) *pebble.Batch {
	if id.tag() != batchTag || t.batches[id.slot()] == nil {
		panic(fmt.Sprintf("metamorphic test internal error: invalid batch ID: %s", id))
	}
	return t.batches[id.slot()]
}

func (t *Test) getCloser(id objID) io.Closer {
	switch id.tag() {
	case dbTag:
		return t.dbs[id.slot()-1]
	case batchTag:
		return t.batches[id.slot()]
	case iterTag:
		return t.iters[id.slot()]
	case snapTag:
		return t.snapshots[id.slot()]
	default:
		panic(fmt.Sprintf("cannot close ID: %s", id))
	}
}

func (t *Test) getIter(id objID) *retryableIter {
	if id.tag() != iterTag {
		panic(fmt.Sprintf("invalid iter ID: %s", id))
	}
	return t.iters[id.slot()]
}

func (t *Test) getReader(id objID) pebble.Reader {
	switch id.tag() {
	case dbTag:
		return t.dbs[id.slot()-1]
	case batchTag:
		return t.batches[id.slot()]
	case snapTag:
		return t.snapshots[id.slot()]
	default:
		panic(fmt.Sprintf("invalid reader ID: %s", id))
	}
}

func (t *Test) getWriter(id objID) pebble.Writer {
	switch id.tag() {
	case dbTag:
		return t.dbs[id.slot()-1]
	case batchTag:
		return t.batches[id.slot()]
	default:
		panic(fmt.Sprintf("invalid writer ID: %s", id))
	}
}

func (t *Test) getDB(id objID) *pebble.DB {
	switch id.tag() {
	case dbTag:
		return t.dbs[id.slot()-1]
	default:
		panic(fmt.Sprintf("invalid writer tag: %v", id.tag()))
	}
}

// Compute the synchronization points between operations. When operating
// with more than 1 thread, operations must synchronize access to shared
// objects. Compute two slices the same length as ops.
//
// opsWaitOn: the value v at index i indicates that operation i must wait
// for the operation at index v to finish before it may run. NB: v < i
//
// opsDone: the channel at index i must be closed when the operation at index i
// completes. This slice is sparse. Operations that are never used as
// synchronization points may have a nil channel.
func computeSynchronizationPoints(ops []op) (opsWaitOn [][]int, opsDone []chan struct{}) {
	opsDone = make([]chan struct{}, len(ops)) // operation index -> done channel
	opsWaitOn = make([][]int, len(ops))       // operation index -> operation index
	lastOpReference := make(map[objID]int)    // objID -> operation index
	for i, o := range ops {
		// Find the last operation that involved the same receiver object. We at
		// least need to wait on that operation.
		receiver := o.receiver()
		waitIndex, ok := lastOpReference[receiver]
		lastOpReference[receiver] = i
		if !ok {
			// Only valid for i=0. For all other operations, the receiver should
			// have been referenced by some other operation before it's used as
			// a receiver.
			if i != 0 && receiver.tag() != dbTag {
				panic(fmt.Sprintf("op %s on receiver %s; first reference of %s", ops[i].String(), receiver, receiver))
			}
			// The initOp is a little special. We do want to store the objects it's
			// syncing on, in `lastOpReference`.
			if i != 0 {
				continue
			}
		}

		// The last operation that referenced `receiver` is the one at index
		// `waitIndex`. All operations with the same receiver are performed on
		// the same thread. We only need to synchronize on the operation at
		// `waitIndex` if `receiver` isn't also the receiver on that operation
		// too.
		if ops[waitIndex].receiver() != receiver {
			opsWaitOn[i] = append(opsWaitOn[i], waitIndex)
		}

		// In additional to synchronizing on the operation's receiver operation,
		// we may need to synchronize on additional objects. For example,
		// batch0.Commit() must synchronize its receiver, batch0, but also on
		// the DB since it mutates database state.
		for _, syncObjID := range o.syncObjs() {
			if vi, vok := lastOpReference[syncObjID]; vok {
				if vi == i {
					panic(fmt.Sprintf("%s has %s as syncObj multiple times", ops[i].String(), syncObjID))
				}
				opsWaitOn[i] = append(opsWaitOn[i], vi)
			}
			lastOpReference[syncObjID] = i
		}

		waitIndexes := opsWaitOn[i]
		sort.Ints(waitIndexes)
		for _, waitIndex := range waitIndexes {
			// If this is the first operation that must wait on the operation at
			// `waitIndex`, then there will be no channel for the operation yet.
			// Create one.
			if opsDone[waitIndex] == nil {
				opsDone[waitIndex] = make(chan struct{})
			}
		}
	}
	return opsWaitOn, opsDone
}

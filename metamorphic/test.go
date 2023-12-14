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
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
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
	if err := t.init(h, dir, opts, 1 /* num instances */); err != nil {
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
	opts      *pebble.Options
	testOpts  *TestOptions
	writeOpts *pebble.WriteOptions
	tmpDir    string
	// The DBs the test is run on.
	dbs []*pebble.DB
	// The slots for the batches, iterators, and snapshots. These are read and
	// written by the ops to pass state from one op to another.
	batches   []*pebble.Batch
	iters     []*retryableIter
	snapshots []readerCloser
}

func newTest(ops []op) *Test {
	return &Test{
		ops: ops,
	}
}

func (t *Test) init(h *history, dir string, testOpts *TestOptions, numInstances int) error {
	t.dir = dir
	t.h = h
	t.testOpts = testOpts
	t.writeOpts = pebble.NoSync
	if testOpts.strictFS {
		t.writeOpts = pebble.Sync
		testOpts.Opts.FS = vfs.NewStrictMem()
	} else {
		t.writeOpts = pebble.NoSync
		testOpts.Opts.FS = vfs.NewMem()
	}
	testOpts.Opts.WithFSDefaults()
	t.opts = testOpts.Opts.EnsureDefaults()
	t.opts.Logger = h
	lel := pebble.MakeLoggingEventListener(t.opts.Logger)
	t.opts.EventListener = &lel
	t.opts.DebugCheck = func(db *pebble.DB) error {
		// Wrap the ordinary DebugCheckLevels with retrying
		// of injected errors.
		return withRetries(func() error {
			return pebble.DebugCheckLevels(db)
		})
	}
	if numInstances < 1 {
		numInstances = 1
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
		t.maybeSaveData()
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
		err = withRetries(func() error {
			db, err = pebble.Open(dir, t.opts)
			return err
		})
		if err != nil {
			return err
		}
		t.dbs[i] = db
		h.log.Printf("// db%d.Open() %v", i+1, err)

		if t.testOpts.sharedStorageEnabled {
			err = withRetries(func() error {
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

func (t *Test) isFMV(dbID objID, fmv pebble.FormatMajorVersion) bool {
	db := t.getDB(dbID)
	return db.FormatMajorVersion() >= fmv
}

func (t *Test) restartDB(dbID objID) error {
	db := t.getDB(dbID)
	if !t.testOpts.strictFS {
		return nil
	}
	t.opts.Cache.Ref()
	// The fs isn't necessarily a MemFS.
	fs, ok := vfs.Root(t.opts.FS).(*vfs.MemFS)
	if ok {
		fs.SetIgnoreSyncs(true)
	}
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
	if ok {
		fs.ResetToSyncedState()
		fs.SetIgnoreSyncs(false)
	}

	// TODO(jackson): Audit errorRate and ensure custom options' hooks semantics
	// are well defined within the context of retries.
	err := withRetries(func() (err error) {
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

func (t *Test) maybeSaveDataInternal() error {
	rootFS := vfs.Root(t.opts.FS)
	if rootFS == vfs.Default {
		return nil
	}
	if err := os.RemoveAll(t.dir); err != nil {
		return err
	}
	if _, err := vfs.Clone(rootFS, vfs.Default, t.dir, t.dir); err != nil {
		return err
	}
	if t.testOpts.sharedStorageEnabled {
		fs := t.testOpts.sharedStorageFS
		outputDir := vfs.Default.PathJoin(t.dir, "shared", string(t.testOpts.Opts.Experimental.CreateOnSharedLocator))
		vfs.Default.MkdirAll(outputDir, 0755)
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
			outputFile, err := vfs.Default.Create(outputPath)
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
	}
	return nil
}

// If an in-memory FS is being used, save the contents to disk.
func (t *Test) maybeSaveData() {
	if err := t.maybeSaveDataInternal(); err != nil {
		t.opts.Logger.Infof("unable to save data: %s: %v", t.dir, err)
	}
}

func (t *Test) step(h *history) bool {
	if t.idx >= len(t.ops) {
		return false
	}
	t.ops[t.idx].run(t, h.recorder(-1 /* thread */, t.idx))
	t.idx++
	return true
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
		iter:    i,
		lastKey: nil,
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
	}
}

func (t *Test) getBatch(id objID) *pebble.Batch {
	if id.tag() != batchTag {
		panic(fmt.Sprintf("invalid batch ID: %s", id))
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
	}
	panic(fmt.Sprintf("cannot close ID: %s", id))
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
	}
	panic(fmt.Sprintf("invalid reader ID: %s", id))
}

func (t *Test) getWriter(id objID) pebble.Writer {
	switch id.tag() {
	case dbTag:
		return t.dbs[id.slot()-1]
	case batchTag:
		return t.batches[id.slot()]
	}
	panic(fmt.Sprintf("invalid writer ID: %s", id))
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

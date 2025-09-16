// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/sstable"
)

const maxValueSize = 20

type iterOpts struct {
	lower    UserKey
	upper    UserKey
	keyTypes uint32 // pebble.IterKeyType
	// maskSuffix may be set if keyTypes is IterKeyTypePointsAndRanges to
	// configure IterOptions.RangeKeyMasking.Suffix.
	maskSuffix UserKeySuffix

	// If filterMax is != nil, this iterator will filter out any keys that have
	// suffixes that don't fall within the range (filterMin,filterMax]
	// [according to the ordering defined by ComparePointSuffixes]. Note that
	// suffixes are used to represent MVCC timestamps, and MVCC timestamps are
	// ordered in numerically descending order, so the timestamp associated with
	// filterMin is more recent than that associated with filterMax. This means
	// according to the ordering of ComparePointSuffixes, filterMin > filterMax.
	//
	// Additionally, the iterator will be constructed with a block-property
	// filter that filters out blocks accordingly. Not all OPTIONS hook up the
	// corresponding block property collector, so block-filtering may still be
	// effectively disabled in some runs. The iterator operations themselves
	// however will always skip past any points that should be filtered to
	// ensure determinism.
	filterMin UserKeySuffix
	filterMax UserKeySuffix

	// see IterOptions.UseL6Filters.
	useL6Filters bool

	// MaximumSuffixProperty is the maximum suffix property used during the lazy
	// position of SeekPrefixGE optimization.
	maximumSuffixProperty pebble.MaximumSuffixProperty
	// NB: If adding or removing fields, ensure IsZero is in sync.
}

func (o iterOpts) IsZero() bool {
	return o.lower == nil && o.upper == nil && o.keyTypes == 0 &&
		o.maskSuffix == nil && o.filterMin == nil && o.filterMax == nil && !o.useL6Filters &&
		o.maximumSuffixProperty == nil
}

// GenerateOps generates n random operations, drawing randomness from the
// provided pseudorandom generator and using cfg to determine the distribution
// of op types.
func GenerateOps(rng *rand.Rand, n uint64, kf KeyFormat, cfg OpConfig) Ops {
	// Generate a new set of random ops, writing them to <dir>/ops. These will be
	// read by the child processes when performing a test run.
	return newGenerator(rng, cfg, newKeyManager(1 /* num instances */, kf)).generate(n)
}

type generator struct {
	cfg OpConfig
	rng *rand.Rand

	init *initOp
	ops  []op

	// keyManager tracks the state of keys a operation generation time.
	keyManager   *keyManager
	keyGenerator KeyGenerator
	dbs          objIDSlice
	// Unordered sets of object IDs for live objects. Used to randomly select on
	// object when generating an operation. There are 4 concrete objects: the DB
	// (of which there is exactly 1), batches, iterators, and snapshots.
	//
	// liveBatches contains the live indexed and write-only batches.
	liveBatches objIDSlice
	// liveIters contains the live iterators.
	liveIters     objIDSlice
	itersLastOpts map[objID]iterOpts
	// liveReaders contains the DB, and any live indexed batches and snapshots. The DB is always
	// at index 0.
	liveReaders objIDSlice
	// liveSnapshots contains the live snapshots.
	liveSnapshots objIDSlice
	// liveWriters contains the DB, and any live batches. The DB is always at index 0.
	liveWriters objIDSlice
	// externalObjects contains the external objects created.
	externalObjects objIDSlice

	// Maps used to find associated objects during generation. These maps are not
	// needed during test execution.
	//
	// batchID -> batch iters: used to keep track of the open iterators on an
	// indexed batch. The iter set value will also be indexed by the readers map.
	batches map[objID]objIDSet
	// iterID -> reader iters: used to keep track of all of the open
	// iterators. The iter set value will also be indexed by either the batches
	// or snapshots maps.
	iters map[objID]objIDSet
	// objectID -> db: used to keep track of the DB a batch, iter, or snapshot
	// was created on. It should be read through the dbIDForObj method.
	objDB map[objID]objID
	// readerID -> reader iters: used to keep track of the open iterators on a
	// reader. The iter set value will also be indexed by either the batches or
	// snapshots maps. This map is the union of batches and snapshots maps.
	readers map[objID]objIDSet
	// snapshotID -> snapshot iters: used to keep track of the open iterators on
	// a snapshot. The iter set value will also be indexed by the readers map.
	snapshots map[objID]objIDSet
	// snapshotID -> bounds of the snapshot: only populated for snapshots that
	// are constrained by bounds.
	snapshotBounds map[objID][]pebble.KeyRange
	// iterVisibleKeys is the set of keys that should be visible to the
	// iterator.
	iterVisibleKeys map[objID][][]byte
	// iterReaderID is a map from an iterID to a readerID.
	iterReaderID map[objID]objID
}

func newGenerator(rng *rand.Rand, cfg OpConfig, km *keyManager) *generator {
	keyGenerator := km.kf.NewGenerator(km, rng, cfg)
	g := &generator{
		cfg:             cfg,
		rng:             rng,
		init:            &initOp{dbSlots: uint32(cfg.numInstances)},
		keyManager:      km,
		keyGenerator:    keyGenerator,
		liveReaders:     objIDSlice{makeObjID(dbTag, 1)},
		liveWriters:     objIDSlice{makeObjID(dbTag, 1)},
		dbs:             objIDSlice{makeObjID(dbTag, 1)},
		objDB:           make(map[objID]objID),
		batches:         make(map[objID]objIDSet),
		iters:           make(map[objID]objIDSet),
		readers:         make(map[objID]objIDSet),
		snapshots:       make(map[objID]objIDSet),
		snapshotBounds:  make(map[objID][]pebble.KeyRange),
		itersLastOpts:   make(map[objID]iterOpts),
		iterVisibleKeys: make(map[objID][][]byte),
		iterReaderID:    make(map[objID]objID),
	}
	for i := 1; i < cfg.numInstances; i++ {
		g.liveReaders = append(g.liveReaders, makeObjID(dbTag, uint32(i+1)))
		g.liveWriters = append(g.liveWriters, makeObjID(dbTag, uint32(i+1)))
		g.dbs = append(g.dbs, makeObjID(dbTag, uint32(i+1)))
	}
	// Note that the initOp fields are populated during generation.
	g.ops = append(g.ops, g.init)
	return g
}

// generate generates [count] operations according to the generator's configured
// distributions.
func (g *generator) generate(count uint64) []op {
	opGenerators := []func(){
		OpBatchAbort:                  g.batchAbort,
		OpBatchCommit:                 g.batchCommit,
		OpDBCheckpoint:                g.dbCheckpoint,
		OpDBCompact:                   g.dbCompact,
		OpDBDownload:                  g.dbDownload,
		OpDBFlush:                     g.dbFlush,
		OpDBRatchetFormatMajorVersion: g.dbRatchetFormatMajorVersion,
		OpDBRestart:                   g.dbRestart,
		OpDBEstimateDiskUsage:         g.dbEstimateDiskUsage,
		OpIterClose:                   g.randIter(g.iterClose),
		OpIterFirst:                   g.randIter(g.iterFirst),
		OpIterLast:                    g.randIter(g.iterLast),
		OpIterNext:                    g.randIter(g.iterNext),
		OpIterNextWithLimit:           g.randIter(g.iterNextWithLimit),
		OpIterNextPrefix:              g.randIter(g.iterNextPrefix),
		OpIterCanSingleDelete:         g.randIter(g.iterCanSingleDelete),
		OpIterPrev:                    g.randIter(g.iterPrev),
		OpIterPrevWithLimit:           g.randIter(g.iterPrevWithLimit),
		OpIterSeekGE:                  g.randIter(g.iterSeekGE),
		OpIterSeekGEWithLimit:         g.randIter(g.iterSeekGEWithLimit),
		OpIterSeekLT:                  g.randIter(g.iterSeekLT),
		OpIterSeekLTWithLimit:         g.randIter(g.iterSeekLTWithLimit),
		OpIterSeekPrefixGE:            g.randIter(g.iterSeekPrefixGE),
		OpIterSetBounds:               g.randIter(g.iterSetBounds),
		OpIterSetOptions:              g.randIter(g.iterSetOptions),
		OpNewBatch:                    g.newBatch,
		OpNewIndexedBatch:             g.newIndexedBatch,
		OpNewIter:                     g.newIter,
		OpNewIterUsingClone:           g.newIterUsingClone,
		OpNewSnapshot:                 g.newSnapshot,
		OpNewExternalObj:              g.newExternalObj,
		OpReaderGet:                   g.readerGet,
		OpReplicate:                   g.replicate,
		OpSnapshotClose:               g.snapshotClose,
		OpWriterApply:                 g.writerApply,
		OpWriterDelete:                g.writerDelete,
		OpWriterDeleteRange:           g.writerDeleteRange,
		OpWriterIngest:                g.writerIngest,
		OpWriterIngestAndExcise:       g.writerIngestAndExcise,
		OpWriterIngestExternalFiles:   g.writerIngestExternalFiles,
		OpWriterLogData:               g.writerLogData,
		OpWriterMerge:                 g.writerMerge,
		OpWriterRangeKeyDelete:        g.writerRangeKeyDelete,
		OpWriterRangeKeySet:           g.writerRangeKeySet,
		OpWriterRangeKeyUnset:         g.writerRangeKeyUnset,
		OpWriterSet:                   g.writerSet,
		OpWriterSingleDelete:          g.writerSingleDelete,
	}

	// TPCC-style deck of cards randomization. Every time the end of the deck is
	// reached, we shuffle the deck.
	deck := randvar.NewDeck(g.rng, g.cfg.ops[:]...)

	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, formatOps(g.keyManager.kf, g.ops))
			panic(r)
		}
	}()
	for i := uint64(0); i < count; i++ {
		opGenerators[deck.Int()]()
	}

	g.dbClose()

	computeDerivedFields(g.ops)
	return g.ops
}

func (g *generator) add(op op) {
	g.ops = append(g.ops, op)
	g.keyManager.update(op)
}

// prefixKeyRange generates a [start, end) pair consisting of two prefix keys.
func (g *generator) prefixKeyRange() ([]byte, []byte) {
	keys := uniqueKeys(g.keyManager.kf.Comparer.Compare, 2, func() []byte {
		return g.keyGenerator.RandPrefix(0.01)
	})
	return keys[0], keys[1]
}

func (g *generator) randKeyToSingleDelete(id objID) []byte {
	keys := g.keyManager.eligibleSingleDeleteKeys(id)
	length := len(keys)
	if length == 0 {
		return nil
	}
	return keys[g.rng.IntN(length)]
}

func resizeBuffer(buf []byte, prefixLen, suffixLen int) []byte {
	if cap(buf) >= prefixLen+suffixLen {
		return buf[:prefixLen+suffixLen]
	}
	return make([]byte, prefixLen+suffixLen)
}

func (g *generator) newBatch() {
	batchID := makeObjID(batchTag, g.init.batchSlots)
	g.init.batchSlots++
	g.liveBatches = append(g.liveBatches, batchID)
	g.liveWriters = append(g.liveWriters, batchID)
	dbID := g.dbs.rand(g.rng)
	g.objDB[batchID] = dbID

	g.add(&newBatchOp{
		dbID:    dbID,
		batchID: batchID,
	})
}

func (g *generator) newIndexedBatch() {
	batchID := makeObjID(batchTag, g.init.batchSlots)
	g.init.batchSlots++
	g.liveBatches = append(g.liveBatches, batchID)
	g.liveReaders = append(g.liveReaders, batchID)
	g.liveWriters = append(g.liveWriters, batchID)

	iters := make(objIDSet)
	g.batches[batchID] = iters
	g.readers[batchID] = iters
	dbID := g.dbs.rand(g.rng)
	g.objDB[batchID] = dbID

	g.add(&newIndexedBatchOp{
		dbID:    dbID,
		batchID: batchID,
	})
}

// removeFromBatchGenerator will not generate a closeOp for the target batch as
// not every batch that is removed from the generator should be closed. For
// example, running a closeOp before an ingestOp that contains the closed batch
// will cause an error.
func (g *generator) removeBatchFromGenerator(batchID objID) {
	g.liveBatches.remove(batchID)
	iters := g.batches[batchID]
	delete(g.batches, batchID)

	if iters != nil {
		g.liveReaders.remove(batchID)
		delete(g.readers, batchID)
	}
	g.liveWriters.remove(batchID)
	for _, id := range iters.sorted() {
		g.liveIters.remove(id)
		delete(g.iters, id)
		g.add(&closeOp{objID: id})
	}
}

func (g *generator) batchAbort() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)
	g.removeBatchFromGenerator(batchID)

	g.add(&closeOp{objID: batchID})
}

func (g *generator) batchCommit() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)
	dbID := g.dbIDForObj(batchID)
	g.removeBatchFromGenerator(batchID)

	// The batch we're applying may contain single delete tombstones that when
	// applied to the writer result in nondeterminism in the deleted key. If
	// that's the case, we can restore determinism by first deleting the key
	// from the writer.
	//
	// Generating additional operations here is not ideal, but it simplifies
	// single delete invariants significantly.
	singleDeleteConflicts := g.keyManager.checkForSingleDelConflicts(batchID, dbID, false /* collapsed */)
	for _, conflict := range singleDeleteConflicts {
		g.add(&deleteOp{
			writerID:    dbID,
			key:         conflict,
			derivedDBID: dbID,
		})
	}

	g.add(&batchCommitOp{
		dbID:    dbID,
		batchID: batchID,
	})
	g.add(&closeOp{objID: batchID})

}

func (g *generator) dbClose() {
	// Close any live iterators and snapshots, so that we can close the DB
	// cleanly.
	for len(g.liveIters) > 0 {
		g.randIter(g.iterClose)()
	}
	for len(g.liveSnapshots) > 0 {
		g.snapshotClose()
	}
	for len(g.liveBatches) > 0 {
		batchID := g.liveBatches[0]
		g.removeBatchFromGenerator(batchID)
		g.add(&closeOp{objID: batchID})
	}
	for len(g.dbs) > 0 {
		db := g.dbs[0]
		g.dbs = g.dbs[1:]
		g.add(&closeOp{objID: db})
	}
}

func (g *generator) dbCheckpoint() {
	numSpans := g.expRandInt(1)
	var spans []pebble.CheckpointSpan
	if numSpans > 0 {
		spans = make([]pebble.CheckpointSpan, numSpans)
	}
	for i := range spans {
		start := g.keyGenerator.RandKey(0.01)
		end := g.keyGenerator.RandKey(0.01)
		if g.cmp(start, end) > 0 {
			start, end = end, start
		}
		spans[i].Start = start
		spans[i].End = end
	}
	dbID := g.dbs.rand(g.rng)
	g.add(&checkpointOp{
		dbID:  dbID,
		spans: spans,
	})
}

func (g *generator) dbCompact() {
	// Generate new key(s) with a 1% probability.
	start := g.keyGenerator.RandKey(0.01)
	end := g.keyGenerator.RandKey(0.01)
	if g.cmp(start, end) > 0 {
		start, end = end, start
	}
	dbID := g.dbs.rand(g.rng)
	g.add(&compactOp{
		dbID:        dbID,
		start:       start,
		end:         end,
		parallelize: g.rng.Float64() < 0.5,
	})
}

func (g *generator) dbEstimateDiskUsage() {
	// Generate new key(s) with a 1% probability.
	start := g.keyGenerator.RandKey(0.01)
	end := g.keyGenerator.RandKey(0.01)
	if g.cmp(start, end) > 0 {
		start, end = end, start
	}
	dbID := g.dbs.rand(g.rng)
	g.add(&estimateDiskUsageOp{
		dbID:  dbID,
		start: start,
		end:   end,
	})
}

func (g *generator) dbDownload() {
	numSpans := 1 + g.expRandInt(1)
	spans := make([]pebble.DownloadSpan, numSpans)
	for i := range spans {
		keys := uniqueKeys(g.keyManager.kf.Comparer.Compare, 2, func() []byte {
			return g.keyGenerator.RandKey(0.001)
		})
		start, end := keys[0], keys[1]
		spans[i].StartKey = start
		spans[i].EndKey = end
		spans[i].ViaBackingFileDownload = g.rng.IntN(2) == 0
	}
	dbID := g.dbs.rand(g.rng)
	g.add(&downloadOp{
		dbID:  dbID,
		spans: spans,
	})
}

func (g *generator) dbFlush() {
	g.add(&flushOp{g.dbs.rand(g.rng)})
}

func (g *generator) dbRatchetFormatMajorVersion() {
	// Ratchet to a random format major version between the minimum the
	// metamorphic tests support and the newest. At runtime, the generated
	// version may be behind the database's format major version, in which case
	// RatchetFormatMajorVersion should deterministically error.

	dbID := g.dbs.rand(g.rng)
	n := int(newestFormatMajorVersionToTest - minimumFormatMajorVersion)
	vers := pebble.FormatMajorVersion(g.rng.IntN(n+1)) + minimumFormatMajorVersion
	g.add(&dbRatchetFormatMajorVersionOp{dbID: dbID, vers: vers})
}

func (g *generator) dbRestart() {
	// Close any live iterators and snapshots, so that we can close the DB
	// cleanly.
	dbID := g.dbs.rand(g.rng)
	for len(g.liveIters) > 0 {
		g.randIter(g.iterClose)()
	}
	for len(g.liveSnapshots) > 0 {
		g.snapshotClose()
	}
	// Close the batches.
	for len(g.liveBatches) > 0 {
		batchID := g.liveBatches[0]
		g.removeBatchFromGenerator(batchID)
		g.add(&closeOp{objID: batchID})
	}
	if len(g.liveReaders) != len(g.dbs) || len(g.liveWriters) != len(g.dbs) {
		panic(fmt.Sprintf("unexpected counts: liveReaders %d, liveWriters: %d",
			len(g.liveReaders), len(g.liveWriters)))
	}
	g.add(&dbRestartOp{dbID: dbID})
}

// maybeSetSnapshotIterBounds must be called whenever creating a new iterator or
// modifying the bounds of an iterator. If the iterator is backed by a snapshot
// that only guarantees consistency within a limited set of key spans, then the
// iterator must set bounds within one of the snapshot's consistent keyspans. It
// returns true if the provided readerID is a bounded snapshot and bounds were
// set.
func (g *generator) maybeSetSnapshotIterBounds(readerID objID, opts *iterOpts) bool {
	snapBounds, isBoundedSnapshot := g.snapshotBounds[readerID]
	if !isBoundedSnapshot {
		return false
	}
	// Pick a random keyrange within one of the snapshot's key ranges.
	parentBounds := pickOneUniform(g.rng, snapBounds)
	// With 10% probability, use the parent start bound as-is.
	if g.rng.Float64() <= 0.1 {
		opts.lower = parentBounds.Start
	} else {
		opts.lower = g.keyGenerator.RandKeyInRange(0.1, parentBounds)
	}
	// With 10% probability, use the parent end bound as-is.
	if g.rng.Float64() <= 0.1 {
		opts.upper = parentBounds.End
	} else {
		opts.upper = g.keyGenerator.RandKeyInRange(0.1, pebble.KeyRange{
			Start: opts.lower,
			End:   parentBounds.End,
		})
	}
	return true
}

func (g *generator) newIter() {
	iterID := makeObjID(iterTag, g.init.iterSlots)
	g.init.iterSlots++
	g.liveIters = append(g.liveIters, iterID)

	readerID := g.liveReaders.rand(g.rng)
	if iters := g.readers[readerID]; iters != nil {
		iters[iterID] = struct{}{}
		g.iters[iterID] = iters
		//lint:ignore SA9003 - readability
	} else {
		// NB: the DB object does not track its open iterators because it never
		// closes.
	}
	g.iterReaderID[iterID] = readerID
	dbID := g.deriveDB(iterID)

	var opts iterOpts
	if !g.maybeSetSnapshotIterBounds(readerID, &opts) {
		// Generate lower/upper bounds with a 10% probability.
		if g.rng.Float64() <= 0.1 {
			// Generate a new key with a .1% probability.
			opts.lower = g.keyGenerator.RandKey(0.001)
		}
		if g.rng.Float64() <= 0.1 {
			// Generate a new key with a .1% probability.
			opts.upper = g.keyGenerator.RandKey(0.001)
		}
		if g.cmp(opts.lower, opts.upper) > 0 {
			opts.lower, opts.upper = opts.upper, opts.lower
		}
	}
	opts.keyTypes, opts.maskSuffix = g.randKeyTypesAndMask()

	// With 10% probability, enable automatic filtering of keys with suffixes
	// not in the provided range. This filtering occurs both through
	// block-property filtering and explicitly within the iterator operations to
	// ensure determinism.
	if g.rng.Float64() <= 0.1 {
		opts.filterMin, opts.filterMax = g.keyGenerator.SuffixRange()
	}

	// Enable L6 filters with a 10% probability.
	if g.rng.Float64() <= 0.1 {
		opts.useL6Filters = true
	}

	// With 20% probability, enable the lazy positioning SeekPrefixGE optimization.
	if g.rng.Float64() <= 0.2 {
		opts.maximumSuffixProperty = g.keyGenerator.MaximumSuffixProperty()
	}

	g.itersLastOpts[iterID] = opts
	g.iterVisibleKeys[iterID] = g.keyManager.getSetOfVisibleKeys(readerID)
	g.iterReaderID[iterID] = readerID
	g.add(&newIterOp{
		readerID:    readerID,
		iterID:      iterID,
		iterOpts:    opts,
		derivedDBID: dbID,
	})
}

func (g *generator) randKeyTypesAndMask() (keyTypes uint32, maskSuffix []byte) {
	// Iterate over different key types.
	p := g.rng.Float64()
	switch {
	case p < 0.2: // 20% probability
		keyTypes = uint32(pebble.IterKeyTypePointsOnly)
	case p < 0.8: // 60% probability
		keyTypes = uint32(pebble.IterKeyTypePointsAndRanges)
		// With 50% probability, enable masking.
		if g.rng.IntN(2) == 1 {
			maskSuffix = g.keyGenerator.UniformSuffix()
		}
	default: // 20% probability
		keyTypes = uint32(pebble.IterKeyTypeRangesOnly)
	}
	return keyTypes, maskSuffix
}

func (g *generator) deriveDB(readerID objID) objID {
	dbParentID := readerID
	if readerID.tag() == iterTag {
		dbParentID = g.iterReaderID[readerID]
	}
	if dbParentID.tag() != dbTag {
		dbParentID = g.dbIDForObj(dbParentID)
	}
	g.objDB[readerID] = dbParentID
	return dbParentID
}

func (g *generator) dbIDForObj(objID objID) objID {
	if g.objDB[objID] == 0 {
		panic(fmt.Sprintf("object %s has no associated DB", objID))
	}
	return g.objDB[objID]
}

func (g *generator) newIterUsingClone() {
	if len(g.liveIters) == 0 {
		return
	}
	existingIterID := g.liveIters.rand(g.rng)
	iterID := makeObjID(iterTag, g.init.iterSlots)
	g.init.iterSlots++
	g.liveIters = append(g.liveIters, iterID)
	if iters := g.iters[existingIterID]; iters != nil {
		iters[iterID] = struct{}{}
		g.iters[iterID] = iters
		//lint:ignore SA9003 - readability
	} else {
		// NB: the DB object does not track its open iterators because it never
		// closes.
	}
	readerID := g.iterReaderID[existingIterID]
	g.iterReaderID[iterID] = readerID
	g.deriveDB(iterID)

	var refreshBatch bool
	visibleKeys := g.iterVisibleKeys[existingIterID]
	if readerID.tag() == batchTag {
		if refreshBatch = g.rng.IntN(2) == 1; refreshBatch {
			visibleKeys = g.keyManager.getSetOfVisibleKeys(readerID)
		}
	}

	opts := g.itersLastOpts[existingIterID]
	// With 50% probability, consider modifying the iterator options used by the
	// clone.
	if g.rng.IntN(2) == 1 {
		g.maybeMutateOptions(readerID, &opts)
	}
	g.itersLastOpts[iterID] = opts

	// Copy the visible keys from the existing iterator.
	g.iterVisibleKeys[iterID] = visibleKeys
	g.iterReaderID[iterID] = g.iterReaderID[existingIterID]
	g.add(&newIterUsingCloneOp{
		existingIterID:  existingIterID,
		iterID:          iterID,
		refreshBatch:    refreshBatch,
		iterOpts:        opts,
		derivedReaderID: readerID,
	})
}

func (g *generator) iterClose(iterID objID) {
	g.liveIters.remove(iterID)
	if readerIters, ok := g.iters[iterID]; ok {
		delete(g.iters, iterID)
		delete(readerIters, iterID)
	}

	g.add(&closeOp{objID: iterID})
}

func (g *generator) iterSetBounds(iterID objID) {
	iterLastOpts := g.itersLastOpts[iterID]
	newOpts := iterLastOpts
	// TODO(jackson): The logic to increase the probability of advancing bounds
	// monotonically only applies if the snapshot is not bounded. Refactor to
	// allow bounded snapshots to benefit too, when possible.
	if !g.maybeSetSnapshotIterBounds(g.iterReaderID[iterID], &newOpts) {
		var lower, upper []byte
		genLower := g.rng.Float64() <= 0.9
		genUpper := g.rng.Float64() <= 0.9
		// When one of ensureLowerGE, ensureUpperLE is true, the new bounds
		// don't overlap with the previous bounds.
		var ensureLowerGE, ensureUpperLE bool
		if genLower && iterLastOpts.upper != nil && g.rng.Float64() <= 0.9 {
			ensureLowerGE = true
		}
		if (!ensureLowerGE || g.rng.Float64() < 0.5) && genUpper && iterLastOpts.lower != nil {
			ensureUpperLE = true
			ensureLowerGE = false
		}
		attempts := 0
		for {
			attempts++
			if genLower {
				// Generate a new key with a .1% probability.
				lower = g.keyGenerator.RandKey(0.001)
			}
			if genUpper {
				// Generate a new key with a .1% probability.
				upper = g.keyGenerator.RandKey(0.001)
			}
			if g.cmp(lower, upper) > 0 {
				lower, upper = upper, lower
			}
			if ensureLowerGE && g.cmp(iterLastOpts.upper, lower) > 0 {
				if attempts < 25 {
					continue
				}
				lower = iterLastOpts.upper
				upper = lower
				break
			}
			if ensureUpperLE && g.cmp(upper, iterLastOpts.lower) > 0 {
				if attempts < 25 {
					continue
				}
				upper = iterLastOpts.lower
				lower = upper
				break
			}
			break
		}
		newOpts.lower = lower
		newOpts.upper = upper
	}
	g.itersLastOpts[iterID] = newOpts
	g.add(&iterSetBoundsOp{
		iterID: iterID,
		lower:  newOpts.lower,
		upper:  newOpts.upper,
	})
	// Additionally seek the iterator in a manner consistent with the bounds,
	// and do some steps (Next/Prev). The seeking exercises typical
	// CockroachDB behavior when using iterators and the steps are trying to
	// stress the region near the bounds. Ideally, we should not do this as
	// part of generating a single op, but this is easier than trying to
	// control future op generation via generator state.
	doSeekLT := newOpts.upper != nil && g.rng.Float64() < 0.5
	doSeekGE := newOpts.lower != nil && g.rng.Float64() < 0.5
	if doSeekLT && doSeekGE {
		// Pick the seek.
		if g.rng.Float64() < 0.5 {
			doSeekGE = false
		} else {
			doSeekLT = false
		}
	}
	if doSeekLT {
		g.add(&iterSeekLTOp{
			iterID:          iterID,
			key:             newOpts.upper,
			derivedReaderID: g.iterReaderID[iterID],
		})
		if g.rng.Float64() < 0.5 {
			g.iterNext(iterID)
		}
		if g.rng.Float64() < 0.5 {
			g.iterNext(iterID)
		}
		if g.rng.Float64() < 0.5 {
			g.iterPrev(iterID)
		}
	} else if doSeekGE {
		g.add(&iterSeekGEOp{
			iterID:          iterID,
			key:             newOpts.lower,
			derivedReaderID: g.iterReaderID[iterID],
		})
		if g.rng.Float64() < 0.5 {
			g.iterPrev(iterID)
		}
		if g.rng.Float64() < 0.5 {
			g.iterPrev(iterID)
		}
		if g.rng.Float64() < 0.5 {
			g.iterNext(iterID)
		}
	}
}

func (g *generator) iterSetOptions(iterID objID) {
	opts := g.itersLastOpts[iterID]
	g.maybeMutateOptions(g.iterReaderID[iterID], &opts)
	g.itersLastOpts[iterID] = opts
	g.add(&iterSetOptionsOp{
		iterID:          iterID,
		iterOpts:        opts,
		derivedReaderID: g.iterReaderID[iterID],
	})

	// Additionally, perform a random absolute positioning operation. The
	// SetOptions contract requires one before the next relative positioning
	// operation. Ideally, we should not do this as part of generating a single
	// op, but this is easier than trying to control future op generation via
	// generator state.
	pickOneUniform(
		g.rng,
		[]func(objID){
			g.iterFirst,
			g.iterLast,
			g.iterSeekGE,
			g.iterSeekGEWithLimit,
			g.iterSeekPrefixGE,
			g.iterSeekLT,
			g.iterSeekLTWithLimit,
		},
	)(iterID)
}

func (g *generator) iterSeekGE(iterID objID) {
	g.add(&iterSeekGEOp{
		iterID:          iterID,
		key:             g.keyGenerator.RandKey(0.001), // 0.1% new keys
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterSeekGEWithLimit(iterID objID) {
	// 0.1% new keys
	key, limit := g.keyGenerator.RandKey(0.001), g.keyGenerator.RandKey(0.001)
	if g.cmp(key, limit) > 0 {
		key, limit = limit, key
	}
	g.add(&iterSeekGEOp{
		iterID:          iterID,
		key:             key,
		limit:           limit,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterSeekPrefixGE(iterID objID) {
	// Purely random key selection is unlikely to pick a key with any visible
	// versions, especially if we don't take iterator bounds into account. We
	// try to err towards picking a key within bounds that contains a value
	// visible to the iterator.
	lower := []byte(g.itersLastOpts[iterID].lower)
	upper := []byte(g.itersLastOpts[iterID].upper)
	var key []byte
	if g.rng.IntN(5) >= 1 {
		visibleKeys := g.iterVisibleKeys[iterID]
		if lower != nil {
			i, _ := slices.BinarySearchFunc(visibleKeys, lower, g.cmp)
			visibleKeys = visibleKeys[i:]
		}
		if upper != nil {
			i, _ := slices.BinarySearchFunc(visibleKeys, upper, g.cmp)
			visibleKeys = visibleKeys[:i]
		}
		if len(visibleKeys) > 0 {
			key = visibleKeys[g.rng.IntN(len(visibleKeys))]
		}
	}
	if key == nil {
		key = g.keyGenerator.RandKey(0) // 0% new keys
	}
	// Sometimes limit the key to just the prefix.
	if g.rng.IntN(3) == 1 {
		key = g.keyManager.kf.Comparer.Split.Prefix(key)
	}
	g.add(&iterSeekPrefixGEOp{
		iterID:          iterID,
		key:             key,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterSeekLT(iterID objID) {
	g.add(&iterSeekLTOp{
		iterID:          iterID,
		key:             g.keyGenerator.RandKey(0.001), // 0.1% new keys
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterSeekLTWithLimit(iterID objID) {
	// 0.1% new keys
	key, limit := g.keyGenerator.RandKey(0.001), g.keyGenerator.RandKey(0.001)
	if g.cmp(limit, key) > 0 {
		key, limit = limit, key
	}
	g.add(&iterSeekLTOp{
		iterID:          iterID,
		key:             key,
		limit:           limit,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

// randIter performs partial func application ("currying"), returning a new
// function that supplies the given func with a random iterator.
func (g *generator) randIter(gen func(objID)) func() {
	return func() {
		if len(g.liveIters) == 0 {
			return
		}
		gen(g.liveIters.rand(g.rng))
	}
}

func (g *generator) iterFirst(iterID objID) {
	g.add(&iterFirstOp{
		iterID:          iterID,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterLast(iterID objID) {
	g.add(&iterLastOp{
		iterID:          iterID,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterNext(iterID objID) {
	g.add(&iterNextOp{
		iterID:          iterID,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterPrev(iterID objID) {
	g.add(&iterPrevOp{
		iterID:          iterID,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterNextWithLimit(iterID objID) {
	g.add(&iterNextOp{
		iterID:          iterID,
		limit:           g.keyGenerator.RandKey(0.001), // 0.1% new keys
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterNextPrefix(iterID objID) {
	g.add(&iterNextPrefixOp{
		iterID:          iterID,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterCanSingleDelete(iterID objID) {
	g.add(&iterCanSingleDelOp{
		iterID:          iterID,
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) iterPrevWithLimit(iterID objID) {
	g.add(&iterPrevOp{
		iterID:          iterID,
		limit:           g.keyGenerator.RandKey(0.001), // 0.1% new keys
		derivedReaderID: g.iterReaderID[iterID],
	})
}

func (g *generator) readerGet() {
	if len(g.liveReaders) == 0 {
		return
	}

	readerID := g.liveReaders.rand(g.rng)

	// If the chosen reader is a snapshot created with user-specified key
	// ranges, restrict the read to fall within one of the provided key ranges.
	var key []byte
	if bounds := g.snapshotBounds[readerID]; len(bounds) > 0 {
		kr := bounds[g.rng.IntN(len(bounds))]
		key = g.keyGenerator.RandKeyInRange(0.001, kr) // 0.1% new keys
	} else {
		key = g.keyGenerator.RandKey(0.001) // 0.1% new keys
	}
	derivedDBID := objID(0)
	if readerID.tag() == batchTag || readerID.tag() == snapTag {
		derivedDBID = g.deriveDB(readerID)
	}
	g.add(&getOp{readerID: readerID, key: key, derivedDBID: derivedDBID})
}

func (g *generator) replicate() {
	if len(g.dbs) < 2 {
		return
	}

	source := g.dbs.rand(g.rng)
	dest := source
	for dest == source {
		dest = g.dbs.rand(g.rng)
	}

	startKey, endKey := g.prefixKeyRange()
	g.add(&replicateOp{
		source: source,
		dest:   dest,
		start:  startKey,
		end:    endKey,
	})
}

// generateDisjointKeyRanges generates n disjoint key ranges.
func (g *generator) generateDisjointKeyRanges(n int) []pebble.KeyRange {
	keys := uniqueKeys(g.keyManager.kf.Comparer.Compare, 2*n, func() []byte {
		return g.keyGenerator.RandPrefix(0.1)
	})
	keyRanges := make([]pebble.KeyRange, n)
	for i := range keyRanges {
		keyRanges[i] = pebble.KeyRange{
			Start: keys[i*2],
			End:   keys[i*2+1],
		}
	}
	return keyRanges
}

func (g *generator) newSnapshot() {
	snapID := makeObjID(snapTag, g.init.snapshotSlots)
	g.init.snapshotSlots++
	g.liveSnapshots = append(g.liveSnapshots, snapID)
	g.liveReaders = append(g.liveReaders, snapID)
	dbID := g.dbs.rand(g.rng)
	g.objDB[snapID] = dbID

	iters := make(objIDSet)
	g.snapshots[snapID] = iters
	g.readers[snapID] = iters

	s := &newSnapshotOp{
		dbID:   dbID,
		snapID: snapID,
	}

	// Impose bounds on the keys that may be read with the snapshot. Setting bounds
	// allows some runs of the metamorphic test to use a EventuallyFileOnlySnapshot
	// instead of a Snapshot, testing equivalence between the two for reads within
	// those bounds.
	s.bounds = g.generateDisjointKeyRanges(
		1 + g.expRandInt(3),
	)
	g.snapshotBounds[snapID] = s.bounds
	g.add(s)
}

func (g *generator) snapshotClose() {
	if len(g.liveSnapshots) == 0 {
		return
	}

	snapID := g.liveSnapshots.rand(g.rng)
	g.liveSnapshots.remove(snapID)
	iters := g.snapshots[snapID]
	delete(g.snapshots, snapID)
	g.liveReaders.remove(snapID)
	delete(g.readers, snapID)

	for _, id := range iters.sorted() {
		g.liveIters.remove(id)
		delete(g.iters, id)
		g.add(&closeOp{objID: id})
	}

	g.add(&closeOp{objID: snapID})
}

func (g *generator) newExternalObj() {
	if len(g.liveBatches) == 0 {
		return
	}
	var batchID objID
	// Try to find a suitable batch.
	for i := 0; ; i++ {
		if i == 10 {
			return
		}
		batchID = g.liveBatches.rand(g.rng)
		okm := g.keyManager.objKeyMeta(batchID)
		if !okm.bounds.IsUnset() {
			break
		}
	}
	g.removeBatchFromGenerator(batchID)
	objID := makeObjID(externalObjTag, g.init.externalObjSlots)
	g.init.externalObjSlots++
	g.externalObjects = append(g.externalObjects, objID)
	g.add(&newExternalObjOp{
		batchID:       batchID,
		externalObjID: objID,
	})
}

func (g *generator) writerApply() {
	if len(g.liveBatches) == 0 {
		return
	}
	if len(g.liveWriters) < 2 {
		panic(fmt.Sprintf("insufficient liveWriters (%d) to apply batch", len(g.liveWriters)))
	}

	batchID := g.liveBatches.rand(g.rng)
	dbID := g.dbIDForObj(batchID)

	var writerID objID
	for {
		// NB: The writer we're applying to, as well as the batch we're applying,
		// must be from the same DB. The writer could be the db itself. Applying
		// a batch from one DB on another DB results in a panic, so avoid that.
		writerID = g.liveWriters.rand(g.rng)
		writerDBID := writerID
		if writerID.tag() != dbTag {
			writerDBID = g.dbIDForObj(writerID)
		}
		if writerID != batchID && writerDBID == dbID {
			break
		}
	}

	// The batch we're applying may contain single delete tombstones that when
	// applied to the writer result in nondeterminism in the deleted key. If
	// that's the case, we can restore determinism by first deleting the key
	// from the writer.
	//
	// Generating additional operations here is not ideal, but it simplifies
	// single delete invariants significantly.
	singleDeleteConflicts := g.keyManager.checkForSingleDelConflicts(batchID, writerID, false /* collapsed */)
	for _, conflict := range singleDeleteConflicts {
		g.add(&deleteOp{
			writerID:    writerID,
			key:         conflict,
			derivedDBID: dbID,
		})
	}

	g.removeBatchFromGenerator(batchID)

	g.add(&applyOp{
		writerID: writerID,
		batchID:  batchID,
	})
	g.add(&closeOp{
		objID: batchID,
	})
}

func (g *generator) writerDelete() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	derivedDBID := writerID
	if derivedDBID.tag() != dbTag {
		derivedDBID = g.dbIDForObj(writerID)
	}
	g.add(&deleteOp{
		writerID:    writerID,
		key:         g.keyGenerator.RandKey(0.001), // 0.1% new keys
		derivedDBID: derivedDBID,
	})
}

func (g *generator) writerDeleteRange() {
	if len(g.liveWriters) == 0 {
		return
	}

	keys := uniqueKeys(g.keyManager.kf.Comparer.Compare, 2, func() []byte {
		return g.keyGenerator.RandKey(0.001)
	})
	start, end := keys[0], keys[1]

	writerID := g.liveWriters.rand(g.rng)
	g.add(&deleteRangeOp{
		writerID: writerID,
		start:    start,
		end:      end,
	})
}

func (g *generator) writerRangeKeyDelete() {
	if len(g.liveWriters) == 0 {
		return
	}
	start, end := g.prefixKeyRange()

	writerID := g.liveWriters.rand(g.rng)
	g.add(&rangeKeyDeleteOp{
		writerID: writerID,
		start:    start,
		end:      end,
	})
}

func (g *generator) writerRangeKeySet() {
	if len(g.liveWriters) == 0 {
		return
	}
	start, end := g.prefixKeyRange()

	// 90% of the time, set a suffix.
	var suffix []byte
	if g.rng.Float64() < 0.90 {
		// Increase the max suffix 5% of the time.
		suffix = g.keyGenerator.SkewedSuffix(0.05)
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&rangeKeySetOp{
		writerID: writerID,
		start:    start,
		end:      end,
		suffix:   suffix,
		value:    randBytes(g.rng, 0, maxValueSize),
	})
}

func (g *generator) writerRangeKeyUnset() {
	if len(g.liveWriters) == 0 {
		return
	}
	start, end := g.prefixKeyRange()

	// 90% of the time, set a suffix.
	var suffix []byte
	if g.rng.Float64() < 0.90 {
		// Increase the max suffix 5% of the time.
		suffix = g.keyGenerator.SkewedSuffix(0.05)
	}

	// TODO(jackson): Increase probability of effective unsets? Purely random
	// unsets are unlikely to remove an active range key.

	writerID := g.liveWriters.rand(g.rng)
	g.add(&rangeKeyUnsetOp{
		writerID: writerID,
		start:    start,
		end:      end,
		suffix:   suffix,
	})
}

func (g *generator) writerIngest() {
	if len(g.liveBatches) == 0 {
		return
	}

	dbID := g.dbs.rand(g.rng)
	n := min(1+g.expRandInt(1), len(g.liveBatches))
	batchIDs := make([]objID, n)
	derivedDBIDs := make([]objID, n)
	for i := 0; i < n; i++ {
		batchID := g.liveBatches.rand(g.rng)
		batchIDs[i] = batchID
		derivedDBIDs[i] = g.dbIDForObj(batchID)
		g.removeBatchFromGenerator(batchID)
	}

	// Ingestions may fail if the ingested sstables overlap one another.
	// Either it succeeds and its keys are committed to the DB, or it fails and
	// the keys are not committed.
	if !g.keyManager.doObjectBoundsOverlap(batchIDs) {
		// This ingestion will succeed.
		//
		// The batches we're ingesting may contain single delete tombstones that
		// when applied to the writer result in nondeterminism in the deleted key.
		// If that's the case, we can restore determinism by first deleting the keys
		// from the writer.
		//
		// Generating additional operations here is not ideal, but it simplifies
		// single delete invariants significantly.
		for _, batchID := range batchIDs {
			singleDeleteConflicts := g.keyManager.checkForSingleDelConflicts(batchID, dbID, true /* collapsed */)
			for _, conflict := range singleDeleteConflicts {
				g.add(&deleteOp{
					writerID:    dbID,
					key:         conflict,
					derivedDBID: dbID,
				})
			}
		}
	}
	g.add(&ingestOp{
		dbID:         dbID,
		batchIDs:     batchIDs,
		derivedDBIDs: derivedDBIDs,
	})
}

func (g *generator) writerIngestAndExcise() {
	if len(g.liveBatches) == 0 {
		return
	}

	dbID := g.dbs.rand(g.rng)
	batchID := g.liveBatches.rand(g.rng)
	g.removeBatchFromGenerator(batchID)

	start, end := g.prefixKeyRange()
	derivedDBID := g.dbIDForObj(batchID)

	// Check for any single delete conflicts. If this batch is single-deleting
	// a key that isn't safe to single delete in the underlying db, _and_ this
	// key is not in the excise span, we add a delete before the ingestAndExcise.
	singleDeleteConflicts := g.keyManager.checkForSingleDelConflicts(batchID, dbID, true /* collapsed */)
	for _, conflict := range singleDeleteConflicts {
		if g.cmp(conflict, start) >= 0 && g.cmp(conflict, end) < 0 {
			// This key will get excised anyway.
			continue
		}
		g.add(&deleteOp{
			writerID:    dbID,
			key:         conflict,
			derivedDBID: dbID,
		})
	}

	g.add(&ingestAndExciseOp{
		dbID:        dbID,
		batchID:     batchID,
		derivedDBID: derivedDBID,
		exciseStart: start,
		exciseEnd:   end,
	})
}

func (g *generator) writerIngestExternalFiles() {
	if len(g.externalObjects) == 0 {
		return
	}
	dbID := g.dbs.rand(g.rng)
	numFiles := 1 + g.expRandInt(1)
	objs := make([]externalObjWithBounds, numFiles)

	// We generate the parameters in multiple passes:
	//  1. Generate objs with random start and end keys. Their bounds can overlap.
	//  2. Sort objects by the start bound and trim the bounds to remove overlap.
	//  3. Remove any objects where the previous step resulted in empty bounds.
	//  4. Randomly add synthetic suffixes.

	for i := range objs {
		// We allow the same object to be selected multiple times.
		id := g.externalObjects.rand(g.rng)
		b := g.keyManager.objKeyMeta(id).bounds

		objStart := g.prefix(b.smallest)
		objEnd := g.prefix(b.largest)
		if !b.largestExcl || len(objEnd) != len(b.largest) {
			objEnd = g.keyGenerator.ExtendPrefix(objEnd)
		}
		if g.cmp(objStart, objEnd) >= 0 {
			panic("bug in generating obj bounds")
		}
		// Generate two random keys within the given bounds.
		// First, generate a start key in the range [objStart, objEnd).
		start := g.keyGenerator.RandKeyInRange(0.01, pebble.KeyRange{
			Start: objStart,
			End:   objEnd,
		})
		start = g.prefix(start)
		// Second, generate an end key in the range (start, objEnd]. To do this, we
		// generate a key in the range [start, objEnd) and if we get `start`, we
		// remap that to `objEnd`.
		end := g.keyGenerator.RandKeyInRange(0.01, pebble.KeyRange{
			Start: start,
			End:   objEnd,
		})
		end = g.prefix(end)
		if g.cmp(start, end) == 0 {
			end = objEnd
		}
		// Randomly set up synthetic prefix.
		var syntheticPrefix sstable.SyntheticPrefix
		if g.rng.IntN(2) == 0 {
			syntheticPrefix = randBytes(g.rng, 1, 5)
			start = syntheticPrefix.Apply(start)
			end = syntheticPrefix.Apply(end)
		}

		objs[i] = externalObjWithBounds{
			externalObjID: id,
			bounds: pebble.KeyRange{
				Start: start,
				End:   end,
			},
			syntheticPrefix: syntheticPrefix,
		}
	}

	// Sort by start bound.
	slices.SortFunc(objs, func(a, b externalObjWithBounds) int {
		return g.cmp(a.bounds.Start, b.bounds.Start)
	})

	// Trim bounds so that there is no overlap.
	for i := 0; i < len(objs)-1; i++ {
		if g.cmp(objs[i].bounds.End, objs[i+1].bounds.Start) > 0 {
			objs[i].bounds.End = objs[i+1].bounds.Start
		}
	}
	// Some bounds might be empty now, remove those objects altogether. Note that
	// the last object is unmodified, so at least that object will remain.
	objs = slices.DeleteFunc(objs, func(o externalObjWithBounds) bool {
		return g.cmp(o.bounds.Start, o.bounds.End) >= 0
	})

	// Randomly set synthetic suffixes.
	for i := range objs {
		if g.rng.IntN(2) == 0 {
			// We can only use a synthetic suffix if we don't have range dels or RangeKeyUnsets.
			if meta := g.keyManager.objKeyMeta(objs[i].externalObjID); meta.hasRangeDels || meta.hasRangeKeyUnset {
				continue
			}

			// We can only use a synthetic suffix if we don't have overlapping range
			// key sets (because they will become logically conflicting when we
			// replace their suffixes with the synthetic one).
			if g.keyManager.ExternalObjectHasOverlappingRangeKeySets(objs[i].externalObjID) {
				continue
			}

			// Generate a suffix that sorts before any previously generated suffix.
			objs[i].syntheticSuffix = g.keyGenerator.IncMaxSuffix()
		}
	}

	// The batches we're ingesting may contain single delete tombstones that when
	// applied to the db result in nondeterminism in the deleted key. If that's
	// the case, we can restore determinism by first deleting the keys from the
	// db.
	//
	// Generating additional operations here is not ideal, but it simplifies
	// single delete invariants significantly.
	dbKeys := g.keyManager.objKeyMeta(dbID)
	for _, o := range objs {
		for _, src := range g.keyManager.KeysForExternalIngest(o) {
			if g.keyManager.checkForSingleDelConflict(src, dbKeys) {
				g.add(&deleteOp{
					writerID:    dbID,
					key:         src.key,
					derivedDBID: dbID,
				})
			}
		}
	}

	// Shuffle the objects.
	g.rng.Shuffle(len(objs), func(i, j int) {
		objs[i], objs[j] = objs[j], objs[i]
	})

	g.add(&ingestExternalFilesOp{
		dbID: dbID,
		objs: objs,
	})
}

func (g *generator) writerLogData() {
	if len(g.liveWriters) == 0 {
		return
	}
	g.add(&logDataOp{
		writerID: g.liveWriters.rand(g.rng),
		data:     randBytes(g.rng, 0, g.expRandInt(10)),
	})
}

func (g *generator) writerMerge() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&mergeOp{
		writerID: writerID,
		// 20% new keys.
		key:   g.keyGenerator.RandKey(0.2),
		value: randBytes(g.rng, 0, maxValueSize),
	})
}

func (g *generator) writerSet() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&setOp{
		writerID: writerID,
		// 50% new keys.
		key:   g.keyGenerator.RandKey(0.5),
		value: randBytes(g.rng, 0, maxValueSize),
	})
}

func (g *generator) writerSingleDelete() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	key := g.randKeyToSingleDelete(writerID)
	if key == nil {
		return
	}
	g.add(&singleDeleteOp{
		writerID: writerID,
		key:      key,
		// Keys eligible for single deletes can be removed with a regular
		// delete. Mutate a percentage of SINGLEDEL ops into DELETEs. Note that
		// here we are only determining whether the replacement *could* happen.
		// At test runtime, the `replaceSingleDelete` test option must also be
		// set to true for the single delete to be replaced.
		maybeReplaceDelete: g.rng.Float64() < 0.25,
	})
}

func (g *generator) maybeMutateOptions(readerID objID, opts *iterOpts) {
	// With 95% probability, allow changes to any options at all. This ensures
	// that in 5% of cases there are no changes, and SetOptions hits its fast
	// path.
	if g.rng.IntN(100) >= 5 {
		if !g.maybeSetSnapshotIterBounds(readerID, opts) {
			// With 1/3 probability, clear existing bounds.
			if opts.lower != nil && g.rng.IntN(3) == 0 {
				opts.lower = nil
			}
			if opts.upper != nil && g.rng.IntN(3) == 0 {
				opts.upper = nil
			}
			// With 1/3 probability, update the bounds.
			if g.rng.IntN(3) == 0 {
				// Generate a new key with a .1% probability.
				opts.lower = g.keyGenerator.RandKey(0.001)
			}
			if g.rng.IntN(3) == 0 {
				// Generate a new key with a .1% probability.
				opts.upper = g.keyGenerator.RandKey(0.001)
			}
			if g.cmp(opts.lower, opts.upper) > 0 {
				opts.lower, opts.upper = opts.upper, opts.lower
			}
		}

		// With 1/3 probability, update the key-types/mask.
		if g.rng.IntN(3) == 0 {
			opts.keyTypes, opts.maskSuffix = g.randKeyTypesAndMask()
		}

		// With 1/3 probability, clear existing filter.
		if opts.filterMin != nil && g.rng.IntN(3) == 0 {
			opts.filterMax, opts.filterMin = nil, nil
		}
		// With 10% probability, set a filter range.
		if g.rng.IntN(10) == 1 {
			opts.filterMin, opts.filterMax = g.keyGenerator.SuffixRange()
		}
		// With 10% probability, flip enablement of L6 filters.
		if g.rng.Float64() <= 0.1 {
			opts.useL6Filters = !opts.useL6Filters
		}
		// With 20% probability, clear existing maximum suffix property.
		if opts.maximumSuffixProperty != nil && g.rng.IntN(5) == 0 {
			opts.maximumSuffixProperty = nil
		}
	}
}

func (g *generator) cmp(a, b []byte) int {
	return g.keyManager.kf.Comparer.Compare(a, b)
}

func (g *generator) prefix(a []byte) []byte {
	n := g.keyManager.kf.Comparer.Split(a)
	return a[:n:n]
}

func (g *generator) String() string {
	var buf bytes.Buffer
	for _, op := range g.ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	return buf.String()
}

// expRandInt returns a random non-negative integer using the exponential
// distribution with the given mean. This is useful when we usually want to test
// with small values, but we want to occasionally test with a larger value.
//
// Large integers are exponentially less likely than small integers;
// specifically, the probability decreases by a factor of `e` every `mean`
// values.
func (g *generator) expRandInt(mean int) int {
	return int(math.Round(g.rng.ExpFloat64() * float64(mean)))
}

// uniqueKeys takes a key-generating function and uses it to generate n unique
// keys, returning them in sorted order.
func uniqueKeys(cmp base.Compare, n int, genFn func() []byte) [][]byte {
	keys := make([][]byte, n)
	used := make(map[string]struct{}, n)
	for i := range keys {
		for attempts := 0; ; attempts++ {
			keys[i] = genFn()
			if _, exists := used[string(keys[i])]; !exists {
				break
			}
			if attempts > 100000 {
				panic("could not generate unique key")
			}
		}
		used[string(keys[i])] = struct{}{}
	}
	slices.SortFunc(keys, cmp)
	return keys
}

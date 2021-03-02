// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble/internal/randvar"
	"golang.org/x/exp/rand"
)

type iterBounds struct {
	lower []byte
	upper []byte
}

type generator struct {
	rng *rand.Rand

	init *initOp
	ops  []op

	// keys that have been set in the DB. Used to reuse already generated keys
	// during random key selection.
	keys [][]byte
	// singleSetKeysInDB is also in the writerToSingleSetKeys map. This tracks
	// keys that are eligible to be single deleted.
	singleSetKeysInDB     singleSetKeysForBatch
	writerToSingleSetKeys map[objID]singleSetKeysForBatch
	// Ensures no duplication of single set keys for the duration of the test.
	generatedWriteKeys map[string]struct{}

	// Unordered sets of object IDs for live objects. Used to randomly select on
	// object when generating an operation. There are 4 concrete objects: the DB
	// (of which there is exactly 1), batches, iterators, and snapshots.
	//
	// liveBatches contains the live indexed and write-only batches.
	liveBatches objIDSlice
	// liveIters contains the live iterators.
	liveIters       objIDSlice
	itersLastBounds map[objID]iterBounds
	// liveReaders contains the DB, and any live indexed batches and snapshots. The DB is always
	// at index 0.
	liveReaders objIDSlice
	// liveSnapshots contains the live snapshots.
	liveSnapshots objIDSlice
	// liveWriters contains the DB, and any live batches. The DB is always at index 0.
	liveWriters objIDSlice

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
	// readerID -> reader iters: used to keep track of the open iterators on a
	// reader. The iter set value will also be indexed by either the batches or
	// snapshots maps. This map is the union of batches and snapshots maps.
	readers map[objID]objIDSet
	// snapshotID -> snapshot iters: used to keep track of the open iterators on
	// a snapshot. The iter set value will also be indexed by the readers map.
	snapshots map[objID]objIDSet
}

func newGenerator(rng *rand.Rand) *generator {
	g := &generator{
		rng:                   rng,
		init:                  &initOp{},
		singleSetKeysInDB:     makeSingleSetKeysForBatch(),
		writerToSingleSetKeys: make(map[objID]singleSetKeysForBatch),
		generatedWriteKeys:    make(map[string]struct{}),
		liveReaders:           objIDSlice{makeObjID(dbTag, 0)},
		liveWriters:           objIDSlice{makeObjID(dbTag, 0)},
		batches:               make(map[objID]objIDSet),
		iters:                 make(map[objID]objIDSet),
		readers:               make(map[objID]objIDSet),
		snapshots:             make(map[objID]objIDSet),
	}
	g.writerToSingleSetKeys[makeObjID(dbTag, 0)] = g.singleSetKeysInDB
	// Note that the initOp fields are populated during generation.
	g.ops = append(g.ops, g.init)
	return g
}

func generate(rng *rand.Rand, count uint64, cfg config) []op {
	g := newGenerator(rng)

	generators := []func(){
		batchAbort:          g.batchAbort,
		batchCommit:         g.batchCommit,
		dbCheckpoint:        g.dbCheckpoint,
		dbCompact:           g.dbCompact,
		dbFlush:             g.dbFlush,
		dbRestart:           g.dbRestart,
		iterClose:           g.iterClose,
		iterFirst:           g.iterFirst,
		iterLast:            g.iterLast,
		iterNext:            g.iterNext,
		iterNextWithLimit:   g.iterNextWithLimit,
		iterPrev:            g.iterPrev,
		iterPrevWithLimit:   g.iterPrevWithLimit,
		iterSeekGE:          g.iterSeekGE,
		iterSeekGEWithLimit: g.iterSeekGEWithLimit,
		iterSeekLT:          g.iterSeekLT,
		iterSeekLTWithLimit: g.iterSeekLTWithLimit,
		iterSeekPrefixGE:    g.iterSeekPrefixGE,
		iterSetBounds:       g.iterSetBounds,
		newBatch:            g.newBatch,
		newIndexedBatch:     g.newIndexedBatch,
		newIter:             g.newIter,
		newIterUsingClone:   g.newIterUsingClone,
		newSnapshot:         g.newSnapshot,
		readerGet:           g.readerGet,
		snapshotClose:       g.snapshotClose,
		writerApply:         g.writerApply,
		writerDelete:        g.writerDelete,
		writerDeleteRange:   g.writerDeleteRange,
		writerIngest:        g.writerIngest,
		writerMerge:         g.writerMerge,
		writerSet:           g.writerSet,
		writerSingleDelete:  g.writerSingleDelete,
	}

	// TPCC-style deck of cards randomization. Every time the end of the deck is
	// reached, we shuffle the deck.
	deck := randvar.NewDeck(g.rng, cfg.ops...)
	for i := uint64(0); i < count; i++ {
		generators[deck.Int()]()
	}

	g.dbClose()
	return g.ops
}

func (g *generator) add(op op) {
	g.ops = append(g.ops, op)
}

// TODO(peter): make the size and distribution of keys configurable. See
// keyDist and keySizeDist in config.go.
func (g *generator) randKey(newKey float64) []byte {
	if n := len(g.keys); n > 0 && g.rng.Float64() > newKey {
		return g.keys[g.rng.Intn(n)]
	}
	key := g.randValue(4, 12)
	g.keys = append(g.keys, key)
	return key
}

func (g *generator) randKeyForWrite(newKey float64, singleSetKey float64, writerID objID) []byte {
	if n := len(g.keys); n > 0 && g.rng.Float64() > newKey {
		return g.keys[g.rng.Intn(n)]
	}
	key := g.randValue(4, 12)
	if g.rng.Float64() < singleSetKey {
		for {
			if _, ok := g.generatedWriteKeys[string(key)]; ok {
				key = g.randValue(4, 12)
				continue
			}
			g.generatedWriteKeys[string(key)] = struct{}{}
			g.writerToSingleSetKeys[writerID].addKey(key)
			break
		}
	} else {
		g.generatedWriteKeys[string(key)] = struct{}{}
		g.keys = append(g.keys, key)
	}
	return key
}

func (g *generator) randKeyToSingleDelete() []byte {
	length := len(*g.singleSetKeysInDB.keys)
	if length == 0 {
		return nil
	}
	return g.singleSetKeysInDB.removeKey(g.rng.Intn(length))
}

// TODO(peter): make the value size configurable. See valueSizeDist in
// config.go.
func (g *generator) randValue(min, max int) []byte {
	// NB: The actual random values are not particularly important. We only use
	// lowercase letters because that makes visual determination of ordering
	// easier, rather than having to remember the lexicographic ordering of
	// uppercase vs lowercase, or letters vs numbers vs punctuation.
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = 12 // floor(log(math.MaxUint64)/log(lettersLen))

	n := min
	if max > min {
		n += g.rng.Intn(max - min)
	}

	buf := make([]byte, n)

	var r uint64
	var q int
	for i := 0; i < len(buf); i++ {
		if q == 0 {
			r = g.rng.Uint64()
			q = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		q--
	}

	return buf
}

func (g *generator) newBatch() {
	batchID := makeObjID(batchTag, g.init.batchSlots)
	g.init.batchSlots++
	g.liveBatches = append(g.liveBatches, batchID)
	g.liveWriters = append(g.liveWriters, batchID)
	g.writerToSingleSetKeys[batchID] = makeSingleSetKeysForBatch()

	g.add(&newBatchOp{
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
	g.writerToSingleSetKeys[batchID] = makeSingleSetKeysForBatch()

	g.add(&newIndexedBatchOp{
		batchID: batchID,
	})
}

func (g *generator) batchClose(batchID objID) {
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
	delete(g.writerToSingleSetKeys, batchID)
}

func (g *generator) batchAbort() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)
	g.batchClose(batchID)

	g.add(&closeOp{objID: batchID})
}

func (g *generator) batchCommit() {
	if len(g.liveBatches) == 0 {
		return
	}

	batchID := g.liveBatches.rand(g.rng)
	g.writerToSingleSetKeys[batchID].transferTo(g.singleSetKeysInDB)
	g.batchClose(batchID)

	g.add(&batchCommitOp{
		batchID: batchID,
	})
}

func (g *generator) dbClose() {
	// Close any live iterators and snapshots, so that we can close the DB
	// cleanly.
	for len(g.liveIters) > 0 {
		g.iterClose()
	}
	for len(g.liveSnapshots) > 0 {
		g.snapshotClose()
	}
	g.add(&closeOp{objID: makeObjID(dbTag, 0)})
}

func (g *generator) dbCheckpoint() {
	g.add(&checkpointOp{})
}

func (g *generator) dbCompact() {
	// Generate new key(s) with a 1% probability.
	start := g.randKey(0.01)
	end := g.randKey(0.01)
	if bytes.Compare(start, end) > 0 {
		start, end = end, start
	}
	g.add(&compactOp{
		start: start,
		end:   end,
	})
}

func (g *generator) dbFlush() {
	g.add(&flushOp{})
}

func (g *generator) dbRestart() {
	// Close any live iterators and snapshots, so that we can close the DB
	// cleanly.
	for len(g.liveIters) > 0 {
		g.iterClose()
	}
	for len(g.liveSnapshots) > 0 {
		g.snapshotClose()
	}
	// Close the batches.
	for len(g.liveBatches) > 0 {
		g.batchClose(g.liveBatches[0])
	}
	if len(g.liveReaders) != 1 || len(g.liveWriters) != 1 {
		panic(fmt.Sprintf("unexpected counts: liveReaders %d, liveWriters: %d",
			len(g.liveReaders), len(g.liveWriters)))
	}
	g.add(&dbRestartOp{})
}

func (g *generator) newIter() {
	iterID := makeObjID(iterTag, g.init.iterSlots)
	g.init.iterSlots++
	g.liveIters = append(g.liveIters, iterID)

	readerID := g.liveReaders.rand(g.rng)
	if iters := g.readers[readerID]; iters != nil {
		iters[iterID] = struct{}{}
		g.iters[iterID] = iters
	} else {
		// NB: the DB object does not track its open iterators because it never
		// closes.
	}

	// Generate lower/upper bounds with a 10% probability.
	var lower, upper []byte
	if g.rng.Float64() <= 0.1 {
		// Generate a new key with a .1% probability.
		lower = g.randKey(0.001)
	}
	if g.rng.Float64() <= 0.1 {
		// Generate a new key with a .1% probability.
		upper = g.randKey(0.001)
	}
	if bytes.Compare(lower, upper) > 0 {
		lower, upper = upper, lower
	}

	g.add(&newIterOp{
		readerID: readerID,
		iterID:   iterID,
		lower:    lower,
		upper:    upper,
	})
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
	} else {
		// NB: the DB object does not track its open iterators because it never
		// closes.
	}

	g.add(&newIterUsingCloneOp{
		existingIterID: existingIterID,
		iterID:         iterID,
	})
}

func (g *generator) iterClose() {
	if len(g.liveIters) == 0 {
		return
	}

	iterID := g.liveIters.rand(g.rng)
	g.liveIters.remove(iterID)
	if readerIters, ok := g.iters[iterID]; ok {
		delete(g.iters, iterID)
		delete(readerIters, iterID)
	} else {
		// NB: the DB object does not track its open iterators because it never
		// closes.
	}

	g.add(&closeOp{objID: iterID})
}

func (g *generator) iterSetBounds() {
	if len(g.liveIters) == 0 {
		return
	}

	iterID := g.liveIters.rand(g.rng)
	if g.itersLastBounds == nil {
		g.itersLastBounds = make(map[objID]iterBounds)
	}
	iterLastBounds := g.itersLastBounds[iterID]
	var lower, upper []byte
	genLower := g.rng.Float64() <= 0.9
	genUpper := g.rng.Float64() <= 0.9
	// When one of ensureLowerGE, ensureUpperLE is true, the new bounds
	// don't overlap with the previous bounds.
	var ensureLowerGE, ensureUpperLE bool
	if genLower && iterLastBounds.upper != nil && g.rng.Float64() <= 0.9 {
		ensureLowerGE = true
	}
	if (!ensureLowerGE || g.rng.Float64() < 0.5) && genUpper && iterLastBounds.lower != nil {
		ensureUpperLE = true
		ensureLowerGE = false
	}
	attempts := 0
	for {
		attempts++
		if genLower {
			// Generate a new key with a .1% probability.
			lower = g.randKey(0.001)
		}
		if genUpper {
			// Generate a new key with a .1% probability.
			upper = g.randKey(0.001)
		}
		if bytes.Compare(lower, upper) > 0 {
			lower, upper = upper, lower
		}
		if ensureLowerGE && bytes.Compare(iterLastBounds.upper, lower) > 0 {
			if attempts < 25 {
				continue
			}
			lower = iterLastBounds.upper
			upper = lower
			break
		}
		if ensureUpperLE && bytes.Compare(upper, iterLastBounds.lower) > 0 {
			if attempts < 25 {
				continue
			}
			upper = iterLastBounds.lower
			lower = upper
			break
		}
		break
	}
	g.itersLastBounds[iterID] = iterBounds{
		lower: lower,
		upper: upper,
	}
	g.add(&iterSetBoundsOp{
		iterID: iterID,
		lower:  lower,
		upper:  upper,
	})
	// Additionally seek the iterator in a manner consistent with the bounds,
	// and do some steps (Next/Prev). The seeking exercises typical
	// CockroachDB behavior when using iterators and the steps are trying to
	// stress the region near the bounds. Ideally, we should not do this as
	// part of generating a single op, but this is easier than trying to
	// control future op generation via generator state.
	doSeekLT := upper != nil && g.rng.Float64() < 0.5
	doSeekGE := lower != nil && g.rng.Float64() < 0.5
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
			iterID: iterID,
			key:    upper,
		})
		if g.rng.Float64() < 0.5 {
			g.add(&iterNextOp{
				iterID: iterID,
			})
		}
		if g.rng.Float64() < 0.5 {
			g.add(&iterNextOp{
				iterID: iterID,
			})
		}
		if g.rng.Float64() < 0.5 {
			g.add(&iterPrevOp{
				iterID: iterID,
			})
		}
	} else if doSeekGE {
		g.add(&iterSeekGEOp{
			iterID: iterID,
			key:    lower,
		})
		if g.rng.Float64() < 0.5 {
			g.add(&iterPrevOp{
				iterID: iterID,
			})
		}
		if g.rng.Float64() < 0.5 {
			g.add(&iterPrevOp{
				iterID: iterID,
			})
		}
		if g.rng.Float64() < 0.5 {
			g.add(&iterNextOp{
				iterID: iterID,
			})
		}
	}
}

func (g *generator) iterSeekGE() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekGEOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterSeekGEWithLimit() {
	if len(g.liveIters) == 0 {
		return
	}
	// 0.1% new keys
	key, limit := g.randKey(0.001), g.randKey(0.001)
	if bytes.Compare(key, limit) > 0 {
		key, limit = limit, key
	}
	g.add(&iterSeekGEOp{
		iterID: g.liveIters.rand(g.rng),
		key:    key,
		limit:  limit,
	})
}

func (g *generator) iterSeekPrefixGE() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekPrefixGEOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKey(0), // 0% new keys
	})
}

func (g *generator) iterSeekLT() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekLTOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterSeekLTWithLimit() {
	if len(g.liveIters) == 0 {
		return
	}
	// 0.1% new keys
	key, limit := g.randKey(0.001), g.randKey(0.001)
	if bytes.Compare(limit, key) > 0 {
		key, limit = limit, key
	}
	g.add(&iterSeekLTOp{
		iterID: g.liveIters.rand(g.rng),
		key:    key,
		limit:  limit,
	})
}

func (g *generator) iterFirst() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterFirstOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterLast() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterLastOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterNext() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterNextOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterNextWithLimit() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterNextOp{
		iterID: g.liveIters.rand(g.rng),
		limit:  g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) iterPrev() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterPrevOp{
		iterID: g.liveIters.rand(g.rng),
	})
}

func (g *generator) iterPrevWithLimit() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterPrevOp{
		iterID: g.liveIters.rand(g.rng),
		limit:  g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) readerGet() {
	if len(g.liveReaders) == 0 {
		return
	}

	g.add(&getOp{
		readerID: g.liveReaders.rand(g.rng),
		key:      g.randKey(0.001), // 0.1% new keys
	})
}

func (g *generator) newSnapshot() {
	snapID := makeObjID(snapTag, g.init.snapshotSlots)
	g.init.snapshotSlots++
	g.liveSnapshots = append(g.liveSnapshots, snapID)
	g.liveReaders = append(g.liveReaders, snapID)

	iters := make(objIDSet)
	g.snapshots[snapID] = iters
	g.readers[snapID] = iters

	g.add(&newSnapshotOp{
		snapID: snapID,
	})
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

func (g *generator) writerApply() {
	if len(g.liveBatches) == 0 {
		return
	}
	if len(g.liveWriters) < 2 {
		panic(fmt.Sprintf("insufficient liveWriters (%d) to apply batch", len(g.liveWriters)))
	}

	batchID := g.liveBatches.rand(g.rng)

	var writerID objID
	for {
		writerID = g.liveWriters.rand(g.rng)
		if writerID != batchID {
			break
		}
	}

	g.writerToSingleSetKeys[batchID].transferTo(g.writerToSingleSetKeys[writerID])
	g.batchClose(batchID)

	g.add(&applyOp{
		writerID: writerID,
		batchID:  batchID,
	})
}

func (g *generator) writerDelete() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&deleteOp{
		writerID: writerID,
		key:      g.randKey(0.001), // 0.1% new keys
	})
	g.tryRepositionBatchIters(writerID)
}

func (g *generator) writerDeleteRange() {
	if len(g.liveWriters) == 0 {
		return
	}

	start := g.randKey(0.001)
	end := g.randKey(0.001)
	if bytes.Compare(start, end) > 0 {
		start, end = end, start
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&deleteRangeOp{
		writerID: writerID,
		start:    start,
		end:      end,
	})
	g.tryRepositionBatchIters(writerID)
}

func (g *generator) writerIngest() {
	if len(g.liveBatches) == 0 {
		return
	}

	// Ingest between 1 and 3 batches.
	batchIDs := make([]objID, 0, 1+g.rng.Intn(3))
	for i := 0; i < cap(batchIDs); i++ {
		batchID := g.liveBatches.rand(g.rng)
		// After the ingest runs, it either succeeds and the keys are in the
		// DB, or it fails and these keys never make it to the DB.
		g.writerToSingleSetKeys[batchID].transferTo(g.singleSetKeysInDB)
		g.batchClose(batchID)
		batchIDs = append(batchIDs, batchID)
		if len(g.liveBatches) == 0 {
			break
		}
	}

	g.add(&ingestOp{
		batchIDs: batchIDs,
	})
}

func (g *generator) writerMerge() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&mergeOp{
		writerID: writerID,
		// 20% new keys, and none are set once.
		key:   g.randKeyForWrite(0.2, 0, writerID),
		value: g.randValue(0, 20),
	})
	g.tryRepositionBatchIters(writerID)
}

func (g *generator) writerSet() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&setOp{
		writerID: writerID,
		// 50% new keys, and of those 50%, half are keys that will be set
		// once.
		key:   g.randKeyForWrite(0.5, 0.5, writerID),
		value: g.randValue(0, 20),
	})
	g.tryRepositionBatchIters(writerID)
}

func (g *generator) writerSingleDelete() {
	if len(g.liveWriters) == 0 {
		return
	}

	writerID := g.liveWriters.rand(g.rng)
	key := g.randKeyToSingleDelete()
	if key == nil {
		return
	}
	g.add(&singleDeleteOp{
		writerID: writerID,
		key:      key,
	})
	g.tryRepositionBatchIters(writerID)
}

func (g *generator) tryRepositionBatchIters(writerID objID) {
	if writerID.tag() != batchTag {
		return
	}
	// Reposition all batch iterators to avoid https://github.com/cockroachdb/pebble/issues/943
	iters, ok := g.batches[writerID]
	if !ok {
		// Not an indexed batch.
		return
	}
	for _, id := range iters.sorted() {
		g.add(&iterFirstOp{iterID: id})
	}
}

func (g *generator) String() string {
	var buf bytes.Buffer
	for _, op := range g.ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	return buf.String()
}

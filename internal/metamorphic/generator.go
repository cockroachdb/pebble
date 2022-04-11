// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"golang.org/x/exp/rand"
)

type iterBounds struct {
	lower []byte
	upper []byte
}

type generator struct {
	cfg config
	rng *rand.Rand

	init *initOp
	ops  []op

	// keyManager tracks the state of keys a operation generation time.
	keyManager *keyManager
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

func newGenerator(rng *rand.Rand, cfg config, km *keyManager) *generator {
	g := &generator{
		cfg:             cfg,
		rng:             rng,
		init:            &initOp{},
		keyManager:      km,
		liveReaders:     objIDSlice{makeObjID(dbTag, 0)},
		liveWriters:     objIDSlice{makeObjID(dbTag, 0)},
		batches:         make(map[objID]objIDSet),
		iters:           make(map[objID]objIDSet),
		readers:         make(map[objID]objIDSet),
		snapshots:       make(map[objID]objIDSet),
		itersLastBounds: make(map[objID]iterBounds),
	}
	// Note that the initOp fields are populated during generation.
	g.ops = append(g.ops, g.init)
	return g
}

func generate(rng *rand.Rand, count uint64, cfg config, km *keyManager) []op {
	g := newGenerator(rng, cfg, km)

	generators := []func(){
		batchAbort:           g.batchAbort,
		batchCommit:          g.batchCommit,
		dbCheckpoint:         g.dbCheckpoint,
		dbCompact:            g.dbCompact,
		dbFlush:              g.dbFlush,
		dbRestart:            g.dbRestart,
		iterClose:            g.iterClose,
		iterFirst:            g.iterFirst,
		iterLast:             g.iterLast,
		iterNext:             g.iterNext,
		iterNextWithLimit:    g.iterNextWithLimit,
		iterPrev:             g.iterPrev,
		iterPrevWithLimit:    g.iterPrevWithLimit,
		iterSeekGE:           g.iterSeekGE,
		iterSeekGEWithLimit:  g.iterSeekGEWithLimit,
		iterSeekLT:           g.iterSeekLT,
		iterSeekLTWithLimit:  g.iterSeekLTWithLimit,
		iterSeekPrefixGE:     g.iterSeekPrefixGE,
		iterSetBounds:        g.iterSetBounds,
		iterSetOptions:       g.iterSetOptions,
		newBatch:             g.newBatch,
		newIndexedBatch:      g.newIndexedBatch,
		newIter:              g.newIter,
		newIterUsingClone:    g.newIterUsingClone,
		newSnapshot:          g.newSnapshot,
		readerGet:            g.readerGet,
		snapshotClose:        g.snapshotClose,
		writerApply:          g.writerApply,
		writerDelete:         g.writerDelete,
		writerDeleteRange:    g.writerDeleteRange,
		writerIngest:         g.writerIngest,
		writerMerge:          g.writerMerge,
		writerRangeKeyDelete: g.writerRangeKeyDelete,
		writerRangeKeySet:    g.writerRangeKeySet,
		writerRangeKeyUnset:  g.writerRangeKeyUnset,
		writerSet:            g.writerSet,
		writerSingleDelete:   g.writerSingleDelete,
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
	g.keyManager.update(op)
}

// randKeyToWrite returns a key for any write other than SingleDelete.
//
// TODO(peter): make the size and distribution of keys configurable. See
// keyDist and keySizeDist in config.go.
func (g *generator) randKeyToWrite(newKey float64) []byte {
	return g.randKeyHelper(g.keyManager.eligibleWriteKeys(), newKey)
}

// prefixKeyRange generates a [start, end) pair consisting of two prefix keys.
func (g *generator) prefixKeyRange() ([]byte, []byte) {
	start := g.randPrefixToWrite(0.001)
	end := g.randPrefixToWrite(0.001)
	for g.cmp(start, end) == 0 {
		end = g.randPrefixToWrite(0.05)
	}
	if g.cmp(start, end) > 0 {
		start, end = end, start
	}
	return start, end
}

// randPrefixToWrite returns a prefix key (a key with no suffix) for a range key
// write operation.
func (g generator) randPrefixToWrite(newPrefix float64) []byte {
	prefixes := g.keyManager.prefixes()
	if len(prefixes) > 0 && g.rng.Float64() > newPrefix {
		// Use an existing prefix.
		p := g.rng.Intn(len(prefixes))
		return prefixes[p]
	}

	// Use a new prefix.
	var prefix []byte
	for {
		prefix = g.randKeyHelperSuffix(nil, 4, 12, 0)
		if !g.keyManager.prefixExists(prefix) {
			if !g.keyManager.addNewKey(prefix) {
				panic("key must not exist if prefix doesn't exist")
			}
			return prefix
		}
	}
}

// randSuffixToWrite generates a random suffix according to the configuration's suffix
// distribution. It takes a probability 0 ≤ p ≤ 1.0 indicating the probability
// with which the generator should increase the max suffix generated by the
// generator.
//
// randSuffixToWrite may return a nil suffix, with the probability the
// configuration's suffix distribution assigns to the zero suffix.
func (g *generator) randSuffixToWrite(incMaxProb float64) []byte {
	if g.rng.Float64() < incMaxProb {
		g.cfg.writeSuffixDist.IncMax(1)
	}
	return suffixFromInt(int(g.cfg.writeSuffixDist.Uint64(g.rng)))
}

// randSuffixToRead generates a random suffix used during reads. The suffixes
// generated by this function are within the same range as suffixes generated by
// randSuffixToWrite, however randSuffixToRead pulls from a uniform
// distribution.
func (g *generator) randSuffixToRead() []byte {
	// When reading, don't apply the recency skewing in order to better exercise
	// a reading a mix of older and newer keys.
	max := g.cfg.writeSuffixDist.Max()
	return suffixFromInt(int(g.rng.Uint64n(max)))
}

func suffixFromInt(suffix int) []byte {
	// Treat the zero as no suffix to match the behavior during point key
	// generation in randKeyHelper.
	if suffix == 0 {
		return nil
	}
	return testkeys.Suffix(suffix)
}

func (g *generator) randKeyToSingleDelete(id objID) []byte {
	keys := g.keyManager.eligibleSingleDeleteKeys(id)
	length := len(keys)
	if length == 0 {
		return nil
	}
	return keys[g.rng.Intn(length)]
}

// randKeyToRead returns a key for read operations.
func (g *generator) randKeyToRead(newKey float64) []byte {
	return g.randKeyHelper(g.keyManager.eligibleReadKeys(), newKey)
}

func (g *generator) randKeyHelper(keys [][]byte, newKey float64) []byte {
	switch {
	case len(keys) > 0 && g.rng.Float64() > newKey:
		// Use an existing user key.
		return keys[g.rng.Intn(len(keys))]

	case len(keys) > 0 && g.rng.Float64() > g.cfg.newPrefix:
		// Use an existing prefix but a new suffix, producing a new user key.
		prefixes := g.keyManager.prefixes()
		var key []byte
		for {
			// Pick a prefix on each iteration in case most or all suffixes are
			// already in use for any individual prefix.
			p := g.rng.Intn(len(prefixes))
			suffix := int(g.cfg.writeSuffixDist.Uint64(g.rng))

			if suffix > 0 {
				key = resizeBuffer(key, len(prefixes[p]), testkeys.SuffixLen(suffix))
				n := copy(key, prefixes[p])
				testkeys.WriteSuffix(key[n:], suffix)
			} else {
				key = resizeBuffer(key, len(prefixes[p]), 0)
				copy(key, prefixes[p])
			}
			if g.keyManager.addNewKey(key) {
				return key
			}

			// If the generated key already existed, increase the suffix
			// distribution to make a generating a new user key with an existing
			// prefix more likely.
			g.cfg.writeSuffixDist.IncMax(1)
		}

	default:
		// Use a new prefix, producing a new user key.
		suffix := int(g.cfg.writeSuffixDist.Uint64(g.rng))
		var key []byte
		for {
			key = g.randKeyHelperSuffix(nil, 4, 12, suffix)
			if !g.keyManager.prefixExists(key[:testkeys.Comparer.Split(key)]) {
				if !g.keyManager.addNewKey(key) {
					panic("key must not exist if prefix doesn't exist")
				}
				return key
			}
		}
	}
}

// randKeyHelperSuffix is a helper function for randKeyHelper, and should not be
// invoked directly.
func (g *generator) randKeyHelperSuffix(dst []byte, minPrefixLen, maxPrefixLen, suffix int) []byte {
	n := minPrefixLen
	if maxPrefixLen > minPrefixLen {
		n += g.rng.Intn(maxPrefixLen - minPrefixLen)
	}
	// In order to test a mix of suffixed and unsuffixed keys, omit the zero
	// suffix.
	if suffix == 0 {
		dst = resizeBuffer(dst, n, 0)
		g.fillRand(dst)
		return dst
	}
	suffixLen := testkeys.SuffixLen(suffix)
	dst = resizeBuffer(dst, n, suffixLen)
	g.fillRand(dst[:n])
	testkeys.WriteSuffix(dst[n:], suffix)
	return dst
}

func resizeBuffer(buf []byte, prefixLen, suffixLen int) []byte {
	if cap(buf) >= prefixLen+suffixLen {
		return buf[:prefixLen+suffixLen]
	}
	return make([]byte, prefixLen+suffixLen)
}

// TODO(peter): make the value size configurable. See valueSizeDist in
// config.go.
func (g *generator) randValue(min, max int) []byte {
	n := min
	if max > min {
		n += g.rng.Intn(max - min)
	}
	buf := make([]byte, n)
	g.fillRand(buf)
	return buf
}

func (g *generator) fillRand(buf []byte) {
	// NB: The actual random values are not particularly important. We only use
	// lowercase letters because that makes visual determination of ordering
	// easier, rather than having to remember the lexicographic ordering of
	// uppercase vs lowercase, or letters vs numbers vs punctuation.
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = 12 // floor(log(math.MaxUint64)/log(lettersLen))

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
}

func (g *generator) newBatch() {
	batchID := makeObjID(batchTag, g.init.batchSlots)
	g.init.batchSlots++
	g.liveBatches = append(g.liveBatches, batchID)
	g.liveWriters = append(g.liveWriters, batchID)

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
	start := g.randKeyToRead(0.01)
	end := g.randKeyToRead(0.01)
	if g.cmp(start, end) > 0 {
		start, end = end, start
	}
	g.add(&compactOp{
		start:       start,
		end:         end,
		parallelize: g.rng.Float64() < 0.5,
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
		//lint:ignore SA9003 - readability
	} else {
		// NB: the DB object does not track its open iterators because it never
		// closes.
	}

	// Generate lower/upper bounds with a 10% probability.
	var lower, upper []byte
	if g.rng.Float64() <= 0.1 {
		// Generate a new key with a .1% probability.
		lower = g.randKeyToRead(0.001)
	}
	if g.rng.Float64() <= 0.1 {
		// Generate a new key with a .1% probability.
		upper = g.randKeyToRead(0.001)
	}
	if g.cmp(lower, upper) > 0 {
		lower, upper = upper, lower
	}
	keyTypes, maskSuffix := g.randKeyTypesAndMask()
	g.add(&newIterOp{
		readerID:           readerID,
		iterID:             iterID,
		lower:              lower,
		upper:              upper,
		keyTypes:           keyTypes,
		rangeKeyMaskSuffix: maskSuffix,
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
		if g.rng.Intn(2) == 1 {
			maskSuffix = g.randSuffixToRead()
		}
	default: // 20% probability
		keyTypes = uint32(pebble.IterKeyTypeRangesOnly)
	}
	return keyTypes, maskSuffix
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
		//lint:ignore SA9003 - readability
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
			lower = g.randKeyToRead(0.001)
		}
		if genUpper {
			// Generate a new key with a .1% probability.
			upper = g.randKeyToRead(0.001)
		}
		if g.cmp(lower, upper) > 0 {
			lower, upper = upper, lower
		}
		if ensureLowerGE && g.cmp(iterLastBounds.upper, lower) > 0 {
			if attempts < 25 {
				continue
			}
			lower = iterLastBounds.upper
			upper = lower
			break
		}
		if ensureUpperLE && g.cmp(upper, iterLastBounds.lower) > 0 {
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

func (g *generator) iterSetOptions() {
	if len(g.liveIters) == 0 {
		return
	}
	iterID := g.liveIters.rand(g.rng)

	bounds := g.itersLastBounds[iterID]
	// With 50% probability, update the bounds.
	if g.rng.Intn(2) == 0 {
		// Generate a new key with a .1% probability.
		bounds.lower = g.randKeyToRead(0.001)
		// Generate a new key with a .1% probability.
		bounds.upper = g.randKeyToRead(0.001)
		if g.cmp(bounds.lower, bounds.upper) > 0 {
			bounds.lower, bounds.upper = bounds.upper, bounds.lower
		}
		g.itersLastBounds[iterID] = bounds
	}

	keyTypes, maskSuffix := g.randKeyTypesAndMask()
	g.add(&iterSetOptionsOp{
		iterID:             iterID,
		lower:              bounds.lower,
		upper:              bounds.upper,
		keyTypes:           keyTypes,
		rangeKeyMaskSuffix: maskSuffix,
	})

	// Additionally, perform a random absolute positioning operation. The
	// SetOptions contract requires one before the next relative positioning
	// operation. Ideally, we should not do this as part of generating a single
	// op, but this is easier than trying to control future op generation via
	// generator state.
	g.pickOneUniform(
		g.iterFirst,
		g.iterLast,
		g.iterSeekGE,
		g.iterSeekGEWithLimit,
		g.iterSeekPrefixGE,
		g.iterSeekLT,
		g.iterSeekLTWithLimit,
	)()
}

func (g *generator) iterSeekGE() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekGEOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKeyToRead(0.001), // 0.1% new keys
	})
}

func (g *generator) iterSeekGEWithLimit() {
	if len(g.liveIters) == 0 {
		return
	}
	// 0.1% new keys
	key, limit := g.randKeyToRead(0.001), g.randKeyToRead(0.001)
	if g.cmp(key, limit) > 0 {
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
		key:    g.randKeyToRead(0), // 0% new keys
	})
}

func (g *generator) iterSeekLT() {
	if len(g.liveIters) == 0 {
		return
	}

	g.add(&iterSeekLTOp{
		iterID: g.liveIters.rand(g.rng),
		key:    g.randKeyToRead(0.001), // 0.1% new keys
	})
}

func (g *generator) iterSeekLTWithLimit() {
	if len(g.liveIters) == 0 {
		return
	}
	// 0.1% new keys
	key, limit := g.randKeyToRead(0.001), g.randKeyToRead(0.001)
	if g.cmp(limit, key) > 0 {
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
		limit:  g.randKeyToRead(0.001), // 0.1% new keys
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
		limit:  g.randKeyToRead(0.001), // 0.1% new keys
	})
}

func (g *generator) readerGet() {
	if len(g.liveReaders) == 0 {
		return
	}

	g.add(&getOp{
		readerID: g.liveReaders.rand(g.rng),
		key:      g.randKeyToRead(0.001), // 0.1% new keys
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
		key:      g.randKeyToWrite(0.001), // 0.1% new keys
	})
}

func (g *generator) writerDeleteRange() {
	if len(g.liveWriters) == 0 {
		return
	}

	start := g.randKeyToWrite(0.001)
	end := g.randKeyToWrite(0.001)
	if g.cmp(start, end) > 0 {
		start, end = end, start
	}

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
		suffix = g.randSuffixToWrite(0.05)
	}

	writerID := g.liveWriters.rand(g.rng)
	g.add(&rangeKeySetOp{
		writerID: writerID,
		start:    start,
		end:      end,
		suffix:   suffix,
		value:    g.randValue(0, 20),
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
		suffix = g.randSuffixToWrite(0.05)
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

	// TODO(nicktrav): this is resulting in too many single batch ingests.
	// Consider alternatives. One possibility would be to pass through whether
	// we can tolerate failure or not, and if the ingestOp encounters a
	// failure, it would retry after splitting into single batch ingests.

	// Ingest between 1 and 3 batches.
	batchIDs := make([]objID, 0, 1+g.rng.Intn(3))
	canFail := cap(batchIDs) > 1
	for i := 0; i < cap(batchIDs); i++ {
		batchID := g.liveBatches.rand(g.rng)
		if canFail && !g.keyManager.canTolerateApplyFailure(batchID) {
			continue
		}
		// After the ingest runs, it either succeeds and the keys are in the
		// DB, or it fails and these keys never make it to the DB.
		g.batchClose(batchID)
		batchIDs = append(batchIDs, batchID)
		if len(g.liveBatches) == 0 {
			break
		}
	}
	if len(batchIDs) == 0 && len(g.liveBatches) > 0 {
		// Unable to find multiple batches because of the
		// canTolerateApplyFailure call above, so just pick one batch.
		batchID := g.liveBatches.rand(g.rng)
		g.batchClose(batchID)
		batchIDs = append(batchIDs, batchID)
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
		// 20% new keys.
		key:   g.randKeyToWrite(0.2),
		value: g.randValue(0, 20),
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
		key:   g.randKeyToWrite(0.5),
		value: g.randValue(0, 20),
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

func (g *generator) pickOneUniform(options ...func()) func() {
	i := g.rng.Intn(len(options))
	return options[i]
}

func (g *generator) cmp(a, b []byte) int {
	return g.keyManager.comparer.Compare(a, b)
}

func (g *generator) String() string {
	var buf bytes.Buffer
	for _, op := range g.ops {
		fmt.Fprintf(&buf, "%s\n", op)
	}
	return buf.String()
}

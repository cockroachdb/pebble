// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

type opType int

const (
	batchAbort opType = iota
	batchCommit
	dbCheckpoint
	dbClose
	dbCompact
	dbFlush
	dbRestart
	iterClose
	iterFirst
	iterLast
	iterNext
	iterNextWithLimit
	iterPrev
	iterPrevWithLimit
	iterSeekGE
	iterSeekGEWithLimit
	iterSeekLT
	iterSeekLTWithLimit
	iterSeekPrefixGE
	iterSetBounds
	newBatch
	newIndexedBatch
	newIter
	newIterUsingClone
	newSnapshot
	readerGet
	snapshotClose
	writerApply
	writerDelete
	writerDeleteRange
	writerIngest
	writerMerge
	writerSet
	writerSingleDelete
)

type config struct {
	// Weights for the operation mix to generate. ops[i] corresponds to the
	// weight for opType(i).
	ops []int

	// TODO(peter): unimplemented
	// keyDist        randvar.Dynamic
	// keySizeDist    randvar.Static
	// valueSizeDist  randvar.Static
	// updateFrac     float64
	// lowerBoundFrac float64
	// upperBoundFrac float64
}

var defaultConfig = config{
	// dbClose is not in this list since it is deterministically generated once, at the end of the test.
	ops: []int{
		batchAbort:          5,
		batchCommit:         5,
		dbCheckpoint:        1,
		dbCompact:           1,
		dbFlush:             2,
		dbRestart:           2,
		iterClose:           5,
		iterFirst:           100,
		iterLast:            100,
		iterNext:            100,
		iterNextWithLimit:   20,
		iterPrev:            100,
		iterPrevWithLimit:   20,
		iterSeekGE:          100,
		iterSeekGEWithLimit: 20,
		iterSeekLT:          100,
		iterSeekLTWithLimit: 20,
		iterSeekPrefixGE:    100,
		iterSetBounds:       100,
		newBatch:            5,
		newIndexedBatch:     5,
		newIter:             10,
		newIterUsingClone:   5,
		newSnapshot:         10,
		readerGet:           100,
		snapshotClose:       10,
		writerApply:         10,
		writerDelete:        100,
		writerDeleteRange:   50,
		writerIngest:        100,
		writerMerge:         100,
		writerSet:           100,
		writerSingleDelete:  25,
	},
}

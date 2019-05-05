// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

type opType int

const (
	batchAbort opType = iota
	batchCommit
	iterClose
	iterFirst
	iterLast
	iterNext
	iterPrev
	iterSeekGE
	iterSeekLT
	iterSeekPrefixGE
	iterSetBounds
	newBatch
	newIndexedBatch
	newIter
	newSnapshot
	readerGet
	snapshotClose
	writerApply
	writerDelete
	writerDeleteRange
	writerIngest
	writerMerge
	writerSet
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
	ops: []int{
		batchAbort:        5,
		batchCommit:       5,
		iterClose:         10,
		iterFirst:         100,
		iterLast:          100,
		iterNext:          100,
		iterPrev:          100,
		iterSeekGE:        100,
		iterSeekLT:        100,
		iterSeekPrefixGE:  0,
		iterSetBounds:     10,
		newBatch:          5,
		newIndexedBatch:   5,
		newIter:           10,
		newSnapshot:       10,
		readerGet:         100,
		snapshotClose:     10,
		writerApply:       10,
		writerDelete:      100,
		writerDeleteRange: 50,
		writerIngest:      100,
		writerMerge:       100,
		writerSet:         100,
	},
}

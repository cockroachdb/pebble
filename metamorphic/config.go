// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import "github.com/cockroachdb/pebble/internal/randvar"

// OpType is an enum of possible operation types.
type OpType int

// These constants define the set of possible operation types performed by the
// metamorphic test.
const (
	OpBatchAbort OpType = iota
	OpBatchCommit
	OpDBCheckpoint
	OpDBClose
	OpDBCompact
	OpDBDownload
	OpDBFlush
	OpDBRatchetFormatMajorVersion
	OpDBRestart
	OpIterClose
	OpIterFirst
	OpIterLast
	OpIterNext
	OpIterNextWithLimit
	OpIterNextPrefix
	OpIterCanSingleDelete
	OpIterPrev
	OpIterPrevWithLimit
	OpIterSeekGE
	OpIterSeekGEWithLimit
	OpIterSeekLT
	OpIterSeekLTWithLimit
	OpIterSeekPrefixGE
	OpIterSetBounds
	OpIterSetOptions
	OpNewBatch
	OpNewIndexedBatch
	OpNewIter
	OpNewIterUsingClone
	OpNewSnapshot
	OpNewExternalObj
	OpReaderGet
	OpReplicate
	OpSnapshotClose
	OpWriterApply
	OpWriterDelete
	OpWriterDeleteRange
	OpWriterIngest
	OpWriterIngestAndExcise
	OpWriterIngestExternalFiles
	OpWriterLogData
	OpWriterMerge
	OpWriterRangeKeyDelete
	OpWriterRangeKeySet
	OpWriterRangeKeyUnset
	OpWriterSet
	OpWriterSingleDelete
	NumOpTypes
)

func (o OpType) isDelete() bool {
	return o == OpWriterDelete || o == OpWriterDeleteRange || o == OpWriterSingleDelete
}

// OpConfig describes the distribution of operations and their attributes.
type OpConfig struct {
	// Weights for the operation mix to generate. ops[i] corresponds to the
	// weight for opType(i).
	ops [NumOpTypes]int

	// newPrefix configures the probability that when generating a new user key,
	// the generated key uses a new key prefix rather than an existing prefix
	// with a suffix.
	newPrefix float64
	// writeSuffixDist defines the distribution of key suffixes during writing.
	// It's a dynamic randvar to roughly emulate workloads with MVCC timestamps,
	// skewing towards most recent timestamps.
	writeSuffixDist randvar.Dynamic

	// numInstances defines the number of pebble instances created for this
	// metamorphic test run.
	numInstances int

	// TODO(peter): unimplemented
	// keyDist        randvar.Dynamic
	// keySizeDist    randvar.Static
	// valueSizeDist  randvar.Static
	// updateFrac     float64
	// lowerBoundFrac float64
	// upperBoundFrac float64
}

// WithNewPrefixProbability returns a modified op configuration with the
// probability of generating a new key prefix set to the provided value in
// [0,1.0].
func (c OpConfig) WithNewPrefixProbability(p float64) OpConfig {
	c.newPrefix = p
	return c
}

// WithOpWeight returns a modified op configuration with the weight of the
// provided operation type overidden.
func (c OpConfig) WithOpWeight(op OpType, weight int) OpConfig {
	c.ops[op] = weight
	return c
}

var presetConfigs = []OpConfig{
	DefaultOpConfig(),
	// Generate a configuration that helps exercise code paths dependent on many
	// versions of keys with the same prefixes. The default configuration does
	// not tend to generate many versions of the same key. Additionally, its
	// relatively high weight for deletion write operations makes it less likely
	// that we'll accumulate enough versions to exercise some code paths (eg,
	// see #2921 which requires >16 SETs for versions of the same prefix to
	// reside in a single block to exercise the code path).
	//
	// To encourage generation of many versions of the same keys, generate a new
	// prefix only 4% of the time when generating a new key. The remaining 96%
	// of new key generations will use an existing prefix. To keep the size of
	// the database growing, we also reduce the probability of delete write
	// operations significantly.
	DefaultOpConfig().
		WithNewPrefixProbability(0.04).
		WithOpWeight(OpWriterDeleteRange, 1).
		WithOpWeight(OpWriterDelete, 5).
		WithOpWeight(OpWriterSingleDelete, 5).
		WithOpWeight(OpWriterMerge, 0),
}

var multiInstancePresetConfig = multiInstanceConfig()

// DefaultOpConfig returns the default distribution of operations.
func DefaultOpConfig() OpConfig {
	return OpConfig{
		// dbClose is not in this list since it is deterministically generated once, at the end of the test.
		ops: [NumOpTypes]int{
			OpBatchAbort:                  5,
			OpBatchCommit:                 5,
			OpDBCheckpoint:                1,
			OpDBCompact:                   1,
			OpDBDownload:                  1,
			OpDBFlush:                     2,
			OpDBRatchetFormatMajorVersion: 1,
			OpDBRestart:                   2,
			OpIterClose:                   5,
			OpIterFirst:                   100,
			OpIterLast:                    100,
			OpIterNext:                    100,
			OpIterNextWithLimit:           20,
			OpIterNextPrefix:              20,
			OpIterCanSingleDelete:         20,
			OpIterPrev:                    100,
			OpIterPrevWithLimit:           20,
			OpIterSeekGE:                  100,
			OpIterSeekGEWithLimit:         20,
			OpIterSeekLT:                  100,
			OpIterSeekLTWithLimit:         20,
			OpIterSeekPrefixGE:            100,
			OpIterSetBounds:               100,
			OpIterSetOptions:              10,
			OpNewBatch:                    5,
			OpNewIndexedBatch:             5,
			OpNewIter:                     10,
			OpNewIterUsingClone:           5,
			OpNewSnapshot:                 10,
			OpReaderGet:                   100,
			OpReplicate:                   0,
			OpSnapshotClose:               10,
			OpWriterApply:                 10,
			OpWriterDelete:                100,
			OpWriterDeleteRange:           50,
			OpWriterIngest:                100,
			OpWriterIngestAndExcise:       50,
			OpWriterLogData:               10,
			OpWriterMerge:                 100,
			OpWriterRangeKeySet:           10,
			OpWriterRangeKeyUnset:         10,
			OpWriterRangeKeyDelete:        5,
			OpWriterSet:                   100,
			OpWriterSingleDelete:          50,
			OpNewExternalObj:              5,
			OpWriterIngestExternalFiles:   100,
		},
		// Use a new prefix 75% of the time (and 25% of the time use an existing
		// prefix with an alternative suffix).
		newPrefix: 0.75,
		// Use a skewed distribution of suffixes to mimic MVCC timestamps. The
		// range will be widened whenever a suffix is found to already be in use
		// for a particular prefix.
		writeSuffixDist: mustDynamic(randvar.NewSkewedLatest(0, 1, 0.99)),
	}
}

// ReadOpConfig builds an OpConfig that performs only read operations.
func ReadOpConfig() OpConfig {
	return OpConfig{
		// dbClose is not in this list since it is deterministically generated once, at the end of the test.
		ops: [NumOpTypes]int{
			OpBatchAbort:                  0,
			OpBatchCommit:                 0,
			OpDBCheckpoint:                0,
			OpDBCompact:                   0,
			OpDBFlush:                     0,
			OpDBRatchetFormatMajorVersion: 0,
			OpDBRestart:                   0,
			OpIterClose:                   5,
			OpIterFirst:                   100,
			OpIterLast:                    100,
			OpIterNext:                    100,
			OpIterNextWithLimit:           20,
			OpIterNextPrefix:              20,
			OpIterPrev:                    100,
			OpIterPrevWithLimit:           20,
			OpIterSeekGE:                  100,
			OpIterSeekGEWithLimit:         20,
			OpIterSeekLT:                  100,
			OpIterSeekLTWithLimit:         20,
			OpIterSeekPrefixGE:            100,
			OpIterSetBounds:               100,
			OpIterSetOptions:              10,
			OpNewBatch:                    0,
			OpNewIndexedBatch:             0,
			OpNewIter:                     10,
			OpNewIterUsingClone:           5,
			OpNewSnapshot:                 10,
			OpReaderGet:                   100,
			OpSnapshotClose:               10,
			OpWriterApply:                 0,
			OpWriterDelete:                0,
			OpWriterDeleteRange:           0,
			OpWriterIngest:                0,
			OpWriterLogData:               0,
			OpWriterMerge:                 0,
			OpWriterRangeKeySet:           0,
			OpWriterRangeKeyUnset:         0,
			OpWriterRangeKeyDelete:        0,
			OpWriterSet:                   0,
			OpWriterSingleDelete:          0,
		},
		// Use a new prefix 75% of the time (and 25% of the time use an existing
		// prefix with an alternative suffix).
		newPrefix: 0.75,
		// Use a skewed distribution of suffixes to mimic MVCC timestamps. The
		// range will be widened whenever a suffix is found to already be in use
		// for a particular prefix.
		writeSuffixDist: mustDynamic(randvar.NewSkewedLatest(0, 1, 0.99)),
	}
}

// WriteOpConfig builds an OpConfig suitable for generating a random test
// database. It generates Writer operations and some meta database operations
// like flushes and manual compactions, but it does not generate any reads.
func WriteOpConfig() OpConfig {
	return OpConfig{
		// dbClose is not in this list since it is deterministically generated once, at the end of the test.
		ops: [NumOpTypes]int{
			OpBatchAbort:                  0,
			OpBatchCommit:                 5,
			OpDBCheckpoint:                0,
			OpDBCompact:                   1,
			OpDBFlush:                     2,
			OpDBRatchetFormatMajorVersion: 1,
			OpDBRestart:                   2,
			OpIterClose:                   0,
			OpIterFirst:                   0,
			OpIterLast:                    0,
			OpIterNext:                    0,
			OpIterNextWithLimit:           0,
			OpIterNextPrefix:              0,
			OpIterPrev:                    0,
			OpIterPrevWithLimit:           0,
			OpIterSeekGE:                  0,
			OpIterSeekGEWithLimit:         0,
			OpIterSeekLT:                  0,
			OpIterSeekLTWithLimit:         0,
			OpIterSeekPrefixGE:            0,
			OpIterSetBounds:               0,
			OpIterSetOptions:              0,
			OpNewBatch:                    10,
			OpNewIndexedBatch:             0,
			OpNewIter:                     0,
			OpNewIterUsingClone:           0,
			OpNewSnapshot:                 10,
			OpReaderGet:                   0,
			OpSnapshotClose:               10,
			OpWriterApply:                 10,
			OpWriterDelete:                100,
			OpWriterDeleteRange:           20,
			OpWriterIngest:                100,
			OpWriterLogData:               10,
			OpWriterMerge:                 100,
			OpWriterRangeKeySet:           10,
			OpWriterRangeKeyUnset:         10,
			OpWriterRangeKeyDelete:        5,
			OpWriterSet:                   100,
			OpWriterSingleDelete:          50,
		},
		// Use a new prefix 75% of the time (and 25% of the time use an existing
		// prefix with an alternative suffix).
		newPrefix: 0.75,
		// Use a skewed distribution of suffixes to mimic MVCC timestamps. The
		// range will be widened whenever a suffix is found to already be in use
		// for a particular prefix.
		writeSuffixDist: mustDynamic(randvar.NewSkewedLatest(0, 1, 0.99)),
	}
}

func multiInstanceConfig() OpConfig {
	cfg := DefaultOpConfig()
	cfg.ops[OpReplicate] = 5
	// Single deletes and merges are disabled in multi-instance mode, as
	// replicateOp doesn't support them.
	cfg.ops[OpWriterSingleDelete] = 0
	cfg.ops[OpWriterMerge] = 0

	// TODO(radu): external file ingest doesn't yet work with OpReplicate ("cannot
	// use skip-shared iteration due to non-shareable files in lower levels").
	cfg.ops[OpNewExternalObj] = 0
	cfg.ops[OpWriterIngestExternalFiles] = 0
	return cfg
}

func mustDynamic(dyn randvar.Dynamic, err error) randvar.Dynamic {
	if err != nil {
		panic(err)
	}
	return dyn
}

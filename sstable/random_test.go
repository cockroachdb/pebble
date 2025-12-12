// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"fmt"
	randv1 "math/rand"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/metamorphic"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// IterIterator_RandomErrors builds random sstables and runs random iterator
// operations against them while randomly injecting errors. It ensures that if
// an error is injected during an operation, operation surfaces the error to the
// caller.
func TestIterator_RandomErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	root := time.Now().UnixNano()
	// Run the test a few times with various seeds for more consistent code
	// coverage.
	for i := int64(0); i < 50; i++ {
		seed := root + i
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runErrorInjectionTest(t, seed)
		})
	}
}

func runErrorInjectionTest(t *testing.T, seed int64) {
	t.Logf("seed %d", seed)
	fs := vfs.NewMem()
	f, err := fs.Create("random.sst", vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))
	cfg := randomTableConfig{
		wopts:     nil, /* leave to randomize */
		keys:      testkeys.Alpha(3 + rng.IntN(2)),
		keyCount:  10_000,
		maxValLen: rng.IntN(64) + 1,
		maxSuffix: rng.Int64N(95) + 5,
		maxSeqNum: rng.Int64N(1000) + 10,
		rng:       rng,
	}
	cfg.randomize()
	_, err = buildRandomSSTable(f, cfg)
	require.NoError(t, err)

	f, err = fs.Open("random.sst")
	require.NoError(t, err)
	// Randomly inject errors into 25% of file operations. We use an
	// errorfs.Toggle to avoid injecting errors until the file has been opened.
	toggle := &errorfs.Toggle{Injector: errorfs.ErrInjected.If(errorfs.Randomly(0.25, seed))}
	counter := &errorfs.Counter{Injector: toggle}
	var stack []byte
	f = errorfs.WrapFile(f, errorfs.InjectorFunc(func(op errorfs.Op) error {
		err := counter.MaybeError(op)
		if err != nil {
			// Save the stack trace of the most recently injected error.
			stack = debug.Stack()
		}
		return err
	}))
	readable, err := objstorage.NewSimpleReadable(f)
	require.NoError(t, err)
	r, err := NewReader(context.Background(), readable, cfg.readerOpts())
	require.NoError(t, err)
	defer r.Close()

	var filterer *BlockPropertiesFilterer
	if rng.Float64() < 0.75 {
		low, high := uint64(cfg.randSuffix()), uint64(cfg.randSuffix())
		if low > high {
			low, high = high, low
		}
		filterer = newBlockPropertiesFilterer([]BlockPropertyFilter{
			NewTestKeysBlockPropertyFilter(low, high),
		}, nil, nil)
	}

	// TOOD(jackson): NewPointIter returns an
	// iterator over point keys only. Should we add variants of this test that run
	// random operations on the range deletion and range key iterators?
	var stats base.InternalIteratorStats
	filterBlockSizeLimit := AlwaysUseFilterBlock
	if rng.IntN(2) == 1 {
		filterBlockSizeLimit = NeverUseFilterBlock
	}
	it, err := r.NewPointIter(context.Background(), IterOptions{
		Transforms:           NoTransforms,
		Filterer:             filterer,
		FilterBlockSizeLimit: filterBlockSizeLimit,
		Env:                  ReadEnv{Block: block.ReadEnv{Stats: &stats, IterStats: nil}},
		ReaderProvider:       MakeTrivialReaderProvider(r),
		BlobContext:          AssertNoBlobHandles,
	})
	require.NoError(t, err)
	defer it.Close()

	// Begin injecting errors.
	toggle.On()

	ops := opRunner{randomTableConfig: cfg, it: it}
	nextOp := metamorphic.Weighted[func() bool]{
		{Item: ops.runSeekGE, Weight: 2},
		{Item: ops.runSeekPrefixGE, Weight: 2},
		{Item: ops.runSeekLT, Weight: 2},
		{Item: ops.runFirst, Weight: 1},
		{Item: ops.runLast, Weight: 1},
		{Item: ops.runNext, Weight: 5},
		{Item: ops.runNextPrefix, Weight: 5},
		{Item: ops.runPrev, Weight: 5},
	}.RandomDeck(randv1.New(randv1.NewSource(rng.Int64())))

	for i := 0; i < 1000; i++ {
		beforeCount := counter.Load()

		// nextOp returns a function that *may* run the operation. If the
		// current test state makes the operation an invalid operation, the the
		// function returns `false` indicating it was not run. If the operation
		// is a valid operation and was performed, `opFunc` returns true.
		//
		// This loop will run exactly 1 operation, skipping randomly chosen
		// operations that cannot be run on an iterator in its current state.
		for opFunc := nextOp(); !opFunc(); {
			opFunc = nextOp()
		}
		var ikey *base.InternalKey
		if ops.kv != nil {
			ikey = &ops.kv.K
		}
		t.Logf("%s = %s [err = %v]", ops.latestOpDesc, ikey, it.Error())
		afterCount := counter.Load()
		// TODO(jackson): Consider running all commands against a parallel
		// iterator constructed over a sstable containing the same data in a
		// standard construction (eg, typical block sizes) and no error
		// injection. Then we can assert the results are identical.

		if afterCount > beforeCount {
			if ops.kv != nil || it.Error() == nil {
				t.Errorf("error swallowed during %s with stack %s",
					ops.latestOpDesc, string(stack))
			}
		}
	}
}

type opRunner struct {
	randomTableConfig
	it Iterator

	latestOpDesc          string
	latestSeekKey         []byte
	dir                   int8
	kv                    *base.InternalKV
	lastOpWasSeekPrefixGE bool
}

func (r *opRunner) runSeekGE() bool {
	k := r.randKey()
	flags := base.SeekGEFlagsNone
	if strings.HasPrefix(r.latestOpDesc, "SeekGE") &&
		r.wopts.Comparer.Compare(k, r.latestSeekKey) > 0 && r.rng.IntN(2) == 1 {
		flags = flags.EnableTrySeekUsingNext()
	}
	r.latestOpDesc = fmt.Sprintf("SeekGE(%q, TrySeekUsingNext()=%t)",
		k, flags.TrySeekUsingNext())
	r.latestSeekKey = k

	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.SeekGE(k, base.SeekGEFlagsNone)
	r.dir = +1
	return true
}

func (r *opRunner) runSeekPrefixGE() bool {
	k := r.randKey()
	i := r.wopts.Comparer.Split(k)
	flags := base.SeekGEFlagsNone
	if strings.HasPrefix(r.latestOpDesc, "SeekPrefixGE") &&
		r.wopts.Comparer.Compare(k, r.latestSeekKey) > 0 && r.rng.IntN(2) == 1 {
		flags = flags.EnableTrySeekUsingNext()
	}
	r.latestOpDesc = fmt.Sprintf("SeekPrefixGE(%q, %q, TrySeekUsingNext()=%t)",
		k[:i], k, flags.TrySeekUsingNext())
	r.latestSeekKey = k

	r.lastOpWasSeekPrefixGE = true
	r.kv = r.it.SeekPrefixGE(k[:i], k, flags)
	r.dir = +1
	return true
}

func (r *opRunner) runSeekLT() bool {
	k := r.randKey()
	r.latestOpDesc = fmt.Sprintf("SeekLT(%q)", k)

	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.SeekLT(k, base.SeekLTFlagsNone)
	r.dir = -1
	return true
}

func (r *opRunner) runFirst() bool {
	r.latestOpDesc = "First()"

	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.First()
	r.dir = +1
	return true
}

func (r *opRunner) runLast() bool {
	r.latestOpDesc = "Last()"

	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.Last()
	r.dir = -1
	return true
}

func (r *opRunner) runNext() bool {
	if r.dir != -1 || r.kv == nil {
		return false
	}

	if r.lastOpWasSeekPrefixGE {
		return false
	}

	r.latestOpDesc = "Next()"
	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.Next()
	r.dir = +1
	return true
}

func (r *opRunner) runNextPrefix() bool {
	// NextPrefix cannot be called to change directions or when an iterator is
	// exhausted.
	if r.dir == -1 || r.kv == nil {
		return false
	}
	p := r.kv.K.UserKey[:r.wopts.Comparer.Split(r.kv.K.UserKey)]
	succKey := r.wopts.Comparer.ImmediateSuccessor(nil, p)
	r.latestOpDesc = fmt.Sprintf("NextPrefix(%q)", succKey)
	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.NextPrefix(succKey)
	r.dir = +1
	return true
}

func (r *opRunner) runPrev() bool {
	if r.dir == -1 && r.kv == nil {
		return false
	}
	r.latestOpDesc = "Prev()"
	r.lastOpWasSeekPrefixGE = false
	r.kv = r.it.Prev()
	r.dir = -1
	return true
}

type randomTableConfig struct {
	wopts     *WriterOptions
	keys      testkeys.Keyspace
	keyCount  int
	maxValLen int
	maxSuffix int64
	maxSeqNum int64
	rng       *rand.Rand
}

func (cfg *randomTableConfig) readerOpts() ReaderOptions {
	rOpts := ReaderOptions{
		Comparer:       testkeys.Comparer,
		FilterDecoders: []base.TableFilterDecoder{bloom.Decoder},
		KeySchemas:     map[string]*colblk.KeySchema{cfg.wopts.KeySchema.Name: cfg.wopts.KeySchema},
	}
	return rOpts
}

var testkeysSchema = colblk.DefaultKeySchema(testkeys.Comparer, 16 /* bundle size */)

func (cfg *randomTableConfig) randomize() {
	if cfg.wopts == nil {
		cfg.wopts = &WriterOptions{
			Comparer: testkeys.Comparer,
			// Test all table formats in [TableFormatLevelDB, TableFormatMax].
			TableFormat:             TableFormat(cfg.rng.IntN(int(TableFormatMax)) + 1),
			BlockRestartInterval:    (1 << cfg.rng.IntN(6)),             // {1, 2, 4, ..., 32}
			BlockSizeThreshold:      min(int(100*cfg.rng.Float64()), 1), // 1-100%
			BlockSize:               (1 << cfg.rng.IntN(18)),            // {1, 2, 4, ..., 128 KiB}
			IndexBlockSize:          (1 << cfg.rng.IntN(20)),            // {1, 2, 4, ..., 512 KiB}
			BlockPropertyCollectors: nil,
			KeySchema:               &testkeysSchema,
			WritingToLowestLevel:    cfg.rng.IntN(2) == 1,
		}
		if bitsPerKey := cfg.rng.Uint32N(11); bitsPerKey > 0 {
			cfg.wopts.FilterPolicy = bloom.FilterPolicy(bitsPerKey)
		}
		if cfg.wopts.TableFormat >= TableFormatPebblev1 && cfg.rng.Float64() < 0.75 {
			cfg.wopts.BlockPropertyCollectors = append(cfg.wopts.BlockPropertyCollectors, NewTestKeysBlockPropertyCollector)
		}
	}
	cfg.wopts.ensureDefaults()
	cfg.wopts.Comparer = testkeys.Comparer
}

func (cfg *randomTableConfig) randKey() []byte {
	return testkeys.KeyAt(cfg.keys, cfg.randKeyIdx(), cfg.randSuffix())
}
func (cfg *randomTableConfig) randSuffix() int64  { return cfg.rng.Int64N(cfg.maxSuffix + 1) }
func (cfg *randomTableConfig) randKeyIdx() uint64 { return cfg.rng.Uint64N(cfg.keys.Count()) }

func buildRandomSSTable(f vfs.File, cfg randomTableConfig) (*WriterMetadata, error) {
	// Construct a weighted distribution of key kinds.
	kinds := metamorphic.Weighted[base.InternalKeyKind]{
		{Item: base.InternalKeyKindSet, Weight: 25},
		{Item: base.InternalKeyKindSetWithDelete, Weight: 25},
		{Item: base.InternalKeyKindDelete, Weight: 5},
		{Item: base.InternalKeyKindSingleDelete, Weight: 2},
		{Item: base.InternalKeyKindMerge, Weight: 1},
	}
	// TODO(jackson): Support writing range deletions and range keys.
	// TestIterator_RandomErrors only reads through the point iterator, so those
	// keys won't be visible regardless, but their existence should be benign.

	// DELSIZED require Pebblev4 or later.
	if cfg.wopts.TableFormat >= TableFormatPebblev4 {
		kinds = append(kinds, metamorphic.ItemWeight[base.InternalKeyKind]{
			Item: base.InternalKeyKindDeleteSized, Weight: 5,
		})
	}
	nextRandomKind := kinds.RandomDeck(randv1.New(randv1.NewSource(cfg.rng.Int64())))

	type keyID struct {
		idx    uint64
		suffix int64
		seqNum base.SeqNum
	}
	keyMap := make(map[keyID]bool)
	// Constrain the space we generate keys to the middle 90% of the keyspace.
	// This helps exercise code paths that are only run when a seek key is
	// beyond or before all index block entries.
	sstKeys := testkeys.Slice(cfg.keys, cfg.keys.Count()/20, cfg.keys.Count()-cfg.keys.Count()/20)
	randomKey := func() keyID {
		k := keyID{
			idx:    cfg.rng.Uint64N(sstKeys.Count()),
			suffix: cfg.rng.Int64N(cfg.maxSuffix + 1),
			seqNum: base.SeqNum(cfg.rng.Int64N(cfg.maxSeqNum + 1)),
		}
		// If we've already generated this exact key, try again.
		for keyMap[k] {
			k = keyID{
				idx:    cfg.rng.Uint64N(sstKeys.Count()),
				suffix: cfg.rng.Int64N(cfg.maxSuffix + 1),
				seqNum: base.SeqNum(cfg.rng.Int64N(cfg.maxSeqNum + 1)),
			}
		}
		keyMap[k] = true
		return k
	}

	var alloc bytealloc.A
	keys := make([]base.InternalKey, cfg.keyCount)
	for i := range keys {
		keyID := randomKey()
		kind := nextRandomKind()

		var keyBuf []byte
		alloc, keyBuf = alloc.Alloc(testkeys.SuffixLen(keyID.suffix) + cfg.keys.MaxLen())
		n := testkeys.WriteKeyAt(keyBuf, sstKeys, keyID.idx, keyID.suffix)
		keys[i] = base.MakeInternalKey(keyBuf[:n], keyID.seqNum, kind)
	}
	// The Writer requires the keys to be written in sorted order. Sort them.
	slices.SortFunc(keys, func(a, b base.InternalKey) int {
		return base.InternalCompare(testkeys.Comparer.Compare, a, b)
	})

	// Release keyMap and alloc; we don't need them and this function can be
	// memory intensive.
	keyMap = nil
	alloc = nil

	valueBuf := make([]byte, cfg.maxValLen)
	w := NewRawWriter(objstorageprovider.NewFileWritable(f), *cfg.wopts)
	for i := 0; i < len(keys); i++ {
		var value []byte
		switch keys[i].Kind() {
		case base.InternalKeyKindSet, base.InternalKeyKindMerge:
			value = valueBuf[:cfg.rng.IntN(cfg.maxValLen+1)]
			for j := range value {
				value[j] = byte(cfg.rng.Uint32())
			}
		}
		if err := w.Add(keys[i], value, false /* forceObsolete */, base.KVMeta{}); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	metadata, err := w.Metadata()
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

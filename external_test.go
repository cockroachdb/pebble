// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble_test

import (
	"bytes"
	"io"
	randv1 "math/rand"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/metamorphic"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

// TestIteratorErrors is a randomized test designed to ensure that errors
// encountered by reads are properly propagated through to the user. It uses the
// metamorphic tests configured with only write operations to first generate a
// random database. It then uses the metamorphic tests to run a random set of
// read operations against the generated database, randomly injecting errors at
// the VFS layer. If an error is injected over the course of an operation, it
// expects the error to surface to the operation output. If it doesn't, the test
// fails.
func TestIteratorErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	// Generate a random database by running the metamorphic test with the
	// WriteOpConfig. We'll perform ~10,000 random operations that mutate the
	// state of the database.
	kf := metamorphic.TestkeysKeyFormat
	testOpts := metamorphic.RandomOptions(rng, kf, metamorphic.RandomOptionsCfg{})
	// With even a very small injection probability, it's relatively
	// unlikely that pebble.DebugCheckLevels will successfully complete
	// without being interrupted by an ErrInjected. Omit these checks.
	// TODO(jackson): Alternatively, we could wrap pebble.DebugCheckLevels,
	// mark the error value as having originated from CheckLevels, and retry
	// at most once. We would need to skip retrying on the second invocation
	// of DebugCheckLevels. It's all likely more trouble than it's worth.
	testOpts.Opts.DebugCheck = nil
	// The FS should be in-memory, so we don't need to worry about paths down
	// below.
	_ = vfs.Root(testOpts.Opts.FS).(*vfs.MemFS)

	{
		test, err := metamorphic.New(metamorphic.GenerateOps(
			rng, 10000, kf, metamorphic.WriteOpConfig()),
			testOpts, "" /* dir */, io.Discard)
		require.NoError(t, err)
		require.NoError(t, metamorphic.Execute(test))
	}
	t.Log("Constructed test database state")
	{
		testOpts.Opts.DisableTableStats = true
		testOpts.Opts.DisableAutomaticCompactions = true

		// Create an errorfs injector that injects ErrInjected on 5% of reads.
		// Wrap it in both a counter and a toggle so that we a) know whether an
		// error was injected over the course of an operation, and b) so that we
		// can disable error injection during Open.
		predicate := errorfs.And(
			errorfs.OpKindIn("ReadsExceptGetDiskUsage", errorfs.ReadOps.Minus(errorfs.OpGetDiskUsage)),
			errorfs.Randomly(0.50, seed),
		)
		counter := errorfs.Counter{Injector: errorfs.ErrInjected.If(predicate)}
		toggle := errorfs.Toggle{Injector: &counter}
		testOpts.Opts.FS = errorfs.Wrap(testOpts.Opts.FS, &toggle)
		testOpts.Opts.ReadOnly = true

		var logBuf bytes.Buffer
		defer func() {
			if t.Failed() {
				// TODO(radu): we don't close the db, which could be emitting a log
				// right now, causing a race here.
				t.Log(logBuf.String())
			}
		}()

		ops := metamorphic.GenerateOps(rng, 5000, metamorphic.TestkeysKeyFormat, metamorphic.ReadOpConfig())
		test, err := metamorphic.New(ops, testOpts, "" /* dir */, &logBuf)
		require.NoError(t, err)

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("last injected error: %+v", counter.LastError())
				panic(r)
			}
		}()

		// Begin injecting errors.
		toggle.On()

		prevCount := counter.Load()
		more := true
		for i := 0; more; i++ {
			var operationOutput string
			more, operationOutput, err = test.Step()
			// test.Step returns an error if the test called Fatalf. Error
			// injection should NOT trigger calls to Fatalf.
			if err != nil {
				t.Fatal(err)
			}
			newCount := counter.Load()
			if diff := newCount - prevCount; diff > 0 {
				if !strings.Contains(operationOutput, errorfs.ErrInjected.Error()) {
					t.Fatalf("Injected %d errors in op %d but the operation output %q does not contain the injected error: %+v",
						diff, i, operationOutput, counter.LastError())
				}
			}
			prevCount = newCount
		}
		t.Logf("Injected %d errors over the course of the test.", counter.Load())
	}
}

func BenchmarkPointLookupSeparatedValues(b *testing.B) {
	type config struct {
		name string
		buildSeparatedValuesDBOpts
	}
	configs := []config{
		{
			name: "keys=10m,valueLen=100",
			buildSeparatedValuesDBOpts: buildSeparatedValuesDBOpts{
				KeyCount: 10_000_000,
				ValueLen: 100,
			},
		},
		{
			name: "keys=10m,valueLen=1024",
			buildSeparatedValuesDBOpts: buildSeparatedValuesDBOpts{
				KeyCount: 10_000_000,
				ValueLen: 1024,
			},
		},
	}

	for _, c := range configs {
		b.Run(c.name, func(b *testing.B) {
			db, keys := buildSeparatedValuesDB(b, c.buildSeparatedValuesDBOpts)
			defer func() { require.NoError(b, db.Close()) }()
			m := db.Metrics()
			b.Logf("%s\n", m.String())

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				func() {
					iter, err := db.NewIter(nil)
					if err != nil {
						b.Fatal(err)
					}
					defer iter.Close()
					iter.SeekGE(keys[i%len(keys)])
					if !iter.Valid() {
						b.Fatal("no key found")
					}
					_, err = iter.ValueAndErr()
					if err != nil {
						b.Fatal(err)
					}
				}()
			}
		})
	}
}

type buildSeparatedValuesDBOpts struct {
	KeyCount int
	ValueLen int
}

func buildSeparatedValuesDB(
	tb testing.TB, opts buildSeparatedValuesDBOpts,
) (db *pebble.DB, keys [][]byte) {
	o := &pebble.Options{
		Comparer:                &cockroachkvs.Comparer,
		BlockPropertyCollectors: cockroachkvs.BlockPropertyCollectors,
		FormatMajorVersion:      pebble.FormatValueSeparation,
		FS:                      vfs.NewMem(),
		KeySchema:               cockroachkvs.KeySchema.Name,
		KeySchemas:              sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		MemTableSize:            2 << 20,
		L0CompactionThreshold:   2,
	}
	o.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           50,
			MaxBlobReferenceDepth: 10,
			RewriteMinimumAge:     15 * time.Minute,
		}
	}
	o.Levels[0].BlockSize = 32 << 10       // 32 KB
	o.Levels[0].IndexBlockSize = 256 << 10 // 256 KB
	o.Levels[0].FilterPolicy = bloom.FilterPolicy(10)
	db, err := pebble.Open("", o)
	require.NoError(tb, err)

	rng := rand.New(rand.NewPCG(0, uint64(time.Now().UnixNano())))
	keys, vals := cockroachkvs.RandomKVs(rng, opts.KeyCount, cockroachkvs.KeyGenConfig{
		PrefixAlphabetLen:  26,
		PrefixLenShared:    2,
		RoachKeyLen:        10,
		AvgKeysPerPrefix:   10,
		BaseWallTime:       uint64(time.Now().UnixNano()),
		PercentLogical:     0,
		PercentEmptySuffix: 0,
		PercentLockSuffix:  0,
	}, opts.ValueLen)

	keysToWrite := keys
	for len(keysToWrite) > 0 {
		b := db.NewBatch()
		n := min(len(keysToWrite), 100)
		for i := 0; i < n; i++ {
			require.NoError(tb, b.Set(keysToWrite[i], vals[i], nil))
		}
		require.NoError(tb, b.Commit(nil))
		keysToWrite = keysToWrite[n:]
	}
	require.NoError(tb, db.Flush())

	// Wait until compaction scores stabilize.
	for {
		m := db.Metrics()
		var maxScore float64
		for l := range m.Levels {
			if m.Levels[l].Score > maxScore {
				maxScore = m.Levels[l].Score
			}
		}
		if maxScore <= 1.0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return db, keys
}

// TestDoubleRestart checks that we are not in a precarious state immediately
// after restart. This could happen if we remove WALs before all necessary
// flushes are complete. Steps:
//
//  1. Use metamorphic to run operations on a store; the resulting filesystem is
//     cloned multiple times in subsequent steps.
//  2. Clone the FS and start a "golden" store; read all KVs that will be used
//     as the source of truth.
//  3. Independently clone the FS again and open a store; sleep for a small
//     random amount then close and reopen the store. Then read KVs and
//     cross-check against the "golden" KVs in step 2.
//  4. Repeat step 3 multiple times, each time with a new independent clone.
//
// This test was used to reproduce the failure in
// https://github.com/cockroachdb/cockroach/issues/148419.
func TestDoubleRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	kf := metamorphic.TestkeysKeyFormat
	testOpts := metamorphic.RandomOptions(rng, kf, metamorphic.RandomOptionsCfg{
		AlwaysStrictFS:  true,
		NoRemoteStorage: true,
	})
	metaFS := testOpts.Opts.FS.(*vfs.MemFS)

	var metaTestOutput bytes.Buffer
	{
		// Disable restart (it changes the FS internally and we don't have access to
		// the new one).
		opCfg := metamorphic.WriteOpConfig().WithOpWeight(metamorphic.OpDBRestart, 0)
		test, err := metamorphic.New(
			metamorphic.GenerateOps(rng, 2000, kf, opCfg),
			testOpts, "" /* dir */, &metaTestOutput,
		)
		require.NoError(t, err)
		require.NoError(t, metamorphic.Execute(test))
	}
	defer func() {
		if t.Failed() {
			t.Logf("Meta test output:\n%s", metaTestOutput.String())
		}
	}()

	// makeOpts creates the options for a db that starts from a clone of metaFS.
	makeOpts := func(logger base.Logger) *pebble.Options {
		opts := testOpts.Opts.Clone()

		// Make a copy of the metaFS.
		opts.FS = vfs.NewMem()
		ok, err := vfs.Clone(metaFS, opts.FS, "", "")
		require.NoError(t, err)
		require.True(t, ok)

		opts.FS = errorfs.Wrap(opts.FS, errorfs.RandomLatency(
			errorfs.Randomly(0.8, rng.Int64()),
			10*time.Microsecond,
			rng.Int64(),
			time.Millisecond,
		))

		if opts.WALFailover != nil {
			wf := *opts.WALFailover
			wf.Secondary.FS = opts.FS
			opts.WALFailover = &wf
		}
		opts.Logger = logger
		opts.LoggerAndTracer = nil
		lel := pebble.MakeLoggingEventListener(logger)
		opts.EventListener = &lel
		return opts
	}

	// Open the "golden" filesystem and scan it to get the expected KVs.
	goldenLog := &base.InMemLogger{}
	defer func() {
		if t.Failed() {
			t.Logf("Golden db logs:\n%s\n", goldenLog.String())
		}
	}()
	db, err := pebble.Open("", makeOpts(goldenLog))
	require.NoError(t, err)
	goldenKeys, goldenVals := getKVs(t, db)
	require.NoError(t, db.Close())

	// Repeatedly open and quickly close the database, verifying that we did not
	// lose any data during this restart (e.g. because we prematurely deleted
	// WALs).
	for iter := 0; iter < 10; iter++ {
		func() {
			dbLog := &base.InMemLogger{}
			defer func() {
				if t.Failed() {
					t.Logf("Db logs:\n%s\n", dbLog.String())
				}
			}()
			opts := makeOpts(dbLog)
			// Sometimes reduce the memtable size to trigger the large batch recovery
			// code path.
			if rng.IntN(2) == 0 {
				opts.MemTableSize = 1200
			}
			dbLog.Infof("Opening db\n")
			db, err := pebble.Open("", opts)
			require.NoError(t, err)
			if rng.IntN(2) == 0 {
				d := time.Duration(rng.IntN(1000)) * time.Microsecond
				dbLog.Infof("Sleeping %s", d)
				time.Sleep(d)
			}
			dbLog.Infof("Closing db")
			require.NoError(t, db.Close())

			dbLog.Infof("Reopening db")
			db, err = pebble.Open("", opts)
			require.NoError(t, err)
			dbLog.Infof("Checking KVs")
			checkKVs(t, db, opts.Comparer, goldenKeys, goldenVals)
			dbLog.Infof("Closing db")
			require.NoError(t, db.Close())
		}()
	}
}

// getKVs retrieves and returns all keys and values from the database in order.
func getKVs(t *testing.T, db *pebble.DB) (keys [][]byte, vals [][]byte) {
	t.Helper()
	it, err := db.NewIter(&pebble.IterOptions{})
	require.NoError(t, err)
	for valid := it.First(); valid; valid = it.Next() {
		keys = append(keys, slices.Clone(it.Key()))
		val, err := it.ValueAndErr()
		require.NoError(t, err)
		vals = append(vals, slices.Clone(val))
	}
	require.NoError(t, it.Close())
	return keys, vals
}

// checkKVs checks that the keys and values in the database match the expected ones.
func checkKVs(
	t *testing.T, db *pebble.DB, cmp *base.Comparer, expectedKeys [][]byte, expectedVals [][]byte,
) {
	keys, vals := getKVs(t, db)
	for i := 0; i < len(keys) || i < len(expectedKeys); i++ {
		if i < len(keys) && i < len(expectedKeys) && cmp.Equal(keys[i], expectedKeys[i]) {
			continue
		}
		if i < len(keys) && (i == len(expectedKeys) || cmp.Compare(keys[i], expectedKeys[i]) < 0) {
			t.Fatalf("extra key: %q\n", cmp.FormatKey(keys[i]))
		}
		t.Fatalf("missing key: %q\n", cmp.FormatKey(expectedKeys[i]))
	}
	for i := range vals {
		// require.Equalf by itself fails if one is nil and the other is a non-nil empty slice.
		if !bytes.Equal(vals[i], expectedVals[i]) {
			require.Equalf(t, expectedVals[i], vals[i], "key %q value msimatch", cmp.FormatKey(keys[i]))
		}
	}
}

func TestReadOnlyRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	// Generate a random database by running the metamorphic test with the
	// WriteOpConfig. We'll perform ~10,000 random operations that mutate the
	// state of the database.
	kf := metamorphic.TestkeysKeyFormat
	testOpts := metamorphic.RandomOptions(rng, kf, metamorphic.RandomOptionsCfg{
		AlwaysStrictFS:  true,
		NoRemoteStorage: true,
		NoWALFailover:   true,
	})

	// Run the metamorphic test grabbing a clone of the filesystem at a random
	// time.
	var cloneFS *vfs.MemFS
	{
		test, err := metamorphic.New(metamorphic.GenerateOps(
			rng, 10000, kf, metamorphic.WriteOpConfig()),
			testOpts, "" /* dir */, io.Discard)
		require.NoError(t, err)
		memFS := vfs.Root(testOpts.Opts.FS).(*vfs.MemFS)
		cloneFS = memFS

		for more := true; more; {
			more, _, err = test.Step()
			require.NoError(t, err)
			if rng.IntN(100) == 1 {
				cloneFS = memFS.CrashClone(vfs.CrashCloneCfg{
					UnsyncedDataPercent: 50, RNG: rng})
			}
		}
	}
	t.Log("Constructed test database state")

	// Re-open the cloned filesystem in read-only mode, asserting that the
	// filesystem is unchanged afterwards.
	before := cloneFS.String()
	opts := testOpts.Opts.Clone()
	opts.FS = cloneFS
	opts.ReadOnly = true
	db, err := pebble.Open("", opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	after := cloneFS.String()
	require.Equal(t, before, after)
}

func TestOptionsClone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	a := metamorphic.RandomOptions(rng, metamorphic.TestkeysKeyFormat, metamorphic.RandomOptionsCfg{}).Opts
	b := a.Clone()
	if rng.IntN(2) == 0 {
		a, b = b, a
	}
	before := a.String()
	mangle(reflect.ValueOf(b).Elem(), rng)
	after := a.String()
	require.Equal(t, before, after)
}

func mangle(v reflect.Value, rng *rand.Rand) {
	if !v.CanSet() {
		return
	}
	// Some of the time, generate a full new value.
	if rng.IntN(2) == 0 {
		genVal(v, rng)
		return
	}
	switch v.Type().Kind() {
	case reflect.Pointer:
		// If the pointer is to a type outside the pebble package, leave it alone;
		// Options.Clone() would never clone those objects.
		if v.Elem().CanSet() && v.Elem().Type().PkgPath() == reflect.TypeOf(pebble.Options{}).PkgPath() {
			mangle(v.Elem(), rng)
		}

	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			mangle(v.Field(i), rng)
		}

	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			mangle(v.Index(i), rng)
		}
	}
}

func genVal(v reflect.Value, rng *rand.Rand) {
	defer func() {
		if r := recover(); r != nil {
			// Ignore errors generating values (caused by unexported fields).
		}
	}()
	newVal, ok := quick.Value(v.Type(), randv1.New(randv1.NewSource(rng.Int64())))
	if ok {
		v.Set(newVal)
	}
}

// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble_test

import (
	"bytes"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/cockroachkvs"
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
	seed := time.Now().UnixNano()
	t.Logf("Using seed %d", seed)
	rng := rand.New(rand.NewPCG(0, uint64(seed)))

	// Generate a random database by running the metamorphic test with the
	// WriteOpConfig. We'll perform ~10,000 random operations that mutate the
	// state of the database.
	kf := metamorphic.TestkeysKeyFormat
	testOpts := metamorphic.RandomOptions(rng, kf, nil /* custom opt parsers */)
	// With even a very small injection probability, it's relatively
	// unlikely that pebble.DebugCheckLevels will successfully complete
	// without being interrupted by an ErrInjected. Omit these checks.
	// TODO(jackson): Alternatively, we could wrap pebble.DebugCheckLevels,
	// mark the error value as having originated from CheckLevels, and retry
	// at most once. We would need to skip retrying on the second invocation
	// of DebugCheckLevels. It's all likely more trouble than it's worth.
	testOpts.Opts.DebugCheck = nil
	// Disable the physical FS so we don't need to worry about paths down below.
	if fs := testOpts.Opts.FS; fs == nil || fs == vfs.Default {
		testOpts.Opts.FS = vfs.NewMem()
	}

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
		predicate := errorfs.And(errorfs.Reads, errorfs.Randomly(0.50, seed))
		counter := errorfs.Counter{Injector: errorfs.ErrInjected.If(predicate)}
		toggle := errorfs.Toggle{Injector: &counter}
		testOpts.Opts.FS = errorfs.Wrap(testOpts.Opts.FS, &toggle)
		testOpts.Opts.ReadOnly = true

		test, err := metamorphic.New(
			metamorphic.GenerateOps(rng, 5000, metamorphic.TestkeysKeyFormat, metamorphic.ReadOpConfig()),
			testOpts, "" /* dir */, &testWriter{t: t})
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

type testWriter struct {
	t *testing.T
}

func (w *testWriter) Write(b []byte) (int, error) {
	w.t.Log(string(bytes.TrimSpace(b)))
	return len(b), nil
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
		FormatMajorVersion:      pebble.FormatExperimentalValueSeparation,
		FS:                      vfs.NewMem(),
		KeySchema:               cockroachkvs.KeySchema.Name,
		KeySchemas:              sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
		Levels:                  make([]pebble.LevelOptions, 7),
		MemTableSize:            2 << 20,
		L0CompactionThreshold:   2,
	}
	o.Experimental.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:               true,
			MinimumSize:           50,
			MaxBlobReferenceDepth: 10,
		}
	}
	for i := 0; i < len(o.Levels); i++ {
		l := &o.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = o.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
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

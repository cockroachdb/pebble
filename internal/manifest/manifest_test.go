// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest_test

import (
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/buildtags"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/metamorphic"
	"github.com/cockroachdb/pebble/record"
	"github.com/stretchr/testify/require"
)

// TestInuseKeyRangesRandomized is a randomized test that generates a random
// database (using the Pebble metamorphic tests), and then tests calculating
// in-use key ranges for random spans. It asserts that all files that overlap
// the span are contained within the resulting list of key ranges.
func TestInuseKeyRangesRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(0, seed))

	// Generate a random database by running the metamorphic test.
	testOpts := metamorphic.RandomOptions(rng, metamorphic.TestkeysKeyFormat, nil /* custom opt parsers */)
	{
		nOps := 10000
		if buildtags.SlowBuild {
			// Reduce the number of operations in instrumented builds (the test can
			// time out if we are unlucky).
			nOps = 2000
		}
		ops := metamorphic.GenerateOps(rng, uint64(nOps), metamorphic.TestkeysKeyFormat, metamorphic.WriteOpConfig())
		test, err := metamorphic.New(ops, testOpts, "" /* dir */, io.Discard)
		require.NoError(t, err)
		require.NoError(t, metamorphic.Execute(test))
	}
	t.Log("Constructed test database state")
	v := replayManifest(t, testOpts.Opts, "")
	t.Log(v.DebugString())

	const maxKeyLen = 12
	const maxSuffix = 20
	ks := testkeys.Alpha(maxKeyLen)
	smallest := make([]byte, maxKeyLen+testkeys.MaxSuffixLen)
	largest := make([]byte, maxKeyLen+testkeys.MaxSuffixLen)
	for i := 0; i < 1000; i++ {
		n := testkeys.WriteKeyAt(smallest[:cap(smallest)], ks, rng.Int64N(ks.Count()), rng.Int64N(maxSuffix))
		smallest = smallest[:n]
		n = testkeys.WriteKeyAt(largest[:cap(largest)], ks, rng.Int64N(ks.Count()), rng.Int64N(maxSuffix))
		largest = largest[:n]
		if testOpts.Opts.Comparer.Compare(smallest, largest) > 0 {
			smallest, largest = largest, smallest
		}

		level := rng.IntN(manifest.NumLevels)
		cmp := testOpts.Opts.Comparer.Compare
		keyRanges := v.CalculateInuseKeyRanges(level, manifest.NumLevels-1, smallest, largest)
		t.Logf("%d: [%s, %s] levels L%d-L6: ", i, smallest, largest, level)
		for _, kr := range keyRanges {
			t.Logf("  %s", kr.String())
		}

		for l := level; l < manifest.NumLevels; l++ {
			o := v.Overlaps(l, base.UserKeyBoundsInclusive(smallest, largest))
			iter := o.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				// CalculateInuseKeyRanges only guarantees that it returns key
				// ranges covering in-use ranges within [smallest, largest]. If
				// this file extends before or after smallest/largest, truncate
				// it to be within [smallest, largest] for the purpose of
				// correctness checking.
				b := f.UserKeyBounds()
				if cmp(b.Start, smallest) < 0 {
					b.Start = smallest
				}
				if b.End.IsUpperBoundFor(cmp, largest) {
					b.End = base.UserKeyInclusive(largest)
				}

				var containedWithin bool
				for _, kr := range keyRanges {
					containedWithin = containedWithin || kr.ContainsBounds(cmp, &b)
				}
				if !containedWithin {
					t.Fatalf("file L%d.%s with bounds %s overlaps [%s, %s] but no in-use key range contains it",
						l, f, b, smallest, largest)
				}
			}
		}
	}
}

func replayManifest(t *testing.T, opts *pebble.Options, dirname string) *manifest.Version {
	desc, err := pebble.Peek(dirname, opts.FS)
	require.NoError(t, err)
	if !desc.Exists {
		t.Fatal(oserror.ErrNotExist)
	}

	// Replay the manifest to get the current version.
	f, err := opts.FS.Open(desc.ManifestFilename)
	require.NoError(t, err)
	defer f.Close()

	cmp := opts.Comparer
	var bve manifest.BulkVersionEdit
	bve.AddedTablesByFileNum = make(map[base.FileNum]*manifest.TableMetadata)
	rr := record.NewReader(f, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var ve manifest.VersionEdit
		require.NoError(t, ve.Decode(r))
		require.NoError(t, bve.Accumulate(&ve))
	}
	v, err := bve.Apply(
		nil /* version */, cmp, opts.FlushSplitBytes,
		opts.Experimental.ReadCompactionRate)
	require.NoError(t, err)
	return v
}

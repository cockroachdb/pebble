// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest_test

import (
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/metamorphic"
	"github.com/cockroachdb/pebble/record"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

// TestInuseKeyRangesRandomized is a randomized test that generates a random
// database (using the Pebble metamorphic tests), and then tests calculating
// in-use key ranges for random spans. It asserts that all files that overlap
// the span are contained within the resulting list of key ranges.
func TestInuseKeyRangesRandomized(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	// Generate a random database by running the metamorphic test.
	testOpts := metamorphic.RandomOptions(rng, nil /* custom opt parsers */)
	testOpts.Opts.Cache.Ref()
	{
		ops := metamorphic.GenerateOps(rng, 10000, metamorphic.WriteOpConfig())
		test, err := metamorphic.New(ops, testOpts, "" /* dir */, io.Discard)
		require.NoError(t, err)
		require.NoError(t, metamorphic.Execute(test))
	}
	t.Log("Constructed test database state")
	v := replayManifest(t, testOpts.Opts, "")
	t.Log(v.DebugString(base.DefaultFormatter))

	const maxKeyLen = 12
	const maxSuffix = 20
	ks := testkeys.Alpha(maxKeyLen)
	smallest := make([]byte, maxKeyLen+testkeys.MaxSuffixLen)
	largest := make([]byte, maxKeyLen+testkeys.MaxSuffixLen)
	for i := 0; i < 1000; i++ {
		n := testkeys.WriteKeyAt(smallest[:cap(smallest)], ks, rng.Int63n(ks.Count()), rng.Int63n(maxSuffix))
		smallest = smallest[:n]
		n = testkeys.WriteKeyAt(largest[:cap(largest)], ks, rng.Int63n(ks.Count()), rng.Int63n(maxSuffix))
		largest = largest[:n]
		if testOpts.Opts.Comparer.Compare(smallest, largest) > 0 {
			smallest, largest = largest, smallest
		}

		level := rng.Intn(manifest.NumLevels)
		cmp := testOpts.Opts.Comparer.Compare
		keyRanges := v.CalculateInuseKeyRanges(cmp, level, manifest.NumLevels-1, smallest, largest)
		t.Logf("%d: [%s, %s] levels L%d-L6: ", i, smallest, largest, level)
		for _, kr := range keyRanges {
			t.Logf("  [%s,%s]", kr.Start, kr.End)
		}

		for l := level; l < manifest.NumLevels; l++ {
			o := v.Overlaps(l, cmp, smallest, largest, false /* exclusiveEnd */)
			iter := o.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				// CalculateInuseKeyRanges only guarantees that it returns key
				// ranges covering in-use ranges within [smallest, largest]. If
				// this file extends before or after smallest/largest, truncate
				// it to be within [smallest,largest] for the purpose of
				// correctness checking.
				fileSmallest := f.Smallest.UserKey
				fileLargest := f.Largest.UserKey
				if cmp(fileSmallest, smallest) < 0 {
					fileSmallest = smallest
				}
				if cmp(fileLargest, largest) > 0 {
					fileLargest = largest
				}

				var containedWithin bool
				for _, kr := range keyRanges {
					containedWithin = containedWithin || (cmp(fileSmallest, kr.Start) >= 0 && cmp(fileLargest, kr.End) <= 0)
				}
				if !containedWithin {
					t.Fatalf("file L%d.%s overlaps [%s, %s] but no in-use key range contains it",
						l, f, smallest, largest)
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
	bve.AddedByFileNum = make(map[base.FileNum]*manifest.FileMetadata)
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
		nil /* version */, cmp.Compare, base.DefaultFormatter, opts.FlushSplitBytes,
		opts.Experimental.ReadCompactionRate, nil /* zombies */)
	require.NoError(t, err)
	return v
}

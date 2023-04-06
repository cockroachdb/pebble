// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func TestSetupInitialState(t *testing.T) {
	// Construct a small database in the test's TempDir.
	initialStatePath := t.TempDir()
	initialDataPath := vfs.Default.PathJoin(initialStatePath, "data")
	{
		d, err := pebble.Open(initialDataPath, &pebble.Options{})
		require.NoError(t, err)
		const maxKeyLen = 2
		ks := testkeys.Alpha(maxKeyLen)
		var key [maxKeyLen]byte
		for i := 0; i < ks.Count(); i++ {
			n := testkeys.WriteKey(key[:], ks, i)
			require.NoError(t, d.Set(key[:n], key[:n], pebble.NoSync))
			if i%100 == 0 {
				require.NoError(t, d.Flush())
			}
		}
		require.NoError(t, d.Close())
	}
	ls, err := vfs.Default.List(initialStatePath)
	require.NoError(t, err)

	// setupInitialState with an initial state path set to the test's TempDir
	// should populate opts.opts.FS with the directory's contents.
	opts := &testOptions{
		opts:             defaultOptions(),
		initialStatePath: initialStatePath,
		initialStateDesc: "test",
	}
	require.NoError(t, setupInitialState("data", opts))
	copied, err := opts.opts.FS.List("")
	require.NoError(t, err)
	require.ElementsMatch(t, ls, copied)
}

func TestOptionsRoundtrip(t *testing.T) {
	// Some fields mut be ignored to avoid spurious diffs.
	ignorePrefixes := []string{
		// Pointers
		"Cache:",
		"Cache.",
		"FS:",
		"TableCache:",
		// Function pointers
		"EventListener:",
		"MaxConcurrentCompactions:",
		"Experimental.EnableValueBlocks:",
		"Experimental.DisableIngestAsFlushable:",
		// Floating points
		"Experimental.PointTombstoneWeight:",
	}

	// Ensure that we unref any caches created, so invariants builds don't
	// complain about the leaked ref counts.
	maybeUnref := func(o *testOptions) {
		if o.opts.Cache != nil {
			o.opts.Cache.Unref()
		}
	}

	checkOptions := func(t *testing.T, o *testOptions) {
		s := optionsToString(o)
		parsed := defaultTestOptions()
		require.NoError(t, parseOptions(parsed, s))
		maybeUnref(parsed)
		got := optionsToString(parsed)
		require.Equal(t, s, got)

		// In some options, the closure obscures the underlying value. Check
		// that the return values are equal.
		require.Equal(t, o.opts.Experimental.EnableValueBlocks == nil, parsed.opts.Experimental.EnableValueBlocks == nil)
		if o.opts.Experimental.EnableValueBlocks != nil {
			require.Equal(t, o.opts.Experimental.EnableValueBlocks(), parsed.opts.Experimental.EnableValueBlocks())
		}
		require.Equal(t, o.opts.Experimental.DisableIngestAsFlushable == nil, parsed.opts.Experimental.DisableIngestAsFlushable == nil)
		if o.opts.Experimental.DisableIngestAsFlushable != nil {
			require.Equal(t, o.opts.Experimental.DisableIngestAsFlushable(), parsed.opts.Experimental.DisableIngestAsFlushable())
		}
		require.Equal(t, o.opts.MaxConcurrentCompactions(), parsed.opts.MaxConcurrentCompactions())

		diff := pretty.Diff(o.opts, parsed.opts)
		cleaned := diff[:0]
		for _, d := range diff {
			var ignored bool
			for _, prefix := range ignorePrefixes {
				if strings.HasPrefix(d, prefix) {
					ignored = true
					break
				}
			}
			if !ignored {
				cleaned = append(cleaned, d)
			}
		}
		require.Equal(t, diff[:0], cleaned)
	}

	standard := standardOptions()
	for i := range standard {
		t.Run(fmt.Sprintf("standard-%03d", i), func(t *testing.T) {
			checkOptions(t, standard[i])
			maybeUnref(standard[i])
		})
	}
	rng := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("random-%03d", i), func(t *testing.T) {
			o := randomOptions(rng)
			checkOptions(t, o)
			maybeUnref(o)
		})
	}
}

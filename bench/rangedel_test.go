// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRangeDelInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*RangeDelConfig)
	}{
		{"no-workers", func(c *RangeDelConfig) { c.Readers, c.Writers = 0, 0 }},
		{"unknown-profile", func(c *RangeDelConfig) { c.DBOptions = "unknown" }},
		{"unknown-shape", func(c *RangeDelConfig) { c.Shape = "unknown" }},
		{"iterator-fraction", func(c *RangeDelConfig) { c.IterFrac = 2 }},
		{"cycle-queues", func(c *RangeDelConfig) { c.Queues++ }},
		{"unique-overlap", func(c *RangeDelConfig) { c.Shape, c.OverlapFrac = rangeDelShapeUnique, 2 }},
		{"unique-batch", func(c *RangeDelConfig) { c.Shape, c.RangeDelsPerBatch = rangeDelShapeUnique, 0 }},
		{"unique-queues", func(c *RangeDelConfig) { c.Shape, c.Queues = rangeDelShapeUnique, 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultRangeDelConfig()
			tc.edit(&cfg)
			require.Error(t, RunRangeDel(t.TempDir(), &CommonConfig{}, &cfg))
		})
	}
}

func TestRangeDelOptions(t *testing.T) {
	defaults := &pebble.Options{}
	defaults.EnsureDefaults()
	var openedOptions *pebble.Options
	hookCalls := 0
	common := CommonConfig{
		CacheSize: 1 << 20,
		Logger:    testutils.Logger{T: t},
		OptionsHook: func(opts *pebble.Options) {
			hookCalls++
			opts.FS = vfs.NewMem()
			opts.MemTableSize = 8 << 20
			openedOptions = opts
		},
	}
	for _, profile := range []string{dbOptionsProdWriteHeavy, dbOptionsDefault} {
		t.Run(profile, func(t *testing.T) {
			cfg := DefaultRangeDelConfig()
			cfg.DBOptions = profile
			c := rangeDelCommonConfig(&common, &cfg)
			callsBefore := hookCalls
			d := NewPebbleDB("db", &c)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			require.Equal(t, callsBefore+1, hookCalls)
			require.Equal(t, &cockroachkvs.Comparer, openedOptions.Comparer)
			require.Equal(t, cockroachkvs.KeySchema.Name, openedOptions.KeySchema)
			if profile == dbOptionsProdWriteHeavy {
				require.Equal(t, uint64(256<<20), openedOptions.MemTableSize)
				require.Equal(t, int64(512<<20), openedOptions.LBaseMaxBytes)
				require.Equal(t, 10*time.Second, openedOptions.FlushDelayDeleteRange)
				require.Equal(t, 10*time.Second, openedOptions.FlushDelayRangeKey)
				require.Equal(t, int64(10<<20), openedOptions.MaxManifestFileSize)
				require.Equal(t, 2, openedOptions.L0CompactionConcurrency)
				require.Equal(t, pebble.FormatIngestBlobFiles, openedOptions.FormatMajorVersion)
				require.Equal(t, defaults.ValueSeparationPolicy(), openedOptions.ValueSeparationPolicy())
				require.Equal(t, 0.4, openedOptions.CompactionGarbageFractionForMaxConcurrency())
				for i := range openedOptions.Levels {
					require.Equal(t, 32<<10, openedOptions.Levels[i].BlockSize)
					require.Equal(t, 256<<10, openedOptions.Levels[i].IndexBlockSize)
					require.Equal(t, bloom.FilterPolicy(10), openedOptions.Levels[i].TableFilterPolicy())
					require.Equal(t, pebble.DBCompressionFastest.Levels[i], openedOptions.Levels[i].Compression())
				}
			} else {
				require.Equal(t, uint64(8<<20), openedOptions.MemTableSize)
				require.Zero(t, openedOptions.FlushDelayDeleteRange)
				require.Equal(t, -1.0, openedOptions.CompactionGarbageFractionForMaxConcurrency())
				require.Equal(t, 512, openedOptions.ValueSeparationPolicy().MinimumSize)
			}
		})
	}
	probe := &pebble.Options{}
	common.OptionsHook(probe)
	require.Equal(t, uint64(8<<20), probe.MemTableSize)
}

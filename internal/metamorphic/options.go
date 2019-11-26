// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
)

var comparer = func() pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Split = func(a []byte) int {
		return len(a)
	}
	return c
}()

func parseOptions(opts *pebble.Options, data string) error {
	hooks := &pebble.ParseHooks{
		NewFilterPolicy: func(name string) (pebble.FilterPolicy, error) {
			if name == "none" {
				return nil, nil
			}
			return bloom.FilterPolicy(10), nil
		},
	}
	return opts.Parse(data, hooks)
}

func defaultOptions() *pebble.Options {
	opts := &pebble.Options{
		Comparer: &comparer,
		FS:       vfs.NewMem(),
		Levels: []pebble.LevelOptions{{
			FilterPolicy: bloom.FilterPolicy(10),
		}},
	}
	opts.EnsureDefaults()
	return opts
}

func standardOptions() []*pebble.Options {
	// The index labels are not strictly necessary, but they make it easier to
	// find which options correspond to a failure.
	stdOpts := []string{
		0: "", // default options
		1: `
[Options]
  cache_size=1
`,
		2: `
[Options]
  disable_wal=true
`,
		3: `
[Options]
  l0_compaction_threshold=1
`,
		4: `
[Options]
  l0_compaction_threshold=1
  l0_stop_writes_threshold=1
`,
		5: `
[Options]
  lbase_max_bytes=1
`,
		6: `
[Options]
  max_manifest_file_size=1
`,
		7: `
[Options]
  max_open_files=1
`,
		8: `
[Options]
  mem_table_size=1000
`,
		9: `
[Options]
  mem_table_stop_writes_threshold=2
`,
		10: `
[Options]
  wal_dir=wal
`,
		11: `
[Level "0"]
  block_restart_interval=1
`,
		12: `
[Level "0"]
  block_size=1
`,
		13: `
[Level "0"]
  compression=NoCompression
`,
		14: `
[Level "0"]
  compression=Snappy
`,
		15: `
[Level "0"]
  index_block_size=1
`,
		16: `
[Level "0"]
  index_block_size=1
`,
		17: `
[Level "0"]
  target_file_size=1
`,
		18: `
[Level "0"]
  filter_policy=none
`,
	}

	opts := make([]*pebble.Options, len(stdOpts))
	for i := range opts {
		opts[i] = defaultOptions()
		if err := parseOptions(opts[i], stdOpts[i]); err != nil {
			panic(err)
		}
	}
	return opts
}

func randomOptions(rng *rand.Rand) *pebble.Options {
	opts := defaultOptions()
	opts.Cache = cache.New(1 << uint(rng.Intn(30))) // 1B - 1GB
	opts.DisableWAL = rng.Intn(2) == 0
	opts.L0CompactionThreshold = 1 + rng.Intn(100) // 1 - 100
	opts.L0StopWritesThreshold = 1 + rng.Intn(100) // 1 - 100
	if opts.L0StopWritesThreshold < opts.L0CompactionThreshold {
		opts.L0StopWritesThreshold = opts.L0CompactionThreshold
	}
	opts.LBaseMaxBytes = 1 << uint(rng.Intn(30))       // 1B - 1GB
	opts.MaxManifestFileSize = 1 << uint(rng.Intn(30)) // 1B  - 1GB
	opts.MemTableSize = 1 << (10 + uint(rng.Intn(17))) // 1KB - 256MB
	opts.MemTableStopWritesThreshold = 2 + rng.Intn(5) // 2 - 5
	if rng.Intn(2) == 0 {
		opts.WALDir = "wal"
	}
	var lopts pebble.LevelOptions
	lopts.BlockRestartInterval = 1 + rng.Intn(64)  // 1 - 64
	lopts.BlockSize = 1 << uint(rng.Intn(24))      // 1 - 16MB
	lopts.BlockSizeThreshold = 50 + rng.Intn(50)   // 50 - 100
	lopts.IndexBlockSize = 1 << uint(rng.Intn(24)) // 1 - 16MB
	lopts.TargetFileSize = 1 << uint(rng.Intn(28)) // 1 - 256MB
	opts.Levels = []pebble.LevelOptions{lopts}
	return opts
}

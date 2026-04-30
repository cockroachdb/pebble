// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/randvar"
)

// BenchmarkYCSB mirrors the YCSB roachtest in
// pkg/cmd/roachtest/tests/pebble_ycsb.go but runs in-process so the I/O paths
// can be profiled with the standard Go profilers. For each value size, it
// builds a 10M-key fixture once (cached on disk and reused across runs) and
// then takes a hard-link Checkpoint per workload so each workload starts from
// the same prepopulated state.
//
// Run example:
//
//	go test -tags invariants -run=^$ -bench=BenchmarkYCSB \
//	    ./bench -timeout=0 -benchtime=1000000x
//
// b.N controls the number of workload operations per sub-benchmark.
func BenchmarkYCSB(b *testing.B) {
	initialKeys := *ycsbBenchInitialKeys
	const fixtureCacheBytes = 4 << 30

	rootDir := *ycsbBenchFixtureDir
	formatVer := int(pebble.FormatNewest)

	for _, size := range ycsbBenchSizes {
		b.Run(fmt.Sprintf("values=%d", size), func(b *testing.B) {
			fixtureDir := filepath.Join(rootDir,
				fmt.Sprintf("values=%d", size),
				fmt.Sprintf("keys=%d", initialKeys),
				fmt.Sprintf("format=%d", formatVer),
				"fixture")

			if err := ensureYCSBFixture(b, fixtureDir, size, initialKeys, fixtureCacheBytes); err != nil {
				b.Fatal(err)
			}

			for _, workload := range []string{"A", "B", "C", "D", "E", "F"} {
				b.Run(workload, func(b *testing.B) {
					runYCSBWorkload(b, fixtureDir, workload, size, initialKeys)
				})
			}
		})
	}
}

var ycsbBenchSizes = []int{64, 1024}

var ycsbBenchFixtureDir = flag.String("ycsb-bench-fixture-dir", defaultYCSBFixtureDir(),
	"directory in which YCSB benchmark fixtures are cached")

var ycsbBenchInitialKeys = flag.Int("ycsb-bench-initial-keys", 10_000_000,
	"number of keys in the YCSB benchmark fixture")

func defaultYCSBFixtureDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "pebble-bench", "ycsb")
	}
	return filepath.Join(home, ".cache", "pebble-bench", "ycsb")
}

// ensureYCSBFixture builds the prepopulated fixture at fixtureDir if a
// `<fixtureDir>.ready` marker is missing. The marker is written only after
// the load phase finishes cleanly so a half-built fixture from a killed run
// is not silently reused.
func ensureYCSBFixture(
	b *testing.B, fixtureDir string, valueSize, initialKeys int, cacheBytes int64,
) error {
	marker := fixtureDir + ".ready"
	if _, err := os.Stat(marker); err == nil {
		b.Logf("reusing YCSB fixture at %s", fixtureDir)
		return nil
	}
	b.Logf("building YCSB fixture at %s (values=%d, initial-keys=%d) — this may take a while",
		fixtureDir, valueSize, initialKeys)

	if err := os.RemoveAll(fixtureDir); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(fixtureDir), 0o755); err != nil {
		return err
	}

	// Mirror the roachtest load phase with --concurrency=1 and the requested
	// value size. Workload weights are unused by loadInitial; pick the default.
	common := &CommonConfig{
		CacheSize:   cacheBytes,
		Concurrency: 1,
	}
	cfg := DefaultYCSBConfig()
	cfg.InitialKeys = initialKeys
	cfg.Values = randvar.NewBytesFlag(fmt.Sprintf("%d", valueSize))

	weights, err := ycsbParseWorkload(cfg.Workload)
	if err != nil {
		return err
	}
	keyDist, err := ycsbParseKeyDist(cfg.Keys, &cfg)
	if err != nil {
		return err
	}

	db := NewPebbleDB(fixtureDir, common).(pebbleDB)
	y := newYcsb(common, &cfg, weights, keyDist, cfg.Batch, cfg.Scans, cfg.Values)
	y.loadInitial(db)

	if err := db.d.Close(); err != nil {
		return err
	}

	f, err := os.Create(marker)
	if err != nil {
		return err
	}
	return f.Close()
}

// runYCSBWorkload checkpoints the fixture into b.TempDir(), opens the
// checkpoint, and runs the requested workload for b.N operations.
func runYCSBWorkload(b *testing.B, fixtureDir, workload string, valueSize, initialKeys int) {
	common := &CommonConfig{
		CacheSize:   4 << 30,
		Concurrency: 256,
		DisableWAL:  false,
	}
	cfg := DefaultYCSBConfig()
	cfg.Workload = workload
	cfg.Keys = "zipf"
	if workload == "D" {
		cfg.Keys = "uniform"
	}
	cfg.InitialKeys = 0
	cfg.PrepopulatedKeys = initialKeys
	// Cap operations at b.N. The workers exit when y.numOps >= cfg.NumOps.
	cfg.NumOps = uint64(b.N)
	cfg.Values = randvar.NewBytesFlag(fmt.Sprintf("%d", valueSize))

	// Open the fixture with compactions disabled and checkpoint it. The
	// checkpoint hard-links sstables so this is effectively free in time and
	// disk; disabling compactions keeps the cached fixture from being mutated
	// between runs.
	srcCfg := *common
	srcCfg.DisableAutoCompactions = true
	srcDB := NewPebbleDB(fixtureDir, &srcCfg).(pebbleDB)
	ckptDir := filepath.Join(b.TempDir(), "ckpt")
	ckptErr := srcDB.d.Checkpoint(ckptDir)
	if closeErr := srcDB.d.Close(); ckptErr == nil {
		ckptErr = closeErr
	}
	if ckptErr != nil {
		b.Fatal(ckptErr)
	}

	db := NewPebbleDB(ckptDir, common)
	defer func() {
		_ = db.(pebbleDB).d.Close()
	}()

	weights, err := ycsbParseWorkload(cfg.Workload)
	if err != nil {
		b.Fatal(err)
	}
	keyDist, err := ycsbParseKeyDist(cfg.Keys, &cfg)
	if err != nil {
		b.Fatal(err)
	}

	y := newYcsb(common, &cfg, weights, keyDist, cfg.Batch, cfg.Scans, cfg.Values)

	var wg sync.WaitGroup
	b.ResetTimer()
	y.init(db, &wg)
	wg.Wait()
	b.StopTimer()
}

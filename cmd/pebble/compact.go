// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
	"github.com/cockroachdb/pebble/internal/replay"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

var compactRunConfig struct {
	pacing float64
}

var compactCmd = &cobra.Command{
	Use:   "compact",
	Short: "compaction benchmarks",
}

var compactRunCmd = &cobra.Command{
	Use:   "run <workload dir>",
	Short: "run a compaction benchmark through ingesting sstables",
	Args:  cobra.ExactArgs(1),
	RunE:  runReplay,
}

func init() {
	compactRunCmd.Flags().Float64Var(&compactRunConfig.pacing,
		"pacing", 1.1, "pacing factor (relative to workload's L0 size)")
}

const numLevels = 7

type levelSizes [numLevels]int64

func (ls *levelSizes) add(o levelSizes) {
	for i, sz := range o {
		ls[i] += sz
	}
}

func (ls levelSizes) sum() int64 {
	var total int64
	for _, v := range ls {
		total += v
	}
	return total
}

type compactionEvent struct {
	levelSizes
	jobID int
}

type compactionTracker struct {
	mu         sync.Mutex
	activeJobs map[int]bool
	waiter     chan struct{}
}

func (t *compactionTracker) countActive() (int, chan struct{}) {
	ch := make(chan struct{})
	t.mu.Lock()
	count := len(t.activeJobs)
	t.waiter = ch
	t.mu.Unlock()
	return count, ch
}

func (t *compactionTracker) apply(jobID int) {
	t.mu.Lock()
	if t.activeJobs[jobID] {
		delete(t.activeJobs, jobID)
	} else {
		t.activeJobs[jobID] = true
	}
	if t.waiter != nil {
		close(t.waiter)
		t.waiter = nil
	}
	t.mu.Unlock()
}

// eventListener returns a Pebble event listener that listens for compaction
// events and appends them to the queue.
func (t *compactionTracker) eventListener() pebble.EventListener {
	return pebble.EventListener{
		CompactionBegin: func(info pebble.CompactionInfo) {
			t.apply(info.JobID)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			t.apply(info.JobID)
		},
	}
}

func open(dir string, listener pebble.EventListener) (*replay.DB, error) {
	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		Comparer:                    mvccComparer,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    2,
		MinCompactionRate:           4 << 20, // 4 MB/s
		MinFlushRate:                1 << 20, // 1 MB/s
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       400,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10,
		}},
		Merger: fauxMVCCMerger,
	}
	opts.EnsureDefaults()

	opts.EventListener = listener
	if verbose {
		opts.EventListener = teeEventListener(opts.EventListener,
			pebble.MakeLoggingEventListener(nil))
	}
	rd, err := replay.Open(dir, opts)
	return rd, err
}

// hardLinkWorkload makes a hardlink of every manifest and sstable file from
// src inside dst. Pebble will remove ingested sstables from their original
// path.  Making a hardlink of all the sstables ensures that we can run the
// same workload multiple times.
func hardLinkWorkload(src, dst string) error {
	ls, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}
	for _, info := range ls {
		typ, _, ok := base.ParseFilename(vfs.Default, info.Name())
		if !ok || (typ != base.FileTypeManifest && typ != base.FileTypeTable) {
			continue
		}
		err := os.Link(filepath.Join(src, info.Name()), filepath.Join(dst, info.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func runReplay(cmd *cobra.Command, args []string) error {
	// Make hard links of all the files in the workload so ingestion doesn't
	// delete our original copy of a workload.
	workloadDir, err := ioutil.TempDir(args[0], "pebble-bench-workload")
	if err != nil {
		return err
	}
	defer removeAll(workloadDir)
	if err := hardLinkWorkload(args[0], workloadDir); err != nil {
		return err
	}

	_, hist, err := replayManifests(workloadDir)
	if err != nil {
		return err
	}
	var workloadSize int64
	var workloadTableCount int
	for _, li := range hist {
		if !li.compaction {
			for _, f := range li.added {
				workloadSize += int64(f.meta.Size)
				workloadTableCount++
			}
		}
	}
	verbosef("Workload contains %s across %d sstables to replay.\n",
		humanize.Int64(workloadSize), workloadTableCount)
	if workloadTableCount == 0 {
		return errors.New("empty workload")
	}

	dir, err := ioutil.TempDir(args[0], "pebble-bench-data")
	if err != nil {
		return err
	}
	defer removeAll(dir)
	verbosef("Opening database in %q.\n", dir)

	compactions := compactionTracker{activeJobs: map[int]bool{}}
	rd, err := open(dir, compactions.eventListener())
	if err != nil {
		return err
	}
	defer rd.Close()

	m := rd.Metrics()
	start := time.Now()

	var ref levelSizes
	var replayedCount int
	var sizeSum, sizeCount uint64
	for _, li := range hist {
		// Maintain ref as the shape of the LSM during the original run.
		// We use the size of L0 to establish pacing.
		ref.add(li.levelSizesDelta())

		// Ignore manifest changes due to compactions.
		if li.compaction {
			continue
		}

		// We try to keep the current db's L0 size similar to the reference
		// db's L0 at the same point in its execution. If we've exceeded it,
		// we wait for compactions to catch up. This isn't perfect because
		// different heuristics may choose not to compact L0 at all at the
		// same size, so we also proceed after five seconds if there are no
		// active compactions.
		for skip := false; !skip; {
			activeCompactions, waiterCh := compactions.countActive()
			m = rd.Metrics()
			if m.Levels[0].Size <= uint64(compactRunConfig.pacing*float64(ref[0])) {
				break
			}

			verbosef("L0 size %s > %.2f × reference L0 size %s; pausing to let compaction catch up.\n",
				humanize.Uint64(m.Levels[0].Size), compactRunConfig.pacing, humanize.Int64(ref[0]))
			select {
			case <-waiterCh:
				// A compaction event was processed, continue to check the
				// size of level zero.
				continue
			case <-time.After(5 * time.Second):
				skip = activeCompactions == 0
				if skip {
					verbosef("No compaction in 5 seconds. Proceeding anyways.\n")
				}
			}
		}

		// Track the average database size.
		sizeCount++
		for _, l := range m.Levels {
			sizeSum += l.Size
		}

		if li.flush {
			for _, f := range li.added {
				name := fmt.Sprintf("%s.sst", f.meta.FileNum)
				tablePath := filepath.Join(workloadDir, name)
				replayedCount++
				verbosef("Flushing table %d/%d %s (%s, seqnums %d-%d)\n", replayedCount, workloadTableCount,
					name, humanize.Int64(int64(f.meta.Size)), f.meta.SmallestSeqNum, f.meta.LargestSeqNum)
				if err := rd.FlushExternal(replay.Table{Path: tablePath, FileMetadata: f.meta}); err != nil {
					return err
				}
			}
		} else {
			var tables []replay.Table
			for _, f := range li.added {
				name := fmt.Sprintf("%s.sst", f.meta.FileNum)
				tablePath := filepath.Join(workloadDir, name)
				replayedCount++
				verbosef("Ingesting %d tables: table %d/%d %s (%s)\n", len(li.added), replayedCount,
					workloadTableCount, name, humanize.Int64(int64(f.meta.Size)))
				tables = append(tables, replay.Table{
					Path:         tablePath,
					FileMetadata: f.meta,
				})
			}
			if err := rd.Ingest(tables); err != nil {
				return err
			}
		}
	}

	verbosef("Replayed all %d tables.\n", replayedCount)
	d := rd.Done()
	m = d.Metrics()
	if backgroundCompactions(m) {
		fmt.Println(m)
		fmt.Println("Waiting for background compactions to complete.")
		for range time.Tick(time.Second) {
			m = d.Metrics()
			if !backgroundCompactions(m) {
				break
			}
		}
	}

	fmt.Println(m)
	fmt.Println("Background compactions finished.")
	// Calculate read amplification before compacting everything.
	var ramp int32
	for _, l := range m.Levels {
		ramp += l.Sublevels
	}

	fmt.Println("Manually compacting entire key space to calculate space amplification.")
	beforeSize := totalSize(m)
	iter := d.NewIter(nil)
	if err != nil {
		return err
	}
	var first, last []byte
	if iter.First() {
		first = append(first, iter.Key()...)
	}
	if iter.Last() {
		last = append(last, iter.Key()...)
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if err := d.Compact(first, last); err != nil {
		return err
	}
	afterSize := totalSize(d.Metrics())
	fmt.Printf("Test took: %0.2fm\n", time.Now().Sub(start).Minutes())
	fmt.Printf("Average database size: %s\n", humanize.Uint64(sizeSum/sizeCount))
	fmt.Printf("Read amplification: %d\n", ramp)
	fmt.Printf("Total write amplification: %.2f\n", totalWriteAmp(m))
	fmt.Printf("Space amplification: %.2f\n", float64(beforeSize)/float64(afterSize))
	return nil
}

func totalWriteAmp(m *pebble.Metrics) float64 {
	var total pebble.LevelMetrics
	for _, lm := range m.Levels {
		total.Add(&lm)
	}
	// Compute total bytes-in as the bytes written to the WAL + bytes ingested
	total.BytesIn = m.WAL.BytesWritten + total.BytesIngested
	// Add the total bytes-in to the total bytes-flushed. This is to account for
	// the bytes written to the log and bytes written externally and then
	// ingested.
	total.BytesFlushed += total.BytesIn
	return total.WriteAmp()
}

func totalSize(m *pebble.Metrics) uint64 {
	sz := m.WAL.Size
	for _, lm := range m.Levels {
		sz += lm.Size
	}
	return sz
}

func backgroundCompactions(m *pebble.Metrics) bool {
	// The last level never gets selected as an input level for compaction,
	// only as an output level, so ignore it for the purposes of determining if
	// background compactions are still needed.
	for i := range m.Levels[:len(m.Levels)-1] {
		if m.Levels[i].Score > 1 {
			return true
		}
	}
	return false
}

func verbosef(fmtstr string, args ...interface{}) {
	if verbose {
		fmt.Printf(fmtstr, args...)
	}
}

func removeAll(dir string) {
	verbosef("Removing %q.\n", dir)
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
}

// teeEventListener wraps two event listeners, forwarding all events to both.
func teeEventListener(a, b pebble.EventListener) pebble.EventListener {
	a.EnsureDefaults(nil)
	b.EnsureDefaults(nil)
	return pebble.EventListener{
		BackgroundError: func(err error) {
			a.BackgroundError(err)
			b.BackgroundError(err)
		},
		CompactionBegin: func(info pebble.CompactionInfo) {
			a.CompactionBegin(info)
			b.CompactionBegin(info)
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			a.CompactionEnd(info)
			b.CompactionEnd(info)
		},
		FlushBegin: func(info pebble.FlushInfo) {
			a.FlushBegin(info)
			b.FlushBegin(info)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			a.FlushEnd(info)
			b.FlushEnd(info)
		},
		ManifestCreated: func(info pebble.ManifestCreateInfo) {
			a.ManifestCreated(info)
			b.ManifestCreated(info)
		},
		ManifestDeleted: func(info pebble.ManifestDeleteInfo) {
			a.ManifestDeleted(info)
			b.ManifestDeleted(info)
		},
		TableCreated: func(info pebble.TableCreateInfo) {
			a.TableCreated(info)
			b.TableCreated(info)
		},
		TableDeleted: func(info pebble.TableDeleteInfo) {
			a.TableDeleted(info)
			b.TableDeleted(info)
		},
		TableIngested: func(info pebble.TableIngestInfo) {
			a.TableIngested(info)
			b.TableIngested(info)
		},
		WALCreated: func(info pebble.WALCreateInfo) {
			a.WALCreated(info)
			b.WALCreated(info)
		},
		WALDeleted: func(info pebble.WALDeleteInfo) {
			a.WALDeleted(info)
			b.WALDeleted(info)
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			a.WriteStallBegin(info)
			b.WriteStallBegin(info)
		},
		WriteStallEnd: func() {
			a.WriteStallEnd()
			b.WriteStallEnd()
		},
	}
}

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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/humanize"
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
	RunE:  runIngest,
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

type dbUpdate struct {
	levelSizes
	compaction bool
	jobID      int
}

// levelSizeEventListener returns a Pebble event listener and a channel.
// It allows the main test loop to track the # of active compactions, the
// progress of requested ingestions and the size of each level of the LSM.
func levelSizeEventListener() (pebble.EventListener, <-chan dbUpdate) {
	ch := make(chan dbUpdate)
	return pebble.EventListener{
		CompactionBegin: func(info pebble.CompactionInfo) {
			ch <- dbUpdate{jobID: info.JobID, compaction: true}
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			u := dbUpdate{jobID: info.JobID, compaction: true}
			for i, tbls := range info.Input.Tables {
				for _, tbl := range tbls {
					u.levelSizes[info.Input.Level+i] -= int64(tbl.Size)
				}
			}
			for _, tbl := range info.Output.Tables {
				u.levelSizes[info.Output.Level] += int64(tbl.Size)
			}
			ch <- u
		},
		TableIngested: func(info pebble.TableIngestInfo) {
			u := dbUpdate{jobID: info.JobID}
			for _, tbl := range info.Tables {
				u.levelSizes[tbl.Level] += int64(tbl.Size)
			}
			ch <- u
		},
	}, ch
}

func open(dir string, listener pebble.EventListener) (*pebble.DB, error) {
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
	d, err := pebble.Open(dir, opts)
	return d, err
}

// hardLinkWorkload makes a hardlink of every sstable and manifest from src
// inside dst. Pebble will remove ingested sstables from their original path.
// Making a hardlink of all the sstables ensures that we can run the same
// workload multiple times.
func hardLinkWorkload(src, dst string) error {
	infos, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}
	for _, info := range infos {
		srcPath := filepath.Join(src, info.Name())
		dstPath := filepath.Join(dst, info.Name())
		typ, _, ok := base.ParseFilename(vfs.Default, srcPath)
		if ok && typ == base.FileTypeTable || typ == base.FileTypeManifest {
			verbosef("Linking %s to %s.\n", srcPath, dstPath)
			err := os.Link(srcPath, dstPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func runIngest(cmd *cobra.Command, args []string) error {
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
	_, hist, err := loadManifestsDir(workloadDir, map[pebble.FileNum]uint64{})
	if err != nil {
		return err
	}

	var workloadTableCount int
	var workloadTableSize uint64
	for _, li := range hist {
		if li.newData {
			workloadTableCount++
			workloadTableSize += li.size
		}
	}
	if workloadTableCount == 0 {
		fmt.Fprintf(os.Stderr, "Empty workload: %s\n", args[0])
		os.Exit(1)
	}
	verbosef("Workload contains %s from %d sstables to ingest.\n",
		humanize.Uint64(workloadTableSize), workloadTableCount)

	dataDir, err := ioutil.TempDir(args[0], "pebble-bench-data")
	if err != nil {
		return err
	}
	defer removeAll(dataDir)
	verbosef("Opening database in %q.\n", dataDir)
	listener, updatesCh := levelSizeEventListener()
	d, err := open(dataDir, listener)
	if err != nil {
		return err
	}
	defer d.Close()

	start := time.Now()

	var sizeSum int64
	var ref levelSizes
	var curr levelSizes
	var ingestedCount int
	var pendingSize int64
	var atomicCompletedIngestCalls int32
	var wg sync.WaitGroup
	activeCompactions := map[int]bool{}
	for _, li := range hist {
		name := fmt.Sprintf("%s.sst", li.num)
		path := filepath.Join(workloadDir, name)

		// Files created through ingests or flushes have been rewritten and
		// will have a different file size than the manifests indicate.
		// Stat these files to get the true file size.
		if li.newData {
			info, err := os.Stat(path)
			if err != nil {
				return err
			}
			li.size = uint64(info.Size())
		}

		// Maintain ref as the shape of the LSM during the original run.
		// We use the size of L0 to establish pacing.
		if li.add {
			ref[li.level] += int64(li.size)
		} else {
			ref[li.level] -= int64(li.size)
		}

		// Skip compactions.
		if !li.newData {
			continue
		}

		// We try to keep the current db's L0 size similar to the reference
		// db's L0 at the same point in its execution. If we've exceeded it,
		// we wait for compactions to catch up. This isn't perfect because our
		// rewritten tables have different file sizes and different heuristics
		// may choose not to compact L0 at all at the same size, so we also
		// proceed after five seconds if there are no active compactions.
		for skip := false; !skip && pendingSize+curr[0] > int64(compactRunConfig.pacing*float64(ref[0])); {
			verbosef("Pending size %s + L0 size %s > %.2f × reference L0 size %s; pausing to let compaction catch up.\n",
				humanize.Int64(pendingSize), humanize.Int64(curr[0]), compactRunConfig.pacing, humanize.Int64(ref[0]))
			select {
			case u := <-updatesCh:
				curr.add(u.levelSizes)
				if u.compaction {
					if activeCompactions[u.jobID] {
						delete(activeCompactions, u.jobID)
					} else {
						activeCompactions[u.jobID] = true
					}
				} else {
					pendingSize -= u.levelSizes.sum()
				}
			case <-time.After(5 * time.Second):
				skip = len(activeCompactions) == 0
				if skip {
					verbosef("No compaction in 5 seconds. Proceeding anyways.\n")
				}
			}
		}

		ingestedCount++
		verbosef("Triggering ingest %d/%d ingest %s (%s)\n",
			ingestedCount, workloadTableCount, name, humanize.Uint64(li.size))

		sizeSum += curr.sum()
		pendingSize += int64(li.size)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer atomic.AddInt32(&atomicCompletedIngestCalls, 1)
			err := d.Ingest([]string{path})
			if err != nil {
				log.Fatal(err)
			}
		}()
	}
	// We no longer care about events surfaced from the event listener, but we
	// need to read from the channel to not block any pebble goroutines.
	go discardUpdates(updatesCh)
	verbosef("Initiated all %d tables. There are %d call(s) to ingest pending.\n",
		ingestedCount, ingestedCount-int(atomic.LoadInt32(&atomicCompletedIngestCalls)))
	wg.Wait()

	m := d.Metrics()
	if backgroundCompactions(m) {
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
	fmt.Printf("Average database size: %s\n", humanize.Int64(sizeSum/int64(ingestedCount)))
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

func discardUpdates(ch <-chan dbUpdate) {
	for range ch {
	}
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

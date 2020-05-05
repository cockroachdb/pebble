// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/humanize"
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

// loadHistory loads and parses a history csv constructed by the
// create-workload command.
func loadHistory(workloadDir string) ([]logItem, error) {
	f, err := os.Open(filepath.Join(workloadDir, "history"))
	if err != nil {
		return nil, errors.Wrap(err, filepath.Join(workloadDir, "history"))
	}
	defer f.Close()
	r := csv.NewReader(f)
	var log []logItem
	for rec, err := r.Read(); err == nil; rec, err = r.Read() {
		level, err := strconv.ParseInt(rec[0], 10, 64)
		if err != nil {
			return nil, err
		}
		fileNum, err := strconv.ParseInt(rec[1], 10, 64)
		if err != nil {
			return nil, err
		}
		addRm := rec[2] == "add"
		fileSize, err := strconv.ParseUint(rec[3], 10, 64)
		if err != nil {
			return nil, err
		}
		var newData bool
		if _, err := fmt.Sscanf(rec[4], "%t", &newData); err != nil {
			return nil, err
		}
		log = append(log, logItem{
			level:   int(level),
			num:     pebble.FileNum(fileNum),
			add:     addRm,
			size:    fileSize,
			newData: newData,
		})
	}
	return log, nil
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
			for _, tbl := range info.Input.Tables[0] {
				u.levelSizes[info.Input.Level] -= int64(tbl.Size)
			}
			for _, tbl := range info.Input.Tables[1] {
				u.levelSizes[info.Output.Level] -= int64(tbl.Size)
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

// hardLinkWorkload makes a hardlink of every .sst file from src inside dst. It also
// makes a hardlink of the './history' file that describes the history of the
// sstables. Pebble will remove ingested sstables from their original path.
// Making a hardlink of all the sstables ensures that we can run the same
// workload multiple times.
func hardLinkWorkload(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dst, relPath)
		if info.IsDir() {
			return os.MkdirAll(dstPath, os.ModePerm)
		}
		if filepath.Base(path) == "history" || filepath.Ext(path) == ".sst" {
			return os.Link(path, dstPath)
		}
		return nil
	})
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

	hist, err := loadHistory(workloadDir)
	if err != nil {
		return err
	}
	var workloadSize int64
	var workloadTableCount int
	for _, li := range hist {
		if li.newData {
			workloadSize += int64(li.size)
			workloadTableCount++
		}
	}
	verbosef("Workload contains %s across %d sstables to ingest.\n",
		humanize.Int64(workloadSize), workloadTableCount)

	dir, err := ioutil.TempDir(args[0], "pebble-bench-data")
	if err != nil {
		return err
	}
	defer removeAll(dir)
	verbosef("Opening database in %q.\n", dir)

	listener, updatesCh := levelSizeEventListener()
	d, err := open(dir, listener)
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
		// Maintain ref as the shape of the LSM during the original run.
		// We use the size of L0 to establish pacing.
		if li.add {
			ref[li.level] += int64(li.size)
		} else {
			ref[li.level] -= int64(li.size)
		}
		if !li.newData {
			// Ignore manifest changes due to compactions.
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

		name := fmt.Sprintf("%s.sst", li.num)
		ingestedCount++
		verbosef("Triggering ingest %d/%d ingest %s (%s)\n",
			ingestedCount, workloadTableCount, name, humanize.Int64(int64(li.size)))

		sizeSum += curr.sum()
		pendingSize += int64(li.size)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer atomic.AddInt32(&atomicCompletedIngestCalls, 1)
			// TODO(jackson): Ingest will ingest into the lowest level it can,
			// but we want to simulate flushes into L0. We can use a private
			// ingest method to force these ingests into L0.
			err := d.Ingest([]string{filepath.Join(workloadDir, name)})
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

// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
)

const (
	// rangeDelShapeCycle cycles through fixed spans, so later tombstones overlap
	// earlier ones.
	rangeDelShapeCycle = "cycle"
	// rangeDelShapeUnique gives every DeleteRange a fresh span that no earlier
	// tombstone covers, except for the fraction that re-delete from a queue's
	// start.
	rangeDelShapeUnique = "unique"

	dbOptionsDefault        = "default"
	dbOptionsProdWriteHeavy = "prod-writeheavy"
)

// RangeDelConfig configures the range-del benchmark.
type RangeDelConfig struct {
	Readers        int
	ReaderInterval time.Duration
	Writers        int
	WriterInterval time.Duration

	// Shape is rangeDelShapeCycle or rangeDelShapeUnique. Queues, OverlapFrac,
	// RangeDelsPerBatch and SetsPerBatch only apply to rangeDelShapeUnique.
	Shape string
	// Queues is the number of key prefixes with independent advancing ack levels.
	Queues int
	// OverlapFrac is the fraction of DeleteRanges that re-delete from a queue's
	// start to its current ack level.
	OverlapFrac float64
	// RangeDelsPerBatch is the number of DeleteRanges in each batch.
	RangeDelsPerBatch int
	// SetsPerBatch is the number of point Sets per batch. If negative, writes
	// alternate between a batch of DeleteRanges and a lone Set.
	SetsPerBatch int
	// IterFrac is the fraction of reads that open an iterator and scan instead
	// of doing a Get. If negative, reads alternate between the two.
	IterFrac float64
	// DBOptions is dbOptionsDefault or dbOptionsProdWriteHeavy.
	DBOptions string
}

// DefaultRangeDelConfig returns the default range-delete benchmark configuration.
func DefaultRangeDelConfig() RangeDelConfig {
	return RangeDelConfig{
		Readers:        4,
		ReaderInterval: 1 * time.Millisecond,
		Writers:        1,
		WriterInterval: 10 * time.Millisecond,

		Shape:             rangeDelShapeCycle,
		Queues:            1024,
		OverlapFrac:       0,
		RangeDelsPerBatch: 1,
		SetsPerBatch:      -1,
		IterFrac:          -1,
		DBOptions:         dbOptionsDefault,
	}
}

// legacyWorkload reports whether to omit the extended output.
func (c *RangeDelConfig) legacyWorkload() bool {
	return c.Shape == rangeDelShapeCycle && c.DBOptions == dbOptionsDefault && c.IterFrac < 0
}

func (c *RangeDelConfig) validate() error {
	if c.Readers <= 0 && c.Writers <= 0 {
		return errors.New("rangedel: --readers and --writers cannot both be zero")
	}
	switch c.DBOptions {
	case dbOptionsDefault, dbOptionsProdWriteHeavy:
	default:
		return errors.Newf("rangedel: unknown --db-options %q (want %s or %s)",
			c.DBOptions, dbOptionsDefault, dbOptionsProdWriteHeavy)
	}
	if c.IterFrac > 1 {
		return errors.Newf("rangedel: --iter-frac %v must be negative or at most 1", c.IterFrac)
	}
	switch c.Shape {
	case rangeDelShapeCycle:
		d := DefaultRangeDelConfig()
		if c.OverlapFrac != d.OverlapFrac || c.RangeDelsPerBatch != d.RangeDelsPerBatch ||
			c.SetsPerBatch != d.SetsPerBatch || c.Queues != d.Queues {
			return errors.New("rangedel: --rangedel-queues, --rangedel-overlap-frac, " +
				"--rangedels-per-batch and --sets-per-batch require --rangedel-shape=unique")
		}
	case rangeDelShapeUnique:
		if c.OverlapFrac < 0 || c.OverlapFrac > 1 {
			return errors.Newf("rangedel: --rangedel-overlap-frac %v must be in [0, 1]", c.OverlapFrac)
		}
		if c.RangeDelsPerBatch < 1 {
			return errors.New("rangedel: --rangedels-per-batch must be at least 1")
		}
		if c.Queues < 1 || c.Queues < c.Writers {
			return errors.New("rangedel: --rangedel-queues must be at least 1 and at least --writers")
		}
	default:
		return errors.Newf("rangedel: unknown --rangedel-shape %q (want cycle or unique)", c.Shape)
	}
	return nil
}

func rangeDelCommonConfig(common *CommonConfig, cfg *RangeDelConfig) CommonConfig {
	c := *common
	if cfg.DBOptions == dbOptionsProdWriteHeavy {
		callerHook := c.OptionsHook
		c.OptionsHook = func(opts *pebble.Options) {
			if callerHook != nil {
				callerHook(opts)
			}
			applyProdWriteHeavyDBOptions(opts)
		}
	}
	return c
}

// RunRangeDel runs the range-del benchmark.
func RunRangeDel(dir string, common *CommonConfig, cfg *RangeDelConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	c := rangeDelCommonConfig(common, cfg)
	if !cfg.legacyWorkload() {
		fmt.Printf("rangedel: shape=%s queues=%d overlap-frac=%v rangedels-per-batch=%d "+
			"sets-per-batch=%d iter-frac=%v db-options=%s gomaxprocs=%d\n",
			cfg.Shape, cfg.Queues, cfg.OverlapFrac, cfg.RangeDelsPerBatch,
			cfg.SetsPerBatch, cfg.IterFrac, cfg.DBOptions, runtime.GOMAXPROCS(0))
	}

	var reads, writes atomic.Uint64
	var counts rangeDelCounters
	var benchDB *pebble.DB
	reg := newHistogramRegistry()
	readLatency := reg.Register("read")
	commits := newCommitLatencies()

	// Reads (Gets and scans) and point Sets target "r/*"; range deletions
	// target "d/*". Reads do not overlap with deletion ranges.
	RunTest(dir, &c, Test{
		Init: func(d DB) {
			pd := d.(pebbleDB).d
			benchDB = pd
			// Pre-populate the read range so cold scans/Gets have content.
			for i := 0; i < 1000; i++ {
				key := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", i), 0, 0)
				if err := pd.Set(key, []byte("v"), pebble.NoSync); err != nil {
					log.Fatal(err)
				}
			}
		},
		Run: func(d DB, wg *sync.WaitGroup) {
			pd := d.(pebbleDB).d
			limiter := c.RateLimiter

			for w := 0; w < cfg.Writers; w++ {
				w := w
				wg.Add(1)
				go func() {
					defer wg.Done()
					var ticker *time.Ticker
					if cfg.WriterInterval > 0 {
						ticker = time.NewTicker(cfg.WriterInterval)
						defer ticker.Stop()
					}
					var uw *uniqueWriter
					if cfg.Shape == rangeDelShapeUnique {
						uw = newUniqueWriter(pd, cfg, w, &counts, commits)
					}
					for i := uint64(0); ; i++ {
						if ticker != nil {
							<-ticker.C
						}
						wait(limiter)
						if uw != nil {
							uw.write(i)
							writes.Add(1)
							continue
						}
						// Alternate DeleteRanges with point Sets.
						slot := i % 1000
						if i%2 == 0 {
							delFrom := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/a", slot), 0, 0)
							delTo := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/z", slot), 0, 0)
							start := time.Now()
							if err := pd.DeleteRange(delFrom, delTo, pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: DeleteRange: %v", w, err)
							}
							commits.record(start, true /* rangeDel */)
							counts.batches.Add(1)
							counts.rangeDels.Add(1)
						} else {
							setKey := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
							start := time.Now()
							if err := pd.Set(setKey, []byte("v"), pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: Set: %v", w, err)
							}
							commits.record(start, false /* rangeDel */)
							counts.sets.Add(1)
						}
						writes.Add(1)
					}
				}()
			}
			for r := 0; r < cfg.Readers; r++ {
				r := r
				wg.Add(1)
				go func() {
					defer wg.Done()
					var ticker *time.Ticker
					if cfg.ReaderInterval > 0 {
						ticker = time.NewTicker(cfg.ReaderInterval)
						defer ticker.Stop()
					}
					var rng *rand.Rand
					if cfg.IterFrac >= 0 {
						rng = rand.New(rand.NewPCG(rangeDelSeed, uint64(cfg.Writers+r)))
					}
					for i := uint64(0); ; i++ {
						if ticker != nil {
							<-ticker.C
						}
						wait(limiter)
						// Alternate point Gets and range scans, unless --iter-frac
						// picks the mix.
						useIter := i%2 == 1
						if rng != nil {
							useIter = rng.Float64() < cfg.IterFrac
						}
						start := time.Now()
						if !useIter {
							key := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", i%1000), 0, 0)
							_, closer, err := pd.Get(key)
							readLatency.Record(time.Since(start))
							if err != nil {
								log.Fatalf("rangedel reader: Get: %v", err)
							}
							_ = closer.Close()
						} else {
							lower := i % 1000
							iter, err := pd.NewIter(&pebble.IterOptions{
								LowerBound: cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", lower), 0, 0),
								UpperBound: cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", lower+10), 0, 0),
							})
							if err != nil {
								log.Fatalf("rangedel reader: NewIter: %v", err)
							}
							for valid := iter.First(); valid; valid = iter.Next() {
							}
							readLatency.Record(time.Since(start))
							if err := iter.Close(); err != nil {
								log.Fatalf("rangedel reader: iter Close: %v", err)
							}
						}
						reads.Add(1)
					}
				}()
			}
		},
		Tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("____elapsed______reads/s_____writes/s___p50(µs)___p95(µs)___p99(µs)___pMax(µs)")
			}
			reg.Tick(func(tick histogramTick) {
				h := tick.Hist
				fmt.Printf("%9.1fs %12d %12d %9.1f %9.1f %9.1f %10.1f\n",
					elapsed.Seconds(),
					reads.Swap(0),
					writes.Swap(0),
					usFromNs(h.ValueAtQuantile(50)),
					usFromNs(h.ValueAtQuantile(95)),
					usFromNs(h.ValueAtQuantile(99)),
					usFromNs(h.ValueAtQuantile(100)),
				)
			})
		},
		Done: func(elapsed time.Duration) {
			fmt.Println("\n____elapsed___read_ops____read_ops/s___p50(µs)___p95(µs)___p99(µs)___pMax(µs)")
			reg.Tick(func(tick histogramTick) {
				h := tick.Cumulative
				fmt.Printf("%9.1fs %10d %13.1f %9.1f %9.1f %9.1f %10.1f\n",
					elapsed.Seconds(),
					h.TotalCount(),
					float64(h.TotalCount())/elapsed.Seconds(),
					usFromNs(h.ValueAtQuantile(50)),
					usFromNs(h.ValueAtQuantile(95)),
					usFromNs(h.ValueAtQuantile(99)),
					usFromNs(h.ValueAtQuantile(100)),
				)
			})
			if !cfg.legacyWorkload() {
				// The writers' commit latencies, over the whole run: every commit,
				// and the commits of batches that contain a DeleteRange.
				fmt.Println("\n____elapsed_commits_of____commits___commits/s___p50(µs)___p95(µs)" +
					"___p99(µs)___pMax(µs)")
				commits.forEach(func(name string, h *hdrhistogram.Histogram) {
					fmt.Printf("%9.1fs %-11s %10d %11.1f %9.1f %9.1f %9.1f %10.1f\n",
						elapsed.Seconds(),
						name,
						h.TotalCount(),
						float64(h.TotalCount())/elapsed.Seconds(),
						usFromNs(h.ValueAtQuantile(50)),
						usFromNs(h.ValueAtQuantile(95)),
						usFromNs(h.ValueAtQuantile(99)),
						usFromNs(h.ValueAtQuantile(100)),
					)
				})
				fmt.Printf("\nrangedel_summary: elapsed=%.1fs rangedel_batches=%d "+
					"rangedel_batches/s=%.1f rangedel_ops=%d overlapping_rangedel_ops=%d "+
					"point_sets=%d flushes=%d\n",
					elapsed.Seconds(),
					counts.batches.Load(),
					float64(counts.batches.Load())/elapsed.Seconds(),
					counts.rangeDels.Load(),
					counts.overlaps.Load(),
					counts.sets.Load(),
					benchDB.Metrics().Flush.Count,
				)
			}
		},
	})
	return nil
}

// rangeDelSeed seeds the pseudo-random choices of the unique shape and of
// --iter-frac, so runs of different binaries issue the same operations.
const rangeDelSeed = 0x72616e6765646c31

// rangeDelCounters counts the writer's operations for the end-of-run summary.
type rangeDelCounters struct {
	// batches counts batches that contain at least one DeleteRange.
	batches atomic.Uint64
	// rangeDels counts DeleteRanges, and overlaps counts the ones that re-delete
	// from a queue's start.
	rangeDels atomic.Uint64
	overlaps  atomic.Uint64
	sets      atomic.Uint64
}

// commitLatencies records all writer commit latencies and separately records
// commits whose batch contains a DeleteRange.
type commitLatencies struct {
	mu            sync.Mutex
	all, rangeDel *hdrhistogram.Histogram
}

func newCommitLatencies() *commitLatencies {
	newHist := func() *hdrhistogram.Histogram {
		return hdrhistogram.New(1, maxLatency.Nanoseconds(), 2)
	}
	return &commitLatencies{all: newHist(), rangeDel: newHist()}
}

func (c *commitLatencies) record(start time.Time, rangeDel bool) {
	d := min(time.Since(start).Nanoseconds(), maxLatency.Nanoseconds())
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.all.RecordValue(d)
	if rangeDel {
		_ = c.rangeDel.RecordValue(d)
	}
}

func (c *commitLatencies) forEach(fn func(name string, h *hdrhistogram.Histogram)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn("all", c.all)
	fn("rangedel", c.rangeDel)
}

// uniqueWriter owns queues where q % Writers equals its writer index. Each
// queue has a key prefix "d/q<queue>/" and an advancing ack level. DeleteRanges
// choose a queue at random and abut within it, except for re-deletions from zero.
type uniqueWriter struct {
	db      *pebble.DB
	cfg     *RangeDelConfig
	counts  *rangeDelCounters
	commits *commitLatencies
	rng     *rand.Rand
	// queues holds the ids of the queues that this writer owns, and acks holds
	// their ack levels.
	queues  []int
	acks    []uint64
	setSlot uint64
}

func newUniqueWriter(
	db *pebble.DB,
	cfg *RangeDelConfig,
	writer int,
	counts *rangeDelCounters,
	commits *commitLatencies,
) *uniqueWriter {
	uw := &uniqueWriter{
		db:      db,
		cfg:     cfg,
		counts:  counts,
		commits: commits,
		rng:     rand.New(rand.NewPCG(rangeDelSeed, uint64(writer))),
	}
	for q := writer; q < cfg.Queues; q += cfg.Writers {
		uw.queues = append(uw.queues, q)
	}
	uw.acks = make([]uint64, len(uw.queues))
	return uw
}

func queueKey(queue int, ack uint64) []byte {
	return cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/q%06d/%012d", queue, ack), 0, 0)
}

func (uw *uniqueWriter) write(i uint64) {
	sets := uw.cfg.SetsPerBatch
	if sets < 0 {
		// Alternate as the cycle shape does: a batch of only DeleteRanges, then a
		// lone Set.
		if i%2 == 1 {
			key := uw.setKey()
			start := time.Now()
			if err := uw.db.Set(key, []byte("v"), pebble.NoSync); err != nil {
				log.Fatalf("rangedel writer: Set: %v", err)
			}
			uw.commits.record(start, false /* rangeDel */)
			uw.counts.sets.Add(1)
			return
		}
		sets = 0
	}

	b := uw.db.NewBatch()
	for k := 0; k < uw.cfg.RangeDelsPerBatch; k++ {
		qi := uw.rng.IntN(len(uw.queues))
		queue, ack := uw.queues[qi], uw.acks[qi]
		var from, to uint64
		if ack > 0 && uw.rng.Float64() < uw.cfg.OverlapFrac {
			// Re-delete from the start of the queue, overlapping every earlier
			// tombstone of the queue.
			from, to = 0, ack
			uw.counts.overlaps.Add(1)
		} else {
			// Delete [previous ack level, new ack level).
			from, to = ack, ack+1+uw.rng.Uint64N(1000)
			uw.acks[qi] = to
		}
		if err := b.DeleteRange(queueKey(queue, from), queueKey(queue, to), nil); err != nil {
			log.Fatalf("rangedel writer: DeleteRange: %v", err)
		}
	}
	for s := 0; s < sets; s++ {
		if err := b.Set(uw.setKey(), []byte("v"), nil); err != nil {
			log.Fatalf("rangedel writer: batch Set: %v", err)
		}
	}
	start := time.Now()
	if err := b.Commit(pebble.NoSync); err != nil {
		log.Fatalf("rangedel writer: Commit: %v", err)
	}
	uw.commits.record(start, true /* rangeDel */)
	if err := b.Close(); err != nil {
		log.Fatalf("rangedel writer: batch Close: %v", err)
	}
	uw.counts.batches.Add(1)
	uw.counts.rangeDels.Add(uint64(uw.cfg.RangeDelsPerBatch))
	uw.counts.sets.Add(uint64(sets))
}

// setKey returns the next point-Set key in the read range.
func (uw *uniqueWriter) setKey() []byte {
	slot := uw.setSlot % 1000
	uw.setSlot++
	return cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
}

// applyProdWriteHeavyDBOptions applies a write-heavy production configuration,
// preserving the benchmark's comparer and key schema.
func applyProdWriteHeavyDBOptions(opts *pebble.Options) {
	opts.FormatMajorVersion = pebble.FormatIngestBlobFiles
	opts.CompactionConcurrencyRange = func() (lower, upper int) {
		return 1, min(3, max(runtime.GOMAXPROCS(0)-1, 1))
	}
	opts.L0CompactionThreshold = 2
	opts.L0StopWritesThreshold = 1000
	opts.MemTableStopWritesThreshold = 4
	opts.FlushDelayDeleteRange = 10 * time.Second
	opts.FlushDelayRangeKey = 10 * time.Second
	opts.MemTableSize = 256 << 20
	opts.LBaseMaxBytes = 512 << 20
	opts.L0CompactionConcurrency = 2
	opts.ValueSeparationPolicy = nil
	opts.CompactionGarbageFractionForMaxConcurrency = nil
	opts.MaxManifestFileSize = 10 << 20

	opts.Levels[0] = pebble.LevelOptions{
		BlockSize:         32 << 10,
		IndexBlockSize:    256 << 10,
		TableFilterPolicy: func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) },
	}
	opts.Levels[0].EnsureL0Defaults()
	for i := 1; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		*l = pebble.LevelOptions{
			BlockSize:         32 << 10,
			IndexBlockSize:    256 << 10,
			TableFilterPolicy: func() pebble.TableFilterPolicy { return bloom.FilterPolicy(10) },
		}
		l.EnsureL1PlusDefaults(&opts.Levels[i-1])
	}
	opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		return pebble.DBCompressionFastest
	})
}

func usFromNs(ns int64) float64 {
	return float64(ns) / 1000
}

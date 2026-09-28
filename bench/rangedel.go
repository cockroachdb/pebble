// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
)

// RangeDelConfig configures the range-del benchmark.
type RangeDelConfig struct {
	Readers        int
	ReaderInterval time.Duration
	Writers        int
	WriterInterval time.Duration
}

// DefaultRangeDelConfig returns defaults approximating a read-heavy workload
// that consistently invalidates the range-del fragment cache.
func DefaultRangeDelConfig() RangeDelConfig {
	return RangeDelConfig{
		Readers:        4,
		ReaderInterval: 1 * time.Millisecond,
		Writers:        1,
		WriterInterval: 10 * time.Millisecond,
	}
}

// RunRangeDel runs the range-del benchmark.
func RunRangeDel(dir string, common *CommonConfig, cfg *RangeDelConfig) error {
	if cfg.Readers <= 0 && cfg.Writers <= 0 {
		return errors.New("rangedel: --readers and --writers cannot both be zero")
	}

	var reads, writes atomic.Uint64
	reg := newHistogramRegistry()
	readLatency := reg.Register("read")

	// Reads (Gets and scans) and point Sets target "r/*"; range deletions
	// target "d/*". Reads do not overlap with deletion ranges. All indices
	// are bounded % 1000 so the key space stays fixed.
	RunTest(dir, common, Test{
		Init: func(d DB) {
			pd := d.(pebbleDB).d
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
			limiter := common.RateLimiter

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
					for i := uint64(0); ; i++ {
						if ticker != nil {
							<-ticker.C
						}
						wait(limiter)
						// Alternate DeleteRanges with point Sets.
						slot := i % 1000
						if i%2 == 0 {
							delFrom := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/a", slot), 0, 0)
							delTo := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "d/%d/z", slot), 0, 0)
							if err := pd.DeleteRange(delFrom, delTo, pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: DeleteRange: %v", w, err)
							}
						} else {
							setKey := cockroachkvs.EncodeMVCCKey(nil, fmt.Appendf(nil, "r/%04d", slot), 0, 0)
							if err := pd.Set(setKey, []byte("v"), pebble.NoSync); err != nil {
								log.Fatalf("rangedel writer %d: Set: %v", w, err)
							}
						}
						writes.Add(1)
					}
				}()
			}
			for r := 0; r < cfg.Readers; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var ticker *time.Ticker
					if cfg.ReaderInterval > 0 {
						ticker = time.NewTicker(cfg.ReaderInterval)
						defer ticker.Stop()
					}
					for i := uint64(0); ; i++ {
						if ticker != nil {
							<-ticker.C
						}
						wait(limiter)
						// Alternate point Gets and range scans.
						start := time.Now()
						if i%2 == 0 {
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
		},
	})
	return nil
}

func usFromNs(ns int64) float64 {
	return float64(ns) / 1000
}

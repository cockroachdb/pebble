// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/randvar"
)

// ScanConfig configures the scan benchmark.
type ScanConfig struct {
	Reverse bool
	Rows    *randvar.Flag
	Values  *randvar.BytesFlag
}

// DefaultScanConfig returns a ScanConfig populated with the same defaults as
// the cmd/pebble CLI.
func DefaultScanConfig() ScanConfig {
	return ScanConfig{
		Rows:   randvar.NewFlag("100"),
		Values: randvar.NewBytesFlag("8"),
	}
}

// RunScan runs the scan benchmark.
func RunScan(dir string, common *CommonConfig, cfg *ScanConfig) {
	var (
		bytes       atomic.Int64
		scanned     atomic.Int64
		lastBytes   int64
		lastScanned int64
		lastElapsed time.Duration
	)

	opts := pebble.Sync
	if common.DisableWAL {
		opts = pebble.NoSync
	}

	rowDist := cfg.Rows

	RunTest(dir, common, Test{
		Init: func(d DB, wg *sync.WaitGroup) {
			const count = 100000
			const batch = 1000

			rng := rand.New(rand.NewPCG(0, 1449168817))
			keys := make([][]byte, count)

			for i := 0; i < count; {
				b := d.NewBatch()
				var value []byte
				for end := i + batch; i < end; i++ {
					keys[i] = cockroachkvs.EncodeMVCCKey(nil, encodeUint32Ascending([]byte("key-"), uint32(i)), uint64(i+1), 0)
					value = cfg.Values.Bytes(rng, value)
					if err := b.Set(keys[i], value, nil); err != nil {
						log.Fatal(err)
					}
				}
				if err := b.Commit(opts); err != nil {
					log.Fatal(err)
				}
			}

			if err := d.Flush(); err != nil {
				log.Fatal(err)
			}

			limiter := common.RateLimiter

			for i := range common.Concurrency {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(0, uint64(i)))
					startKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
					endKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
					minTS := encodeUint64Ascending(nil, math.MaxUint64)

					for {
						wait(limiter)

						rows := int(rowDist.Uint64(rng))
						startIdx := rng.Int32N(int32(len(keys) - rows))
						startKey := encodeUint32Ascending(startKeyBuf[:4], uint32(startIdx))
						endKey := encodeUint32Ascending(endKeyBuf[:4], uint32(startIdx+int32(rows)))

						var count int
						var nbytes int64
						if cfg.Reverse {
							count, nbytes = mvccReverseScan(d, startKey, endKey, minTS)
						} else {
							count, nbytes = mvccForwardScan(d, startKey, endKey, minTS)
						}

						if count != rows {
							log.Fatalf("scanned %d, expected %d\n", count, rows)
						}

						bytes.Add(nbytes)
						scanned.Add(int64(count))
					}
				})
			}
		},

		Tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("_elapsed_______rows/sec_______MB/sec_______ns/row")
			}

			curBytes := bytes.Load()
			curScanned := scanned.Load()
			dur := elapsed - lastElapsed
			fmt.Printf("%8s %14.1f %12.1f %12.1f\n",
				time.Duration(elapsed.Seconds()+0.5)*time.Second,
				float64(curScanned-lastScanned)/dur.Seconds(),
				float64(curBytes-lastBytes)/(dur.Seconds()*(1<<20)),
				float64(dur)/float64(curScanned-lastScanned),
			)
			lastBytes = curBytes
			lastScanned = curScanned
			lastElapsed = elapsed
		},

		Done: func(elapsed time.Duration) {
			curBytes := bytes.Load()
			curScanned := scanned.Load()
			fmt.Println("\n_elapsed___ops/sec(cum)__MB/sec(cum)__ns/row(avg)")
			fmt.Printf("%7.1fs %14.1f %12.1f %12.1f\n\n",
				elapsed.Seconds(),
				float64(curScanned)/elapsed.Seconds(),
				float64(curBytes)/(elapsed.Seconds()*(1<<20)),
				float64(elapsed)/float64(curScanned),
			)
		},
	})
}

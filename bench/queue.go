// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/randvar"
)

// QueueConfig configures the queue workload (used by the tombstone benchmark).
type QueueConfig struct {
	Size   int
	Values *randvar.BytesFlag
}

// DefaultQueueConfig returns a QueueConfig populated with the same defaults as
// the cmd/pebble CLI.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		Size:   256,
		Values: randvar.NewBytesFlag("16384"),
	}
}

// newQueueTest builds a Test running a single-writer fixed-size queue
// workload. The returned counter exposes the number of completed ops.
func newQueueTest(common *CommonConfig, cfg *QueueConfig) (Test, *atomic.Int64) {
	ops := new(atomic.Int64)
	var (
		lastOps     int64
		lastElapsed time.Duration
	)
	return Test{
		Init: func(d DB, wg *sync.WaitGroup) {
			var (
				value []byte
				rng   = rand.New(rand.NewPCG(0, 1449168817))
				queue = make([][]byte, cfg.Size)
			)
			for i := 0; i < cfg.Size; i++ {
				b := d.NewBatch()
				queue[i] = cockroachkvs.EncodeMVCCKey(nil, encodeUint32Ascending([]byte("queue-"), uint32(i)), uint64(i+1), 0)
				value = cfg.Values.Bytes(rng, value)
				if err := b.Set(queue[i], value, pebble.NoSync); err != nil {
					log.Fatal(err)
				}
				if err := b.Commit(pebble.NoSync); err != nil {
					log.Fatal(err)
				}
			}
			if err := d.Flush(); err != nil {
				log.Fatal(err)
			}

			limiter := common.RateLimiter
			wg.Go(func() {
				for i := cfg.Size; ; i++ {
					idx := i % cfg.Size

					// Delete the head.
					b := d.NewBatch()
					if err := b.Delete(queue[idx], pebble.Sync); err != nil {
						log.Fatal(err)
					}
					if err := b.Commit(pebble.Sync); err != nil {
						log.Fatal(err)
					}
					_ = b.Close()
					wait(limiter)

					// Append to the tail.
					b = d.NewBatch()
					queue[idx] = cockroachkvs.EncodeMVCCKey(queue[idx][:0], encodeUint32Ascending([]byte("queue-"), uint32(i)), uint64(i+1), 0)
					value = cfg.Values.Bytes(rng, value)
					if err := b.Set(queue[idx], value, nil); err != nil {
						log.Fatal(err)
					}
					if err := b.Commit(pebble.Sync); err != nil {
						log.Fatal(err)
					}
					_ = b.Close()
					wait(limiter)
					ops.Add(1)
				}
			})
		},
		Tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("Queue___elapsed_______ops/sec")
			}

			curOps := ops.Load()
			dur := elapsed - lastElapsed
			fmt.Printf("%15s %13.1f\n",
				time.Duration(elapsed.Seconds()+0.5)*time.Second,
				float64(curOps-lastOps)/dur.Seconds(),
			)
			lastOps = curOps
			lastElapsed = elapsed
		},
		Done: func(elapsed time.Duration) {
			curOps := ops.Load()
			fmt.Println("\nQueue___elapsed___ops/sec(cum)")
			fmt.Printf("%13.1fs %14.1f\n\n",
				elapsed.Seconds(),
				float64(curOps)/elapsed.Seconds())
		},
	}, ops
}

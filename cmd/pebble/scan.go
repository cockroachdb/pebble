package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
	"github.com/spf13/cobra"
)

var (
	scanRows      = 100
	scanValueSize = 8
	scanReverse   = false
)

var scanCmd = &cobra.Command{
	Use:   "scan <dir>",
	Short: "run the scan benchmark",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run:   runScan,
}

func runScan(cmd *cobra.Command, args []string) {
	var (
		scanned     int64
		lastScanned int64
		lastElapsed time.Duration
	)

	runTest(args[0], test{
		init: func(d *pebble.DB, wg *sync.WaitGroup) {
			const count = 100000
			const batch = 1000

			rng := rand.New(rand.NewSource(1449168817))
			randBytes := func(size int) []byte {
				data := make([]byte, size)
				for i := range data {
					data[i] = byte(rng.Int() & 0xff)
				}
				return data
			}
			keys := make([][]byte, count)

			for i := 0; i < count; {
				b := d.NewBatch()
				for end := i + batch; i < end; i++ {
					keys[i] = encodeUint32Ascending([]byte("key-"), uint32(i))
					value := randBytes(scanValueSize)
					if err := b.Set(keys[i], value, nil); err != nil {
						log.Fatal(err)
					}
				}
				if err := b.Commit(db.Sync); err != nil {
					log.Fatal(err)
				}
			}

			if err := d.Flush(); err != nil {
				log.Fatal(err)
			}

			wg.Add(concurrency)
			for i := 0; i < concurrency; i++ {
				go func(i int) {
					defer wg.Done()

					rng := rand.New(rand.NewSource(int64(i)))
					startKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)
					endKeyBuf := append(make([]byte, 0, 64), []byte("key-")...)

					for {
						startIdx := rng.Int31n(int32(len(keys) - scanRows))
						startKey := encodeUint32Ascending(startKeyBuf[:4], uint32(startIdx))
						endKey := encodeUint32Ascending(endKeyBuf[:4], uint32(startIdx+int32(scanRows)))

						it := d.NewIter(nil)
						count := 0
						if scanReverse {
							for it.SeekLT(endKey); it.Valid(); it.Prev() {
								if bytes.Compare(it.Key(), startKey) < 0 {
									break
								}
								count++
							}
						} else {
							for it.SeekGE(startKey); it.Valid(); it.Next() {
								if bytes.Compare(it.Key(), endKey) >= 0 {
									break
								}
								count++
							}
						}
						it.Close()

						if count != scanRows {
							log.Fatalf("scanned %d, expected %d\n", count, scanRows)
						}

						atomic.AddInt64(&scanned, int64(count))
					}
				}(i)
			}
		},

		tick: func(elapsed time.Duration, i int) {
			if i%20 == 0 {
				fmt.Println("_elapsed_______rows/sec_______MB/sec_______ns/row")
			}

			cur := atomic.LoadInt64(&scanned)
			dur := elapsed - lastElapsed
			fmt.Printf("%8s %14.1f %12.1f %12.1f\n",
				time.Duration(elapsed.Seconds()+0.5)*time.Second,
				float64(cur-lastScanned)/dur.Seconds(),
				float64(int64(scanValueSize)*(cur-lastScanned))/(dur.Seconds()*(1<<20)),
				float64(dur)/float64(cur-lastScanned),
			)
			lastScanned = cur
			lastElapsed = elapsed
		},

		done: func(elapsed time.Duration) {
			cur := atomic.LoadInt64(&scanned)
			fmt.Println("\n_elapsed___ops/sec(cum)__MB/sec(cum)__ns/row(avg)")
			fmt.Printf("%7.1fs %14.1f %12.1f %12.1f\n\n",
				elapsed.Seconds(),
				float64(cur)/elapsed.Seconds(),
				float64(int64(scanValueSize)*cur)/(elapsed.Seconds()*(1<<20)),
				float64(elapsed)/float64(cur),
			)
		},
	})
}

// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/pebble"
	"github.com/petermattis/pebble/internal/ackseq"
	"github.com/petermattis/pebble/internal/randvar"
	"github.com/spf13/cobra"
	"golang.org/x/exp/rand"
)

const (
	ycsbInsert = iota
	ycsbRead
	ycsbScan
	ycsbUpdate
	ycsbNumOps
)

var ycsb2Config struct {
	batch       string
	keys        string
	initialKeys int
	numOps      uint64
	scans       string
	workload    string
	values      string
}

var ycsb2Cmd = &cobra.Command{
	Use:   "ycsb2 <dir>",
	Short: "run customizable YCSB workload",
	Long: `
Run a customizable YCSB workload. The workload is specified by the --workload
flag which can take either one of the standard workload mixes (A-F), or
customizable workload fixes specified as a command separated list of op=weight
pairs. For example, --workload=read=50,update=50 performs a workload composed
of 50% reads and 50% updates. This is identical to the standard workload A.

The --batch, --scans, and --values flags take the specification for a random
variable: [<type>:]<min>[-<max>]. The <type> parameter must be one of "uniform"
or "zipf". If <type> is omitted, a uniform distribution is used. If <max> is
omitted it is set to the same value as <min>. The specification "1000" results
in a constant 1000. The specification "10-100" results in a uniformly random
variable in the range [10,100). The specification "zipf(10,100)" results in a
zipf distribution with a minimum value of 10 and a maximum value of 100.

The --batch flag controls the size of batches used for insert and update
operations. The --scans flag controls the number of iterations performed by a
scan operation. Read operations always read a single key.

The --values flag provides for an optional "/<target-compression-ratio>"
suffix. The default target compression ratio is 1.0 (i.e. incompressible random
data). A value of 2 will cause random data to be generated that should compress
to 50% of its uncompressed size.

Standard workloads:

  A:  50% reads   /  50% updates
  B:  95% reads   /   5% updates
  C: 100% reads
  D:  95% reads   /   5% inserts
  E:  95% scans   /   5% inserts
  F: 100% inserts
`,
	Args: cobra.ExactArgs(1),
	RunE: runYcsb2,
}

func init() {
	ycsb2Cmd.Flags().StringVar(
		&ycsb2Config.batch, "batch", "1",
		"batch size distribution [{zipf,uniform}:]min[-max]")
	ycsb2Cmd.Flags().StringVar(
		&ycsb2Config.keys, "keys", "zipf", "latest, uniform, or zipf")
	ycsb2Cmd.Flags().IntVar(
		&ycsb2Config.initialKeys, "initial-keys", 10000,
		"initial number of keys to insert before beginning workload")
	ycsb2Cmd.Flags().Uint64VarP(
		&ycsb2Config.numOps, "num-ops", "n", 0,
		"maximum number of operations (0 means unlimited)")
	ycsb2Cmd.Flags().StringVar(
		&ycsb2Config.scans, "scans", "zipf:1-1000",
		"scan length distribution [{zipf,uniform}:]min[-max]")
	ycsb2Cmd.Flags().StringVar(
		&ycsb2Config.workload, "workload", "B",
		"workload type (A-F) or spec (read=X,update=Y,...)")
	ycsb2Cmd.Flags().StringVar(
		&ycsb2Config.values, "values", "1000",
		"value size distribution [{zipf,uniform}:]min[-max][/<target-compression>]")
}

type ycsbWeights []float64

func (w ycsbWeights) get(i int) float64 {
	if i >= len(w) {
		return 0
	}
	return w[i]
}

var ycsbWorkloads = map[string]ycsbWeights{
	"A": ycsbWeights{
		ycsbRead:   0.5,
		ycsbUpdate: 0.5,
	},
	"B": ycsbWeights{
		ycsbRead:   0.95,
		ycsbUpdate: 0.05,
	},
	"C": ycsbWeights{
		ycsbRead: 1.0,
	},
	"D": ycsbWeights{
		ycsbInsert: 0.05,
		ycsbRead:   0.95,
		// TODO(peter): default to skewed-latest distribution.
	},
	"E": ycsbWeights{
		ycsbInsert: 0.05,
		ycsbScan:   0.95,
	},
	"F": ycsbWeights{
		ycsbInsert: 1.0,
		// TODO(peter): the real workload is read-modify-write.
	},
}

func ycsbParseWorkload(w string) (ycsbWeights, error) {
	if weights := ycsbWorkloads[w]; weights != nil {
		return weights, nil
	}
	iWeights := make([]int, 4)
	for _, p := range strings.Split(w, ",") {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed weights: %s", w)
		}
		weight, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
		switch parts[0] {
		case "insert":
			iWeights[ycsbInsert] = weight
		case "read":
			iWeights[ycsbRead] = weight
		case "scan":
			iWeights[ycsbScan] = weight
		case "update":
			iWeights[ycsbUpdate] = weight
		}
	}

	var sum int
	for _, w := range iWeights {
		sum += w
	}
	if sum == 0 {
		return nil, fmt.Errorf("zero weight specified: %s", w)
	}

	weights := make(ycsbWeights, 4)
	for i := range weights {
		weights[i] = float64(iWeights[i]) / float64(sum)
	}
	return weights, nil
}

func ycsbParseKeyDist(d string) (randvar.Dynamic, error) {
	switch strings.ToLower(d) {
	case "latest":
		return randvar.NewDefaultSkewedLatest(nil)
	case "uniform":
		return randvar.NewUniform(nil, 1, uint64(ycsb2Config.initialKeys)), nil
	case "zipf":
		return randvar.NewZipf(nil, 1, uint64(ycsb2Config.initialKeys), 0.99)
	default:
		return nil, fmt.Errorf("unknown distribution: %s", d)
	}
}

func runYcsb2(cmd *cobra.Command, args []string) error {
	weights, err := ycsbParseWorkload(ycsb2Config.workload)
	if err != nil {
		return err
	}

	keyDist, err := ycsbParseKeyDist(ycsb2Config.keys)
	if err != nil {
		return err
	}

	batchDist, err := parseRandVarSpec(ycsb2Config.batch)
	if err != nil {
		return err
	}

	scanDist, err := parseRandVarSpec(ycsb2Config.scans)
	if err != nil {
		return err
	}

	valueDist, targetCompression, err := parseValuesSpec(ycsb2Config.values)
	if err != nil {
		return err
	}

	y := newYcsb2(weights, keyDist, batchDist, scanDist, valueDist, targetCompression)
	runTest(args[0], test{
		init: y.init,
		tick: y.tick,
		done: y.done,
	})
	return nil
}

type ycsb2 struct {
	writeOpts         *pebble.WriteOptions
	reg               *histogramRegistry
	weights           ycsbWeights
	ops               *randvar.Weighted
	keyDist           randvar.Dynamic
	batchDist         randvar.Static
	scanDist          randvar.Static
	valueDist         randvar.Static
	targetCompression float64
	keyNum            *ackseq.S
	numOps            uint64
	numKeys           [ycsbNumOps]uint64
	prevNumKeys       [ycsbNumOps]uint64
	opsMap            map[string]int
	latency           [ycsbNumOps]*namedHistogram
}

func newYcsb2(
	weights ycsbWeights,
	keyDist randvar.Dynamic,
	batchDist, scanDist, valueDist randvar.Static,
	targetCompression float64,
) *ycsb2 {
	y := &ycsb2{
		reg:               newHistogramRegistry(),
		ops:               randvar.NewWeighted(nil, weights...),
		keyDist:           keyDist,
		batchDist:         batchDist,
		scanDist:          scanDist,
		valueDist:         valueDist,
		targetCompression: targetCompression,
	}
	y.writeOpts = pebble.Sync
	if disableWAL {
		y.writeOpts = pebble.NoSync
	}

	y.opsMap = make(map[string]int)
	maybeRegister := func(op int, name string) *namedHistogram {
		w := weights.get(op)
		if w == 0 {
			return nil
		}
		wstr := fmt.Sprint(int(100 * w))
		fill := strings.Repeat("_", 3-len(wstr))
		if fill == "" {
			fill = "_"
		}
		fullName := fmt.Sprintf("%s%s%s", name, fill, wstr)
		y.opsMap[fullName] = op
		return y.reg.Register(fullName)
	}

	y.latency[ycsbInsert] = maybeRegister(ycsbInsert, "insert")
	y.latency[ycsbRead] = maybeRegister(ycsbRead, "read")
	y.latency[ycsbScan] = maybeRegister(ycsbScan, "scan")
	y.latency[ycsbUpdate] = maybeRegister(ycsbUpdate, "update")
	return y
}

func (y *ycsb2) init(db *pebble.DB, wg *sync.WaitGroup) {
	if ycsb2Config.initialKeys > 0 {
		rng := randvar.NewRand()

		b := db.NewBatch()
		for i := 1; i <= ycsb2Config.initialKeys; i++ {
			if len(b.Repr()) >= 1<<20 {
				if err := b.Commit(y.writeOpts); err != nil {
					log.Fatal(err)
				}
				b = db.NewBatch()
			}
			_ = b.Set(y.makeKey(uint64(i)), y.randBytes(rng), nil)
		}
		if err := b.Commit(y.writeOpts); err != nil {
			log.Fatal(err)
		}

		y.keyNum = ackseq.New(uint64(ycsb2Config.initialKeys))
	}

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go y.run(db, wg)
	}
}

func (y *ycsb2) run(db *pebble.DB, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := randvar.NewRand()
	for {
		start := time.Now()

		op := y.ops.Int()
		switch op {
		case ycsbInsert:
			y.insert(db, rng)
		case ycsbRead:
			y.read(db, rng)
		case ycsbScan:
			y.scan(db, rng)
		case ycsbUpdate:
			y.update(db, rng)
		default:
			panic("not reached")
		}

		y.latency[op].Record(time.Since(start))
		if ycsb2Config.numOps > 0 &&
			atomic.AddUint64(&y.numOps, 1) >= ycsb2Config.numOps {
			break
		}
	}
}

func (y *ycsb2) hashKey(key uint64) uint64 {
	// Inlined version of fnv.New64 + Write.
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211

	h := uint64(offset64)
	for i := 0; i < 8; i++ {
		h *= prime64
		h ^= uint64(key & 0xff)
		key >>= 8
	}
	return h
}

func (y *ycsb2) makeKey(keyNum uint64) []byte {
	key := make([]byte, 4, 24)
	copy(key, "user")
	return strconv.AppendUint(key, y.hashKey(keyNum), 10)
}

func (y *ycsb2) nextReadKey() []byte {
	keyNum := y.hashKey(y.keyDist.Uint64()) % y.keyNum.Base()
	return y.makeKey(keyNum)
}

func (y *ycsb2) randBytes(rng *rand.Rand) []byte {
	length := int(y.valueDist.Uint64())
	return randomBlock(rng, length, length, y.targetCompression)
}

func (y *ycsb2) insert(db *pebble.DB, rng *rand.Rand) {
	count := y.batchDist.Uint64()
	keyNums := make([]uint64, count)

	b := db.NewBatch()
	for i := range keyNums {
		keyNums[i] = y.keyNum.Next()
		_ = b.Set(y.makeKey(keyNums[i]), y.randBytes(rng), nil)
	}
	if err := b.Commit(y.writeOpts); err != nil {
		log.Fatal(err)
	}
	atomic.AddUint64(&y.numKeys[ycsbInsert], uint64(len(keyNums)))

	for i := range keyNums {
		delta, err := y.keyNum.Ack(keyNums[i])
		if err != nil {
			log.Fatal(err)
		}
		for j := 0; j < delta; j++ {
			if err := y.keyDist.IncMax(); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func (y *ycsb2) read(db *pebble.DB, rng *rand.Rand) {
	key := y.nextReadKey()
	iter := db.NewIter(nil)
	iter.SeekGE(key)
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	atomic.AddUint64(&y.numKeys[ycsbRead], 1)
}

func (y *ycsb2) scan(db *pebble.DB, rng *rand.Rand) {
	i, count := 0, y.scanDist.Uint64()
	iter := db.NewIter(nil)
	key := y.nextReadKey()
	for valid := iter.SeekGE([]byte(key)); valid; valid = iter.Next() {
		i++
		if i >= int(count) {
			break
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
	atomic.AddUint64(&y.numKeys[ycsbScan], count)
}

func (y *ycsb2) update(db *pebble.DB, rng *rand.Rand) {
	count := int(y.batchDist.Uint64())
	b := db.NewBatch()
	for i := 0; i < count; i++ {
		_ = b.Set(y.nextReadKey(), y.randBytes(rng), nil)
	}
	if err := b.Commit(y.writeOpts); err != nil {
		log.Fatal(err)
	}
	atomic.AddUint64(&y.numKeys[ycsbUpdate], uint64(count))
}

func (y *ycsb2) tick(elapsed time.Duration, i int) {
	if i%20 == 0 {
		fmt.Println("____optype__elapsed____ops/sec___keys/sec__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	}
	y.reg.Tick(func(tick histogramTick) {
		op := y.opsMap[tick.Name]
		numKeys := atomic.LoadUint64(&y.numKeys[op])
		h := tick.Hist

		fmt.Printf("%10s %8s %10.1f %10.1f %8.1f %8.1f %8.1f %8.1f\n",
			tick.Name,
			time.Duration(elapsed.Seconds()+0.5)*time.Second,
			float64(h.TotalCount())/tick.Elapsed.Seconds(),
			float64(numKeys-y.prevNumKeys[op])/tick.Elapsed.Seconds(),
			time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
		)

		y.prevNumKeys[op] = numKeys
	})
}

func (y *ycsb2) done(elapsed time.Duration) {
	fmt.Println("\n____optype__elapsed_____ops(total)___ops/sec(cum)__keys/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	y.reg.Tick(func(tick histogramTick) {
		op := y.opsMap[tick.Name]
		numKeys := atomic.LoadUint64(&y.numKeys[op])
		h := tick.Cumulative

		fmt.Printf("%10s %7.1fs %14d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
			tick.Name, elapsed.Seconds(), h.TotalCount(),
			float64(h.TotalCount())/elapsed.Seconds(),
			float64(numKeys)/elapsed.Seconds(),
			time.Duration(h.Mean()).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
	})
	fmt.Println()
}

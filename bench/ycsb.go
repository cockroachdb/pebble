// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"fmt"
	"log"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/ackseq"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/cockroachdb/pebble/internal/rate"
)

const (
	ycsbInsert = iota
	ycsbRead
	ycsbScan
	ycsbReverseScan
	ycsbUpdate
	ycsbNumOps
)

// YCSBConfig configures the YCSB benchmark workload.
type YCSBConfig struct {
	Batch            randvar.Static
	Keys             string
	InitialKeys      int
	PrepopulatedKeys int
	NumOps           uint64
	Scans            randvar.Static
	Values           randvar.StaticBytes
	Workload         string
}

// DefaultYCSBConfig returns a YCSBConfig populated with the same defaults as
// the cmd/pebble CLI.
func DefaultYCSBConfig() YCSBConfig {
	return YCSBConfig{
		Batch:       randvar.NewFlag("1"),
		Keys:        "zipf",
		InitialKeys: 10000,
		Scans:       randvar.NewFlag("zipf:1-1000"),
		Workload:    "B",
		Values:      randvar.NewBytesFlag("1000"),
	}
}

type ycsbWeights []float64

func (w ycsbWeights) get(i int) float64 {
	if i >= len(w) {
		return 0
	}
	return w[i]
}

var ycsbWorkloads = map[string]ycsbWeights{
	"A": {
		ycsbRead:   0.5,
		ycsbUpdate: 0.5,
	},
	"B": {
		ycsbRead:   0.95,
		ycsbUpdate: 0.05,
	},
	"C": {
		ycsbRead: 1.0,
	},
	"D": {
		ycsbInsert: 0.05,
		ycsbRead:   0.95,
		// TODO(peter): default to skewed-latest distribution.
	},
	"E": {
		ycsbInsert: 0.05,
		ycsbScan:   0.95,
	},
	"F": {
		ycsbInsert: 1.0,
		// TODO(peter): the real workload is read-modify-write.
	},
}

func ycsbParseWorkload(w string) (ycsbWeights, error) {
	if weights := ycsbWorkloads[w]; weights != nil {
		return weights, nil
	}
	iWeights := make([]int, ycsbNumOps)
	for p := range strings.SplitSeq(w, ",") {
		parts := strings.Split(p, "=")
		if len(parts) != 2 {
			return nil, errors.Errorf("malformed weights: %s", errors.Safe(w))
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
		case "rscan":
			iWeights[ycsbReverseScan] = weight
		case "update":
			iWeights[ycsbUpdate] = weight
		}
	}

	var sum int
	for _, w := range iWeights {
		sum += w
	}
	if sum == 0 {
		return nil, errors.Errorf("zero weight specified: %s", errors.Safe(w))
	}

	weights := make(ycsbWeights, ycsbNumOps)
	for i := range weights {
		weights[i] = float64(iWeights[i]) / float64(sum)
	}
	return weights, nil
}

func ycsbParseKeyDist(d string, cfg *YCSBConfig) (randvar.Dynamic, error) {
	totalKeys := uint64(cfg.InitialKeys + cfg.PrepopulatedKeys)
	switch strings.ToLower(d) {
	case "latest":
		return randvar.NewDefaultSkewedLatest()
	case "uniform":
		return randvar.NewUniform(1, totalKeys), nil
	case "zipf":
		return randvar.NewZipf(1, totalKeys, 0.99)
	default:
		return nil, errors.Errorf("unknown distribution: %s", errors.Safe(d))
	}
}

// RunYCSB runs the YCSB benchmark with the provided configuration.
func RunYCSB(dir string, common *CommonConfig, cfg *YCSBConfig) error {
	if common.Wipe && cfg.PrepopulatedKeys > 0 {
		return errors.New("--wipe and --prepopulated-keys both specified which is nonsensical")
	}

	weights, err := ycsbParseWorkload(cfg.Workload)
	if err != nil {
		return err
	}

	keyDist, err := ycsbParseKeyDist(cfg.Keys, cfg)
	if err != nil {
		return err
	}

	y := newYcsb(common, cfg, weights, keyDist, cfg.Batch, cfg.Scans, cfg.Values)
	RunTest(dir, common, Test{
		Init: y.init,
		Tick: y.tick,
		Done: y.done,
	})
	return nil
}

type ycsbBuf struct {
	rng      *rand.Rand
	keyBuf   []byte
	valueBuf []byte
	keyNums  []uint64
}

type ycsb struct {
	common       *CommonConfig
	cfg          *YCSBConfig
	db           DB
	writeOpts    *pebble.WriteOptions
	weights      ycsbWeights
	reg          *histogramRegistry
	keyDist      randvar.Dynamic
	batchDist    randvar.Static
	scanDist     randvar.Static
	valueDist    randvar.StaticBytes
	readAmpCount atomic.Uint64
	readAmpSum   atomic.Uint64
	keyNum       *ackseq.S
	numOps       atomic.Uint64
	limiter      *rate.Limiter
	opsMap       map[string]int
}

func newYcsb(
	common *CommonConfig,
	cfg *YCSBConfig,
	weights ycsbWeights,
	keyDist randvar.Dynamic,
	batchDist, scanDist randvar.Static,
	valueDist randvar.StaticBytes,
) *ycsb {
	y := &ycsb{
		common:    common,
		cfg:       cfg,
		reg:       newHistogramRegistry(),
		weights:   weights,
		keyDist:   keyDist,
		batchDist: batchDist,
		scanDist:  scanDist,
		valueDist: valueDist,
		opsMap:    make(map[string]int),
	}
	y.writeOpts = pebble.Sync
	if common.DisableWAL {
		y.writeOpts = pebble.NoSync
	}

	ops := map[string]int{
		"insert": ycsbInsert,
		"read":   ycsbRead,
		"rscan":  ycsbReverseScan,
		"scan":   ycsbScan,
		"update": ycsbUpdate,
	}
	for name, op := range ops {
		w := y.weights.get(op)
		if w == 0 {
			continue
		}
		wstr := fmt.Sprint(int(100 * w))
		fill := strings.Repeat("_", 3-len(wstr))
		if fill == "" {
			fill = "_"
		}
		fullName := fmt.Sprintf("%s%s%s", name, fill, wstr)
		y.opsMap[fullName] = op
	}
	return y
}

// loadInitial inserts y.cfg.InitialKeys into db (if > 0) and waits for
// compactions to stabilize. It does not start any worker goroutines and is
// safe to call without a subsequent y.init.
func (y *ycsb) loadInitial(db DB) {
	if y.cfg.InitialKeys <= 0 {
		return
	}
	buf := &ycsbBuf{rng: randvar.NewRand()}

	b := db.NewBatch()
	size := 0
	start := time.Now()
	last := start
	for i := 1; i <= y.cfg.InitialKeys; i++ {
		if now := time.Now(); now.Sub(last) >= time.Second {
			fmt.Printf("%5s inserted %d keys (%0.1f%%)\n",
				time.Duration(now.Sub(start).Seconds()+0.5)*time.Second,
				i-1, 100*float64(i-1)/float64(y.cfg.InitialKeys))
			last = now
		}
		if size >= 1<<20 {
			if err := b.Commit(y.writeOpts); err != nil {
				log.Fatal(err)
			}
			b = db.NewBatch()
			size = 0
		}
		key := y.makeKey(uint64(i+y.cfg.PrepopulatedKeys), buf)
		value := y.randBytes(buf)
		if err := b.Set(key, value, nil); err != nil {
			log.Fatal(err)
		}
		size += len(key) + len(value)
	}
	if err := b.Commit(y.writeOpts); err != nil {
		log.Fatal(err)
	}
	_ = b.Close()
	fmt.Printf("inserted keys [%d-%d)\n",
		1+y.cfg.PrepopulatedKeys,
		1+y.cfg.PrepopulatedKeys+y.cfg.InitialKeys)

	// Flush so the resulting on-disk state is self-contained (no pending
	// WAL); otherwise reopening the directory in read-only mode would fail.
	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}

	// Wait for compactions to stabilize.
	fmt.Println("waiting for compactions to stabilize...")
	m := db.Metrics()
	for {
		maxScore := 0.0
		for i := range m.Levels {
			maxScore = max(maxScore, m.Levels[i].Score)
		}
		if maxScore <= 1.05 && m.Compact.NumInProgress == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
		m = db.Metrics()
	}
	fmt.Println(m.String())
}

func (y *ycsb) init(db DB, wg *sync.WaitGroup) {
	y.db = db
	y.loadInitial(db)
	y.keyNum = ackseq.New(uint64(y.cfg.InitialKeys + y.cfg.PrepopulatedKeys))

	y.limiter = y.common.RateLimiter

	// If this workload doesn't produce reads, sample the worst case read-amp
	// from Metrics() periodically.
	if y.weights.get(ycsbRead) == 0 && y.weights.get(ycsbScan) == 0 && y.weights.get(ycsbReverseScan) == 0 {
		wg.Go(func() { y.sampleReadAmp(db) })
	}

	for range y.common.Concurrency {
		wg.Go(func() { y.run(db) })
	}
}

func (y *ycsb) run(db DB) {
	var latency [ycsbNumOps]*namedHistogram
	for name, op := range y.opsMap {
		latency[op] = y.reg.Register(name)
	}

	buf := &ycsbBuf{rng: randvar.NewRand()}

	ops := randvar.NewWeighted(nil, y.weights...)
	for {
		wait(y.limiter)

		start := time.Now()

		op := ops.Int()
		switch op {
		case ycsbInsert:
			y.insert(db, buf)
		case ycsbRead:
			y.read(db, buf)
		case ycsbScan:
			y.scan(db, buf, false /* reverse */)
		case ycsbReverseScan:
			y.scan(db, buf, true /* reverse */)
		case ycsbUpdate:
			y.update(db, buf)
		default:
			panic(errors.AssertionFailedf("not reached"))
		}

		latency[op].Record(time.Since(start))
		if y.cfg.NumOps > 0 && y.numOps.Add(1) >= y.cfg.NumOps {
			break
		}
	}
}

func (y *ycsb) sampleReadAmp(db DB) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for range ticker.C {
		m := db.Metrics()
		y.readAmpCount.Add(1)
		y.readAmpSum.Add(uint64(m.ReadAmp()))
		if y.cfg.NumOps > 0 && y.numOps.Load() >= y.cfg.NumOps {
			break
		}
	}
}

func (y *ycsb) hashKey(key uint64) uint64 {
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

func (y *ycsb) makeKey(keyNum uint64, buf *ycsbBuf) []byte {
	const size = 24 + 10
	if cap(buf.keyBuf) < size {
		buf.keyBuf = make([]byte, size)
	}
	key := buf.keyBuf[:4]
	copy(key, "user")
	key = strconv.AppendUint(key, y.hashKey(keyNum), 10)
	// Use the MVCC encoding for keys. This appends a timestamp with
	// walltime=1. That knowledge is utilized by rocksDB.Scan.
	key = append(key, '\x00', '\x00', '\x00', '\x00', '\x00',
		'\x00', '\x00', '\x00', '\x01', '\x09')
	buf.keyBuf = key
	return key
}

func (y *ycsb) nextReadKey(buf *ycsbBuf) []byte {
	keyNum := y.keyDist.Uint64(buf.rng)
	return y.makeKey(keyNum, buf)
}

func (y *ycsb) randBytes(buf *ycsbBuf) []byte {
	buf.valueBuf = y.valueDist.Bytes(buf.rng, buf.valueBuf)
	return buf.valueBuf
}

func (y *ycsb) insert(db DB, buf *ycsbBuf) {
	count := y.batchDist.Uint64(buf.rng)
	if cap(buf.keyNums) < int(count) {
		buf.keyNums = make([]uint64, count)
	}
	keyNums := buf.keyNums[:count]

	b := db.NewBatch()
	for i := range keyNums {
		keyNums[i] = y.keyNum.Next()
		_ = b.Set(y.makeKey(keyNums[i], buf), y.randBytes(buf), nil)
	}
	if err := b.Commit(y.writeOpts); err != nil {
		log.Fatal(err)
	}
	_ = b.Close()

	for i := range keyNums {
		delta, err := y.keyNum.Ack(keyNums[i])
		if err != nil {
			log.Fatal(err)
		}
		if delta > 0 {
			y.keyDist.IncMax(uint64(delta))
		}
	}
}

func (y *ycsb) read(db DB, buf *ycsbBuf) {
	key := y.nextReadKey(buf)
	iter := db.NewIter(nil)
	iter.SeekGE(key)
	if iter.Valid() {
		_ = iter.Key()
		_ = iter.Value()
	}

	type metrics interface {
		Metrics() pebble.IteratorMetrics
	}
	if m, ok := iter.(metrics); ok {
		y.readAmpCount.Add(1)
		y.readAmpSum.Add(uint64(m.Metrics().ReadAmp))
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

func (y *ycsb) scan(db DB, buf *ycsbBuf, reverse bool) {
	count := y.scanDist.Uint64(buf.rng)
	key := y.nextReadKey(buf)
	iter := db.NewIter(nil)
	if err := db.Scan(iter, key, int64(count), reverse); err != nil {
		log.Fatal(err)
	}

	type metrics interface {
		Metrics() pebble.IteratorMetrics
	}
	if m, ok := iter.(metrics); ok {
		y.readAmpCount.Add(1)
		y.readAmpSum.Add(uint64(m.Metrics().ReadAmp))
	}

	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}
}

func (y *ycsb) update(db DB, buf *ycsbBuf) {
	count := int(y.batchDist.Uint64(buf.rng))
	b := db.NewBatch()
	for i := 0; i < count; i++ {
		_ = b.Set(y.nextReadKey(buf), y.randBytes(buf), nil)
	}
	if err := b.Commit(y.writeOpts); err != nil {
		log.Fatal(err)
	}
	_ = b.Close()
}

func (y *ycsb) tick(elapsed time.Duration, i int) {
	if i%20 == 0 {
		fmt.Println("____optype__elapsed__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	}
	y.reg.Tick(func(tick histogramTick) {
		h := tick.Hist

		fmt.Printf("%10s %8s %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
			tick.Name,
			time.Duration(elapsed.Seconds()+0.5)*time.Second,
			float64(h.TotalCount())/tick.Elapsed.Seconds(),
			float64(tick.Cumulative.TotalCount())/elapsed.Seconds(),
			time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
		)
	})
}

func (y *ycsb) done(elapsed time.Duration) {
	fmt.Println("\n____optype__elapsed_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")

	resultTick := histogramTick{}
	y.reg.Tick(func(tick histogramTick) {
		h := tick.Cumulative
		if resultTick.Cumulative == nil {
			resultTick.Now = tick.Now
			resultTick.Cumulative = h
		} else {
			resultTick.Cumulative.Merge(h)
		}

		fmt.Printf("%10s %7.1fs %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
			tick.Name, elapsed.Seconds(), h.TotalCount(),
			float64(h.TotalCount())/elapsed.Seconds(),
			time.Duration(h.Mean()).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(100)).Seconds()*1000)
	})
	fmt.Println()

	resultHist := resultTick.Cumulative
	m := y.db.Metrics()
	total := m.Total()

	readAmpCount := y.readAmpCount.Load()
	readAmpSum := y.readAmpSum.Load()
	if readAmpCount == 0 {
		readAmpSum = 0
		readAmpCount = 1
	}

	fmt.Printf("Benchmarkycsb/%s/values=%s %d  %0.1f ops/sec  %d read  %d write  %.2f r-amp  %0.2f w-amp\n\n",
		y.cfg.Workload, y.cfg.Values,
		resultHist.TotalCount(),
		float64(resultHist.TotalCount())/elapsed.Seconds(),
		total.TableBytesRead,
		total.TablesFlushed.Bytes+total.TablesCompacted.Bytes+total.BlobBytesFlushed+total.BlobBytesCompacted,
		float64(readAmpSum)/float64(readAmpCount),
		total.WriteAmp(),
	)
}

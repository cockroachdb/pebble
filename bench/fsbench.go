// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bench

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

const writeBatchSize = 1 << 10

// FsBenchConfig configures the file system benchmark runner.
type FsBenchConfig struct {
	// MaxOps is an upper limit on the number of operations to run, 0 = no limit.
	MaxOps int
	// BenchName selects which entry of FsBenchmarks to run.
	BenchName string
	// NumTimes is how many times to repeat the selected benchmark.
	NumTimes int
	// FS is the filesystem to benchmark. If nil, vfs.Default is used.
	FS vfs.FS
	// Verbose enables noisy logging in helpers.
	Verbose bool
}

// DefaultFsBenchConfig returns an FsBenchConfig with the same defaults as the
// cmd/pebble CLI.
func DefaultFsBenchConfig() FsBenchConfig {
	return FsBenchConfig{
		NumTimes: 1,
		FS:       vfs.Default,
	}
}

// fsEnv bundles the file system and precomputed buffers that benchmark
// closures need to share.
type fsEnv struct {
	fs                    vfs.FS
	precomputedWriteBatch []byte
	verbose               bool
}

func newFsEnv(cfg *FsBenchConfig) *fsEnv {
	fs := cfg.FS
	if fs == nil {
		fs = vfs.Default
	}
	return &fsEnv{
		fs:                    fs,
		precomputedWriteBatch: bytes.Repeat([]byte("a"), writeBatchSize),
		verbose:               cfg.Verbose,
	}
}

// FsBenchmark describes one entry in the FsBenchmarks registry.
type FsBenchmark struct {
	createBench func(string) *fsBench
	Name        string
	Description string
}

// FsBenchmarks returns the map of available file system benchmarks.
func FsBenchmarks(fs vfs.FS) map[string]FsBenchmark {
	cfg := &FsBenchConfig{FS: fs}
	env := newFsEnv(cfg)
	return map[string]FsBenchmark{
		"create_empty": env.createBench("create_empty", "create empty file, sync par dir"),
		"delete_10k_2MiB": env.deleteUniformBench(
			"delete_10k_2MiB", "create 10k 2MiB size files, measure deletion times", 10_000, 2<<20,
		),
		"delete_100k_2MiB": env.deleteUniformBench(
			"delete_100k_2MiB", "create 100k 2MiB size files, measure deletion times", 100_000, 2<<20,
		),
		"delete_200k_2MiB": env.deleteUniformBench(
			"delete_200k_2MiB", "create 200k 2MiB size files, measure deletion times", 200_000, 2<<20,
		),
		"write_sync_1MiB": env.writeSyncBench(
			"write_sync_1MiB", "Write 1MiB to a file, then sync, while timing the sync.", 2<<30, 1<<20,
		),
		"write_sync_16MiB": env.writeSyncBench(
			"write_sync_16MiB", "Write 16MiB to a file, then sync, while timing the sync.", 2<<30, 16<<20,
		),
		"write_sync_128MiB": env.writeSyncBench(
			"write_sync_128MiB", "Write 128MiB to a file, then sync, while timing the sync.", 2<<30, 128<<20,
		),
		"disk_usage_128MB": env.diskUsageBench(
			"disk_usage_128MB",
			"Write 128MiB to a file, measure GetDiskUsage call. Create a new file, when file size is 1GB.",
			1<<30, 128<<20,
		),
		"disk_usage_many_files": env.diskUsageBench(
			"disk_usage_many_files",
			"Create new file, Write 128KiB to a file, measure GetDiskUsage call.",
			128<<10, 128<<10,
		),
		"delete_large_dir_256MiB": env.deleteBench(
			"delete_large_dir_256MiB", "Prepopulate directory with 100k 1MiB files, measure delete peformance of 256MiB files",
			1e5, 1<<20, 256<<20,
		),
		"delete_large_dir_2MiB": env.deleteBench(
			"delete_large_dir_2MiB", "Prepopulate directory with 100k 1MiB files, measure delete peformance of 2MiB files",
			1e5, 1<<20, 2<<20,
		),
		"delete_small_dir_2GiB": env.deleteBench(
			"delete_small_dir_2GiB", "Prepopulate directory with 1k 1MiB files, measure delete peformance of 2GiB files",
			1e3, 1<<20, 2<<30,
		),
		"delete_small_dir_256MiB": env.deleteBench(
			"delete_small_dir_256MiB", "Prepopulate directory with 1k 1MiB files, measure delete peformance of 256MiB files",
			1e3, 1<<20, 256<<20,
		),
		"delete_small_dir_2MiB": env.deleteBench(
			"delete_small_dir_2MiB", "Prepopulate directory with 1k 1MiB files, measure delete peformance of 2MiB files",
			1e3, 1<<20, 2<<20,
		),
	}
}

// State relevant to a benchmark.
type fsBench struct {
	env         *fsEnv
	name        string
	description string

	numOps  int
	dir     vfs.File
	dirName string

	reg *histogramRegistry

	run func(*namedHistogram) bool

	stop func()

	clean func()
}

// Helpers ------------------------------------------------------------------

func (e *fsEnv) createFile(filepath string) vfs.File {
	fh, err := e.fs.Create(filepath, vfs.WriteCategoryUnspecified)
	if err != nil {
		log.Fatalln(err)
	}
	return fh
}

func (e *fsEnv) deleteFile(filepath string) {
	if err := e.fs.Remove(filepath); err != nil {
		log.Fatalln(err)
	}
}

func (e *fsEnv) writeToFile(fh vfs.File, size int64) {
	for size > 0 {
		var toWrite []byte
		if size >= writeBatchSize {
			toWrite = e.precomputedWriteBatch
		} else {
			toWrite = e.precomputedWriteBatch[:size]
		}
		written, err := fh.Write(toWrite)
		if err != nil {
			log.Fatalln(err)
		}
		if written != len(toWrite) {
			log.Fatalf("Couldn't write %d bytes to file\n", size)
		}
		size -= int64(len(toWrite))
	}
}

func (e *fsEnv) syncFile(fh vfs.File) {
	if err := fh.Sync(); err != nil {
		log.Fatalln(err)
	}
}

func (e *fsEnv) closeFile(fh vfs.File) {
	if err := fh.Close(); err != nil {
		log.Fatalln(err)
	}
}

func (e *fsEnv) getDiskUsage(filepath string) {
	if _, err := e.fs.GetDiskUsage(filepath); err != nil {
		log.Fatalln(err)
	}
}

func (e *fsEnv) openDir(filepath string) vfs.File {
	fh, err := e.fs.OpenDir(filepath)
	if err != nil {
		log.Fatalln(err)
	}
	return fh
}

func (e *fsEnv) mkDir(filepath string) {
	if err := e.fs.MkdirAll(filepath, 0755); err != nil {
		log.Fatalln(err)
	}
}

func (e *fsEnv) removeAllFiles(filepath string) {
	if err := e.fs.RemoveAll(filepath); err != nil {
		log.Fatalln(err)
	}
}

func (e *fsEnv) verbosef(fmtstr string, args ...interface{}) {
	if e.verbose {
		fmt.Printf(fmtstr, args...)
	}
}

func (e *fsEnv) removeAll(dir string) {
	e.verbosef("Removing %q.\n", dir)
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
}

// Benchmarks ---------------------------------------------------------------

func (e *fsEnv) createBench(benchName string, benchDescription string) FsBenchmark {
	createBench := func(dirpath string) *fsBench {
		bench := &fsBench{env: e}
		e.mkDir(dirpath)
		fh := e.openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		pref := "temp_"
		var numFiles int
		var done atomic.Bool

		bench.run = func(hist *namedHistogram) bool {
			if done.Load() {
				return false
			}

			start := time.Now()
			fh := e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, numFiles)))
			e.syncFile(bench.dir)
			hist.Record(time.Since(start))

			e.closeFile(fh)
			numFiles++
			return true
		}

		bench.stop = func() { done.Store(true) }

		bench.clean = func() {
			e.removeAllFiles(dirpath)
			e.closeFile(bench.dir)
		}

		return bench
	}

	return FsBenchmark{createBench, benchName, benchDescription}
}

func (e *fsEnv) deleteBench(
	benchName string, benchDescription string, preNumFiles int, preFileSize int64, fileSize int64,
) FsBenchmark {
	createBench := func(dirpath string) *fsBench {
		bench := &fsBench{env: e}
		e.mkDir(dirpath)
		fh := e.openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		prePref := "pre_temp_"
		for i := 0; i < preNumFiles; i++ {
			fh := e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", prePref, i)))
			if preFileSize > 0 {
				e.writeToFile(fh, preFileSize)
				e.syncFile(fh)
			}
			e.closeFile(fh)
		}
		e.syncFile(bench.dir)

		var done atomic.Bool
		bench.run = func(hist *namedHistogram) bool {
			if done.Load() {
				return false
			}

			filename := "newfile"
			fh := e.createFile(path.Join(dirpath, filename))
			e.writeToFile(fh, fileSize)
			e.syncFile(fh)

			start := time.Now()
			e.deleteFile(path.Join(dirpath, filename))
			hist.Record(time.Since(start))

			return true
		}

		bench.stop = func() { done.Store(true) }

		bench.clean = func() {
			e.removeAllFiles(dirpath)
			e.closeFile(bench.dir)
		}

		return bench
	}

	return FsBenchmark{createBench, benchName, benchDescription}
}

func (e *fsEnv) deleteUniformBench(
	benchName string, benchDescription string, numFiles int, fileSize int64,
) FsBenchmark {
	createBench := func(dirpath string) *fsBench {
		bench := &fsBench{env: e}
		e.mkDir(dirpath)
		fh := e.openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		pref := "temp_"
		for i := 0; i < numFiles; i++ {
			fh := e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, i)))
			if fileSize > 0 {
				e.writeToFile(fh, fileSize)
				e.syncFile(fh)
			}
			e.closeFile(fh)
		}
		e.syncFile(bench.dir)

		var done atomic.Bool
		bench.run = func(hist *namedHistogram) bool {
			if done.Load() {
				return false
			}

			if numFiles == 0 {
				return false
			}

			start := time.Now()
			e.deleteFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, numFiles-1)))
			hist.Record(time.Since(start))

			numFiles--
			return true
		}

		bench.stop = func() { done.Store(true) }

		bench.clean = func() {
			e.removeAll(dirpath)
			e.closeFile(bench.dir)
		}

		return bench
	}

	return FsBenchmark{createBench, benchName, benchDescription}
}

func (e *fsEnv) writeSyncBench(
	benchName string, benchDescription string, maxFileSize int64, writeSize int64,
) FsBenchmark {
	if writeSize > maxFileSize {
		log.Fatalln("File write threshold is greater than max file size.")
	}

	createBench := func(dirpath string) *fsBench {
		bench := &fsBench{env: e}
		e.mkDir(dirpath)
		fh := e.openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		pref := "temp_"
		var benchData struct {
			done         atomic.Bool
			fh           vfs.File
			fileNum      int
			bytesWritten int64
		}
		benchData.fh = e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, benchData.fileNum)))

		bench.run = func(hist *namedHistogram) bool {
			if benchData.done.Load() {
				return false
			}

			if benchData.bytesWritten+writeSize > maxFileSize {
				e.closeFile(benchData.fh)
				benchData.fileNum++
				benchData.bytesWritten = 0
				benchData.fh = e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, benchData.fileNum)))
			}

			benchData.bytesWritten += writeSize
			e.writeToFile(benchData.fh, writeSize)

			start := time.Now()
			e.syncFile(benchData.fh)
			hist.Record(time.Since(start))

			return true
		}

		bench.stop = func() { benchData.done.Store(true) }

		bench.clean = func() {
			e.closeFile(benchData.fh)
			e.removeAllFiles(dirpath)
			e.closeFile(bench.dir)
		}

		return bench
	}

	return FsBenchmark{createBench, benchName, benchDescription}
}

func (e *fsEnv) diskUsageBench(
	benchName string, benchDescription string, maxFileSize int64, writeSize int64,
) FsBenchmark {
	if writeSize > maxFileSize {
		log.Fatalln("File write threshold is greater than max file size.")
	}

	createBench := func(dirpath string) *fsBench {
		bench := &fsBench{env: e}
		e.mkDir(dirpath)
		fh := e.openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		pref := "temp_"
		var benchData struct {
			done         atomic.Bool
			fh           vfs.File
			fileNum      int
			bytesWritten int64
		}
		benchData.fh = e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, benchData.fileNum)))

		bench.run = func(hist *namedHistogram) bool {
			if benchData.done.Load() {
				return false
			}

			if benchData.bytesWritten+writeSize > maxFileSize {
				e.closeFile(benchData.fh)
				benchData.fileNum++
				benchData.bytesWritten = 0
				benchData.fh = e.createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, benchData.fileNum)))
			}

			benchData.bytesWritten += writeSize
			e.writeToFile(benchData.fh, writeSize)
			e.syncFile(benchData.fh)

			start := time.Now()
			e.getDiskUsage(dirpath)
			hist.Record(time.Since(start))

			return true
		}

		bench.stop = func() { benchData.done.Store(true) }

		bench.clean = func() {
			e.closeFile(benchData.fh)
			e.removeAllFiles(dirpath)
			e.closeFile(bench.dir)
		}

		return bench
	}

	return FsBenchmark{createBench, benchName, benchDescription}
}

// RunFsBench runs the file system benchmark named in cfg.BenchName.
func RunFsBench(dir string, common *CommonConfig, cfg *FsBenchConfig) error {
	fs := cfg.FS
	if fs == nil {
		fs = vfs.Default
	}
	benchmarks := FsBenchmarks(fs)
	benchmark, ok := benchmarks[cfg.BenchName]
	if !ok {
		return errors.Errorf("trying to run an unknown benchmark: %s", cfg.BenchName)
	}

	numTimes := cfg.NumTimes
	if numTimes <= 0 {
		numTimes = 1
	}

	fmt.Printf("The benchmark will be run %d time(s).\n", numTimes)
	for i := 0; i < numTimes; i++ {
		fmt.Println("Starting benchmark:", i)
		benchStruct := benchmark.createBench(dir)
		RunTestWithoutDB(common, TestWithoutDB{
			Init: func(wg *sync.WaitGroup) { benchStruct.init(wg, cfg.MaxOps) },
			Tick: func(elapsed time.Duration, i int) { benchStruct.tick(elapsed, i) },
			Done: func(wg *sync.WaitGroup, elapsed time.Duration) {
				benchStruct.done(wg, elapsed)
			},
		})
	}
	return nil
}

func (bench *fsBench) init(wg *sync.WaitGroup, maxOps int) {
	fmt.Println("Running benchmark:", bench.name)
	fmt.Println("Description:", bench.description)
	wg.Go(func() { bench.execute(maxOps) })
}

func (bench *fsBench) execute(maxOps int) {
	latencyHist := bench.reg.Register(bench.name)

	for {
		bench.numOps++
		continueBench := bench.run(latencyHist)
		if !continueBench || (maxOps > 0 && bench.numOps >= maxOps) {
			break
		}
	}
}

func (bench *fsBench) tick(elapsed time.Duration, i int) {
	if i%20 == 0 {
		fmt.Println("____optype__elapsed__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)__pMax(ms)")
	}
	bench.reg.Tick(func(tick histogramTick) {
		h := tick.Hist

		fmt.Printf("%10s %8s %14.1f %14.1f %5.6f %5.6f %5.6f %5.6f\n",
			tick.Name[:10],
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

func (bench *fsBench) done(wg *sync.WaitGroup, elapsed time.Duration) {
	bench.stop()
	wg.Wait()
	defer bench.clean()

	fmt.Println("\n____optype__elapsed_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)__pMax(ms)")

	resultTick := histogramTick{}
	bench.reg.Tick(func(tick histogramTick) {
		h := tick.Cumulative
		if resultTick.Cumulative == nil {
			resultTick.Now = tick.Now
			resultTick.Cumulative = h
		} else {
			resultTick.Cumulative.Merge(h)
		}

		fmt.Printf("%10s %7.1fs %14d %14.1f %5.6f %5.6f %5.6f %5.6f %5.6f\n",
			tick.Name[:10], elapsed.Seconds(), h.TotalCount(),
			float64(h.TotalCount())/elapsed.Seconds(),
			time.Duration(h.Mean()).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(h.ValueAtQuantile(100)).Seconds()*1000,
		)
	})
	fmt.Println()

	resultHist := resultTick.Cumulative

	fmt.Printf("Benchmarkfsbench/%s  %d %0.1f ops/sec\n\n",
		bench.name,
		resultHist.TotalCount(),
		float64(resultHist.TotalCount())/elapsed.Seconds(),
	)
}

package main

import (
	"fmt"
	"log"
	"path"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

var fsBenchCmd = &cobra.Command{
	Use:   "fs <dir>",
	Short: "Run file system benchmarks.",
	Long: `
Run file system benchmarks. Each benchmark is predefined and can be
run using the command "bench fs <dir> --bench-name <benchmark>".
Each possible <benchmark> which can be run is defined in the code.
Benchmarks may require the specification of a --duration or
--max-ops flag, to prevent the benchmark from running forever
or running out of memory.

The --num-times flag can be used to run the entire benchmark, more than
once. If the flag isn't provided, then the benchmark is only run once.
`,
	Args: cobra.ExactArgs(1),
	RunE: runFsBench,
}

var fsConfig struct {
	// An upper limit on the number of ops which can be run.
	maxOps int

	// Benchmark to run.
	benchname string

	// Number of times each benchmark should be run.
	numTimes int

	fs vfs.FS
}

func init() {
	fsBenchCmd.Flags().IntVar(
		&fsConfig.maxOps, "max-ops", 0,
		"Maximum number of times the operation which is being benchmarked should be run.",
	)

	fsBenchCmd.Flags().StringVar(
		&fsConfig.benchname, "bench-name", "", "The benchmark to run.")
	fsBenchCmd.MarkFlagRequired("bench-name")

	fsBenchCmd.Flags().IntVar(
		&fsConfig.numTimes, "num-times", 1,
		"Number of times each benchmark should be run.")

	// Add subcommand to list
	fsBenchCmd.AddCommand(listFsBench)

	// Just use the default vfs implementation for now.
	fsConfig.fs = vfs.Default
}

// State relevant to a benchmark.
type fsBench struct {
	// A short name for the benchmark.
	name string

	// A one line description for the benchmark.
	description string

	// numOps is the total number of ops which
	// have been run for the benchmark. This is used
	// to make sure that we don't benchmark the operation
	// more than max-ops times.
	numOps int

	// directory under which the benchmark is run.
	dir     vfs.File
	dirName string

	// Stats associated with the benchmark.
	reg *histogramRegistry

	// The operation which we're benchmarking. This
	// will be called over and over again.
	running func(*namedHistogram)

	// A cleanup func which must be called after
	// the benchmark has finished running.
	clean func()
}

// createFile can be used to create an empty file.
// Invariant: File shouldn't already exist.
func createFile(filepath string) vfs.File {
	fh, err := fsConfig.fs.Create(filepath)
	if err != nil {
		log.Fatalln(err)
	}
	return fh
}

// Invariant: file with filepath should exist.
func deleteFile(filepath string) {
	err := fsConfig.fs.Remove(filepath)
	if err != nil {
		log.Fatalln(err)
	}
}

// Write fileSize bytes to the file.
// Note that certain vfs.File implementations could
// end up modifying the byte slice.
func writeToFile(fh vfs.File, bs []byte) {
	written, err := fh.Write(bs)
	if err != nil {
		log.Fatalln(err)
	}
	if written != len(bs) {
		log.Fatalf("Couldn't write %d bytes to file\n", len(bs))
	}
}

func syncFile(fh vfs.File) {
	err := fh.Sync()
	if err != nil {
		log.Fatalln(err)
	}
}

func closeFile(fh vfs.File) {
	err := fh.Close()
	if err != nil {
		log.Fatalln(err)
	}
}

func getDiskUsage(filepath string) {
	_, err := fsConfig.fs.GetDiskUsage(filepath)
	if err != nil {
		log.Fatalln(err)
	}
}

func openDir(filepath string) vfs.File {
	fh, err := fsConfig.fs.OpenDir(filepath)
	if err != nil {
		log.Fatalln(err)
	}
	return fh
}

func mkDir(filepath string) {
	err := fsConfig.fs.MkdirAll(filepath, 0755)
	if err != nil {
		log.Fatalln(err)
	}
}

func removeAllFiles(filepath string) {
	err := fsConfig.fs.RemoveAll(filepath)
	if err != nil {
		log.Fatalln(err)
	}
}

func getByteSlice(size int) []byte {
	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = 'a'
	}
	return b
}

// fileSize is in bytes.
func createBench(benchName string, benchDescription string) fsbenchmark {
	benchFunc := func(dirpath string) *fsBench {
		bench := &fsBench{}
		mkDir(dirpath)
		fh := openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		// setup the operation to benchmark, and the cleanup functions.
		pref := "temp_"
		var mu struct {
			sync.Mutex
			done     bool
			numFiles int
		}

		bench.running = func(hist *namedHistogram) {
			start := time.Now()
			defer func() {
				hist.Record(time.Since(start))
			}()

			mu.Lock()
			defer mu.Unlock()
			if mu.done {
				return
			}

			fh := createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, mu.numFiles)))
			syncFile(bench.dir)
			closeFile(fh)
			mu.numFiles++
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			removeAllFiles(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}

	return fsbenchmark{
		benchFunc,
		benchName,
		benchDescription,
	}
}

// This benchmark prepopulates a directory with some files of a given size. Then, it creates and deletes
// a file of some size, while measuring only the performance of the delete.
func deleteBench(benchName string, benchDescription string, preNumFiles int,
	preFileSize int, fileSize int) fsbenchmark {

	benchFunc := func(dirpath string) *fsBench {
		bench := &fsBench{}
		mkDir(dirpath)
		fh := openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		// prepopulate the directory
		prePref := "pre_temp_"
		for i := 0; i < preNumFiles; i++ {
			fh := createFile(path.Join(dirpath, fmt.Sprintf("%s%d", prePref, i)))
			if preFileSize > 0 {
				writeToFile(fh, getByteSlice(preFileSize))
				syncFile(fh)
			}
			closeFile(fh)
		}
		syncFile(bench.dir)

		// setup operation/clean functions
		var mu struct {
			sync.Mutex
			done bool
		}

		bench.running = func(hist *namedHistogram) {
			mu.Lock()
			defer mu.Unlock()
			if mu.done {
				return
			}

			filename := "newfile"
			fh := createFile(path.Join(dirpath, filename))
			writeToFile(fh, getByteSlice(fileSize))
			syncFile(fh)

			start := time.Now()
			deleteFile(path.Join(dirpath, filename))
			hist.Record(time.Since(start))
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			removeAllFiles(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}

	return fsbenchmark{
		benchFunc,
		benchName,
		benchDescription,
	}
}

// This benchmark creates some files in a directory, and then measures the performance
// of the os.Remove function in Go, which in turn calls the unlink syscall.
// fileSize is in bytes.
func deleteUniformBench(benchName string, benchDescription string, numFiles int, fileSize int) fsbenchmark {
	benchFunc := func(dirpath string) *fsBench {
		bench := &fsBench{}
		mkDir(dirpath)
		fh := openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		// setup the operation to benchmark, and the cleaup functions.
		pref := "temp_"
		for i := 0; i < numFiles; i++ {
			fh := createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, i)))
			if fileSize > 0 {
				writeToFile(fh, getByteSlice(fileSize))
				syncFile(fh)
			}
			closeFile(fh)
		}
		syncFile(bench.dir)

		var mu struct {
			sync.Mutex
			done     bool
			numFiles int
		}
		mu.numFiles = numFiles

		bench.running = func(hist *namedHistogram) {
			start := time.Now()
			defer func() {
				hist.Record(time.Since(start))
			}()

			mu.Lock()
			defer mu.Unlock()
			if mu.done {
				return
			}
			if mu.numFiles == 0 {
				log.Fatalln("No files to delete.")
			}

			deleteFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, mu.numFiles-1)))
			mu.numFiles--
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			removeAll(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}

	return fsbenchmark{
		benchFunc,
		benchName,
		benchDescription,
	}
}

// Tests the performance of syncing data to disk.
// Only measures the sync performance.
// The writes will be synced after every writeSize bytes have been written.
func writeSyncBench(benchName string, benchDescription string,
	maxFileSize int, writeSize int) fsbenchmark {

	if writeSize > maxFileSize {
		log.Fatalln("File write threshold is greater than max file size.")
	}

	benchFunc := func(dirpath string) *fsBench {
		bench := &fsBench{}
		mkDir(dirpath)
		fh := openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		pref := "temp_"
		var mu struct {
			sync.Mutex
			done         bool
			fh           vfs.File
			fileNum      int
			bytesWritten int
		}
		mu.fh = createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, mu.fileNum)))

		bench.running = func(hist *namedHistogram) {
			mu.Lock()
			defer mu.Unlock()
			if mu.done {
				return
			}

			if mu.bytesWritten+writeSize > maxFileSize {
				closeFile(mu.fh)
				mu.fileNum++
				mu.bytesWritten = 0
				mu.fh = createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, mu.fileNum)))
			}

			mu.bytesWritten += writeSize
			writeToFile(mu.fh, getByteSlice(writeSize))

			start := time.Now()
			syncFile(mu.fh)
			hist.Record(time.Since(start))
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			closeFile(mu.fh)
			removeAllFiles(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}

	return fsbenchmark{
		benchFunc,
		benchName,
		benchDescription,
	}
}

// Tests the peformance of calling the fstat sys call on a directory,
// as the number of files/total size of files in the directory grows.
func statfsBench(benchName string, benchDescription string,
	maxFileSize int, writeSize int) fsbenchmark {

	if writeSize > maxFileSize {
		log.Fatalln("File write threshold is greater than max file size.")
	}

	benchFunc := func(dirpath string) *fsBench {
		bench := &fsBench{}
		mkDir(dirpath)
		fh := openDir(dirpath)

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		pref := "temp_"
		var mu struct {
			sync.Mutex
			done         bool
			fh           vfs.File
			fileNum      int
			bytesWritten int
		}
		mu.fh = createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, mu.fileNum)))

		bench.running = func(hist *namedHistogram) {
			mu.Lock()
			defer mu.Unlock()
			if mu.done {
				return
			}

			if mu.bytesWritten+writeSize > maxFileSize {
				closeFile(mu.fh)
				mu.fileNum++
				mu.bytesWritten = 0
				mu.fh = createFile(path.Join(dirpath, fmt.Sprintf("%s%d", pref, mu.fileNum)))
			}

			mu.bytesWritten += writeSize
			writeToFile(mu.fh, getByteSlice(writeSize))
			syncFile(mu.fh)

			start := time.Now()
			getDiskUsage(dirpath)
			hist.Record(time.Since(start))
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			closeFile(mu.fh)
			removeAllFiles(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}

	return fsbenchmark{
		benchFunc,
		benchName,
		benchDescription,
	}
}

// A benchmark is a function which takes a directory
// as input and returns the fsBench struct which has
// all the information required to run the benchmark.
type fsbenchmark struct {
	benchFunc   func(string) *fsBench
	name        string
	description string
}

// The various benchmarks which can be run.
var benchmarks = map[string]fsbenchmark{
	"create_empty": createBench("create_empty", "create empty file, sync par dir"),
	"delete_10k_2MB": deleteUniformBench(
		"delete_10k_2MB", "create 10k 2MB size files, measure deletion times", 10_000, 2_000_000,
	),
	"delete_100k_2MB": deleteUniformBench(
		"delete_100k_2MB", "create 100k 2MB size files, measure deletion times", 100_000, 2_000_000,
	),
	"delete_200k_2MB": deleteUniformBench(
		"delete_200k_2MB", "create 200k 2MB size files, measure deletion times", 200_000, 2_000_000,
	),
	"write_sync_1MB": writeSyncBench(
		"write_sync_1MB", "Write 1MB to a file, then sync, while timing the sync.", 2e9, 1e6,
	),
	"write_sync_10MB": writeSyncBench(
		"write_sync_10MB", "Write 10MB to a file, then sync, while timing the sync.", 2e9, 1e7,
	),
	"write_sync_100MB": writeSyncBench(
		"write_sync_100MB", "Write 100MB to a file, then sync, while timing the sync.", 2e9, 1e8,
	),
	"statfs_100MB": statfsBench(
		"statfs_100MB",
		"Write 100MB to a file, measure statfs call. Create a new file, when file size is 1GB.",
		1e9, 1e8,
	),
	"statfs_many_files": statfsBench(
		"statfs_many_files",
		"Create new file, Write 100KB to a file, measure statfs call.",
		1e5+5, 1e5,
	),
	"delete_large_dir_200MB": deleteBench(
		"delete_large_dir_200MB", "Prepopulate directory with 100k 1MB files, measure delete peformance of 200MB files",
		1e5, 1e6, 2e8,
	),
	"delete_large_dir_2MB": deleteBench(
		"delete_large_dir_2MB", "Prepopulate directory with 100k 1MB files, measure delete peformance of 2MB files",
		1e5, 1e6, 2e6,
	),
	"delete_small_dir_2GB": deleteBench(
		"delete_small_dir_2GB", "Prepopulate directory with 1k 1MB files, measure delete peformance of 2GB files",
		1e3, 1e6, 2e9,
	),
	"delete_small_dir_200MB": deleteBench(
		"delete_small_dir_200MB", "Prepopulate directory with 1k 1MB files, measure delete peformance of 200MB files",
		1e3, 1e6, 2e8,
	),
	"delete_small_dir_2MB": deleteBench(
		"delete_small_dir_2MB", "Prepopulate directory with 1k 1MB files, measure delete peformance of 2MB files",
		1e3, 1e6, 2e6,
	),
}

func runFsBench(_ *cobra.Command, args []string) error {
	benchmark, ok := benchmarks[fsConfig.benchname]
	if !ok {
		log.Fatalln("Trying to run an unknown benchmark.")
	}

	// Run the benchmark a comple of times.
	fmt.Printf("The benchmark will be run %d time(s).\n", fsConfig.numTimes)
	for i := 0; i < fsConfig.numTimes; i++ {
		fmt.Println("Starting benchmark:", i)
		benchStruct := benchmark.benchFunc(args[0])
		runTestWithoutDB(testWithoutDB{
			init: benchStruct.init,
			tick: benchStruct.tick,
			done: benchStruct.done,
		})
	}
	return nil
}

func (bench *fsBench) init(wg *sync.WaitGroup) {
	fmt.Println("Running benchmark:", bench.name)
	fmt.Println("Description:", bench.description)

	wg.Add(1)
	go bench.run(wg)
}

func (bench *fsBench) run(wg *sync.WaitGroup) {
	defer wg.Done()

	latencyHist := bench.reg.Register(bench.name)

	for {
		// run the op which we're benchmarking.
		bench.numOps++

		// The running function will determine exactly what to latency
		// it wants to measure.
		bench.running(latencyHist)

		if fsConfig.maxOps > 0 && bench.numOps >= fsConfig.maxOps {
			break
		}
	}
}

func (bench *fsBench) tick(elapsed time.Duration, i int) {
	if i%20 == 0 {
		fmt.Println("____optype__elapsed__ops/sec(inst)___ops/sec(cum)____p50(ms)____p95(ms)____p99(ms)_pMax(ms)")
	}
	bench.reg.Tick(func(tick histogramTick) {
		h := tick.Hist

		fmt.Printf("%10s %8s %14.1f %14.1f %5.5f %5.5f %5.5f %5.5f\n",
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

func (bench *fsBench) done(elapsed time.Duration) {
	// Do the cleanup.
	bench.clean()

	fmt.Println("\n____optype__elapsed_____ops(total)___ops/sec(cum)____avg(ms)____p50(ms)____p95(ms)____p99(ms)_pMax(ms)")

	resultTick := histogramTick{}
	bench.reg.Tick(func(tick histogramTick) {
		h := tick.Cumulative
		if resultTick.Cumulative == nil {
			resultTick.Now = tick.Now
			resultTick.Cumulative = h
		} else {
			resultTick.Cumulative.Merge(h)
		}

		fmt.Printf("%10s %7.1fs %14d %14.1f %5.5f %5.5f %5.5f %5.5f %5.5f\n",
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

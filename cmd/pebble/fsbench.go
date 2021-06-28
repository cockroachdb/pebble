package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

// Note: You might have to increase the per process file descriptor
// limit of the system, before running benchmarks. Make sure that the
// limit is at least equal to fsConfig.maxFiles.
// This can be set using `ulimit -n <num_descriptors>` or
// `sudo launchctl limit maxfiles 200000 200000` on a mac.

var fsBenchCmd = &cobra.Command{
	Use:   "fs <dir>",
	Short: "Run customizable file system benchmarks.",
	Long:  "Run customizable file system benchmarks.",
	Args:  cobra.ExactArgs(1),
	RunE:  runFsBench,
}

var fsConfig struct {
	// An upper limit on the number of ops which can be run.
	maxOps int

	// Benchmark to run.
	benchname string

	// Number of times each benchmark should be run.
	numTimes int
}

func init() {
	fsBenchCmd.Flags().IntVar(
		&fsConfig.maxOps, "max-ops", 0,
		"Maximum number of ops which should be run when benchmarking.")
	fsBenchCmd.Flags().StringVar(
		&fsConfig.benchname, "bench-name", "", "The benchmark to run.")
	fsBenchCmd.MarkFlagRequired("bench-name")
	fsBenchCmd.Flags().IntVar(
		&fsConfig.numTimes, "num-times", 1,
		"Number of times each benchmark should be run.")
}

// State relevant to a benchmark.
type fsBench struct {
	// A short name for the benchmark.
	name string

	// One line describing what the benchmark is doing.
	description string

	// numOps is the total number of ops which
	// have been run for the benchmark.
	numOps int

	// directory under which the benchmark is run.
	dir     *os.File
	dirName string

	// Stats associated with the benchmark.
	reg *histogramRegistry

	// The operation which we're benchmarking.
	running func(*namedHistogram)

	// A cleanup func which must be called after
	// the benchmark has finished running.
	clean func()
}

// createFile can be used to create an empty file.
// Invariant: File shouldn't already exist.
func createFile(filepath string) *os.File {
	fh, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatalln(err)
	}
	return fh
}

// Invariant: file with filepath should exist.
func deleteFile(filepath string) {
	err := os.Remove(filepath)
	if err != nil {
		log.Fatalln(err)
	}
}

// Write fileSize bytes to the file.
// Invariant: fh shouldn't be nil.
func writeToFile(fh *os.File, bs []byte) {
	written, err := fh.Write(bs)
	if err != nil {
		log.Fatalln(err)
	}
	if written != len(bs) {
		log.Fatalf("Couldn't write %d bytes to file\n", len(bs))
	}
}

// Invariant: fh shouldn't be nil.
func syncFile(fh *os.File) {
	err := fh.Sync()
	if err != nil {
		log.Fatalln(err)
	}
}

// Invariant: fh shouldn't be nil.
func closeFile(fh *os.File) {
	err := fh.Close()
	if err != nil {
		log.Fatalln(err)
	}
}

// Calls statfs on the file path.
func statfs(filepath string) *unix.Statfs_t {
	var stat unix.Statfs_t
	unix.Statfs(filepath, &stat)
	return &stat
}

// A benchmark is a function which takes a directory
// as input and returns the fsBench struct which has
// all the information required to run the benchmark.
type benchmark func(string) *fsBench

func getByteSlice(size int) []byte {
	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = 'a'
	}
	return b
}

// fileSize is in bytes.
func createBench(benchName string, benchDescription string, fileSize int) benchmark {
	toWrite := getByteSlice(fileSize)

	return func(dirpath string) *fsBench {
		bench := &fsBench{}
		err := os.Mkdir(dirpath, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		fh, err := os.OpenFile(dirpath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalln(err)
		}

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		// setup the operation to benchmark, and the cleaup functions.
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
			fh := createFile(path.Join(dirpath, pref+strconv.Itoa(mu.numFiles)))
			syncFile(bench.dir)
			if fileSize > 0 {
				writeToFile(fh, toWrite)
				syncFile(fh)
			}
			closeFile(fh)
			mu.numFiles++
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			for i := 0; i < mu.numFiles; i++ {
				deleteFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			}
			deleteFile(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}
}

// This benchmark creates some files in a directory, and then measures the performance
// of the os.Remove function in Go, which in turn calls the unlink syscall.
// fileSize is in bytes.
func deleteUniformBench(benchName string, benchDescription string, numFiles int, fileSize int) benchmark {
	toWrite := getByteSlice(fileSize)
	return func(dirpath string) *fsBench {
		bench := &fsBench{}
		err := os.Mkdir(dirpath, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		fh, err := os.OpenFile(dirpath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalln(err)
		}

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		// setup the operation to benchmark, and the cleaup functions.
		pref := "temp_"
		for i := 0; i < numFiles; i++ {
			fh := createFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			if fileSize > 0 {
				writeToFile(fh, toWrite)
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

			deleteFile(path.Join(dirpath, pref+strconv.Itoa(mu.numFiles-1)))
			mu.numFiles--
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			for i := 0; i < mu.numFiles; i++ {
				deleteFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			}
			deleteFile(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}
}

// This benchmark creates both small/large files, and then measures the performance
// of the os.Remove when deleting the large files. Make sure that there is enough
// space to run this.
// file sizes are in in bytes.
func deleteNonUniformBench(
	benchName string, benchDescription string, numFiles int,
	numLarge int, smallFileSize int, largeFileSize int) benchmark {
	if numLarge > numFiles {
		log.Fatalln("Number of large files greater than total number of files.")
	}

	toWriteLarge := getByteSlice(largeFileSize)
	toWriteSmall := getByteSlice(smallFileSize)

	return func(dirpath string) *fsBench {
		bench := &fsBench{}
		err := os.Mkdir(dirpath, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		fh, err := os.OpenFile(dirpath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalln(err)
		}

		bench.dir = fh
		bench.dirName = dirpath
		bench.reg = newHistogramRegistry()
		bench.numOps = 0
		bench.name = benchName
		bench.description = benchDescription

		// setup the operation to benchmark, and the cleaup functions.
		pref := "temp_"
		for i := 0; i < numFiles; i++ {
			fh := createFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			if i < numLarge {
				writeToFile(fh, toWriteLarge)
			} else {
				writeToFile(fh, toWriteSmall)
			}
			syncFile(fh)
			closeFile(fh)
		}
		syncFile(bench.dir)

		var mu struct {
			sync.Mutex
			done                bool
			undeletedStartIndex int
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
			if mu.undeletedStartIndex >= numLarge {
				log.Fatalln("No large files left to delete.")
			}

			deleteFile(path.Join(dirpath, pref+strconv.Itoa(mu.undeletedStartIndex)))
			mu.undeletedStartIndex++
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			for i := mu.undeletedStartIndex; i < numFiles; i++ {
				deleteFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			}
			deleteFile(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}
}

// Tests the performance of syncing data to disk.
// Only measures the sync performance.
// The writes will be synced after every writeSize bytes have been written.
func writeSyncBench(benchName string, benchDescription string,
	maxFileSize int, writeSize int) benchmark {

	if writeSize > maxFileSize {
		log.Fatalln("File write threshold is greater than max file size.")
	}

	toWrite := getByteSlice(writeSize)

	return func(dirpath string) *fsBench {
		bench := &fsBench{}
		err := os.Mkdir(dirpath, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		fh, err := os.OpenFile(dirpath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalln(err)
		}

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
			fh           *os.File
			fileNum      int
			bytesWritten int
		}
		mu.fh = createFile(path.Join(dirpath, pref+strconv.Itoa(mu.fileNum)))

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
				mu.fh = createFile(path.Join(dirpath, pref+strconv.Itoa(mu.fileNum)))
			}

			mu.bytesWritten += writeSize
			writeToFile(mu.fh, toWrite)

			start := time.Now()
			mu.fh.Sync()
			hist.Record(time.Since(start))
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			closeFile(mu.fh)
			for i := 0; i <= mu.fileNum; i++ {
				deleteFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			}
			deleteFile(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}
}

// Tests the peformance of calling the fstat sys call on a directory,
// as the number of files/total size of files in the directory grows.
func statfsBench(benchName string, benchDescription string,
	maxFileSize int, writeSize int) benchmark {

	if writeSize > maxFileSize {
		log.Fatalln("File write threshold is greater than max file size.")
	}

	toWrite := getByteSlice(writeSize)

	return func(dirpath string) *fsBench {
		bench := &fsBench{}
		err := os.Mkdir(dirpath, 0777)
		if err != nil {
			log.Fatalln(err)
		}
		fh, err := os.OpenFile(dirpath, os.O_RDONLY, 0777)
		if err != nil {
			log.Fatalln(err)
		}

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
			fh           *os.File
			fileNum      int
			bytesWritten int
		}
		mu.fh = createFile(path.Join(dirpath, pref+strconv.Itoa(mu.fileNum)))

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
				mu.fh = createFile(path.Join(dirpath, pref+strconv.Itoa(mu.fileNum)))
			}

			mu.bytesWritten += writeSize
			writeToFile(mu.fh, toWrite)
			mu.fh.Sync()

			// call statfs.
			start := time.Now()
			statfs(dirpath)
			hist.Record(time.Since(start))
		}

		bench.clean = func() {
			mu.Lock()
			defer mu.Unlock()
			closeFile(mu.fh)
			for i := 0; i <= mu.fileNum; i++ {
				deleteFile(path.Join(dirpath, pref+strconv.Itoa(i)))
			}
			deleteFile(dirpath)
			closeFile(bench.dir)
			mu.done = true
		}

		return bench
	}
}

// The various benchmarks which can be run.
var benchmarks = map[string]benchmark{
	"create_empty": createBench("create_empty", "create empty file, sync par dir", 0),
	"create_100KB": createBench("create_100KB", "create 100KB file, sync par dir, sync file", 100_000),
	"create_2MB":   createBench("create_2MB", "create 2MB file, sync par dir, sync file", 2_000_000),
	"delete_10k_2MB": deleteUniformBench(
		"delete_10k_2MB", "create 10k 2MB size files, measure deletion times", 10_000, 2_000_000,
	),
	"delete_100k_2MB": deleteUniformBench(
		"delete_100k_2MB", "create 100k 2MB size files, measure deletion times", 100_000, 2_000_000,
	),
	"delete_200k_2MB": deleteUniformBench(
		"delete_200k_2MB", "create 200k 2MB size files, measure deletion times", 200_000, 2_000_000,
	),
	"delete_10k_2k_200MB": deleteNonUniformBench(
		"delete_10k_2k_200MB", "create 10k files, with 2k 200MB files, measure deletion times of 200MB files",
		10_000, 2_000, 0, 200_000_000,
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
		1e5 + 5, 1e5,
	),
}

func runFsBench(_ *cobra.Command, args []string) error {
	benchfunc, ok := benchmarks[fsConfig.benchname]
	if !ok {
		log.Fatalln("Trying to run an unknown benchmark.")
	}

	// Run the benchmark a comple of times.
	fmt.Printf("The benchmark will be run %d time(s).\n", fsConfig.numTimes)
	for i := 0; i < fsConfig.numTimes; i++ {
		fmt.Println("Starting benchmark:", i)
		bench := benchfunc(args[0])
		runTestWithoutDB(testWithoutDB{
			init: bench.init,
			tick: bench.tick,
			done: bench.done,
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
		fmt.Println("____optype__elapsed__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	}
	bench.reg.Tick(func(tick histogramTick) {
		h := tick.Hist

		fmt.Printf("%10s %8s %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
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
	fmt.Println("\n____optype__elapsed_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")

	resultTick := histogramTick{}
	bench.reg.Tick(func(tick histogramTick) {
		h := tick.Cumulative
		if resultTick.Cumulative == nil {
			resultTick.Now = tick.Now
			resultTick.Cumulative = h
		} else {
			resultTick.Cumulative.Merge(h)
		}

		fmt.Printf("%10s %7.1fs %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
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

	fmt.Printf("fsbench/%s  %d %0.1f ops/sec\n\n",
		bench.name,
		resultHist.TotalCount(),
		float64(resultHist.TotalCount())/elapsed.Seconds(),
	)

	// Do the cleanup.
	bench.clean()
}

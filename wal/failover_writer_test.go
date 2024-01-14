package wal

import (
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

type closeKind uint8

const (
	closeSync closeKind = iota
	closeAsync
	waitForCloseToFinish
	noneOfTheAbove
)

func TestFailoverWriter(t *testing.T) {
	datadriven.Walk(t, "testdata/failover_writer", func(t *testing.T, path string) {
		memFS := vfs.NewStrictMem()
		dirs := [numDirIndices]dirAndFileHandle{
			{Dir: Dir{Dirname: "pri"}},
			{Dir: Dir{Dirname: "sec"}},
		}
		var testDirs [numDirIndices]dirAndFileHandle
		for i, dir := range dirs {
			require.NoError(t, memFS.MkdirAll(dir.Dirname, 0755))
			f, err := memFS.OpenDir("")
			require.NoError(t, err)
			require.NoError(t, f.Sync())
			require.NoError(t, f.Close())
			testDirs[i].Dir = dir.Dir
		}
		setDirsFunc := func(t *testing.T, fs vfs.FS, dirs *[numDirIndices]dirAndFileHandle) {
			for i := range *dirs {
				f := (*dirs)[i].File
				if f != nil {
					_ = f.Close()
				}
				(*dirs)[i].FS = fs
				f, err := fs.OpenDir((*dirs)[i].Dirname)
				require.NoError(t, err)
				(*dirs)[i].File = f
			}
		}
		setDirsFunc(t, memFS, &dirs)
		setDirsFunc(t, memFS, &testDirs)
		dirIndex := 0

		printLogFiles := func(b *strings.Builder, num NumWAL) {
			memFS.ResetToSyncedState()
			type filenameAndFS struct {
				name string
				fs   vfs.FS
			}
			var filenames []filenameAndFS
			prefix := base.DiskFileNum(num).String()
			for i := range dirs {
				fns, err := dirs[i].FS.List(dirs[i].Dirname)
				require.NoError(t, err)
				for _, fn := range fns {
					if strings.HasPrefix(fn, prefix) {
						filenames = append(filenames, filenameAndFS{
							name: dirs[i].FS.PathJoin(dirs[i].Dirname, fn),
							fs:   dirs[i].FS,
						})
					}
				}
			}
			slices.SortFunc(filenames, func(a, b filenameAndFS) int {
				return strings.Compare(a.name, b.name)
			})
			if len(filenames) > 0 {
				fmt.Fprintf(b, "log files:\n")
			}
			for _, fn := range filenames {
				fmt.Fprintf(b, "  %s\n", fn.name)
				func() {
					f, err := fn.fs.Open(fn.name)
					require.NoError(t, err)
					defer f.Close()
					rr := record.NewReader(f, base.DiskFileNum(num))
					for {
						offset := rr.Offset()
						r, err := rr.Next()
						if err == nil {
							var bb strings.Builder
							_, err = io.Copy(&bb, r)
							if err == nil {
								fmt.Fprintf(b, "    %d: %s\n", offset, bb.String())
							}
						}
						if err != nil {
							fmt.Fprintf(b, "    %s\n", err.Error())
							break
						}
					}
				}()
			}
		}
		var w *failoverWriter
		waitForQueueLen := func(t *testing.T, qlen int) {
			for {
				n := w.q.length()
				require.LessOrEqual(t, qlen, n)
				if qlen != n {
					time.Sleep(10 * time.Millisecond)
				} else {
					return
				}
			}
		}
		checkLogWriters := func(t *testing.T, b *strings.Builder) {
			if w == nil {
				return
			}
			fmt.Fprintf(b, "log writers:\n")
			for i := logNameIndex(0); i < w.mu.nextWriterIndex; i++ {
				rLatency, rErr := w.writers[i].r.ongoingLatencyOrError()
				require.Equal(t, time.Duration(0), rLatency)
				if w.writers[i].createError != nil {
					require.Equal(t, rErr, w.writers[i].createError)
				}
				errStr := "no error"
				if rErr != nil {
					errStr = rErr.Error()
				}
				fmt.Fprintf(b, "  writer %d: %s\n", i, errStr)
			}
		}
		var nextWALNum NumWAL
		queueSemChanCap := 100
		queueSemChan := make(chan struct{}, queueSemChanCap)
		countSem := func() int {
			return queueSemChanCap - len(queueSemChan)
		}
		var stopper *stopper
		var logWriterCreated chan struct{}
		var syncs []SyncOptions
		resetStateAfterClose := func(t *testing.T) {
			done := false
			for !done {
				select {
				case <-queueSemChan:
				default:
					done = true
				}
			}
			syncs = nil
			w = nil
			dirIndex = 0
			setDirsFunc(t, memFS, &testDirs)
		}
		var (
			closeSemCount int
			closeErr      error
			closeWG       *sync.WaitGroup
			closeOffset   int64
		)
		datadriven.RunTest(t, path,
			func(t *testing.T, td *datadriven.TestData) string {
				closeFunc := func(closeKind closeKind, stopGoroutines bool) string {
					if closeKind != waitForCloseToFinish {
						closeSemCount = queueSemChanCap
						closeErr = nil
						closeWG = nil
						closeOffset = 0
					}
					if td.HasArg("sem-count") {
						td.ScanArgs(t, "sem-count", &closeSemCount)
					}
					if closeKind == waitForCloseToFinish {
						closeWG.Wait()
					} else if closeKind == closeAsync {
						closeWG = &sync.WaitGroup{}
						closeWG.Add(1)
						go func() {
							closeOffset, closeErr = w.Close()
							closeWG.Done()
						}()
						return ""
					} else if closeKind == closeSync {
						closeOffset, closeErr = w.Close()
					}
					var b strings.Builder
					if closeKind != noneOfTheAbove {
						// Print the close error and the record dispositions.
						errStr := "ok"
						if closeErr != nil {
							errStr = closeErr.Error()
						}
						fmt.Fprintf(&b, "close: %s, offset: %d\n", errStr, closeOffset)
						if len(syncs) > 0 {
							fmt.Fprintf(&b, "records:\n")
						}
						for i := range syncs {
							infoStr := "no sync"
							if syncs[i].Done != nil {
								infoStr = "synced"
								// Should already be done.
								syncs[i].Done.Wait()
								err := *syncs[i].Err
								if err != nil {
									infoStr = fmt.Sprintf("sync error %s", err.Error())
								}
							}
							fmt.Fprintf(&b, "  record %d: %s\n", i, infoStr)
						}
						metrics := w.Metrics()
						fmt.Fprintf(&b, "write bytes metric: %d\n", metrics.WriteThroughput.Bytes)
						if metrics.WriteThroughput.Bytes > 0 {
							require.Less(t, time.Duration(0), metrics.WriteThroughput.WorkDuration)
						}
					}
					if stopGoroutines {
						// We expect the Close to complete without stopping all the
						// goroutines. But for deterministic log file output we stop all
						// goroutines.
						stopper.stop()
						printLogFiles(&b, nextWALNum-1)
						checkLogWriters(t, &b)
						require.Equal(t, closeSemCount, countSem())
						resetStateAfterClose(t)
					}
					return b.String()
				}
				createWriter := func(noWaitForLogWriterCreation bool) {
					wn := nextWALNum
					nextWALNum++
					var err error
					stopper = newStopper()
					logWriterCreated = make(chan struct{}, 100)
					w, err = newFailoverWriter(failoverWriterOpts{
						wn:                   wn,
						preallocateSize:      func() int { return 0 },
						queueSemChan:         queueSemChan,
						stopper:              stopper,
						writerCreatedForTest: logWriterCreated,
					}, testDirs[dirIndex], nil)
					require.NoError(t, err)
					if !noWaitForLogWriterCreation {
						<-logWriterCreated
					}
				}
				switch td.Cmd {
				case "init":
					var injs []errorfs.Injector
					var noWriter bool
					for _, cmdArg := range td.CmdArgs {
						switch cmdArg.Key {
						case "inject-errors":
							if len(injs) != 0 {
								return "duplicate inject-errors"
							}
							injs = make([]errorfs.Injector, len(cmdArg.Vals))
							for i := 0; i < len(cmdArg.Vals); i++ {
								inj, err := errorfs.ParseDSL(cmdArg.Vals[i])
								if err != nil {
									return fmt.Sprintf("%s: %s", cmdArg.Vals[i], err.Error())
								}
								injs[i] = inj
							}
						case "no-writer":
							noWriter = true
						default:
							return fmt.Sprintf("unknown arg %s", cmdArg.Key)
						}
					}
					fs := vfs.FS(memFS)
					if len(injs) != 0 {
						fs = errorfs.Wrap(memFS, errorfs.Any(injs...))
					}
					fs = newBlockingFS(fs)
					setDirsFunc(t, fs, &testDirs)
					if !noWriter {
						createWriter(false)
					}
					return ""

				case "create-writer-after-init":
					noWaitForLogWriterCreation := false
					if td.HasArg("no-wait") {
						noWaitForLogWriterCreation = true
					}
					createWriter(noWaitForLogWriterCreation)
					return ""

				case "write":
					var synco SyncOptions
					var doSync bool
					td.ScanArgs(t, "sync", &doSync)
					if doSync {
						wg := &sync.WaitGroup{}
						wg.Add(1)
						synco = SyncOptions{
							Done: wg,
							Err:  new(error),
						}
						queueSemChan <- struct{}{}
					}
					syncs = append(syncs, synco)
					var value string
					td.ScanArgs(t, "value", &value)
					offset, err := w.WriteRecord([]byte(value), synco)
					require.NoError(t, err)
					// The offset can be non-deterministic depending on which LogWriter
					// is being written to, so print it only when requested.
					if td.HasArg("print-offset") {
						return fmt.Sprintf("offset: %d\n", offset)
					}
					return ""

				case "wait-for-queue":
					var qlen int
					td.ScanArgs(t, "length", &qlen)
					waitForQueueLen(t, qlen)
					return ""

				case "switch":
					noWaitForLogWriterCreation := false
					if td.HasArg("no-wait") {
						noWaitForLogWriterCreation = true
					}
					dirIndex = (dirIndex + 1) % 2
					err := w.switchToNewDir(testDirs[dirIndex])
					if err == nil {
						if !noWaitForLogWriterCreation {
							<-logWriterCreated
						}
						return "ok"
					}
					return err.Error()

				case "close":
					return closeFunc(closeSync, true)

				case "close-async":
					return closeFunc(closeAsync, false)

				case "ongoing-latency":
					var index int
					td.ScanArgs(t, "writer-index", &index)
					for i := 0; i < 10; i++ {
						time.Sleep(time.Duration(i+1) * time.Millisecond)
						d, _ := w.writers[index].r.ongoingLatencyOrError()
						if d > 0 {
							return "found ongoing"
						}
					}
					return "no ongoing"

				case "wait-for-close":
					stopGoroutines := true
					if td.HasArg("do-not-stop-goroutines") {
						stopGoroutines = false
					}
					return closeFunc(waitForCloseToFinish, stopGoroutines)

				case "stop-goroutines-after-close":
					return closeFunc(noneOfTheAbove, true)

				case "blocking-conf":
					var filename string
					td.ScanArgs(t, "filename", &filename)
					var conf blockingConf
					if td.HasArg("create") {
						conf |= blockingCreate
					}
					if td.HasArg("write") {
						conf |= blockingWrite
					}
					if td.HasArg("sync") {
						conf |= blockingSync
					}
					if td.HasArg("close") {
						conf |= blockingClose
					}
					if td.HasArg("open-dir") {
						conf |= blockingOpenDir
					}
					testDirs[0].FS.(*blockingFS).setConf(filename, conf)
					return fmt.Sprintf("%s: 0b%b", filename, uint8(conf))

				case "wait-for-and-unblock":
					var filename string
					td.ScanArgs(t, "filename", &filename)
					testDirs[0].FS.(*blockingFS).waitForBlockAndUnblock(filename)
					return ""

				case "sleep":
					time.Sleep(time.Millisecond)
					return ""

				default:
					return fmt.Sprintf("unknown command: %s", td.Cmd)
				}
			})
	})
}

type blockingFS struct {
	vfs.FS
	mu struct {
		sync.Mutex
		conf map[string]confAndState
	}
}

type blockingConf uint32

type confAndState struct {
	blockingConf
	block chan struct{}
}

const (
	blockingCreate  blockingConf = 1
	blockingWrite   blockingConf = 1 << 1
	blockingSync    blockingConf = 1 << 2
	blockingClose   blockingConf = 1 << 3
	blockingOpenDir blockingConf = 1 << 4
	blockingAll     blockingConf = math.MaxUint32
)

var _ vfs.FS = &blockingFS{}

func newBlockingFS(fs vfs.FS) *blockingFS {
	bfs := &blockingFS{
		FS: fs,
	}
	bfs.mu.conf = make(map[string]confAndState)
	return bfs
}

func (fs *blockingFS) setConf(baseFilename string, conf blockingConf) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	cs, ok := fs.mu.conf[baseFilename]
	if ok {
		close(cs.block)
	}
	if conf == 0 {
		delete(fs.mu.conf, baseFilename)
		return
	}
	fs.mu.conf[baseFilename] = confAndState{
		blockingConf: conf,
		block:        make(chan struct{}),
	}
}

func (fs *blockingFS) waitForBlockAndUnblock(baseFilename string) {
	fs.mu.Lock()
	cs, ok := fs.mu.conf[baseFilename]
	if !ok {
		panic(errors.AssertionFailedf("no conf for %s", baseFilename))
	}
	fs.mu.Unlock()
	cs.block <- struct{}{}
}

func (fs *blockingFS) maybeBlock(baseFilename string, op blockingConf) {
	fs.mu.Lock()
	cs, ok := fs.mu.conf[baseFilename]
	fs.mu.Unlock()
	if ok && cs.blockingConf&op != 0 {
		<-cs.block
	}
}

func (fs *blockingFS) Create(name string) (vfs.File, error) {
	baseFilename := fs.FS.PathBase(name)
	fs.maybeBlock(baseFilename, blockingCreate)
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &blockingFile{baseFilename: baseFilename, File: f, fs: fs}, nil
}

func (fs *blockingFS) OpenDir(name string) (vfs.File, error) {
	baseFilename := fs.FS.PathBase(name)
	fs.maybeBlock(baseFilename, blockingOpenDir)
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &blockingFile{baseFilename: baseFilename, File: f, fs: fs}, nil
}

type blockingFile struct {
	baseFilename string
	vfs.File
	fs *blockingFS
}

var _ vfs.File = blockingFile{}

func (f blockingFile) Write(p []byte) (n int, err error) {
	f.fs.maybeBlock(f.baseFilename, blockingWrite)
	return f.File.Write(p)
}

func (f blockingFile) Sync() error {
	f.fs.maybeBlock(f.baseFilename, blockingSync)
	return f.File.Sync()
}

func (f blockingFile) SyncData() error {
	f.fs.maybeBlock(f.baseFilename, blockingSync)
	return f.File.SyncData()
}

func (f blockingFile) Close() error {
	f.fs.maybeBlock(f.baseFilename, blockingClose)
	return f.File.Close()
}

// TestConcurrentWritersWithManyRecords tests (a) resizing of the recordQueue,
// (b) resizing of the SyncOptions in recordQueue.pop, (c) competition to pop
// with CAS failure, and resulting retries. (c) is observable in this test by
// adding print statements in recordQueue.pop.
func TestConcurrentWritersWithManyRecords(t *testing.T) {
	seed := *seed
	if seed == 0 {
		seed = time.Now().UnixNano()
		t.Logf("seed: %d", seed)
	}
	rng := rand.New(rand.NewSource(seed))
	records := make([][]byte, 20<<10)
	recordsMap := map[string]int{}
	for i := range records {
		records[i] = make([]byte, 50+rng.Intn(100))
		for {
			randStr(records[i], rng)
			if _, ok := recordsMap[string(records[i])]; ok {
				continue
			} else {
				recordsMap[string(records[i])] = i
				break
			}
		}
	}
	const numLogWriters = 4
	memFS := vfs.NewStrictMem()
	dirs := [numDirIndices]dirAndFileHandle{{Dir: Dir{Dirname: "pri"}}, {Dir: Dir{Dirname: "sec"}}}
	for _, dir := range dirs {
		require.NoError(t, memFS.MkdirAll(dir.Dirname, 0755))
		f, err := memFS.OpenDir("")
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())
	}
	bFS := newBlockingFS(memFS)
	for i := range dirs {
		dirs[i].FS = bFS
		f, err := bFS.OpenDir(dirs[i].Dirname)
		require.NoError(t, err)
		dirs[i].File = f
	}
	for i := 0; i < numLogWriters; i++ {
		bFS.setConf(makeLogFilename(0, logNameIndex(i)), blockingWrite)
	}
	stopper := newStopper()
	logWriterCreated := make(chan struct{}, 100)
	queueSemChan := make(chan struct{}, len(records))
	dirIndex := 0
	ww, err := newFailoverWriter(failoverWriterOpts{
		wn:                   0,
		preallocateSize:      func() int { return 0 },
		queueSemChan:         queueSemChan,
		stopper:              stopper,
		writerCreatedForTest: logWriterCreated,
	}, dirs[dirIndex], nil)
	require.NoError(t, err)
	var syncs []SyncOptions
	wg := &sync.WaitGroup{}
	switchInterval := len(records) / 4
	for i := 0; i < len(records); i++ {
		queueSemChan <- struct{}{}
		wg.Add(1)
		synco := SyncOptions{Done: wg, Err: new(error)}
		syncs = append(syncs, synco)
		_, err := ww.WriteRecord(records[i], synco)
		require.NoError(t, err)
		if i > 0 && i%switchInterval == 0 {
			dirIndex = (dirIndex + 1) % 2
			ww.switchToNewDir(dirs[dirIndex])
		}
	}
	time.Sleep(5 * time.Millisecond)
	for i := 0; i < numLogWriters; i++ {
		bFS.setConf(makeLogFilename(0, logNameIndex(i)), 0)
	}
	_, err = ww.Close()
	require.NoError(t, err)
	wg.Wait()
	require.Equal(t, 0, len(queueSemChan))
	type indexInterval struct {
		first, last int
	}
	for i := 0; i < numLogWriters; i++ {
		func() {
			f, err := memFS.Open(memFS.PathJoin(dirs[i%2].Dirname, makeLogFilename(0, logNameIndex(i))))
			if err != nil {
				t.Logf("file %d: %s", i, err.Error())
				return
			}
			defer f.Close()
			rr := record.NewReader(f, base.DiskFileNum(0))
			interval := indexInterval{}
			for {
				r, err := rr.Next()
				if err != nil {
					require.Equal(t, io.EOF, err)
					break
				}
				var bb strings.Builder
				_, err = io.Copy(&bb, r)
				require.NoError(t, err)
				index, ok := recordsMap[bb.String()]
				require.True(t, ok)
				if interval.first == interval.last {
					interval.first = index
					interval.last = index + 1
				} else {
					require.Equal(t, interval.last, index)
					interval.last++
				}
			}
			require.Equal(t, 0, interval.first)
			if i == numLogWriters-1 {
				require.Equal(t, len(records), interval.last)
			}
		}()
	}
}

var seed = flag.Int64("seed", 0, "a pseudorandom number generator seed")

func randStr(fill []byte, rng *rand.Rand) {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = len(letters)
	for i := 0; i < len(fill); i++ {
		fill[i] = letters[rng.Intn(lettersLen)]
	}
}

// TODO(sumeer): randomized error injection and delay injection test.

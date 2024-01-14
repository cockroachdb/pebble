package wal

import (
	"fmt"
	"io"
	"math"
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
		)
		datadriven.RunTest(t, path,
			func(t *testing.T, td *datadriven.TestData) string {
				closeFunc := func(closeKind closeKind, stopGoroutines bool) string {
					if closeKind != waitForCloseToFinish {
						closeSemCount = queueSemChanCap
						closeErr = nil
						closeWG = nil
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
							closeErr = w.Close()
							closeWG.Done()
						}()
						return ""
					} else if closeKind == closeSync {
						closeErr = w.Close()
					}
					var b strings.Builder
					if closeKind != noneOfTheAbove {
						// Print the close error and the record dispositions.
						errStr := "ok"
						if closeErr != nil {
							errStr = closeErr.Error()
						}
						fmt.Fprintf(&b, "close: %s\n", errStr)
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
				switch td.Cmd {
				case "init":
					var injs []errorfs.Injector
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
					wn := nextWALNum
					nextWALNum++
					var err error
					stopper = newStopper()
					logWriterCreated = make(chan struct{}, 1)
					w, err = newFailoverWriter(failoverWriterOpts{
						wn:                   wn,
						preallocateSize:      func() int { return 0 },
						queueSemChan:         queueSemChan,
						stopper:              stopper,
						writerCreatedForTest: logWriterCreated,
					}, testDirs[dirIndex], nil)
					require.NoError(t, err)
					<-logWriterCreated
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
					require.NoError(t, w.WriteRecord([]byte(value), synco))
					return ""

				case "wait-for-queue":
					var qlen int
					td.ScanArgs(t, "length", &qlen)
					waitForQueueLen(t, qlen)
					return ""

				case "switch":
					dirIndex = (dirIndex + 1) % 2
					err := w.switchToNewDir(testDirs[dirIndex])
					if err == nil {
						<-logWriterCreated
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

// TODO(sumeer): ensure that Close is timely by injecting a FS
// delay that does not end until we signal it to end here
// after the Close returns.

// TODO(sumeer): reallocate queue that exceeds initialBufferLen: wait for
// initial writer creation; write all unsynced; then switch writer; write some
// more; ensure that log files have right contents.

// TODO(sumeer): randomized error injection test.

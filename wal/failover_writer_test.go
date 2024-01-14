package wal

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func TestFailoverWriter(t *testing.T) {
	memFS := vfs.NewStrictMem()
	dirs := []dirAndFileHandle{
		{Dir: Dir{FS: memFS, Dirname: "pri"}},
		{Dir: Dir{FS: memFS, Dirname: "sec"}},
	}
	for i, dir := range dirs {
		require.NoError(t, dir.FS.MkdirAll(dir.Dirname, 0755))
		f, err := dir.FS.OpenDir("")
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())
		f, err = dir.FS.OpenDir(dir.Dirname)
		require.NoError(t, err)
		dirs[i].File = f
	}
	dirIndex := 0

	type filenameAndFS struct {
		name string
		fs   vfs.FS
	}
	printLogFiles := func(b *strings.Builder, num NumWAL) {
		memFS.ResetToSyncedState()
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
	var nextWALNum NumWAL
	queueSemChanCap := 100
	queueSemChan := make(chan struct{}, queueSemChanCap)
	countSem := func() int {
		return queueSemChanCap - len(queueSemChan)
	}
	var stopper *stopper
	var logWriterCreated chan struct{}
	var syncs []SyncOptions
	// TODO(sumeer): latency injection; test latency measurement.
	datadriven.RunTest(t, "testdata/failover_writer",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			// TODO: error injection; choice of initial dir; latency injection.
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
								return err.Error()
							}
							injs[i] = inj
						}
					default:
						return fmt.Sprintf("unknown arg %s", cmdArg.Key)
					}
				}
				if len(injs) != 0 {
					fs := errorfs.Wrap(memFS, errorfs.Any(injs...))
					for i := range dirs {
						dirs[i].FS = fs
					}
				}
				wn := nextWALNum
				nextWALNum++
				var err error
				stopper = newStopper()
				logWriterCreated = make(chan struct{})
				w, err = newFailoverWriter(failoverWriterOpts{
					wn:                   wn,
					preallocateSize:      func() int { return 0 },
					queueSemChan:         queueSemChan,
					stopper:              stopper,
					writerCreatedForTest: logWriterCreated,
				}, dirs[0], nil)
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
				dirIndex := (dirIndex + 1) % 2
				require.NoError(t, w.switchToNewDir(dirs[dirIndex]))
				<-logWriterCreated
				return ""

			case "close":
				var b strings.Builder
				// TODO(sumeer): ensure that Close is timely by injecting a FS
				// delay that does not end until we signal it to end here
				// after the Close returns.
				err := w.Close()
				errStr := "success"
				if err != nil {
					errStr = err.Error()
				}
				fmt.Fprintf(&b, "close: %s\n", errStr)
				for i := range syncs {
					errStr := "no error"
					if syncs[i].Done != nil {
						// Should already be done.
						syncs[i].Done.Wait()
						err := *syncs[i].Err
						if err != nil {
							errStr = err.Error()
						}
					}
					fmt.Fprintf(&b, "sync %d: %s\n", i, errStr)
				}
				// We expect the Close to complete without stopping all the
				// goroutines. But for deterministic log file output we stop all
				// goroutines.
				stopper.stop()
				printLogFiles(&b, nextWALNum-1)
				require.Equal(t, queueSemChanCap, countSem())
				syncs = nil
				w = nil
				dirIndex = 0
				for i := range dirs {
					dirs[i].FS = memFS
				}
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

// TODO(sumeer): randomized error injection test.

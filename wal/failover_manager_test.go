// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"container/list"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func (p *dirProber) printStateForTesting() string {
	var b strings.Builder
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := p.mu.firstProbeIndex; i < p.mu.nextProbeIndex; i++ {
		sampleDur := p.mu.history[i%probeHistoryLength]
		fmt.Fprintf(&b, " %d: %s", i, sampleDur.String())
	}
	return b.String()
}

func (m *failoverMonitor) printStateForTesting() string {
	var b strings.Builder
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintf(&b, "dir index: %d", m.mu.dirIndex)
	if !(m.mu.lastFailBackTime == time.Time{}) {
		fmt.Fprintf(
			&b, " failback time: %s", time.Duration(m.mu.lastFailBackTime.UnixNano()).String())
	}
	return b.String()
}

// manualTime implements timeSource.
type manualTime struct {
	mu struct {
		sync.Mutex
		now time.Time
		// tickers is a list with element type *manualTicker.
		tickers list.List
	}
}

func newManualTime(initialTime time.Time) *manualTime {
	mt := manualTime{}
	mt.mu.now = initialTime
	mt.mu.tickers.Init()
	return &mt
}

var _ timeSource = &manualTime{}

func (m *manualTime) now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mu.now
}

func (m *manualTime) newTicker(duration time.Duration) tickerI {
	if duration <= 0 {
		panic("non-positive interval for NewTicker")
	}
	t := &manualTicker{
		m: m,
		// We allocate a big buffer so that sending a tick never blocks.
		channel: make(chan time.Time, 10000),
	}
	return m.insertTicker(t, duration)
}

func (m *manualTime) insertTicker(t *manualTicker, duration time.Duration) tickerI {
	m.mu.Lock()
	defer m.mu.Unlock()
	t.duration = duration
	t.nextTick = m.mu.now.Add(duration)
	t.element = m.mu.tickers.PushBack(t)
	return t
}

// advance forwards the current time by the given duration.
func (m *manualTime) advance(duration time.Duration) {
	now := m.now().Add(duration)
	m.mu.Lock()
	defer m.mu.Unlock()
	if !now.After(m.mu.now) {
		return
	}
	m.mu.now = now
	// Fire off any tickers.
	for e := m.mu.tickers.Front(); e != nil; e = e.Next() {
		t := e.Value.(*manualTicker)
		for !t.nextTick.After(now) {
			select {
			case t.channel <- t.nextTick:
			default:
				panic("ticker channel full")
			}
			t.nextTick = t.nextTick.Add(t.duration)
		}
	}
}

func (m *manualTime) removeTicker(t *manualTicker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.element != nil {
		m.mu.tickers.Remove(t.element)
		t.element = nil
	}
}

// manualTicker implements tickerI.
type manualTicker struct {
	m       *manualTime
	element *list.Element

	duration time.Duration
	nextTick time.Time
	channel  chan time.Time
}

func (t *manualTicker) reset(duration time.Duration) {
	t.m.removeTicker(t)
loop:
	for {
		select {
		case <-t.channel:
		default:
			break loop
		}
	}
	t.m.insertTicker(t, duration)
}

func (t *manualTicker) stop() {
	t.m.removeTicker(t)
}

func (t *manualTicker) ch() <-chan time.Time {
	return t.channel
}

func TestDirProber(t *testing.T) {
	memFS := vfs.NewMem()
	var ts *manualTime
	var fs *blockingFS
	var prober *dirProber
	var stopper *stopper
	var iterationForTesting chan struct{}
	datadriven.RunTest(t, "testdata/dir_prober",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				stopper = newStopper()
				ts = newManualTime(time.UnixMilli(0))
				var injs []errorfs.Injector
				var interval time.Duration
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
					case "interval":
						if len(cmdArg.Vals) != 1 {
							return fmt.Sprintf("len of interval is incorrect %d", len(cmdArg.Vals))
						}
						var err error
						interval, err = time.ParseDuration(cmdArg.Vals[0])
						if err != nil {
							return err.Error()
						}
					default:
						return fmt.Sprintf("unknown arg %s", cmdArg.Key)
					}
				}
				fsToWrap := vfs.FS(memFS)
				if len(injs) != 0 {
					fsToWrap = errorfs.Wrap(fsToWrap, errorfs.Any(injs...))
				}
				fs = newBlockingFS(fsToWrap)
				iterationForTesting = make(chan struct{}, 50)
				prober = &dirProber{}
				prober.init(fs, "foo", interval, stopper, ts, iterationForTesting)
				return ""

			case "enable":
				prober.enableProbing()
				if td.HasArg("wait") {
					recvWithDeadline(t, td, "", iterationForTesting)
				}
				return "enabled"

			case "disable":
				prober.disableProbing()
				if td.HasArg("wait") {
					recvWithDeadline(t, td, "", iterationForTesting)
				}
				return "disabled"

			case "stop":
				stopper.stop()
				return "stopped"

			case "block-io-config":
				conf := blockingWrite | blockingSync
				if td.HasArg("unblock") {
					conf = 0
				}
				fs.setConf("foo", conf)
				return ""

			case "wait-for-and-unblock-io":
				sendWithDeadline(t, td, "", fs.waitForBlockAndUnblock("foo"))
				return ""

			case "get-mean-max":
				durArg, ok := td.Arg("dur")
				require.True(t, ok)
				dur, err := time.ParseDuration(durArg.Vals[0])
				require.NoError(t, err)
				mean, max := prober.getMeanMax(dur)
				return fmt.Sprintf("mean: %s, max: %s, state:%s",
					mean.String(), max.String(), prober.printStateForTesting())

			case "advance-time":
				durArg, ok := td.Arg("dur")
				require.True(t, ok)
				dur, err := time.ParseDuration(durArg.Vals[0])
				require.NoError(t, err)
				ts.advance(dur)
				if td.HasArg("wait") {
					recvWithDeadline(t, td, "", iterationForTesting)
				}
				return fmt.Sprintf("now: %dms", ts.now().UnixNano()/int64(time.Millisecond))

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func TestManagerFailover(t *testing.T) {
	errorToStr := func(err error) string {
		if err != nil {
			return err.Error()
		}
		return "ok"
	}
	waitForOngoingLatencyOrErr := func(t *testing.T, td *datadriven.TestData, ww *failoverWriter) {
		const sleepDur = time.Millisecond
		for i := 0; i < 20000; i++ {
			r := ww.recorderForCurDir()
			if r.ongoingOperationStart.Load() != 0 {
				return
			}
			if r.error.Load() != nil {
				return
			}
			time.Sleep(sleepDur)
		}
		t.Fatalf("waitForOngoingLatencyOrErr expired at %s", td.Pos)
	}
	var proberIterationForTesting chan struct{}
	var monitorIterationForTesting chan struct{}
	var monitorStateBuf strings.Builder
	monitorStateForTesting := func(numSwitches int, ongoingLatencyAtSwitch time.Duration) {
		fmt.Fprintf(&monitorStateBuf, " num-switches: %d, ongoing-latency-at-switch: %s",
			numSwitches, ongoingLatencyAtSwitch.String())
	}
	var logWriterCreatedForTesting chan struct{}
	var ts *manualTime
	var memFS *vfs.MemFS
	var fs *blockingFS
	var fm *failoverManager
	var fw *failoverWriter
	var allowFailover bool
	dirs := [numDirIndices]string{"pri", "sec"}
	datadriven.RunTest(t, "testdata/manager_failover",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init-manager":
				ts = newManualTime(time.UnixMilli(0))
				if !td.HasArg("reuse-fs") {
					memFS = vfs.NewMem()
				}
				allowFailover = !td.HasArg("disable-failover")
				proberIterationForTesting = make(chan struct{}, 50000)
				monitorIterationForTesting = make(chan struct{}, 50000)
				monitorStateBuf.Reset()
				logWriterCreatedForTesting = make(chan struct{}, 100)
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
					case "reuse-fs", "disable-failover":
						// Ignore, already handled above.
					default:
						return fmt.Sprintf("unknown arg %s", cmdArg.Key)
					}
				}
				fsToWrap := vfs.FS(memFS)
				if len(injs) != 0 {
					fsToWrap = errorfs.Wrap(fsToWrap, errorfs.Any(injs...))
				}
				fs = newBlockingFS(fsToWrap)
				fm = &failoverManager{}
				o := Options{
					Primary:                     Dir{FS: fs, Dirname: dirs[primaryDirIndex]},
					Secondary:                   Dir{FS: fs, Dirname: dirs[secondaryDirIndex]},
					MinUnflushedWALNum:          0,
					MaxNumRecyclableLogs:        1,
					NoSyncOnClose:               false,
					BytesPerSync:                0,
					PreallocateSize:             func() int { return 0 },
					MinSyncInterval:             nil,
					QueueSemChan:                nil,
					Logger:                      nil,
					EventListener:               nil,
					FailoverWriteAndSyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{}),
					WriteWALSyncOffsets:         func() bool { return false },
				}
				require.NoError(t, memFS.MkdirAll(dirs[primaryDirIndex], 0755))
				require.NoError(t, memFS.MkdirAll(dirs[secondaryDirIndex], 0755))
				// All thresholds are 50ms.
				o.FailoverOptions = FailoverOptions{
					PrimaryDirProbeInterval:      100 * time.Millisecond,
					HealthyProbeLatencyThreshold: 50 * time.Millisecond,
					// Healthy history of length 2 is sufficient to switch back.
					HealthyInterval: 200 * time.Millisecond,
					// Use 75ms to not align with PrimaryDirProbeInterval, to avoid
					// races.
					UnhealthySamplingInterval:          75 * time.Millisecond,
					UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) { return 50 * time.Millisecond, allowFailover },
					ElevatedWriteStallThresholdLag:     10 * time.Second,
					timeSource:                         ts,
					monitorIterationForTesting:         monitorIterationForTesting,
					proberIterationForTesting:          proberIterationForTesting,
					monitorStateForTesting:             monitorStateForTesting,
					logWriterCreatedForTesting:         logWriterCreatedForTesting,
				}
				logs, err := Scan(o.Dirs()...)
				require.NoError(t, err)
				err = fm.init(o, logs)
				var b strings.Builder
				fmt.Fprintf(&b, "%s\n", errorToStr(err))
				if err == nil {
					fmt.Fprintf(&b, "recycler min-log-num: %d\n", fm.recycler.MinRecycleLogNum())
				}
				return b.String()

			case "create-writer":
				var walNum int
				td.ScanArgs(t, "wal-num", &walNum)
				w, err := fm.Create(NumWAL(walNum), 0)
				if err != nil {
					return err.Error()
				}
				fw = w.(*failoverWriter)
				return "ok"

			case "close-writer":
				_, err := fw.Close()
				return errorToStr(err)

			case "obsolete":
				var minUnflushed int
				td.ScanArgs(t, "min-unflushed", &minUnflushed)
				var noRecycle bool
				if td.HasArg("no-recycle") {
					noRecycle = true
				}
				toDelete, err := fm.Obsolete(NumWAL(minUnflushed), noRecycle)
				var b strings.Builder
				fmt.Fprintf(&b, "%s\n", errorToStr(err))
				if err == nil {
					fileInfo, ok := fm.recycler.Peek()
					fmt.Fprintf(&b, "recycler ")
					if ok {
						fmt.Fprintf(&b, "non-empty, front filenum: %d size: %d\n", fileInfo.FileNum, fileInfo.FileSize)
					} else {
						fmt.Fprintf(&b, "empty\n")
					}
					if len(toDelete) > 0 {
						fmt.Fprintf(&b, "to delete:\n")
						for _, f := range toDelete {
							fmt.Fprintf(&b, "  wal %d: path: %s size: %d\n", f.NumWAL, f.Path, f.ApproxFileSize)
						}
					}
				}
				return b.String()

			case "list-and-stats":
				logs := fm.List()
				stats := fm.Stats()
				var b strings.Builder
				if len(logs) > 0 {
					fmt.Fprintf(&b, "logs:\n")
					for _, f := range logs {
						fmt.Fprintf(&b, "  %s\n", f.String())
					}
				}
				fmt.Fprintf(&b, "stats:\n")
				fmt.Fprintf(&b, "  obsolete: count %d size %d\n", stats.ObsoleteFileCount, stats.ObsoleteFileSize)
				fmt.Fprintf(&b, "  live: count %d size %d\n", stats.LiveFileCount, stats.LiveFileSize)
				fmt.Fprintf(&b, "  failover: switches %d pri-dur %s sec-dur %s\n", stats.Failover.DirSwitchCount,
					stats.Failover.PrimaryWriteDuration.String(), stats.Failover.SecondaryWriteDuration.String())
				var latencyProto io_prometheus_client.Metric
				stats.Failover.FailoverWriteAndSyncLatency.Write(&latencyProto)
				latencySampleCount := *latencyProto.Histogram.SampleCount
				if latencySampleCount > 0 {
					fmt.Fprintf(&b, "  latency sample count: %d\n", latencySampleCount)
				}
				return b.String()

			case "write-record":
				var value string
				td.ScanArgs(t, "value", &value)
				var so SyncOptions
				if td.HasArg("sync") {
					wg := &sync.WaitGroup{}
					wg.Add(1)
					so = SyncOptions{
						Done: wg,
						Err:  new(error),
					}
				}
				offset, err := fw.WriteRecord([]byte(value), so, nil)
				require.NoError(t, err)
				return fmt.Sprintf("offset: %d", offset)

			case "close-manager":
				err := fm.Close()
				return errorToStr(err)

			case "advance-time":
				durArg, ok := td.Arg("dur")
				require.True(t, ok)
				dur, err := time.ParseDuration(durArg.Vals[0])
				require.NoError(t, err)
				ts.advance(dur)
				var b strings.Builder
				if td.HasArg("wait-monitor") {
					recvWithDeadline(t, td, "monitor", monitorIterationForTesting)
					fmt.Fprintf(&b, "monitor state: %s%s\n", fm.monitor.printStateForTesting(),
						monitorStateBuf.String())
					monitorStateBuf.Reset()
				}
				if td.HasArg("wait-prober") {
					recvWithDeadline(t, td, "prober", proberIterationForTesting)
					fmt.Fprintf(&b, "prober state:%s\n", fm.monitor.prober.printStateForTesting())
				}
				if td.HasArg("wait-ongoing-io") {
					waitForOngoingLatencyOrErr(t, td, fw)
				}
				if td.HasArg("wait-for-log-writer") {
					recvWithDeadline(t, td, "log-writer", logWriterCreatedForTesting)
				}
				fmt.Fprintf(&b, "now: %s", time.Duration(ts.now().UnixNano()).String())
				return b.String()

			case "elevate-write-stall-threshold":
				return fmt.Sprintf("%t\n", fm.ElevateWriteStallThresholdForFailover())

			case "block-io-config":
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
				fs.setConf(filename, conf)
				return fmt.Sprintf("%s: 0b%b", filename, uint8(conf))

			case "wait-for-and-unblock-io":
				var filename string
				td.ScanArgs(t, "filename", &filename)
				sendWithDeadline(t, td, "", fs.waitForBlockAndUnblock(filename))
				return ""

			case "list-fs":
				var filenames []string
				for i := range dirs {
					fns, err := memFS.List(dirs[i])
					require.NoError(t, err)
					for _, fn := range fns {
						filenames = append(filenames, memFS.PathJoin(dirs[i], fn))
					}
				}
				slices.Sort(filenames)
				var b strings.Builder
				for _, f := range filenames {
					fmt.Fprintf(&b, "%s\n", f)
				}
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

func sendWithDeadline(t *testing.T, td *datadriven.TestData, waitStr string, ch chan<- struct{}) {
	timer := time.NewTimer(20 * time.Second)
	select {
	case ch <- struct{}{}:
		timer.Stop()
	case <-timer.C:
		t.Fatalf("send expired at %s %s", td.Pos, waitStr)
	}
}

func recvWithDeadline(t *testing.T, td *datadriven.TestData, waitStr string, ch <-chan struct{}) {
	timer := time.NewTimer(20 * time.Second)
	select {
	case <-ch:
		timer.Stop()
	case <-timer.C:
		t.Fatalf("send expired at %s %s", td.Pos, waitStr)
	}
}

func TestFailoverManager_Quiesce(t *testing.T) {
	seed := time.Now().UnixNano()
	memFS := vfs.NewMem()
	require.NoError(t, memFS.MkdirAll("primary", os.ModePerm))
	require.NoError(t, memFS.MkdirAll("secondary", os.ModePerm))
	fs := errorfs.Wrap(memFS, errorfs.RandomLatency(
		errorfs.Randomly(0.50, seed), 10*time.Millisecond, seed, 0 /* no limit */))

	var m failoverManager
	require.NoError(t, m.init(Options{
		Primary:              Dir{FS: fs, Dirname: "primary"},
		Secondary:            Dir{FS: fs, Dirname: "secondary"},
		MaxNumRecyclableLogs: 2,
		PreallocateSize:      func() int { return 4 },
		FailoverOptions: FailoverOptions{
			PrimaryDirProbeInterval:            250 * time.Microsecond,
			HealthyProbeLatencyThreshold:       time.Millisecond,
			HealthyInterval:                    3 * time.Millisecond,
			UnhealthySamplingInterval:          250 * time.Microsecond,
			UnhealthyOperationLatencyThreshold: func() (time.Duration, bool) { return time.Millisecond, true },
		},
		FailoverWriteAndSyncLatency: prometheus.NewHistogram(prometheus.HistogramOpts{}),
		WriteWALSyncOffsets:         func() bool { return false },
	}, nil /* initial  logs */))
	for i := 0; i < 3; i++ {
		w, err := m.Create(NumWAL(i), i)
		require.NoError(t, err)
		_, err = w.WriteRecord([]byte("hello world"), SyncOptions{}, nil)
		require.NoError(t, err)
		_, err = w.Close()
		require.NoError(t, err)
	}
	require.NoError(t, m.Close())
}

func TestFailoverManager_SecondaryIsWritable(t *testing.T) {
	var m failoverManager
	require.EqualError(t, m.init(Options{
		Primary:         Dir{FS: vfs.NewMem(), Dirname: "primary"},
		Secondary:       Dir{FS: errorfs.Wrap(vfs.NewMem(), errorfs.ErrInjected), Dirname: "secondary"},
		PreallocateSize: func() int { return 4 },
	}, nil /* initial  logs */), "failed to write to WAL secondary dir: injected error")
}

// TODO(sumeer): test wrap around of history in dirProber.

// TODO(sumeer): the failover datadriven test cases are not easy to write,
// since we need to wait until all activities triggered by manualTime.advance
// are complete. Currently this is done by waiting on various channels etc.
// which exposes implementation detail. See concurrency_test.monitor, in
// CockroachDB, for an alternative.

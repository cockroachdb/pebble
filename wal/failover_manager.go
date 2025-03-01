// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/vfs"
)

// dirProber probes the primary dir, until it is confirmed to be healthy. If
// it doesn't have enough samples, it is deemed to be unhealthy. It is only
// used for failback to the primary.
type dirProber struct {
	fs vfs.FS
	// The full path of the file to use for the probe. The probe is destructive
	// in that it deletes and (re)creates the file.
	filename string
	// The probing interval, when enabled.
	interval time.Duration
	timeSource
	// buf holds the random bytes that are written during the probe.
	buf [100 << 10]byte
	// enabled is signaled to enable and disable probing. The initial state of
	// the prober is disabled.
	enabled chan bool
	mu      struct {
		sync.Mutex
		// Circular buffer of history samples.
		history [probeHistoryLength]time.Duration
		// The history is in [firstProbeIndex, nextProbeIndex).
		firstProbeIndex int
		nextProbeIndex  int
	}
	iterationForTesting chan<- struct{}
}

const probeHistoryLength = 128

// Large value.
const failedProbeDuration = 24 * 60 * 60 * time.Second

// init takes a stopper in order to connect the dirProber's long-running
// goroutines with the stopper's wait group, but the dirProber has its own
// stop() method that should be invoked to trigger the shutdown.
func (p *dirProber) init(
	fs vfs.FS,
	filename string,
	interval time.Duration,
	stopper *stopper,
	ts timeSource,
	notifyIterationForTesting chan<- struct{},
) {
	*p = dirProber{
		fs:                  fs,
		filename:            filename,
		interval:            interval,
		timeSource:          ts,
		enabled:             make(chan bool),
		iterationForTesting: notifyIterationForTesting,
	}
	// Random bytes for writing, to defeat any FS compression optimization.
	for i := range p.buf {
		p.buf[i] = byte(rand.Uint32())
	}
	// dirProber has an explicit stop() method instead of listening on
	// stopper.shouldQuiesce. This structure helps negotiate the shutdown
	// between failoverMonitor and dirProber. If the dirProber independently
	// listened to shouldQuiesce, it might exit before the failover monitor. The
	// [enable|disable]Probing methods require sending on a channel expecting
	// that the dirProber goroutine will receive on the same channel. If the
	// dirProber noticed the queiscence and exited first, the failoverMonitor
	// could deadlock waiting for a goroutine that no longer exists.
	//
	// Instead, shutdown of the dirProber is coordinated by the failoverMonitor:
	//   - the failoverMonitor listens to stopper.shouldQuiesce.
	//   - when the failoverMonitor is exiting, it will no longer attempt to
	//   interact with the dirProber. Only then does it invoke dirProber.stop.
	stopper.runAsync(p.probeLoop)
}

func (p *dirProber) probeLoop() {
	ticker := p.timeSource.newTicker(p.interval)
	ticker.stop()
	tickerCh := ticker.ch()
	shouldContinue := true
	var enabled bool
	for shouldContinue {
		select {
		case <-tickerCh:
			if !enabled {
				// Could have a tick waiting before we disabled it. Ignore.
				continue
			}
			probeDur := func() time.Duration {
				// Delete, create, write, sync.
				start := p.timeSource.now()
				_ = p.fs.Remove(p.filename)
				f, err := p.fs.Create(p.filename, "pebble-wal")
				if err != nil {
					return failedProbeDuration
				}
				defer f.Close()
				n, err := f.Write(p.buf[:])
				if err != nil {
					return failedProbeDuration
				}
				if n != len(p.buf) {
					panic("invariant violation")
				}
				err = f.Sync()
				if err != nil {
					return failedProbeDuration
				}
				return p.timeSource.now().Sub(start)
			}()
			p.mu.Lock()
			nextIndex := p.mu.nextProbeIndex % probeHistoryLength
			p.mu.history[nextIndex] = probeDur
			p.mu.nextProbeIndex++
			numSamples := p.mu.nextProbeIndex - p.mu.firstProbeIndex
			if numSamples > probeHistoryLength {
				// Length has exceeded capacity, i.e., overwritten the first probe.
				p.mu.firstProbeIndex++
			}
			p.mu.Unlock()

		case enabled, shouldContinue = <-p.enabled:
			if !enabled || !shouldContinue {
				ticker.stop()
				p.mu.Lock()
				p.mu.firstProbeIndex = 0
				p.mu.nextProbeIndex = 0
				p.mu.Unlock()
			} else {
				ticker.reset(p.interval)
			}
		}
		if p.iterationForTesting != nil {
			p.iterationForTesting <- struct{}{}
		}
	}
}

func (p *dirProber) enableProbing() {
	p.enabled <- true
}

func (p *dirProber) disableProbing() {
	p.enabled <- false
}

func (p *dirProber) stop() {
	close(p.enabled)
}

func (p *dirProber) getMeanMax(interval time.Duration) (time.Duration, time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	numSamples := p.mu.nextProbeIndex - p.mu.firstProbeIndex
	samplesNeeded := int((interval + p.interval - 1) / p.interval)
	if samplesNeeded == 0 {
		panic("interval is too short")
	} else if samplesNeeded > probeHistoryLength {
		panic("interval is too long")
	}
	if samplesNeeded > numSamples {
		// Not enough samples, so assume not yet healthy.
		return failedProbeDuration, failedProbeDuration
	}
	offset := numSamples - samplesNeeded
	var sum, max time.Duration
	for i := p.mu.firstProbeIndex + offset; i < p.mu.nextProbeIndex; i++ {
		sampleDur := p.mu.history[i%probeHistoryLength]
		sum += sampleDur
		if max < sampleDur {
			max = sampleDur
		}
	}
	mean := sum / time.Duration(samplesNeeded)
	return mean, max
}

type dirIndex int

const (
	primaryDirIndex dirIndex = iota
	secondaryDirIndex
	numDirIndices
)

type dirAndFileHandle struct {
	Dir
	vfs.File
}

// switchableWriter is a subset of failoverWriter needed by failoverMonitor.
type switchableWriter interface {
	switchToNewDir(dirAndFileHandle) error
	ongoingLatencyOrErrorForCurDir() (time.Duration, error)
}

type failoverMonitorOptions struct {
	// The primary and secondary dir.
	dirs [numDirIndices]dirAndFileHandle

	FailoverOptions
	stopper *stopper
}

// failoverMonitor monitors the latency and error observed by the
// switchableWriter, and does failover by switching the dir. It also monitors
// the primary dir for failback.
type failoverMonitor struct {
	opts   failoverMonitorOptions
	prober dirProber
	mu     struct {
		sync.Mutex
		// dirIndex and lastFailbackTime are only modified by monitorLoop. They
		// are protected by the mutex for concurrent reads.

		// dirIndex is the directory to use by writer (if non-nil), or when the
		// writer becomes non-nil.
		dirIndex
		// The time at which the monitor last failed back to the primary. This is
		// only relevant when dirIndex == primaryDirIndex.
		lastFailBackTime time.Time
		// The current failoverWriter, exposed via a narrower interface.
		writer switchableWriter

		// Stats.
		dirSwitchCount              int64
		lastAccumulateIntoDurations time.Time
		primaryWriteDuration        time.Duration
		secondaryWriteDuration      time.Duration
	}
}

func newFailoverMonitor(opts failoverMonitorOptions) *failoverMonitor {
	m := &failoverMonitor{
		opts: opts,
	}
	m.mu.lastAccumulateIntoDurations = opts.timeSource.now()
	m.prober.init(opts.dirs[primaryDirIndex].FS,
		opts.dirs[primaryDirIndex].FS.PathJoin(opts.dirs[primaryDirIndex].Dirname, "probe-file"),
		opts.PrimaryDirProbeInterval, opts.stopper, opts.timeSource, opts.proberIterationForTesting)
	opts.stopper.runAsync(func() {
		m.monitorLoop(opts.stopper.shouldQuiesce())
	})
	return m
}

// Called when previous writer is closed
func (m *failoverMonitor) noWriter() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.writer = nil
	m.accumulateDurationLocked(m.opts.timeSource.now())
}

// writerCreateFunc is allowed to return nil, if there is an error. It is not
// the responsibility of failoverMonitor to handle that error. So this should
// not be due to an IO error (which failoverMonitor is interested in).
func (m *failoverMonitor) newWriter(writerCreateFunc func(dir dirAndFileHandle) switchableWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.writer != nil {
		panic("previous writer not closed")
	}
	m.mu.writer = writerCreateFunc(m.opts.dirs[m.mu.dirIndex])
}

func (m *failoverMonitor) elevateWriteStallThresholdForFailover() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.mu.dirIndex == secondaryDirIndex {
		return true
	}
	intervalSinceFailedback := m.opts.timeSource.now().Sub(m.mu.lastFailBackTime)
	return intervalSinceFailedback < m.opts.ElevatedWriteStallThresholdLag
}

func (m *failoverMonitor) accumulateDurationLocked(now time.Time) {
	dur := now.Sub(m.mu.lastAccumulateIntoDurations)
	if invariants.Enabled && dur < 0 {
		panic(errors.AssertionFailedf("time regressed: last accumulated %s; now is %s",
			m.mu.lastAccumulateIntoDurations, now))
	}
	m.mu.lastAccumulateIntoDurations = now
	if m.mu.dirIndex == primaryDirIndex {
		m.mu.primaryWriteDuration += dur
		return
	}
	m.mu.secondaryWriteDuration += dur
}

func (m *failoverMonitor) stats() FailoverStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accumulateDurationLocked(m.opts.timeSource.now())
	return FailoverStats{
		DirSwitchCount:         m.mu.dirSwitchCount,
		PrimaryWriteDuration:   m.mu.primaryWriteDuration,
		SecondaryWriteDuration: m.mu.secondaryWriteDuration,
	}
}

// lastWriterInfo is state maintained in the monitorLoop for the latest
// switchable writer. It is mainly used to dampen the switching.
type lastWriterInfo struct {
	writer                 switchableWriter
	numSwitches            int
	ongoingLatencyAtSwitch time.Duration
	errorCounts            [numDirIndices]int
}

func (m *failoverMonitor) monitorLoop(shouldQuiesce <-chan struct{}) {
	ticker := m.opts.timeSource.newTicker(m.opts.UnhealthySamplingInterval)
	if m.opts.monitorIterationForTesting != nil {
		m.opts.monitorIterationForTesting <- struct{}{}
	}
	tickerCh := ticker.ch()
	dirIndex := primaryDirIndex
	var lastWriter lastWriterInfo
	for {
		select {
		case <-shouldQuiesce:
			ticker.stop()
			m.prober.stop()
			return
		case <-tickerCh:
			writerOngoingLatency, writerErr := func() (time.Duration, error) {
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.mu.writer != lastWriter.writer {
					lastWriter = lastWriterInfo{writer: m.mu.writer}
				}
				if lastWriter.writer == nil {
					return 0, nil
				}
				return lastWriter.writer.ongoingLatencyOrErrorForCurDir()
			}()
			switchDir := false
			// Arbitrary value.
			const highSecondaryErrorCountThreshold = 2
			// We don't consider a switch if currently using the primary dir and the
			// secondary dir has high enough errors. It is more likely that someone
			// has misconfigured a secondary e.g. wrong permissions or not enough
			// disk space. We only remember the error history in the context of the
			// lastWriter since an operator can fix the underlying misconfiguration.
			unhealthyThreshold, failoverEnabled := m.opts.UnhealthyOperationLatencyThreshold()

			if !(lastWriter.errorCounts[secondaryDirIndex] >= highSecondaryErrorCountThreshold &&
				dirIndex == primaryDirIndex) && failoverEnabled {
				// Switching heuristics. Subject to change based on real world experience.
				if writerErr != nil {
					// An error causes an immediate switch, since a LogWriter with an
					// error is useless.
					lastWriter.errorCounts[dirIndex]++
					switchDir = true
				} else if writerOngoingLatency > unhealthyThreshold {
					// Arbitrary value.
					const switchImmediatelyCountThreshold = 2
					// High latency. Switch immediately if the number of switches that
					// have been done is below the threshold, since that gives us an
					// observation of both dirs. Once above that threshold, decay the
					// switch rate by increasing the observed latency needed for a
					// switch.
					if lastWriter.numSwitches < switchImmediatelyCountThreshold ||
						writerOngoingLatency > 2*lastWriter.ongoingLatencyAtSwitch {
						switchDir = true
						lastWriter.ongoingLatencyAtSwitch = writerOngoingLatency
					}
					// Else high latency, but not high enough yet to motivate switch.
				} else if dirIndex == secondaryDirIndex {
					// The writer looks healthy. We can still switch if the writer is using the
					// secondary dir and the primary is healthy again.
					primaryMean, primaryMax := m.prober.getMeanMax(m.opts.HealthyInterval)
					if primaryMean < m.opts.HealthyProbeLatencyThreshold &&
						primaryMax < m.opts.HealthyProbeLatencyThreshold {
						switchDir = true
					}
				}
			}
			if switchDir {
				lastWriter.numSwitches++
				if dirIndex == secondaryDirIndex {
					// Switching back to primary, so don't need to probe to see if
					// primary is healthy.
					m.prober.disableProbing()
					dirIndex = primaryDirIndex
				} else {
					m.prober.enableProbing()
					dirIndex = secondaryDirIndex
				}
				dir := m.opts.dirs[dirIndex]
				m.mu.Lock()
				now := m.opts.timeSource.now()
				m.accumulateDurationLocked(now)
				m.mu.dirIndex = dirIndex
				m.mu.dirSwitchCount++
				if dirIndex == primaryDirIndex {
					m.mu.lastFailBackTime = now
				}
				if m.mu.writer != nil {
					m.mu.writer.switchToNewDir(dir)
				}
				m.mu.Unlock()
			}
		}
		if m.opts.monitorStateForTesting != nil {
			m.opts.monitorStateForTesting(lastWriter.numSwitches, lastWriter.ongoingLatencyAtSwitch)
		}
		if m.opts.monitorIterationForTesting != nil {
			m.opts.monitorIterationForTesting <- struct{}{}
		}
	}
}

type logicalLogWithSizesEtc struct {
	num      NumWAL
	segments []segmentWithSizeEtc
}

type segmentWithSizeEtc struct {
	segment
	approxFileSize      uint64
	synchronouslyClosed bool
}

type failoverManager struct {
	opts Options
	// initialObsolete holds the set of DeletableLogs that formed the logs
	// passed into Init. The initialObsolete logs are all obsolete. Once
	// returned via Manager.Obsolete, initialObsolete is cleared. The
	// initialObsolete logs are stored separately from mu.queue because they may
	// include logs that were NOT created by the standalone manager, and
	// multiple physical log files may form one logical WAL.
	initialObsolete []DeletableLog

	// TODO(jackson/sumeer): read-path etc.

	dirHandles [numDirIndices]vfs.File
	stopper    *stopper
	monitor    *failoverMonitor
	mu         struct {
		sync.Mutex
		closedWALs []logicalLogWithSizesEtc
		ww         *failoverWriter
	}
	recycler LogRecycler
	// Due to async creation of files in failoverWriter, multiple goroutines can
	// concurrently try to get a file from the recycler. This mutex protects the
	// logRecycler.{Peek,Pop} pair.
	recyclerPeekPopMu sync.Mutex
}

var _ Manager = &failoverManager{}

// TODO(sumeer):
// - log deletion: if record.LogWriter did not close yet, the cleaner may
//   get an error when deleting or renaming (only under windows?).

// init implements Manager.
func (wm *failoverManager) init(o Options, initial Logs) error {
	if o.timeSource == nil {
		o.timeSource = defaultTime{}
	}
	o.FailoverOptions.EnsureDefaults()

	// Synchronously ensure that we're able to write to the secondary before we
	// proceed. An operator doesn't want to encounter an issue writing to the
	// secondary the first time there's a need to failover. We write a bit of
	// metadata to a file in the secondary's directory.
	f, err := o.Secondary.FS.Create(o.Secondary.FS.PathJoin(o.Secondary.Dirname, "failover_source"), "pebble-wal")
	if err != nil {
		return errors.Newf("failed to write to WAL secondary dir: %v", err)
	}
	if _, err := io.WriteString(f, fmt.Sprintf("primary: %s\nprocess start: %s\n",
		o.Primary.Dirname,
		time.Now(),
	)); err != nil {
		return errors.Newf("failed to write metadata to WAL secondary dir: %v", err)
	}
	if err := errors.CombineErrors(f.Sync(), f.Close()); err != nil {
		return err
	}

	stopper := newStopper()
	var dirs [numDirIndices]dirAndFileHandle
	for i, dir := range []Dir{o.Primary, o.Secondary} {
		dirs[i].Dir = dir
		f, err := dir.FS.OpenDir(dir.Dirname)
		if err != nil {
			return err
		}
		dirs[i].File = f
	}
	fmOpts := failoverMonitorOptions{
		dirs:            dirs,
		FailoverOptions: o.FailoverOptions,
		stopper:         stopper,
	}
	monitor := newFailoverMonitor(fmOpts)
	*wm = failoverManager{
		opts:       o,
		dirHandles: [numDirIndices]vfs.File{dirs[primaryDirIndex].File, dirs[secondaryDirIndex].File},
		stopper:    stopper,
		monitor:    monitor,
	}
	wm.recycler.Init(o.MaxNumRecyclableLogs)
	for _, ll := range initial {
		if wm.recycler.MinRecycleLogNum() <= ll.Num {
			wm.recycler.SetMinRecycleLogNum(ll.Num + 1)
		}
		var err error
		wm.initialObsolete, err = appendDeletableLogs(wm.initialObsolete, ll)
		if err != nil {
			return err
		}
	}
	return nil
}

// List implements Manager.
func (wm *failoverManager) List() Logs {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	n := len(wm.mu.closedWALs)
	if wm.mu.ww != nil {
		n++
	}
	wals := make(Logs, n)
	setLogicalLog := func(index int, llse logicalLogWithSizesEtc) {
		segments := make([]segment, len(llse.segments))
		for j := range llse.segments {
			segments[j] = llse.segments[j].segment
		}
		wals[index] = LogicalLog{
			Num:      llse.num,
			segments: segments,
		}
	}
	for i, llse := range wm.mu.closedWALs {
		setLogicalLog(i, llse)
	}
	if wm.mu.ww != nil {
		setLogicalLog(n-1, wm.mu.ww.getLog())
	}
	return wals
}

// Obsolete implements Manager.
func (wm *failoverManager) Obsolete(
	minUnflushedNum NumWAL, noRecycle bool,
) (toDelete []DeletableLog, err error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// If this is the first call to Obsolete after Open, we may have deletable
	// logs outside the queue.
	toDelete, wm.initialObsolete = wm.initialObsolete, nil

	i := 0
	for ; i < len(wm.mu.closedWALs); i++ {
		ll := wm.mu.closedWALs[i]
		if ll.num >= minUnflushedNum {
			break
		}
		// Recycle only the primary at logNameIndex=0, if there was no failover,
		// and synchronously closed. It may not be safe to recycle a file that is
		// still being written to. And recycling when there was a failover may
		// fill up the recycler with smaller log files. The restriction regarding
		// logNameIndex=0 is because logRecycler.Peek only exposes the
		// DiskFileNum, and we need to use that to construct the path -- we could
		// remove this restriction by changing the logRecycler interface, but we
		// don't bother.
		canRecycle := !noRecycle && len(ll.segments) == 1 && ll.segments[0].synchronouslyClosed &&
			ll.segments[0].logNameIndex == 0 &&
			ll.segments[0].dir == wm.opts.Primary
		if !canRecycle || !wm.recycler.Add(base.FileInfo{
			FileNum:  base.DiskFileNum(ll.num),
			FileSize: ll.segments[0].approxFileSize,
		}) {
			for _, s := range ll.segments {
				toDelete = append(toDelete, DeletableLog{
					FS:             s.dir.FS,
					Path:           s.dir.FS.PathJoin(s.dir.Dirname, makeLogFilename(ll.num, s.logNameIndex)),
					NumWAL:         ll.num,
					ApproxFileSize: s.approxFileSize,
				})
			}
		}
	}
	wm.mu.closedWALs = wm.mu.closedWALs[i:]
	return toDelete, nil
}

// Create implements Manager.
func (wm *failoverManager) Create(wn NumWAL, jobID int) (Writer, error) {
	func() {
		wm.mu.Lock()
		defer wm.mu.Unlock()
		if wm.mu.ww != nil {
			panic("previous wal.Writer not closed")
		}
	}()
	fwOpts := failoverWriterOpts{
		wn:                          wn,
		logger:                      wm.opts.Logger,
		timeSource:                  wm.opts.timeSource,
		jobID:                       jobID,
		logCreator:                  wm.logCreator,
		noSyncOnClose:               wm.opts.NoSyncOnClose,
		bytesPerSync:                wm.opts.BytesPerSync,
		preallocateSize:             wm.opts.PreallocateSize,
		minSyncInterval:             wm.opts.MinSyncInterval,
		fsyncLatency:                wm.opts.FsyncLatency,
		queueSemChan:                wm.opts.QueueSemChan,
		stopper:                     wm.stopper,
		failoverWriteAndSyncLatency: wm.opts.FailoverWriteAndSyncLatency,
		writerClosed:                wm.writerClosed,
		writerCreatedForTest:        wm.opts.logWriterCreatedForTesting,
		writeWALSyncOffsets:         wm.opts.WriteWALSyncOffsets,
	}
	var err error
	var ww *failoverWriter
	writerCreateFunc := func(dir dirAndFileHandle) switchableWriter {
		ww, err = newFailoverWriter(fwOpts, dir)
		if err != nil {
			return nil
		}
		return ww
	}
	wm.monitor.newWriter(writerCreateFunc)
	if ww != nil {
		wm.mu.Lock()
		defer wm.mu.Unlock()
		wm.mu.ww = ww
	}
	return ww, err
}

// ElevateWriteStallThresholdForFailover implements Manager.
func (wm *failoverManager) ElevateWriteStallThresholdForFailover() bool {
	return wm.monitor.elevateWriteStallThresholdForFailover()
}

func (wm *failoverManager) writerClosed(llse logicalLogWithSizesEtc) {
	wm.monitor.noWriter()
	wm.mu.Lock()
	defer wm.mu.Unlock()
	wm.mu.closedWALs = append(wm.mu.closedWALs, llse)
	wm.mu.ww = nil
}

// Stats implements Manager.
func (wm *failoverManager) Stats() Stats {
	obsoleteLogsCount, obsoleteLogSize := wm.recycler.Stats()
	failoverStats := wm.monitor.stats()
	failoverStats.FailoverWriteAndSyncLatency = wm.opts.FailoverWriteAndSyncLatency
	wm.mu.Lock()
	defer wm.mu.Unlock()
	var liveFileCount int
	var liveFileSize uint64
	updateStats := func(segments []segmentWithSizeEtc) {
		for _, s := range segments {
			liveFileCount++
			liveFileSize += s.approxFileSize
		}
	}
	for _, llse := range wm.mu.closedWALs {
		updateStats(llse.segments)
	}
	if wm.mu.ww != nil {
		updateStats(wm.mu.ww.getLog().segments)
	}
	for i := range wm.initialObsolete {
		if i == 0 || wm.initialObsolete[i].NumWAL != wm.initialObsolete[i-1].NumWAL {
			obsoleteLogsCount++
		}
		obsoleteLogSize += wm.initialObsolete[i].ApproxFileSize
	}
	return Stats{
		ObsoleteFileCount: obsoleteLogsCount,
		ObsoleteFileSize:  obsoleteLogSize,
		LiveFileCount:     liveFileCount,
		LiveFileSize:      liveFileSize,
		Failover:          failoverStats,
	}
}

// Close implements Manager.
func (wm *failoverManager) Close() error {
	wm.stopper.stop()
	// Since all goroutines are stopped, can close the dirs.
	var err error
	for _, f := range wm.dirHandles {
		err = firstError(err, f.Close())
	}
	return err
}

// RecyclerForTesting implements Manager.
func (wm *failoverManager) RecyclerForTesting() *LogRecycler {
	return nil
}

// logCreator implements the logCreator func type.
func (wm *failoverManager) logCreator(
	dir Dir, wn NumWAL, li LogNameIndex, r *latencyAndErrorRecorder, jobID int,
) (logFile vfs.File, initialFileSize uint64, err error) {
	logFilename := dir.FS.PathJoin(dir.Dirname, makeLogFilename(wn, li))
	isPrimary := dir == wm.opts.Primary
	// Only recycling when logNameIndex is 0 is a somewhat arbitrary choice.
	considerRecycle := li == 0 && isPrimary
	createInfo := CreateInfo{
		JobID:       jobID,
		Path:        logFilename,
		IsSecondary: !isPrimary,
		Num:         wn,
		Err:         nil,
	}
	defer func() {
		createInfo.Err = err
		if wm.opts.EventListener != nil {
			wm.opts.EventListener.LogCreated(createInfo)
		}
	}()
	if considerRecycle {
		// Try to use a recycled log file. Recycling log files is an important
		// performance optimization as it is faster to sync a file that has
		// already been written, than one which is being written for the first
		// time. This is due to the need to sync file metadata when a file is
		// being written for the first time. Note this is true even if file
		// preallocation is performed (e.g. fallocate).
		var recycleLog base.FileInfo
		var recycleOK bool
		func() {
			wm.recyclerPeekPopMu.Lock()
			defer wm.recyclerPeekPopMu.Unlock()
			recycleLog, recycleOK = wm.recycler.Peek()
			if recycleOK {
				if err = wm.recycler.Pop(recycleLog.FileNum); err != nil {
					panic(err)
				}
			}
		}()
		if recycleOK {
			createInfo.RecycledFileNum = recycleLog.FileNum
			recycleLogName := dir.FS.PathJoin(dir.Dirname, makeLogFilename(NumWAL(recycleLog.FileNum), 0))
			r.writeStart()
			logFile, err = dir.FS.ReuseForWrite(recycleLogName, logFilename, "pebble-wal")
			r.writeEnd(err)
			// TODO(sumeer): should we fatal since primary dir? At some point it is
			// better to fatal instead of continuing to failover.
			// base.MustExist(dir.FS, logFilename, wm.opts.Logger, err)
			if err != nil {
				// TODO(sumeer): we have popped from the logRecycler, which is
				// arguably correct, since we don't want to keep trying to reuse a log
				// that causes some error. But the original or new file may exist, and
				// no one will clean it up unless the process restarts.
				return nil, 0, err
			}
			// Figure out the recycled WAL size. This Stat is necessary because
			// ReuseForWrite's contract allows for removing the old file and
			// creating a new one. We don't know whether the WAL was actually
			// recycled.
			//
			// TODO(jackson): Adding a boolean to the ReuseForWrite return value
			// indicating whether or not the file was actually reused would allow us
			// to skip the stat and use recycleLog.FileSize.
			var finfo os.FileInfo
			finfo, err = logFile.Stat()
			if err != nil {
				logFile.Close()
				return nil, 0, err
			}
			initialFileSize = uint64(finfo.Size())
			return logFile, initialFileSize, nil
		}
	}
	// Did not recycle.
	//
	// Create file.
	r.writeStart()
	logFile, err = dir.FS.Create(logFilename, "pebble-wal")
	r.writeEnd(err)
	return logFile, 0, err
}

type stopper struct {
	quiescer chan struct{} // Closed when quiescing
	wg       sync.WaitGroup
}

func newStopper() *stopper {
	return &stopper{
		quiescer: make(chan struct{}),
	}
}

func (s *stopper) runAsync(f func()) {
	s.wg.Add(1)
	go func() {
		f()
		s.wg.Done()
	}()
}

// shouldQuiesce returns a channel which will be closed when stop() has been
// invoked and outstanding goroutines should begin to quiesce.
func (s *stopper) shouldQuiesce() <-chan struct{} {
	return s.quiescer
}

func (s *stopper) stop() {
	close(s.quiescer)
	s.wg.Wait()
}

// timeSource and tickerI are extracted from CockroachDB's timeutil, with
// removal of support for Timer, and added support in the manual
// implementation for reset.

// timeSource is used to interact with the clock and tickers. Abstracts
// time.Now and time.NewTicker for testing.
type timeSource interface {
	now() time.Time
	newTicker(duration time.Duration) tickerI
}

// tickerI is an interface wrapping time.Ticker.
type tickerI interface {
	reset(duration time.Duration)
	stop()
	ch() <-chan time.Time
}

// defaultTime is a timeSource using the time package.
type defaultTime struct{}

var _ timeSource = defaultTime{}

func (defaultTime) now() time.Time {
	return time.Now()
}

func (defaultTime) newTicker(duration time.Duration) tickerI {
	return (*defaultTicker)(time.NewTicker(duration))
}

// defaultTicker uses time.Ticker.
type defaultTicker time.Ticker

var _ tickerI = &defaultTicker{}

func (t *defaultTicker) reset(duration time.Duration) {
	(*time.Ticker)(t).Reset(duration)
}

func (t *defaultTicker) stop() {
	(*time.Ticker)(t).Stop()
}

func (t *defaultTicker) ch() <-chan time.Time {
	return (*time.Ticker)(t).C
}

// Make lint happy.
var _ = (*failoverMonitor).noWriter
var _ = (*failoverManager).writerClosed
var _ = (&stopper{}).shouldQuiesce

// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package wal

import (
	"sync"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/exp/rand"
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

// There is no close/stop method on the dirProber -- use stopper.
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
	_, err := rand.Read(p.buf[:])
	if err != nil {
		panic(err)
	}
	stopper.runAsync(func() {
		p.probeLoop(stopper.shouldQuiesce())
	})
}

func (p *dirProber) probeLoop(shouldQuiesce <-chan struct{}) {
	ticker := p.timeSource.newTicker(p.interval)
	ticker.stop()
	tickerCh := ticker.ch()
	done := false
	var enabled bool
	for !done {
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
				f, err := p.fs.Create(p.filename)
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

		case <-shouldQuiesce:
			done = true

		case enabled = <-p.enabled:
			if enabled {
				ticker.reset(p.interval)
			} else {
				ticker.stop()
				p.mu.Lock()
				p.mu.firstProbeIndex = 0
				p.mu.nextProbeIndex = 0
				p.mu.Unlock()
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
	}
}

func newFailoverMonitor(opts failoverMonitorOptions) *failoverMonitor {
	m := &failoverMonitor{
		opts: opts,
	}
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
			if !(lastWriter.errorCounts[secondaryDirIndex] >= highSecondaryErrorCountThreshold &&
				dirIndex == primaryDirIndex) {
				// Switching heuristics. Subject to change based on real world experience.
				if writerErr != nil {
					// An error causes an immediate switch, since a LogWriter with an
					// error is useless.
					lastWriter.errorCounts[dirIndex]++
					switchDir = true
				} else if writerOngoingLatency > m.opts.UnhealthyOperationLatencyThreshold {
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
				m.mu.dirIndex = dirIndex
				if dirIndex == primaryDirIndex {
					m.mu.lastFailBackTime = m.opts.timeSource.now()
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

func (o *FailoverOptions) ensureDefaults() {
	if o.PrimaryDirProbeInterval == 0 {
		o.PrimaryDirProbeInterval = time.Second
	}
	if o.HealthyProbeLatencyThreshold == 0 {
		o.HealthyProbeLatencyThreshold = 100 * time.Millisecond
	}
	if o.HealthyInterval == 0 {
		o.HealthyInterval = 2 * time.Minute
	}
	if o.UnhealthySamplingInterval == 0 {
		o.UnhealthySamplingInterval = 100 * time.Millisecond
	}
	if o.UnhealthyOperationLatencyThreshold == 0 {
		o.UnhealthyOperationLatencyThreshold = 200 * time.Millisecond
	}
}

type failoverManager struct {
	opts Options
	// TODO(jackson/sumeer): read-path etc.

	dirHandles [numDirIndices]vfs.File
	stopper    *stopper
	monitor    *failoverMonitor
}

var _ Manager = &failoverManager{}

// TODO(sumeer):
//
// - log recycling: only those in primary dir. Keep track of those where
//   record.LogWriter closed. Those are the only ones we can recycle.
//
// - log deletion: if record.LogWriter did not close yet, the cleaner may
//   get an error when deleting or renaming (only under windows?).
//
// - calls to EventListener

// Init implements Manager.
func (wm *failoverManager) Init(o Options, initial Logs) error {
	if o.timeSource == nil {
		o.timeSource = defaultTime{}
	}
	o.FailoverOptions.ensureDefaults()
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
	// TODO(jackson): list dirs and assemble a list of all NumWALs and
	// corresponding log files.

	*wm = failoverManager{
		opts:       o,
		dirHandles: [numDirIndices]vfs.File{dirs[primaryDirIndex].File, dirs[secondaryDirIndex].File},
		stopper:    stopper,
		monitor:    monitor,
	}
	return nil
}

// List implements Manager.
func (wm *failoverManager) List() (Logs, error) {
	return Scan(wm.opts.Primary, wm.opts.Secondary)
}

// Obsolete implements Manager.
func (wm *failoverManager) Obsolete(
	minUnflushedNum NumWAL, noRecycle bool,
) (toDelete []DeletableLog, err error) {
	// TODO(sumeer):
	return nil, nil
}

// Create implements Manager.
func (wm *failoverManager) Create(wn NumWAL, jobID int) (Writer, error) {
	fwOpts := failoverWriterOpts{
		wn:                   wn,
		logger:               wm.opts.Logger,
		timeSource:           wm.opts.timeSource,
		noSyncOnClose:        wm.opts.NoSyncOnClose,
		bytesPerSync:         wm.opts.BytesPerSync,
		preallocateSize:      wm.opts.PreallocateSize,
		minSyncInterval:      wm.opts.MinSyncInterval,
		fsyncLatency:         wm.opts.FsyncLatency,
		queueSemChan:         wm.opts.QueueSemChan,
		stopper:              wm.stopper,
		writerClosed:         wm.writerClosed,
		writerCreatedForTest: wm.opts.logWriterCreatedForTesting,
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
	return ww, err
}

// ElevateWriteStallThresholdForFailover implements Manager.
func (wm *failoverManager) ElevateWriteStallThresholdForFailover() bool {
	return wm.monitor.elevateWriteStallThresholdForFailover()
}

func (wm *failoverManager) writerClosed() {
	wm.monitor.noWriter()
}

// Stats implements Manager.
func (wm *failoverManager) Stats() Stats {
	// TODO(sumeer):
	return Stats{}
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

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
	fs       vfs.FS
	filename string
	interval time.Duration
	buf      [100 << 10]byte
	enabled  chan bool
	mu       struct {
		sync.Mutex
		history         [probeHistoryLength]time.Duration
		firstProbeIndex int
		nextProbeIndex  int
	}
}

const probeHistoryLength = 128

// Large value.
const failedProbeDuration = 24 * 60 * 60 * time.Second

// There is no close/stop method on the dirProber -- use stopper.
func (p *dirProber) init(fs vfs.FS, filename string, interval time.Duration, stopper *stopper) {
	*p = dirProber{
		fs:       fs,
		filename: filename,
		interval: interval,
		enabled:  make(chan bool),
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
	ticker := time.NewTicker(p.interval)
	ticker.Stop()
	done := false
	var enabled bool
	for !done {
		select {
		case <-ticker.C:
			if !enabled {
				// Could have a tick waiting before we disabled it. Ignore.
				continue
			}
			probeDur := func() time.Duration {
				// Delete, create, write, sync.
				start := time.Now()
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
				return time.Since(start)
			}()
			p.mu.Lock()
			nextIndex := p.mu.nextProbeIndex % probeHistoryLength
			p.mu.history[nextIndex] = probeDur
			firstIndex := p.mu.firstProbeIndex % probeHistoryLength
			if firstIndex == nextIndex {
				// Wrapped around
				p.mu.firstProbeIndex++
			}
			p.mu.nextProbeIndex++
			p.mu.Unlock()

		case <-shouldQuiesce:
			done = true

		case enabled = <-p.enabled:
			if enabled {
				ticker.Reset(p.interval)
			} else {
				ticker.Stop()
				p.mu.Lock()
				p.mu.firstProbeIndex = 0
				p.mu.nextProbeIndex = 0
				p.mu.Unlock()
			}
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
	if samplesNeeded < numSamples {
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

type failoverMonitorOptions struct {
	dirs [numDirIndices]dirAndFileHandle

	primaryDirProbeInterval      time.Duration
	healthyProbeLatencyThreshold time.Duration
	healthyInterval              time.Duration

	unhealthySamplingInterval          time.Duration
	unhealthyOperationLatencyThreshold time.Duration

	elevatedWriteStallThresholdLag time.Duration

	stopper *stopper
}

// switchableWriter is a subset of failoverWriter needed by failoverMonitor.
type switchableWriter interface {
	switchToNewDir(dirAndFileHandle) error
	ongoingLatencyOrErrorForCurDir() (time.Duration, error)
}

// failoverMonitor monitors the latency and error observed by the
// switchableWriter, and does failover by switching the dir. It also monitors
// the primary dir for failback.
type failoverMonitor struct {
	opts   failoverMonitorOptions
	prober dirProber
	mu     struct {
		sync.Mutex
		// dirIndex and lastFailbackTime are only modified by monitorLoop.
		dirIndex
		lastFailBackTime time.Time
		writer           switchableWriter
	}
}

func newFailoverMonitor(opts failoverMonitorOptions) *failoverMonitor {
	m := &failoverMonitor{
		opts: opts,
	}
	m.prober.init(opts.dirs[primaryDirIndex].FS,
		opts.dirs[primaryDirIndex].FS.PathJoin(opts.dirs[primaryDirIndex].Dirname, "probe-file"),
		opts.primaryDirProbeInterval, opts.stopper)
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

// writerCreateFunc is allowed to return nil.
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
	intervalSinceFailedback := time.Since(m.mu.lastFailBackTime)
	return intervalSinceFailedback > m.opts.elevatedWriteStallThresholdLag
}

type lastWriterInfo struct {
	writer                 switchableWriter
	numSwitches            int
	ongoingLatencyAtSwitch time.Duration
	errorCounts            [numDirIndices]int
}

func (m *failoverMonitor) monitorLoop(shouldQuiesce <-chan struct{}) {
	ticker := time.NewTicker(m.opts.unhealthySamplingInterval)
	dirIndex := primaryDirIndex
	var lastWriter lastWriterInfo
	for {
		select {
		case <-shouldQuiesce:
			return
		case <-ticker.C:
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
			// We don't consider a switch if currently using the primary dir and the
			// secondary dir has high enough errors. It is more likely that someone
			// has misconfigured a secondary e.g. wrong permissions or not enough
			// disk space. We only remember the error history in the context of the
			// lastWriter since an operator can fix the underlying misconfiguration.
			if !(lastWriter.errorCounts[secondaryDirIndex] > 2 && dirIndex == primaryDirIndex) {
				// Switching heuristics. Subject to change based on real world experience.
				if writerErr != nil {
					// An error causes an immediate switch, since a *LogWriter with an error is useless.
					lastWriter.errorCounts[dirIndex]++
					switchDir = true

				} else if writerOngoingLatency > m.opts.unhealthyOperationLatencyThreshold {
					// High latency. Switch immediately two times, since that gives us an
					// observation of both dirs. After that, decay the switch rate by
					// increasing the observed latency needed for a switch.
					if lastWriter.numSwitches < 2 || writerOngoingLatency > 2*lastWriter.ongoingLatencyAtSwitch {
						switchDir = true
						lastWriter.ongoingLatencyAtSwitch = writerOngoingLatency
					}
					// Else high latency, but not high enough yet to motivate switch.
				} else if dirIndex == secondaryDirIndex {
					// The writer looks healthy. We can still switch if the writer is using the
					// secondary dir and the primary is healthy again.
					primaryMean, primaryMax := m.prober.getMeanMax(m.opts.healthyInterval)
					if primaryMean < m.opts.healthyProbeLatencyThreshold && primaryMax < m.opts.healthyProbeLatencyThreshold {
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
					m.mu.lastFailBackTime = time.Now()
				}
				if m.mu.writer != nil {
					m.mu.writer.switchToNewDir(dir)
				}
				m.mu.Unlock()
			}
		}
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

// Init implements Manager.
func (wm *failoverManager) Init(o Options) error {
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
		dirs: dirs,
		// TODO(sumeer): make configurable.
		primaryDirProbeInterval:            time.Second,
		healthyProbeLatencyThreshold:       100 * time.Millisecond,
		healthyInterval:                    2 * time.Minute,
		unhealthySamplingInterval:          100 * time.Millisecond,
		unhealthyOperationLatencyThreshold: 200 * time.Millisecond,
		elevatedWriteStallThresholdLag:     o.ElevatedWriteStallThresholdLag,
		stopper:                            stopper,
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
func (wm *failoverManager) List() ([]NumWAL, error) {
	// TODO(jackson):
	return nil, nil
}

// Delete implements Manager.
func (wm *failoverManager) Delete(highestObsoleteNum NumWAL) error {
	// TODO(sumeer):
	return nil
}

// OpenForRead implements Manager.
func (wm *failoverManager) OpenForRead(wn NumWAL, strictWALTail bool) (Reader, error) {
	// TODO(jackson):
	return nil, nil
}

// Create implements Manager.
func (wm *failoverManager) Create(wn NumWAL) (Writer, error) {
	fwOpts := failoverWriterOpts{
		wn:              wn,
		logger:          wm.opts.Logger,
		noSyncOnClose:   wm.opts.NoSyncOnClose,
		bytesPerSync:    wm.opts.BytesPerSync,
		preallocateSize: wm.opts.PreallocateSize,
		minSyncInterval: wm.opts.MinSyncInterval,
		fsyncLatency:    wm.opts.FsyncLatency,
		queueSemChan:    wm.opts.QueueSemChan,
		stopper:         wm.stopper,
	}
	var err error
	var ww *failoverWriter
	writerCreateFunc := func(dir dirAndFileHandle) switchableWriter {
		ww, err = newFailoverWriter(fwOpts, dir, wm)
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

// firstError returns the first non-nil error of err0 and err1, or nil if both
// are nil.
func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}

// Make lint happy.
var _ = (*failoverMonitor).noWriter
var _ = (*failoverManager).writerClosed

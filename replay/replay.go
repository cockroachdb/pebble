// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package replay implements collection and replaying of compaction benchmarking
// workloads. A workload is a collection of flushed and ingested sstables, along
// with the corresponding manifests describing the order and grouping with which
// they were applied. Replaying a workload flushes and ingests the same keys and
// sstables to reproduce the write workload for the purpose of evaluating
// compaction heuristics.
package replay

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/sync/errgroup"
)

// A Pacer paces replay of a workload, determining when to apply the next
// incoming write.
type Pacer interface {
	pace(r *Runner, step workloadStep) time.Duration
}

// computeReadAmp calculates the read amplification from a manifest.Version
func computeReadAmp(v *manifest.Version) int {
	refRAmp := v.L0Sublevels.ReadAmplification()
	for _, lvl := range v.Levels[1:] {
		if !lvl.Empty() {
			refRAmp++
		}
	}
	return refRAmp
}

// waitForReadAmpLE is a common function used by PaceByReferenceReadAmp and
// PaceByFixedReadAmp to wait on the dbMetricsNotifier condition variable if the
// read amplification observed is greater than the specified target (refRAmp).
func waitForReadAmpLE(r *Runner, rAmp int) {
	r.dbMetricsNotifier.L.Lock()
	m := r.dbMetrics
	ra := m.ReadAmp()
	for ra > rAmp {
		r.dbMetricsNotifier.Wait()
		ra = r.dbMetrics.ReadAmp()
	}
	r.dbMetricsNotifier.L.Unlock()
}

// Unpaced implements Pacer by applying each new write as soon as possible. It
// may be useful for examining performance under high read amplification.
type Unpaced struct{}

func (Unpaced) pace(*Runner, workloadStep) (d time.Duration) { return }

// PaceByReferenceReadAmp implements Pacer by applying each new write following
// the collected workloads read amplification.
type PaceByReferenceReadAmp struct{}

func (PaceByReferenceReadAmp) pace(r *Runner, w workloadStep) time.Duration {
	startTime := time.Now()
	refRAmp := computeReadAmp(w.pv)
	waitForReadAmpLE(r, refRAmp)
	return time.Since(startTime)
}

// PaceByFixedReadAmp implements Pacer by applying each new write following a
// fixed read amplification.
type PaceByFixedReadAmp int

func (pra PaceByFixedReadAmp) pace(r *Runner, _ workloadStep) time.Duration {
	startTime := time.Now()
	waitForReadAmpLE(r, int(pra))
	return time.Since(startTime)
}

// Metrics holds the various statistics on a replay run and its performance.
type Metrics struct {
	CompactionCounts struct {
		Total       int64
		Default     int64
		DeleteOnly  int64
		ElisionOnly int64
		Move        int64
		Read        int64
		Rewrite     int64
		MultiLevel  int64
	}
	Final  *pebble.Metrics
	Ingest struct {
		BytesIntoL0 uint64
		// BytesWeightedByLevel is calculated as the number of bytes ingested
		// into a level multiplied by the level's distance from the bottommost
		// level (L6), summed across all levels. It can be used to guage how
		// effective heuristics are at ingesting files into lower levels, saving
		// write amplification.
		BytesWeightedByLevel uint64
	}
	TotalWriteAmp       float64
	WriteStalls         uint64
	WriteStallsDuration time.Duration
}

// BenchmarkString returns the metrics in the form of a series of 'Benchmark'
// lines understandable by benchstat.
func (m *Metrics) BenchmarkString(name string) string {
	groups := []struct {
		label string
		pairs []string
	}{
		{label: "CompactionCounts", pairs: []string{
			fmt.Sprint(m.CompactionCounts.Total), "compactions",
			fmt.Sprint(m.CompactionCounts.Default), "default",
			fmt.Sprint(m.CompactionCounts.DeleteOnly), "delete",
			fmt.Sprint(m.CompactionCounts.ElisionOnly), "elision",
			fmt.Sprint(m.CompactionCounts.Move), "move",
			fmt.Sprint(m.CompactionCounts.Read), "read",
			fmt.Sprint(m.CompactionCounts.Rewrite), "rewrite",
			fmt.Sprint(m.CompactionCounts.MultiLevel), "multilevel",
		}},
		{label: "WriteAmp", pairs: []string{
			fmt.Sprint(m.TotalWriteAmp), "wamp",
		}},
		{label: "WriteStalls", pairs: []string{
			fmt.Sprint(m.WriteStalls), "stalls",
			fmt.Sprint(m.WriteStallsDuration.Seconds()), "stall-secs",
		}},
		{label: "IngestedIntoL0", pairs: []string{
			fmt.Sprint(m.Ingest.BytesIntoL0), "bytes",
		}},
		{label: "IngestWeightedByLevel", pairs: []string{
			fmt.Sprint(m.Ingest.BytesWeightedByLevel), "bytes",
		}},
	}

	var maxLenMetric int
	var maxLenGroup int
	for _, grp := range groups {
		if v := len(grp.label); maxLenGroup < v {
			maxLenGroup = v
		}
		for _, p := range grp.pairs {
			if v := len(p); maxLenMetric < v {
				maxLenMetric = v
			}
		}
	}
	benchmarkPrefixSpecifier := fmt.Sprintf("BenchmarkReplay/%s/%%-%ds 1", name, maxLenGroup)
	pairFormatSpecifier := fmt.Sprintf("    %%%ds", maxLenMetric)

	var buf bytes.Buffer
	for _, grp := range groups {
		fmt.Fprintf(&buf, benchmarkPrefixSpecifier, grp.label)
		for _, p := range grp.pairs {
			fmt.Fprintf(&buf, pairFormatSpecifier, p)
		}
		fmt.Fprintln(&buf)
	}
	return buf.String()
}

// Runner runs a captured workload against a test database, collecting
// metrics on performance.
type Runner struct {
	RunDir       string
	WorkloadFS   vfs.FS
	WorkloadPath string
	Pacer        Pacer
	Opts         *pebble.Options

	// Internal state.
	cancel func()

	// workloadExhausted is a channel that is used by refreshMetrics in order to
	// receive a single message once all the workload steps have been applied.
	workloadExhausted chan struct{}

	d        *pebble.DB
	err      atomic.Value
	errgroup *errgroup.Group
	metrics  struct {
		writeStalls             uint64
		writeStallsDurationNano uint64
	}
	readerOpts sstable.ReaderOptions
	stagingDir string
	steps      chan workloadStep

	// compactionEnded is a channel used to notify the refreshMetrics method that
	// a compaction has finished and hence it can update the metrics.
	compactionEnded            chan struct{}
	finishedCompactionNotifier sync.Cond

	// compactionEndedCount keeps track of the cumulative number of compactions.
	// It is guarded by the finishedCompactionNotifier condition variable lock.
	compactionEndedCount int64

	// atomicCompactionStartCount keeps track of the cumulative number of
	// compactions started. It's incremented by a pebble.EventListener's
	// CompactionStart handler.
	//
	// Must be accessed with atomics.
	atomicCompactionStartCount int64

	// compactionsHaveQuiesced is a channel that is closed once compactions have
	// quiesced allowing the compactionNotified goroutine to complete.
	compactionsHaveQuiesced chan struct{}

	// dbMetrics and dbMetricsNotifier work in unison to update the metrics and
	// notify (broadcast) to any waiting clients that metrics have been updated.
	dbMetricsNotifier sync.Cond
	dbMetrics         *pebble.Metrics

	workload struct {
		manifests []string
		// manifest{Idx,Off} record the starting position of the workload
		// relative to the initial database state.
		manifestIdx int
		manifestOff int64
		// sstables records the set of captured workload sstables by file num.
		sstables map[base.FileNum]struct{}
	}
}

// Run begins executing the workload and returns.
//
// The workload application will respect the provided context's cancellation.
func (r *Runner) Run(ctx context.Context) error {
	// Find the workload start relative to the RunDir's existing database state.
	// A prefix of the workload's manifest edits are expected to have already
	// been applied to the checkpointed existing database state.
	var err error
	r.workload.manifests, r.workload.sstables, err = findWorkloadFiles(r.WorkloadPath, r.WorkloadFS)
	if err != nil {
		return err
	}
	r.workload.manifestIdx, r.workload.manifestOff, err = findManifestStart(r.RunDir, r.Opts.FS, r.workload.manifests)
	if err != nil {
		return err
	}

	// Set up a staging dir for files that will be ingested.
	r.stagingDir = r.Opts.FS.PathJoin(r.RunDir, "staging")
	if err := r.Opts.FS.MkdirAll(r.stagingDir, os.ModePerm); err != nil {
		return err
	}

	r.dbMetricsNotifier = sync.Cond{
		L: &sync.Mutex{},
	}
	r.finishedCompactionNotifier = sync.Cond{
		L: &sync.Mutex{},
	}

	// Extend the user-provided Options with extensions necessary for replay
	// mechanics.
	r.Opts.AddEventListener(r.eventListener())
	r.Opts.EnsureDefaults()
	r.readerOpts = r.Opts.MakeReaderOptions()
	r.Opts.DisableWAL = true
	r.d, err = pebble.Open(r.RunDir, r.Opts)
	if err != nil {
		return err
	}

	r.dbMetrics = r.d.Metrics()
	r.workloadExhausted = make(chan struct{}, 1)
	r.compactionEnded = make(chan struct{})
	r.compactionsHaveQuiesced = make(chan struct{})

	// Use a buffered channel to allow the prepareWorkloadSteps to read ahead,
	// buffering up to cap(r.steps) steps ahead of the current applied state.
	// Flushes need to be buffered and ingested sstables need to be copied, so
	// pipelining this preparation makes it more likely the step will be ready
	// to apply when the pacer decides to apply it.
	r.steps = make(chan workloadStep, 5)

	ctx, r.cancel = context.WithCancel(ctx)
	r.errgroup, ctx = errgroup.WithContext(ctx)
	r.errgroup.Go(func() error { return r.prepareWorkloadSteps(ctx) })
	r.errgroup.Go(func() error { return r.applyWorkloadSteps(ctx) })
	r.errgroup.Go(func() error { return r.refreshMetrics(ctx) })
	r.errgroup.Go(func() error { return r.compactionNotified(ctx) })
	return nil
}

func (r *Runner) refreshMetrics(ctx context.Context) error {
	workloadExhausted := false
	done := false

	fetchDoneState := func() {
		r.finishedCompactionNotifier.L.Lock()
		done = r.compactionEndedCount == atomic.LoadInt64(&r.atomicCompactionStartCount)
		r.finishedCompactionNotifier.L.Unlock()
	}

	for !done || !workloadExhausted {
		select {
		// r.workloadExhausted receives a single message when the workload is exhausted
		case <-r.workloadExhausted:
			workloadExhausted = true
			fetchDoneState()
			if done {
				r.stopCompactionNotifier()
			}
		case <-ctx.Done():
			r.stopCompactionNotifier()
			return ctx.Err()
		case <-r.compactionEnded:
			r.dbMetricsNotifier.L.Lock()
			r.dbMetrics = r.d.Metrics()
			r.dbMetricsNotifier.Broadcast()
			r.dbMetricsNotifier.L.Unlock()

			fetchDoneState()
			if done && workloadExhausted {
				r.stopCompactionNotifier()
			}
		}
	}
	return nil
}

func (r *Runner) stopCompactionNotifier() {
	// Close the channel which will cause the compactionNotified goroutine to exit
	close(r.compactionsHaveQuiesced)

	// Wake up compaction notifier if it is waiting on the
	// r.finishedCompactionNotifier condition and make sure it doesn't go back
	// to sleep
	r.finishedCompactionNotifier.L.Lock()
	r.compactionEndedCount++
	r.finishedCompactionNotifier.Broadcast()
	r.finishedCompactionNotifier.L.Unlock()
}

// Wait waits for the workload replay to complete. Wait returns once the entire
// workload has been replayed, and compactions have quiesced.
func (r *Runner) Wait() (Metrics, error) {
	err := r.errgroup.Wait()
	if storedErr := r.err.Load(); storedErr != nil {
		err = storedErr.(error)
	}
	pm := r.d.Metrics()
	var total pebble.LevelMetrics
	var ingestBytesWeighted uint64
	for l := 0; l < len(pm.Levels); l++ {
		total.Add(&pm.Levels[l])
		total.Sublevels += pm.Levels[l].Sublevels
		ingestBytesWeighted += pm.Levels[l].BytesIngested * uint64(len(pm.Levels)-l-1)
	}

	m := Metrics{
		Final:               pm,
		TotalWriteAmp:       total.WriteAmp(),
		WriteStalls:         r.metrics.writeStalls,
		WriteStallsDuration: time.Duration(r.metrics.writeStallsDurationNano),
	}
	m.CompactionCounts.Total = pm.Compact.Count
	m.CompactionCounts.Default = pm.Compact.DefaultCount
	m.CompactionCounts.DeleteOnly = pm.Compact.DeleteOnlyCount
	m.CompactionCounts.ElisionOnly = pm.Compact.ElisionOnlyCount
	m.CompactionCounts.Move = pm.Compact.MoveCount
	m.CompactionCounts.Read = pm.Compact.ReadCount
	m.CompactionCounts.Rewrite = pm.Compact.RewriteCount
	m.CompactionCounts.MultiLevel = pm.Compact.MultiLevelCount
	m.Ingest.BytesIntoL0 = pm.Levels[0].BytesIngested
	m.Ingest.BytesWeightedByLevel = ingestBytesWeighted
	return m, err
}

// Close closes remaining open resources, including the database. It must be
// called after Wait.
func (r *Runner) Close() error {
	return r.d.Close()
}

// A workloadStep describes a single manifest edit in the workload. It may be a
// flush or ingest that should be applied to the test database, or it may be a
// compaction that is surfaced to allow the replay logic to compare against the
// state of the database at workload collection time.
type workloadStep struct {
	kind stepKind
	ve   manifest.VersionEdit

	// a Version describing the state of the LSM *before* the workload was
	// collected.
	pv *manifest.Version
	// a Version describing the state of the LSM when the workload was
	// collected.
	v *manifest.Version

	// non-nil for flushStepKind
	flushBatch *pebble.Batch

	tablesToIngest []string
}

type stepKind uint8

const (
	flushStepKind stepKind = iota
	ingestStepKind
	compactionStepKind
)

// eventListener returns a Pebble EventListener that is installed on the replay
// database so that the replay runner has access to internal Pebble events.
func (r *Runner) eventListener() pebble.EventListener {
	var writeStallBegin time.Time
	l := pebble.EventListener{
		BackgroundError: func(err error) {
			r.err.Store(err)
			r.cancel()
		},
		WriteStallBegin: func(pebble.WriteStallBeginInfo) {
			atomic.AddUint64(&r.metrics.writeStalls, 1)
			writeStallBegin = time.Now()
		},
		WriteStallEnd: func() {
			atomic.AddUint64(&r.metrics.writeStallsDurationNano,
				uint64(time.Since(writeStallBegin).Nanoseconds()))
		},
		CompactionBegin: func(_ pebble.CompactionInfo) {
			atomic.AddInt64(&r.atomicCompactionStartCount, 1)
		},
		CompactionEnd: func(_ pebble.CompactionInfo) {
			r.finishedCompactionNotifier.L.Lock()
			defer r.finishedCompactionNotifier.L.Unlock()
			r.compactionEndedCount++
			r.finishedCompactionNotifier.Broadcast()
		},
	}
	l.EnsureDefaults(nil)
	return l
}

// applyWorkloadSteps runs in its own goroutine, reading workload steps off the
// r.steps channel and applying them to the test database.
func (r *Runner) applyWorkloadSteps(ctx context.Context) error {
	for {
		var ok bool
		var step workloadStep
		select {
		case <-ctx.Done():
			return ctx.Err()
		case step, ok = <-r.steps:
			if !ok {
				// Exhausted the workload. Exit.
				r.workloadExhausted <- struct{}{}
				return nil
			}
		}

		// TODO(leon,jackson): Make sure to sum the duration statistics
		r.Pacer.pace(r, step)

		switch step.kind {
		case flushStepKind:
			if err := step.flushBatch.Commit(&pebble.WriteOptions{Sync: false}); err != nil {
				return err
			}
		case ingestStepKind:
			if err := r.d.Ingest(step.tablesToIngest); err != nil {
				return err
			}

		case compactionStepKind:
			// TODO(jackson,leon): Apply the step to runner state.
		}

		var _ = step
	}
}

// prepareWorkloadSteps runs in its own goroutine, reading the workload
// manifests in order to reconstruct the workload and prepare each step to be
// applied. It sends each workload step to the r.steps channel.
func (r *Runner) prepareWorkloadSteps(ctx context.Context) error {
	defer func() { close(r.steps) }()

	idx := r.workload.manifestIdx

	var flushBufs flushBuffers
	var v *manifest.Version
	var previousVersion *manifest.Version
	var bve manifest.BulkVersionEdit
	var blobLevels manifest.BlobLevels
	bve.AddedByFileNum = make(map[base.FileNum]*manifest.FileMetadata)
	applyVE := func(ve *manifest.VersionEdit) error {
		return bve.Accumulate(ve)
	}
	currentVersion := func() (*manifest.Version, error) {
		var err error
		v, _, _, err = bve.Apply(v,
			r.Opts.Comparer.Compare,
			r.Opts.Comparer.FormatKey,
			r.Opts.FlushSplitBytes,
			r.Opts.Experimental.ReadCompactionRate, &blobLevels)
		bve = manifest.BulkVersionEdit{AddedByFileNum: bve.AddedByFileNum}
		return v, err
	}

	for ; idx < len(r.workload.manifests); idx++ {
		err := func() error {
			manifestName := r.workload.manifests[idx]
			f, err := r.WorkloadFS.Open(r.WorkloadFS.PathJoin(r.WorkloadPath, manifestName))
			if err != nil {
				return err
			}
			defer f.Close()

			rr := record.NewReader(f, 0 /* logNum */)
			// A manifest's first record always holds the initial version state.
			// If this is the first manifest we're examining, we load it in
			// order to seed `metas` with the file metadata of the existing
			// files. Otherwise, we can skip it because we already know all the
			// file metadatas up to this point.
			rec, err := rr.Next()
			if err != nil {
				return err
			}
			if idx == r.workload.manifestIdx {
				var ve manifest.VersionEdit
				if err := ve.Decode(rec); err != nil {
					return err
				}
				if err := applyVE(&ve); err != nil {
					return err
				}
			}

			// Read the remaining of the manifests version edits, one-by-one.
			for {
				rec, err := rr.Next()
				if err == io.EOF || record.IsInvalidRecord(err) {
					break
				} else if err != nil {
					return err
				}
				var ve manifest.VersionEdit
				if err = ve.Decode(rec); err == io.EOF || record.IsInvalidRecord(err) {
					break
				} else if err != nil {
					return err
				}
				if err := applyVE(&ve); err != nil {
					return err
				}
				if idx == r.workload.manifestIdx && rr.Offset() <= r.workload.manifestOff {
					// The record rec began at an offset strictly less than
					// rr.Offset(), which means it's strictly less than
					// r.workload.manifestOff, and we should skip it.
					continue
				}
				if len(ve.NewFiles) == 0 && len(ve.DeletedFiles) == 0 {
					// Skip WAL rotations and other events that don't affect the
					// files of the LSM.
					continue
				}

				s := workloadStep{ve: ve}
				if len(ve.DeletedFiles) > 0 {
					// If a version edit deletes files, we assume it's a compaction.
					s.kind = compactionStepKind
				} else {
					// Default to ingest. If any files have unequal
					// smallest,largest sequence numbers, we'll update this to a
					// flush.
					s.kind = ingestStepKind
				}
				var newFiles []base.FileNum
				for _, nf := range ve.NewFiles {
					newFiles = append(newFiles, nf.Meta.FileNum)
					if s.kind == ingestStepKind && (nf.Meta.SmallestSeqNum != nf.Meta.LargestSeqNum || nf.Level != 0) {
						s.kind = flushStepKind
					}
				}
				// Add the current reference *Version to the step. This provides
				// access to, for example, the read-amplification of the
				// database at this point when the workload was collected. This
				// can be useful for pacing.
				if s.v, err = currentVersion(); err != nil {
					return err
				}
				// On the first time through, we set the previous version to the current
				// version otherwise we set it to the actual previous version.
				if previousVersion == nil {
					previousVersion = s.v
				}
				s.pv = previousVersion
				previousVersion = s.v

				// It's possible that the workload collector captured this
				// version edit, but wasn't able to collect all of the
				// corresponding sstables before being terminated.
				if s.kind == flushStepKind || s.kind == ingestStepKind {
					for _, fileNum := range newFiles {
						if _, ok := r.workload.sstables[fileNum]; !ok {
							// TODO(jackson,leon): This isn't exactly an error
							// condition. Give this more thought; do we want to
							// require graceful exiting of workload collection,
							// such that the last version edit must have had its
							// corresponding sstables collected?
							return errors.Newf("sstable %s not found", fileNum)
						}
					}
				}

				switch s.kind {
				case flushStepKind:
					// Load all of the flushed sstables' keys into a batch.
					s.flushBatch = r.d.NewBatch()
					if err := loadFlushedSSTableKeys(s.flushBatch, r.WorkloadFS, r.WorkloadPath, newFiles, r.readerOpts, &flushBufs); err != nil {
						return errors.Wrapf(err, "flush in %q at offset %d", manifestName, rr.Offset())
					}
				case ingestStepKind:
					// Copy the ingested sstables into a staging area within the
					// run dir. This is necessary for two reasons:
					//  a) Ingest will remove the source file, and we don't want
					//     to mutate the workload.
					//  b) If the workload stored on another volume, Ingest
					//     would need to fall back to copying the file since
					//     it's not possible to link across volumes. The true
					//     workload likely linked the file. Staging the file
					//     ahead of time ensures that we're able to Link the
					//     file like the original workload did.
					for _, fileNum := range newFiles {
						src := base.MakeFilepath(r.WorkloadFS, r.WorkloadPath, base.FileTypeTable, fileNum)
						dst := base.MakeFilepath(r.Opts.FS, r.stagingDir, base.FileTypeTable, fileNum)
						if err := vfs.CopyAcrossFS(r.WorkloadFS, src, r.Opts.FS, dst); err != nil {
							return errors.Wrapf(err, "ingest in %q at offset %d", manifestName, rr.Offset())
						}
						s.tablesToIngest = append(s.tablesToIngest, dst)
					}
				case compactionStepKind:
					// Nothing to do.
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case r.steps <- s:
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) compactionNotified(ctx context.Context) error {
	r.finishedCompactionNotifier.L.Lock()
	for {
		if r.compactionEndedCount < atomic.LoadInt64(&r.atomicCompactionStartCount) {
			r.finishedCompactionNotifier.Wait()
		}
		r.finishedCompactionNotifier.L.Unlock()
		select {
		case <-r.compactionsHaveQuiesced:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case r.compactionEnded <- struct{}{}:
		}
		r.finishedCompactionNotifier.L.Lock()
	}
}

// findWorkloadFiles finds all manifests and tables in the provided path on fs.
//
// TODO(sumeer): for now we can replay with DB state that has no blob files
// since we can simply configure the DB on which the replay is happening to
// write blob files.
func findWorkloadFiles(
	path string, fs vfs.FS,
) (manifests []string, sstables map[base.FileNum]struct{}, err error) {
	dirents, err := fs.List(path)
	if err != nil {
		return nil, nil, err
	}
	sstables = make(map[base.FileNum]struct{})
	for _, dirent := range dirents {
		typ, fileNum, ok := base.ParseFilename(fs, dirent)
		if !ok {
			continue
		}
		switch typ {
		case base.FileTypeManifest:
			manifests = append(manifests, dirent)
		case base.FileTypeTable:
			sstables[fileNum] = struct{}{}
		}
	}
	if len(manifests) == 0 {
		return nil, nil, errors.Newf("no manifests found")
	}
	sort.Strings(manifests)
	return manifests, sstables, err
}

// findManifestStart takes a database directory and FS containing the initial
// database state that a workload will be run against, and a list of a workloads
// manifests. It examines the database's current manifest to determine where
// workload replay should begin, so as to not duplicate already-applied version
// edits.
//
// It returns the index of the starting manifest, and the database's current
// offset within the manifest.
func findManifestStart(
	dbDir string, dbFS vfs.FS, manifests []string,
) (index int, offset int64, err error) {
	// Identify the database's current manifest.
	dbDesc, err := pebble.Peek(dbDir, dbFS)
	if err != nil {
		return 0, 0, err
	}
	dbManifest := dbFS.PathBase(dbDesc.ManifestFilename)
	// If there is no initial database state, begin workload replay from the
	// beginning of the first manifest.
	if !dbDesc.Exists {
		return 0, 0, nil
	}
	for index = 0; index < len(manifests); index++ {
		if manifests[index] == dbManifest {
			break
		}
	}
	if index == len(manifests) {
		// The initial database state has a manifest that does not appear within
		// the workload's set of manifests. This is possible if we began
		// recording the workload at the same time as a manifest rotation, but
		// more likely we're applying a workload to a different initial database
		// state than the one from which the workload was collected. Either way,
		// start from the beginning of the first manifest.
		return 0, 0, nil
	}
	// Find the initial database's offset within the manifest.
	info, err := dbFS.Stat(dbFS.PathJoin(dbDir, dbManifest))
	if err != nil {
		return 0, 0, err
	}
	return index, info.Size(), nil
}

// loadFlushedSSTableKeys copies keys from the sstables specified by `fileNums`
// in the directory specified by `path` into the provided the batch. Keys are
// applied to the batch in the order dictated by their sequence numbers within
// the sstables, ensuring the relative relationship between sequence numbers is
// maintained.
//
// Preserving the relative relationship between sequence numbers is not strictly
// necessary, but it ensures we accurately exercise some microoptimizations (eg,
// detecting user key changes by descending trailer). There may be additional
// dependencies on sequence numbers in the future.
func loadFlushedSSTableKeys(
	b *pebble.Batch,
	fs vfs.FS,
	path string,
	fileNums []base.FileNum,
	readOpts sstable.ReaderOptions,
	bufs *flushBuffers,
) error {
	// Load all the keys across all the sstables.
	for _, fileNum := range fileNums {
		if err := func() error {
			filePath := base.MakeFilepath(fs, path, base.FileTypeTable, fileNum)
			f, err := fs.Open(filePath)
			if err != nil {
				return err
			}
			r, err := sstable.NewReader(f, readOpts)
			if err != nil {
				return err
			}
			defer r.Close()

			// Load all the point keys.
			iter, err := r.NewIter(nil, nil)
			if err != nil {
				return err
			}
			defer iter.Close()
			for k, lv := iter.First(); k != nil; k, lv = iter.Next() {
				var key flushedKey
				key.Trailer = k.Trailer
				bufs.alloc, key.UserKey = bufs.alloc.Copy(k.UserKey)
				if v, callerOwned, err := lv.Value(nil); err != nil {
					return err
				} else if callerOwned {
					key.value = v
				} else {
					bufs.alloc, key.value = bufs.alloc.Copy(v)
				}
				bufs.keys = append(bufs.keys, key)
			}

			// Load all the range tombstones.
			if iter, err := r.NewRawRangeDelIter(); err != nil {
				return err
			} else if iter != nil {
				defer iter.Close()
				for s := iter.First(); s != nil; s = iter.Next() {
					if err := rangedel.Encode(s, func(k base.InternalKey, v []byte) error {
						var key flushedKey
						key.Trailer = k.Trailer
						bufs.alloc, key.UserKey = bufs.alloc.Copy(k.UserKey)
						bufs.alloc, key.value = bufs.alloc.Copy(v)
						bufs.keys = append(bufs.keys, key)
						return nil
					}); err != nil {
						return err
					}
				}
			}

			// Load all the range keys.
			if iter, err := r.NewRawRangeKeyIter(); err != nil {
				return err
			} else if iter != nil {
				defer iter.Close()
				for s := iter.First(); s != nil; s = iter.Next() {
					if err := rangekey.Encode(s, func(k base.InternalKey, v []byte) error {
						var key flushedKey
						key.Trailer = k.Trailer
						bufs.alloc, key.UserKey = bufs.alloc.Copy(k.UserKey)
						bufs.alloc, key.value = bufs.alloc.Copy(v)
						bufs.keys = append(bufs.keys, key)
						return nil
					}); err != nil {
						return err
					}
				}
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	// Sort the flushed keys by their sequence numbers so that we can apply them
	// to the batch in the same order, maintaining the relative relationship
	// between keys.
	// NB: We use a stable sort so that keys corresponding to span fragments
	// (eg, range tombstones and range keys) have a deterministic ordering for
	// testing.
	sort.Stable(bufs.keys)

	// Add the keys to the batch in the order they were committed when the
	// workload was captured.
	for i := 0; i < len(bufs.keys); i++ {
		var err error
		switch bufs.keys[i].Kind() {
		case base.InternalKeyKindDelete:
			err = b.Delete(bufs.keys[i].UserKey, nil)
		case base.InternalKeyKindSet, base.InternalKeyKindSetWithDelete:
			err = b.Set(bufs.keys[i].UserKey, bufs.keys[i].value, nil)
		case base.InternalKeyKindMerge:
			err = b.Merge(bufs.keys[i].UserKey, bufs.keys[i].value, nil)
		case base.InternalKeyKindSingleDelete:
			err = b.SingleDelete(bufs.keys[i].UserKey, nil)
		case base.InternalKeyKindRangeDelete:
			err = b.DeleteRange(bufs.keys[i].UserKey, bufs.keys[i].value, nil)
		case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
			s, err := rangekey.Decode(bufs.keys[i].InternalKey, bufs.keys[i].value, nil)
			if err != nil {
				return err
			}
			if len(s.Keys) != 1 {
				return errors.Newf("range key span unexpectedly contains %d keys", len(s.Keys))
			}
			switch bufs.keys[i].Kind() {
			case base.InternalKeyKindRangeKeySet:
				err = b.RangeKeySet(s.Start, s.End, s.Keys[0].Suffix, s.Keys[0].Value, nil)
			case base.InternalKeyKindRangeKeyUnset:
				err = b.RangeKeyUnset(s.Start, s.End, s.Keys[0].Suffix, nil)
			case base.InternalKeyKindRangeKeyDelete:
				err = b.RangeKeyDelete(s.Start, s.End, nil)
			default:
				err = errors.Newf("unexpected key kind %q", bufs.keys[i].Kind())
			}
			if err != nil {
				return err
			}
		default:
			err = errors.Newf("unexpected key kind %q", bufs.keys[i].Kind())
		}
		if err != nil {
			return err
		}
	}

	// Done with the flushBuffers. Reset.
	bufs.keys = bufs.keys[:0]
	return nil
}

type flushBuffers struct {
	keys  flushedKeysByTrailer
	alloc bytealloc.A
}

type flushedKeysByTrailer []flushedKey

func (s flushedKeysByTrailer) Len() int           { return len(s) }
func (s flushedKeysByTrailer) Less(i, j int) bool { return s[i].Trailer < s[j].Trailer }
func (s flushedKeysByTrailer) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type flushedKey struct {
	base.InternalKey
	value []byte
}

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
	"context"
	"io"
	"os"
	"sort"
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
	todo()
}

// Unpaced implements Pacer by applying each new write as soon as possible. It
// may be useful for examining performance under high read amplification.
type Unpaced struct{}

func (Unpaced) todo() {}

// TODO(jackson,leon): Add PaceByFixedReadAmp to pace by keeping
// read amplification below a fixed value.

// TODO(jackson,leon): Add PaceByReferenceReadAmp to pace by keeping read
// amplification at or below the read amplification of the database at the time
// the workload was collected. (pebble#2057)

// Runner runs a captured workload against a test database, collecting
// metrics on performance.
type Runner struct {
	RunDir       string
	WorkloadFS   vfs.FS
	WorkloadPath string
	Pacer        Pacer
	Opts         *pebble.Options

	// Internal state.
	cancel   func()
	d        *pebble.DB
	err      atomic.Value
	errgroup *errgroup.Group
	metrics  struct {
		writeStalls            uint64
		writeStallDurationNano uint64
	}
	readerOpts sstable.ReaderOptions
	stagingDir string
	steps      chan workloadStep
	workload   struct {
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

	// Extend the user-provided Options with extensions necessary for replay
	// mechanics.
	var l pebble.EventListener
	if r.Opts.EventListener != nil {
		l = pebble.TeeEventListener(*r.Opts.EventListener, r.eventListener())
	} else {
		l = r.eventListener()
	}
	r.Opts.EventListener = &l
	r.Opts.EnsureDefaults()
	r.readerOpts = r.Opts.MakeReaderOptions()
	r.d, err = pebble.Open(r.RunDir, r.Opts)
	if err != nil {
		return err
	}

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
	return nil
}

// Wait waits for the workload replay to complete.
func (r *Runner) Wait() error {
	err := r.errgroup.Wait()
	if storedErr := r.err.Load(); storedErr != nil {
		err = storedErr.(error)
	}
	return err
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
	// a Version describing the state of the LSM when the workload was
	// collected.
	v *manifest.Version
	// non-nil for flushStepKind
	flushBatch *pebble.Batch
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
			atomic.AddUint64(&r.metrics.writeStallDurationNano,
				uint64(time.Since(writeStallBegin).Nanoseconds()))
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
				return nil
			}
		}

		// TODO(jackson,leon): Apply pacing.

		switch step.kind {
		case flushStepKind:
			if err := step.flushBatch.Commit(nil); err != nil {
				return err
			}
		case ingestStepKind:
			// TODO(jackson,leon): Apply the step to the database.
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

	metas := make(map[base.FileNum]*manifest.FileMetadata)
	idx := r.workload.manifestIdx

	var flushBufs flushBuffers
	var v *manifest.Version
	var bve manifest.BulkVersionEdit
	applyVE := func(ve *manifest.VersionEdit) error {
		for _, nf := range ve.NewFiles {
			metas[nf.Meta.FileNum] = nf.Meta
		}
		return bve.Accumulate(ve)
	}
	currentVersion := func() (*manifest.Version, error) {
		var err error
		v, _, err = bve.Apply(v,
			r.Opts.Comparer.Compare,
			r.Opts.Comparer.FormatKey,
			r.Opts.FlushSplitBytes,
			r.Opts.Experimental.ReadCompactionRate)
		bve = manifest.BulkVersionEdit{}
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

// findWorkloadFiles finds all manfiests and tables in the provided path on fs.
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

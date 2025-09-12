// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"container/heap"
	"context"
	"fmt"
	"iter"
	"runtime/pprof"
	"slices"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/problemspans"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
)

// A pickedBlobFileCompaction is a blob file rewrite compaction that has been
// picked by the compaction picker.
type pickedBlobFileCompaction struct {
	// highPriority is set to true if the compaction was picked because the
	// ValueSeparationPolcy.GarbageThresholdHighPriority heuristic was
	// triggered. In this case, the resulting compaction will be permitted to
	// use a burst compaction concurrency slot to avoid starving default
	// compactions.
	highPriority      bool
	vers              *manifest.Version
	file              manifest.BlobFileMetadata
	referencingTables []*manifest.TableMetadata
}

// Assert that *pickedBlobFileCompaction implements the pickedCompaction
// interface.
var _ pickedCompaction = (*pickedBlobFileCompaction)(nil)

func (c *pickedBlobFileCompaction) ManualID() uint64 { return 0 }

func (c *pickedBlobFileCompaction) WaitingCompaction() WaitingCompaction {
	entry := scheduledCompactionMap[compactionKindBlobFileRewrite]
	return WaitingCompaction{
		Optional: entry.optional,
		Priority: entry.priority,
	}
}

func (c *pickedBlobFileCompaction) ConstructCompaction(
	d *DB, grantHandle CompactionGrantHandle,
) compaction {
	// Add a reference to the version. The compaction will release the reference
	// when it completes.
	c.vers.Ref()
	return &blobFileRewriteCompaction{
		beganAt:           d.timeNow(),
		grantHandle:       grantHandle,
		version:           c.vers,
		input:             c.file,
		referencingTables: c.referencingTables,
		objCreateOpts: objstorage.CreateOptions{
			// TODO(jackson): Enable shared storage for blob files.
			PreferSharedStorage: false,
			WriteCategory:       getDiskWriteCategoryForCompaction(d.opts, compactionKindBlobFileRewrite),
		},
		highPriority: c.highPriority,
	}
}

// A blobFileRewriteCompaction is a special variant of a compaction that
// rewrites a blob file without rewriting sstables. When the compaction
// completes, the Version's mapping of blob file ID to disk file number is
// updated to point to the new blob file. The blob file is rewritten without
// copying over values that are no longer referenced by any tables, reclaiming
// disk space.
type blobFileRewriteCompaction struct {
	// cancel is a bool that can be used by other goroutines to signal a compaction
	// to cancel, such as if a conflicting excise operation raced it to manifest
	// application. Only holders of the manifest lock will write to this atomic.
	cancel atomic.Bool
	// beganAt is the time when the compaction began.
	beganAt time.Time
	// grantHandle is a handle to the compaction that can be used to track
	// progress.
	grantHandle CompactionGrantHandle
	// version is a referenced version obtained when the compaction was picked.
	// This version must be unreferenced when the compaction is complete.
	version *manifest.Version
	// versionEditApplied is set to true when a compaction has completed and the
	// resulting version has been installed (if successful), but the compaction
	// goroutine is still cleaning up (eg, deleting obsolete files).
	versionEditApplied bool
	// input is the blob file that is being rewritten.
	input manifest.BlobFileMetadata
	// referencingTables is the set of sstables that reference the input blob
	// file in version.
	referencingTables     []*manifest.TableMetadata
	objCreateOpts         objstorage.CreateOptions
	internalIteratorStats base.InternalIteratorStats
	bytesWritten          atomic.Int64 // Total bytes written to the new blob file.
	// highPriority is set to true if the compaction was picked because the
	// ValueSeparationPolcy.GarbageThresholdHighPriority heuristic was
	// triggered. In this case, the resulting compaction will be permitted to
	// use a burst compaction concurrency slot to avoid starving default
	// compactions. This field is set when the compaction is created.
	highPriority bool
}

func (c *blobFileRewriteCompaction) String() string {
	s := fmt.Sprintf("blob file (ID: %s) %s (%s) being rewritten",
		c.input.FileID, c.input.Physical.FileNum, humanizeBytes(uint64(c.input.Physical.Size)))
	if c.highPriority {
		s += " (high priority)"
	}
	return s
}

// Assert that *blobFileRewriteCompaction implements the Compaction interface.
var _ compaction = (*blobFileRewriteCompaction)(nil)

func (c *blobFileRewriteCompaction) AddInProgressLocked(d *DB) {
	d.mu.compact.inProgress[c] = struct{}{}
	if c.highPriority {
		d.mu.compact.burstConcurrency.Add(1)
	}
	// TODO(jackson): Currently the compaction picker iterates through all
	// ongoing compactions in order to limit the number of concurrent blob
	// rewrite compactions to 1.
	//
	// Consider instead tracking which blob files are being rewritten, and we
	// can allow multiple concurrent blob rewrite compactions as long as they
	// compact different blob files.
}

func (c *blobFileRewriteCompaction) BeganAt() time.Time                 { return c.beganAt }
func (c *blobFileRewriteCompaction) Bounds() *base.UserKeyBounds        { return nil }
func (c *blobFileRewriteCompaction) Cancel()                            { c.cancel.Store(true) }
func (c *blobFileRewriteCompaction) IsDownload() bool                   { return false }
func (c *blobFileRewriteCompaction) IsFlush() bool                      { return false }
func (c *blobFileRewriteCompaction) GrantHandle() CompactionGrantHandle { return c.grantHandle }
func (c *blobFileRewriteCompaction) Tables() iter.Seq2[int, *manifest.TableMetadata] {
	// No tables; return an empty iterator.
	return func(yield func(int, *manifest.TableMetadata) bool) {}
}

func (c *blobFileRewriteCompaction) ObjioTracingContext(ctx context.Context) context.Context {
	if objiotracing.Enabled {
		ctx = objiotracing.WithReason(ctx, objiotracing.ForCompaction)
	}
	return ctx
}

func (c *blobFileRewriteCompaction) PprofLabels(UserKeyCategories) pprof.LabelSet {
	return pprof.Labels("pebble", "blob-rewrite")
}

func (c *blobFileRewriteCompaction) VersionEditApplied() bool {
	return c.versionEditApplied
}

func (c *blobFileRewriteCompaction) Execute(jobID JobID, d *DB) error {
	ctx := context.TODO()
	if objiotracing.Enabled {
		ctx = objiotracing.WithReason(ctx, objiotracing.ForCompaction)
	}
	c.grantHandle.Started()
	// The version stored in the compaction is ref'd when the compaction is
	// created. We're responsible for un-refing it when the compaction is
	// complete.
	defer c.version.UnrefLocked()

	// Notify the event listener that the compaction has begun.
	info := BlobFileRewriteInfo{
		JobID: int(jobID),
		Input: BlobFileInfo{
			BlobFileID:  c.input.FileID,
			DiskFileNum: c.input.Physical.FileNum,
			Size:        c.input.Physical.Size,
			ValueSize:   c.input.Physical.ValueSize,
		},
	}
	d.opts.EventListener.BlobFileRewriteBegin(info)
	startTime := d.timeNow()

	// Run the blob file rewrite.
	objMeta, ve, err := d.runBlobFileRewriteLocked(ctx, jobID, c)

	info.Duration = d.timeNow().Sub(startTime)

	// Ensure the rewrite did reduce the aggregate value size. If it didn't, we
	// should have never selected this blob file for rewrite and there must be a
	// bug in the statistics we maintain.
	if ve.NewBlobFiles[0].Physical.ValueSize >= c.input.Physical.ValueSize {
		return errors.AssertionFailedf("pebble: blob file %s rewrite did not reduce value size", c.input.FileID)
	}

	// Update the version with the remapped blob file.
	if err == nil {
		info.Output.BlobFileID = ve.NewBlobFiles[0].FileID
		info.Output.DiskFileNum = ve.NewBlobFiles[0].Physical.FileNum
		info.Output.Size = ve.NewBlobFiles[0].Physical.Size
		info.Output.ValueSize = ve.NewBlobFiles[0].Physical.ValueSize
		_, err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
			// It's possible that concurrent compactions removed references to
			// the blob file while the blob file rewrite compaction was running.
			// Now that we have the manifest lock, check if the blob file is
			// still current. If not, we bubble up ErrCancelledCompaction.
			v := d.mu.versions.currentVersion()
			currentDiskFile, ok := v.BlobFiles.LookupPhysical(c.input.FileID)
			if !ok {
				return versionUpdate{}, errors.Wrapf(ErrCancelledCompaction,
					"blob file %s became unreferenced", c.input.FileID)
			}
			currentDiskFileNum := currentDiskFile.FileNum
			// Assert that the current version's disk file number for the blob
			// matches the one we rewrote. This compaction should be the only
			// rewrite compaction running for this blob file.
			if currentDiskFileNum != c.input.Physical.FileNum {
				return versionUpdate{}, base.AssertionFailedf(
					"blob file %s was rewritten to %s during rewrite compaction of %s",
					c.input.FileID, currentDiskFileNum, c.input.Physical.FileNum)
			}
			return versionUpdate{
				VE:    ve,
				JobID: jobID,
				InProgressCompactionsFn: func() []compactionInfo {
					return d.getInProgressCompactionInfoLocked(c)
				},
			}, nil
		})
	}

	d.mu.versions.incrementCompactions(compactionKindBlobFileRewrite, nil, c.bytesWritten.Load(), err)
	d.mu.versions.incrementCompactionBytes(-c.bytesWritten.Load())

	// Update the read state to publish the new version.
	if err == nil {
		d.updateReadStateLocked(d.opts.DebugCheck)
	}

	// Ensure we clean up the blob file we created on failure.
	if err != nil {
		if objMeta.DiskFileNum != 0 {
			d.mu.versions.obsoleteBlobs = mergeObsoleteFiles(d.mu.versions.obsoleteBlobs, []obsoleteFile{
				{
					fileType: base.FileTypeBlob,
					fs:       d.opts.FS,
					path:     d.objProvider.Path(objMeta),
					fileNum:  objMeta.DiskFileNum,
					// We don't know the size of the output blob file--it may have
					// been half-written. We use the input blob file size as an
					// approximation for deletion pacing.
					fileSize: c.input.Physical.Size,
					isLocal:  true,
				},
			})
		}
	}

	// Notify the event listener that the compaction has ended.
	now := d.timeNow()
	info.TotalDuration = now.Sub(c.beganAt)
	info.Done = true
	info.Err = err
	d.opts.EventListener.BlobFileRewriteEnd(info)
	return nil
}

func (c *blobFileRewriteCompaction) Info() compactionInfo {
	return compactionInfo{
		kind:               compactionKindBlobFileRewrite,
		versionEditApplied: c.versionEditApplied,
		outputLevel:        -1,
	}
}

func (c *blobFileRewriteCompaction) UsesBurstConcurrency() bool {
	return c.highPriority
}

func (c *blobFileRewriteCompaction) RecordError(*problemspans.ByLevel, error) {
	// TODO(jackson): Track problematic blob files and avoid re-picking the same
	// blob file compaction.
}

// runBlobFileRewriteLocked runs a blob file rewrite. d.mu must be held when
// calling this, although it may be dropped and re-acquired during the course of
// the method.
func (d *DB) runBlobFileRewriteLocked(
	ctx context.Context, jobID JobID, c *blobFileRewriteCompaction,
) (objstorage.ObjectMetadata, *manifest.VersionEdit, error) {
	// Drop the database mutex while we perform the rewrite, and re-acquire it
	// before returning.
	d.mu.Unlock()
	defer d.mu.Lock()

	// Construct the block.ReadEnv configured with a buffer pool. Setting the
	// buffer pool ensures we won't cache blocks in the block cache. As soon as
	// the compaction finishes new iterators will read the new blob file, so it
	// would be unlikely the cached blocks would be reused.
	var bufferPool block.BufferPool
	bufferPool.Init(4)
	defer bufferPool.Release()
	env := block.ReadEnv{
		Stats:              &c.internalIteratorStats,
		BufferPool:         &bufferPool,
		ReportCorruptionFn: d.reportCorruption,
	}

	// Create a new file for the rewritten blob file.
	writable, objMeta, err := d.newCompactionOutputBlob(jobID, compactionKindBlobFileRewrite, -1, &c.bytesWritten, c.objCreateOpts)
	if err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}
	// Initialize a blob file rewriter. We pass L6 to MakeBlobWriterOptions.
	// There's no single associated level with a blob file. A long-lived blob
	// file that gets rewritten is likely to mostly be referenced from L6.
	// TODO(jackson): Consider refactoring to remove the level association.
	rewriter := newBlobFileRewriter(
		d.fileCache,
		env,
		objMeta.DiskFileNum,
		writable,
		d.opts.MakeBlobWriterOptions(6, d.BlobFileFormat()),
		c.referencingTables,
		c.input,
	)
	// Perform the rewrite.
	stats, err := rewriter.Rewrite(ctx)
	if err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}

	// Sync the object provider to ensure the metadata for the blob file is
	// persisted.
	if err := d.objProvider.Sync(); err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}

	physical := &manifest.PhysicalBlobFile{
		FileNum:      objMeta.DiskFileNum,
		Size:         stats.FileLen,
		ValueSize:    stats.UncompressedValueBytes,
		CreationTime: uint64(d.timeNow().Unix()),
	}
	physical.PopulateProperties(&stats.Properties)

	ve := &manifest.VersionEdit{
		DeletedBlobFiles: map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile{
			{
				FileID:  c.input.FileID,
				FileNum: c.input.Physical.FileNum,
			}: c.input.Physical,
		},
		NewBlobFiles: []manifest.BlobFileMetadata{
			{
				FileID:   c.input.FileID,
				Physical: physical,
			},
		},
	}
	return objMeta, ve, nil
}

// blockHeap is a min-heap of blob reference liveness encodings, ordered by
// blockID. We use this to help us determine the overall liveness of values in
// each blob block by combining the blob reference liveness encodings of all
// referencing sstables for a particular blockID.
type blockHeap []*sstable.BlobRefLivenessEncoding

// Len implements sort.Interface.
func (h blockHeap) Len() int { return len(h) }

// Less implements sort.Interface.
func (h blockHeap) Less(i, j int) bool { return h[i].BlockID < h[j].BlockID }

// Swap implements sort.Interface.
func (h blockHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Push implements heap.Interface.
func (h *blockHeap) Push(x any) {
	blobEnc := x.(*sstable.BlobRefLivenessEncoding)
	*h = append(*h, blobEnc)
}

// Pop implements heap.Interface.
func (h *blockHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

// blockValues holds the accumulated liveness data for blockID.
type blockValues struct {
	blockID      blob.BlockID
	valuesSize   int
	liveValueIDs []int
}

// blobFileRewriter is responsible for rewriting blob files by combining and
// processing blob reference liveness encodings from multiple SSTables. It
// maintains state for writing to an output blob file.
type blobFileRewriter struct {
	fc        *fileCacheHandle
	readEnv   block.ReadEnv
	sstables  []*manifest.TableMetadata
	inputBlob manifest.BlobFileMetadata
	rw        *blob.FileRewriter
	blkHeap   blockHeap
}

func newBlobFileRewriter(
	fc *fileCacheHandle,
	readEnv block.ReadEnv,
	outputFileNum base.DiskFileNum,
	w objstorage.Writable,
	opts blob.FileWriterOptions,
	sstables []*manifest.TableMetadata,
	inputBlob manifest.BlobFileMetadata,
) *blobFileRewriter {
	rw := blob.NewFileRewriter(inputBlob.FileID, inputBlob.Physical, fc, readEnv, outputFileNum, w, opts)
	return &blobFileRewriter{
		fc:        fc,
		readEnv:   readEnv,
		rw:        rw,
		sstables:  sstables,
		inputBlob: inputBlob,
		blkHeap:   blockHeap{},
	}
}

// generateHeap populates rw.blkHeap with the blob reference liveness encodings
// for each referencing sstable, rw.sstables.
func (rw *blobFileRewriter) generateHeap(ctx context.Context) error {
	heap.Init(&rw.blkHeap)

	var decoder colblk.ReferenceLivenessBlockDecoder
	// For each sstable that references the input blob file, push its
	// sstable.BlobLivenessEncoding on to the heap.
	for _, sst := range rw.sstables {
		// Validate that the sstable contains a reference to the input blob
		// file.
		refID, ok := sst.BlobReferences.IDByBlobFileID(rw.inputBlob.FileID)
		if !ok {
			return errors.AssertionFailedf("table %s doesn't contain a reference to blob file %s",
				sst.TableNum, rw.inputBlob.FileID)
		}
		err := rw.fc.withReader(ctx, rw.readEnv, sst, func(r *sstable.Reader, readEnv sstable.ReadEnv) error {
			h, err := r.ReadBlobRefIndexBlock(ctx, readEnv.Block)
			if err != nil {
				return err
			}
			defer h.Release()
			decoder.Init(h.BlockData())
			bitmapEncodings := slices.Clone(decoder.LivenessAtReference(int(refID)))
			// TODO(annie): We should instead maintain 1 heap item per sstable
			// instead of 1 heap item per sstable block ref to reduce the heap
			// comparisons to O(sstables).
			for _, enc := range sstable.DecodeBlobRefLivenessEncoding(bitmapEncodings) {
				heap.Push(&rw.blkHeap, &enc)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (rw *blobFileRewriter) Rewrite(ctx context.Context) (blob.FileWriterStats, error) {
	if err := rw.generateHeap(ctx); err != nil {
		return blob.FileWriterStats{}, err
	}
	if rw.blkHeap.Len() == 0 {
		return blob.FileWriterStats{}, errors.AssertionFailedf("heap empty")
	}

	// Begin constructing our output blob file. We maintain a map of blockID
	// to accumulated liveness data across all referencing sstables.
	firstBlock := heap.Pop(&rw.blkHeap).(*sstable.BlobRefLivenessEncoding)
	pending := blockValues{
		blockID:      firstBlock.BlockID,
		valuesSize:   firstBlock.ValuesSize,
		liveValueIDs: slices.Collect(sstable.IterSetBitsInRunLengthBitmap(firstBlock.Bitmap)),
	}
	for rw.blkHeap.Len() > 0 {
		nextBlock := heap.Pop(&rw.blkHeap).(*sstable.BlobRefLivenessEncoding)

		// If we are encountering a new block, write the last accumulated block
		// to the blob file.
		if pending.blockID != nextBlock.BlockID {
			// Write the last accumulated block's values to the blob file.
			err := rw.rw.CopyBlock(ctx, pending.blockID, pending.valuesSize, pending.liveValueIDs)
			if err != nil {
				return blob.FileWriterStats{}, err
			}
			pending = blockValues{blockID: nextBlock.BlockID, liveValueIDs: pending.liveValueIDs[:0]}
		}
		// Update the accumulated encoding for this block.
		pending.valuesSize += nextBlock.ValuesSize
		pending.liveValueIDs = slices.AppendSeq(pending.liveValueIDs,
			sstable.IterSetBitsInRunLengthBitmap(nextBlock.Bitmap))
	}

	// Copy the last accumulated block.
	err := rw.rw.CopyBlock(ctx, pending.blockID, pending.valuesSize, pending.liveValueIDs)
	if err != nil {
		return blob.FileWriterStats{}, err
	}
	return rw.rw.Close()
}

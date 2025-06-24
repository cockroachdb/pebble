// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"iter"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/problemspans"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider/objiotracing"
)

// A pickedBlobFileCompaction is a blob file rewrite compaction that has been
// picked by the compaction picker.
type pickedBlobFileCompaction struct {
	vers *manifest.Version
	file manifest.BlobFileMetadata
}

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
	return &blobFileRewriteCompaction{
		beganAt:     d.timeNow(),
		grantHandle: grantHandle,
		version:     c.vers,
		input:       c.file,
		objCreateOpts: objstorage.CreateOptions{
			PreferSharedStorage: false, // TODO
			WriteCategory:       getDiskWriteCategoryForCompaction(d.opts, compactionKindBlobFileRewrite),
		},
	}
}

// A blobFileRewriteCompaction is a special variant of a compaction that
// rewrites a blob file without rewriting sstables.
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
	input         manifest.BlobFileMetadata
	objCreateOpts objstorage.CreateOptions
}

// Assert that *blobFileRewriteCompaction implements the Compaction interface.
var _ compaction = (*blobFileRewriteCompaction)(nil)

func (c *blobFileRewriteCompaction) AddInProgressLocked(d *DB) {
	d.mu.compact.inProgress[c] = struct{}{}
	// TODO(jackson): Record the blob file as being compacted.
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
	_, ve, err := d.runBlobFileRewriteLocked(ctx, c)

	info.Duration = d.timeNow().Sub(startTime)

	// Update the version with the remapped blob file.
	if err == nil {
		info.Output.BlobFileID = ve.NewBlobFiles[0].FileID
		info.Output.DiskFileNum = ve.NewBlobFiles[0].Physical.FileNum
		info.Output.Size = ve.NewBlobFiles[0].Physical.Size
		info.Output.ValueSize = ve.NewBlobFiles[0].Physical.ValueSize
		err = d.mu.versions.UpdateVersionLocked(func() (versionUpdate, error) {
			// It's possible that concurrent compactions removed references to
			// the blob file while the blob file rewrite compaction was running.
			// Now that we have the manifest lock, check if the blob file is
			// still current. If not, we bubble up ErrCancelledCompaction.
			v := d.mu.versions.currentVersion()
			currentDiskFileNum, ok := v.BlobFiles.Lookup(c.input.FileID)
			if !ok {
				return versionUpdate{}, errors.Wrapf(ErrCancelledCompaction,
					"blob file %s became unreferenced", c.input.FileID)
			}
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

	// Update the read state to publish the new version.
	if err == nil {
		d.updateReadStateLocked(d.opts.DebugCheck)
	}

	// TODO(jackson): Ensure we clean up the blob file we created on failure.

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

func (c *blobFileRewriteCompaction) RecordError(*problemspans.ByLevel, error) {
	// TODO(jackson): Track problematic blob files and avoid re-picking the same
	// blob file compaction.
}

// runBlobFileRewriteLocked runs a blob file rewrite. d.mu must be held when
// calling this, although it may be dropped and re-acquired during the course of
// the method.
func (d *DB) runBlobFileRewriteLocked(
	ctx context.Context, c *blobFileRewriteCompaction,
) (objstorage.ObjectMetadata, *manifest.VersionEdit, error) {
	d.mu.Unlock()
	defer d.mu.Lock()

	// Grab the existing blob file.
	valueRef, err := d.fileCache.findOrCreateBlob(ctx, c.input.Physical.FileNum)
	if err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}
	defer valueRef.Unref()
	r := valueRef.Value().mustBlob()

	var preallocRH objstorageprovider.PreallocatedReadHandle
	rh := r.InitReadHandle(&preallocRH)

	// Copy the existing blob file to a new disk file number, simulating a
	// rewrite.
	//
	// TODO(jackson): Don't just copy, rewrite the blob file.
	newDiskFileNum := d.mu.versions.getNextDiskFileNum()
	writable, objMeta, err := d.objProvider.Create(ctx, base.FileTypeBlob, newDiskFileNum, c.objCreateOpts)
	if err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}
	if err := objstorage.Copy(ctx, rh, writable, 0, c.input.Physical.Size); err != nil {
		writable.Abort()
		return objstorage.ObjectMetadata{}, nil, err
	}
	if err := writable.Finish(); err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}
	if err := d.objProvider.Sync(); err != nil {
		return objstorage.ObjectMetadata{}, nil, err
	}

	ve := &manifest.VersionEdit{
		DeletedBlobFiles: map[manifest.DeletedBlobFileEntry]*manifest.PhysicalBlobFile{
			{
				FileID:  c.input.FileID,
				FileNum: c.input.Physical.FileNum,
			}: c.input.Physical,
		},
		NewBlobFiles: []manifest.BlobFileMetadata{
			{
				FileID: c.input.FileID,
				Physical: &manifest.PhysicalBlobFile{
					FileNum:      newDiskFileNum,
					Size:         c.input.Physical.Size,
					ValueSize:    c.input.Physical.ValueSize,
					CreationTime: uint64(d.timeNow().Unix()),
				},
			},
		},
	}
	return objMeta, ve, nil
}

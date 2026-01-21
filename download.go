// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
)

// DownloadSpan is a key range passed to the Download method.
type DownloadSpan struct {
	StartKey []byte
	// EndKey is exclusive.
	EndKey []byte
	// ViaBackingFileDownload, if true, indicates the span should be downloaded by
	// downloading any remote backing files byte-for-byte and replacing them with
	// the downloaded local files, while otherwise leaving the virtual SSTables
	// as-is. If false, a "normal" rewriting compaction of the span, that iterates
	// the keys and produces a new SSTable, is used instead. Downloading raw files
	// can be faster when the whole file is being downloaded, as it avoids some
	// cpu-intensive steps involved in iteration and new file construction such as
	// compression, however it can also be wasteful when only a small portion of a
	// larger backing file is being used by a virtual file. Additionally, if the
	// virtual file has expensive read-time transformations, such as prefix
	// replacement, rewriting once can persist the result of these for future use
	// while copying only the backing file will obligate future reads to continue
	// to compute such transforms.
	ViaBackingFileDownload bool
}

// Download ensures that the LSM does not use any external sstables for the
// given key ranges. It does so by performing appropriate compactions so that
// all external data becomes available locally.
//
// Note that calling this method does not imply that all other compactions stop;
// it simply informs Pebble of a list of spans for which external data should be
// downloaded with high priority.
//
// The method returns once no external sstables overlap the given spans, the
// context is canceled, the db is closed, or an error is hit.
//
// Note that despite the best effort of this method, if external ingestions
// happen in parallel, a new external file can always appear right as we're
// returning.
//
// TODO(radu): consider passing a priority/impact knob to express how important
// the download is (versus live traffic performance, LSM health).
func (d *DB) Download(ctx context.Context, spans []DownloadSpan) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	info := DownloadInfo{
		JobID: int(d.newJobID()),
		Spans: spans,
	}
	startTime := d.opts.private.timeNow()
	d.opts.EventListener.DownloadBegin(info)

	for info.RestartCount = 0; ; info.RestartCount++ {
		tasks := d.createDownloadTasks(spans)
		info.Duration = d.opts.private.timeNow().Sub(startTime)
		if len(tasks) == 0 {
			// We are done.
			info.Done = true
			d.opts.EventListener.DownloadEnd(info)
			return nil
		}
		if info.RestartCount > 0 {
			d.opts.EventListener.DownloadBegin(info)
		}

		// Install the tasks.
		d.mu.Lock()
		d.mu.compact.downloads = append(d.mu.compact.downloads, tasks...)
		d.maybeScheduleCompaction()
		d.mu.Unlock()

		err := d.waitForDownloadTasks(ctx, tasks)
		for _, t := range tasks {
			info.DownloadCompactionsLaunched += t.numLaunchedDownloads
		}

		if err != nil {
			info.Err = err
			info.Duration = d.opts.private.timeNow().Sub(startTime)
			d.opts.EventListener.DownloadEnd(info)
			return err
		}
	}
}

// createDownloadTasks creates downloadSpanTasks for the download spans that
// overlap external files in the given version.
func (d *DB) createDownloadTasks(spans []DownloadSpan) []*downloadSpanTask {
	d.mu.Lock()
	vers := d.mu.versions.currentVersion()
	d.mu.Unlock()

	tasks := make([]*downloadSpanTask, 0, len(spans))
	for i := range spans {
		task, ok := d.newDownloadSpanTask(vers, spans[i])
		// If !ok, there are no external files in this span.
		if ok {
			tasks = append(tasks, task)
		}
	}
	return tasks
}

// waitForDownloadTasks waits until all download tasks complete.
func (d *DB) waitForDownloadTasks(ctx context.Context, tasks []*downloadSpanTask) error {
	for i := range tasks {
		select {
		case <-ctx.Done():
			d.removeDownloadTasks(tasks)
			return ctx.Err()

		case err := <-tasks[i].taskCompletedChan:
			if err != nil {
				d.removeDownloadTasks(tasks)
				return err
			}
		}
	}
	return nil
}

// removeDownloadTasks removes all tasks in the given slice from
// d.mu.compact.downloads.
func (d *DB) removeDownloadTasks(tasks []*downloadSpanTask) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.mu.compact.downloads = slices.DeleteFunc(d.mu.compact.downloads, func(t *downloadSpanTask) bool {
		return slices.Contains(tasks, t)
	})
}

// downloadSpanTask tracks the task of downloading external files that overlap
// with a DownloadSpan.
//
// A downloadSpanTask is spawned only if at least one overlapping external file
// is found in the current version.
//
// When a downloadSpanTask completes (i.e. taskCompletedChan is signaled)
// without an error, it is guaranteed that all external files that were
// overlapping the download span at the beginning of the task are downloaded.
//
// == Implementation ==
//
// A download span task moves through the LSM within the given bounds in
// top-down level order (L0, L1, etc.), and in Smallest.UserKey order within
// each level (and breaking ties in L0 according to LargestSeqNum). We introduce
// the concept of a "download cursor" to keep track of where we are in this
// process, in a way that is independent of any one version. A cursor stores the
// level, a user key which is a lower bound for Smallest.UserKey within that
// level, and a sequence number which is a lower bound for the LargestSeqNum for
// files on that level starting at exactly that key.
//
// While a download task is running, tables with external backings can disappear
// due to excises or compactions; tables can move *down* (to a lower LSM level);
// or tables can have their bounds shrink due to excises (and these will appear
// as new tables, even though they have the same backing). The top-down,
// left-to-right-start-key ordering ensures that we don't miss any table
// (instead, we may examine it multiple times).
//
// We use a cursor that advances as our download task makes progress. Each time
// we encounter a file that needs downloading, we create a "bookmark". A
// bookmark conceptually represents a key range within a level and it
// corresponds to the bounds of the file that we discovered. It is represented
// as a cursor position (corresponding to the start) and an end boundary key. We
// need to remember the bookmark because the download compaction can fail (e.g.
// it can get canceled by an excise) and the file might get excised so we need
// to look again at all files within the original key range.
//
// It is also possible that we encounter files that are already part of a
// compaction. These can be move compaction, or can get canceled, so we can't
// just ignore these files; we create bookmarks for such files as well.
//
// We maintain no more than maxConcurrentDownloads bookmarks - the idea being
// that files that are part of compactions are getting downloaded anyway and we
// can effectively count them toward the limit. When we cannot create any more
// bookmarks, we stop advancing the task cursor. Note that it is not this code's
// job to enforce the maximum concurrency, this is simply a reasonable limit - we
// don't want to accumulate arbitrarily many bookmarks, since we check each one
// whenever tryLaunchDownloadCompaction is called (after every compaction
// completing).
//
// This implementation achieves O(maxConcurrentDownloads * N) level iterator
// operations across the entire task, where N is the (average) number of files
// within the bounds.
type downloadSpanTask struct {
	downloadSpan DownloadSpan

	// The download task pertains to sstables which *start* (as per
	// Smallest.UserKey) within these bounds.
	bounds base.UserKeyBounds

	// taskCompletedChan is signaled when we have finished download compactions
	// for all external files encountered within the bounds, or when one of these
	// compactions reports an error (other than ErrCancelledCompaction).
	taskCompletedChan chan error

	numLaunchedDownloads int

	// Keeps track of the current position; all files up to these position were
	// examined and were either downloaded or we have bookmarks for them.
	cursor manifest.ScanCursor

	// Bookmarks remember areas which correspond to downloads that we started or
	// files that were undergoing other compactions and which we need to check
	// again before completing the task.
	bookmarks []downloadBookmark

	// Testing hooks.
	testing struct {
		launchDownloadCompaction func(f *manifest.TableMetadata) (chan error, bool)
	}
}

// downloadBookmark represents an area that was swept by the task cursor which
// corresponds to a file that was part of a running compaction or download.
type downloadBookmark struct {
	start    manifest.ScanCursor
	endBound base.UserKeyBoundary
	// downloadDoneCh is set if this bookmark corresponds to a download we
	// started; in this case the channel will report the status of that
	// compaction.
	downloadDoneCh chan error
}

func (d *DB) newDownloadSpanTask(
	vers *manifest.Version, sp DownloadSpan,
) (_ *downloadSpanTask, ok bool) {
	bounds := base.UserKeyBoundsEndExclusive(sp.StartKey, sp.EndKey)
	// We are interested in all external sstables that *overlap* with
	// [sp.StartKey, sp.EndKey). Expand the bounds to the left so that we
	// include the start keys of any external sstables that overlap with
	// sp.StartKey.
	for _, ls := range vers.AllLevelsAndSublevels() {
		iter := ls.Iter()
		if f := iter.SeekGE(d.cmp, sp.StartKey); f != nil &&
			objstorage.IsExternalTable(d.objProvider, f.TableBacking.DiskFileNum) &&
			d.cmp(f.Smallest().UserKey, bounds.Start) < 0 {
			bounds.Start = f.Smallest().UserKey
		}
	}
	startCursor := manifest.ScanCursor{
		Level:  0,
		Key:    bounds.Start,
		SeqNum: 0,
	}
	f, level := startCursor.NextExternalFile(d.cmp, d.objProvider, bounds, vers)
	if f == nil {
		// No external files in the given span.
		return nil, false
	}

	return &downloadSpanTask{
		downloadSpan:      sp,
		bounds:            bounds,
		taskCompletedChan: make(chan error, 1),
		cursor:            manifest.MakeScanCursor(f, level),
	}, true
}

// tryLaunchDownloadForFile attempt to launch a download compaction for the
// given file. Returns true on success, or false if the file is already
// involved in a compaction.
func (d *DB) tryLaunchDownloadForFile(
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	env compactionEnv,
	download *downloadSpanTask,
	level int,
	f *manifest.TableMetadata,
) (doneCh chan error, ok bool) {
	if f.IsCompacting() {
		return nil, false
	}
	if download.testing.launchDownloadCompaction != nil {
		return download.testing.launchDownloadCompaction(f)
	}
	kind := compactionKindRewrite
	if download.downloadSpan.ViaBackingFileDownload {
		kind = compactionKindCopy
	}
	pc := pickDownloadCompaction(vers, l0Organizer, d.opts, env, d.mu.versions.picker.getBaseLevel(), kind, level, f)
	if pc == nil {
		// We are not able to run this download compaction at this time.
		return nil, false
	}

	download.numLaunchedDownloads++
	doneCh = make(chan error, 1)
	c := newCompaction(pc, d.opts, d.opts.private.timeNow(), d.objProvider, noopGrantHandle{},
		d.shouldCreateShared(pc.outputLevel.level), d.determineCompactionValueSeparation)
	c.isDownload = true
	d.mu.compact.downloadingCount++
	c.AddInProgressLocked(d)
	go d.compact(c, doneCh)
	return doneCh, true
}

type launchDownloadResult int8

const (
	launchedCompaction launchDownloadResult = iota
	didNotLaunchCompaction
	downloadTaskCompleted
)

func (d *DB) tryLaunchDownloadCompaction(
	download *downloadSpanTask,
	vers *manifest.Version,
	l0Organizer *manifest.L0Organizer,
	env compactionEnv,
	maxConcurrentDownloads int,
) launchDownloadResult {
	// First, check the bookmarks.
	for i := 0; i < len(download.bookmarks); i++ {
		b := &download.bookmarks[i]
		if b.downloadDoneCh != nil {
			// First check if the compaction we launched completed.
			select {
			case compactionErr := <-b.downloadDoneCh:
				if compactionErr != nil && !errors.Is(compactionErr, ErrCancelledCompaction) {
					download.taskCompletedChan <- compactionErr
					return downloadTaskCompleted
				}
				b.downloadDoneCh = nil

				// Even if the compaction finished without an error, we still want to
				// check the rest of the bookmark range for external files.
				//
				// For example, say that we encounter a file ["a", "f"] and start a
				// download (creating a bookmark). Then that file gets excised into new
				// files ["a", "b"] and ["e", "f"] and the excise causes the download
				// compaction to be cancelled. We will start another download compaction
				// for ["a", "c"]; once that is complete, we still need to look at the
				// rest of the bookmark range (i.e. up to "f") to discover the
				// ["e", "f"] file.

			default:
				// The compaction is still running, go to the next bookmark.
				continue
			}
		}

		// If downloadDoneCh was nil, we are waiting on a compaction that we did not
		// start. We are effectively polling the status by checking the external
		// files within the bookmark. This is ok because this method is called (for
		// this download task) at most once every time a compaction completes.

		f := b.start.NextExternalFileOnLevel(d.cmp, d.objProvider, b.endBound, vers)
		if f == nil {
			// No more external files for this bookmark, remove it.
			download.bookmarks = slices.Delete(download.bookmarks, i, i+1)
			i--
			continue
		}

		// Move up the bookmark position to point at this file.
		b.start = manifest.MakeScanCursor(f, b.start.Level)
		doneCh, ok := d.tryLaunchDownloadForFile(vers, l0Organizer, env, download, b.start.Level, f)
		if ok {
			b.downloadDoneCh = doneCh
			return launchedCompaction
		}
		// We could not launch a download, which means the file is part of another
		// compaction. We leave the bookmark in place and will poll the status in
		// the code above.
	}

	// Try to advance the cursor and launch more downloads.
	for len(download.bookmarks) < maxConcurrentDownloads {
		f, level := download.cursor.NextExternalFile(d.cmp, d.objProvider, download.bounds, vers)
		if f == nil {
			download.cursor = manifest.EndScanCursor
			if len(download.bookmarks) == 0 {
				download.taskCompletedChan <- nil
				return downloadTaskCompleted
			}
			return didNotLaunchCompaction
		}
		download.cursor = manifest.MakeScanCursorAfterFile(f, level)

		download.bookmarks = append(download.bookmarks, downloadBookmark{
			start:    manifest.MakeScanCursor(f, level),
			endBound: base.UserKeyInclusive(f.Largest().UserKey),
		})
		doneCh, ok := d.tryLaunchDownloadForFile(vers, l0Organizer, env, download, level, f)
		if ok {
			// We launched a download for this file.
			download.bookmarks[len(download.bookmarks)-1].downloadDoneCh = doneCh
			return launchedCompaction
		}
	}

	return didNotLaunchCompaction
}

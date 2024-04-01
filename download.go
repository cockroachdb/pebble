// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"cmp"
	"context"
	"fmt"
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
	startTime := d.timeNow()
	d.opts.EventListener.DownloadBegin(info)

	for info.RestartCount = 0; ; info.RestartCount++ {
		tasks := d.createDownloadTasks(spans)
		info.Duration = d.timeNow().Sub(startTime)
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
			info.DownloadCompactionsLaunched += len(t.compactionDoneChans)
		}

		if err != nil {
			info.Err = err
			info.Duration = d.timeNow().Sub(startTime)
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

// waitForDownloadTasks waits until all tasks and the download compactions they launched complete.
func (d *DB) waitForDownloadTasks(ctx context.Context, tasks []*downloadSpanTask) error {
	for len(tasks) > 0 {
		task := tasks[0]
		// First wait on the taskDoneChan.
		select {
		case <-ctx.Done():
			d.removeDownloadTasks(tasks)
			return ctx.Err()

		case <-task.taskCompletedChan:
		}
		tasks = tasks[1:]

		// Now wait for all download compactions that were started.
		var err error
		for _, ch := range task.compactionDoneChans {
			select {
			case <-ctx.Done():
				d.removeDownloadTasks(tasks)
				return ctx.Err()

			case compactionErr := <-ch:
				// We ignore canceled compaction errors; the higher level code will
				// retry the task.
				if compactionErr != nil && !errors.Is(compactionErr, ErrCancelledCompaction) {
					err = firstError(err, compactionErr)
				}
			}
		}
		if err != nil {
			d.removeDownloadTasks(tasks)
			return err
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
// When a downloadSpanTask completes (i.e. taskCompletedChan is signaled), it is
// guaranteed that all external files that were overlapping the download span at
// the beginning of the task are either downloaded or have download compactions
// running (which will report their completion status via compactionDoneChans).
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
// In the normal case, we use a single cursor and launch download compactions on
// the way. However, it is possible that we encounter files that are already
// part of a compaction. The compaction might be a move compaction or it might
// fail, so we can't just ignore these files. Thus every time we encounter one
// of them, we place an extra cursor at that position (like a bookmark). Every
// time we are ready to launch a new compaction, we check and advance all
// previous cursors.
//
// We maintain no more than maxConcurrentDownloads+1 cursors - the idea being
// that files that are part of long-running compactions are getting downloaded
// anyway and we can effectively count them toward the limit.
//
// This implementation achieves O(maxConcurrentDownloads * N) level iterator
// operations across the entire task, where N is the (average) number of files
// within the bounds.
type downloadSpanTask struct {
	downloadSpan DownloadSpan

	// The downlaod task pertains to sstables which *start* (as per
	// Smallest.UserKey) within these bounds.
	bounds base.UserKeyBounds

	// taskCompletedChan is signaled when we have launched download compactions
	// for all external files encountered within the bounds. Each of these
	// compactions will report its own completion via a channel in
	// compactionDoneChans.
	taskCompletedChan chan struct{}

	// compactionDoneChans contains a list of channels passed into all download
	// compactions we launched so far. Each channel has a buffer size of 1 and is
	// only passed into one compaction. This slice can grow over the lifetime of a
	// downloadSpanTask (until taskCompletedChan is signaled).
	compactionDoneChans []chan error

	// skippedPositions keeps track of positions where we found an external file
	// but where we couldn't launch a download (e.g. the file was already
	// compacting). We only maintain up to maxConcurrentDownload positions.
	// The skipped positions are ordered.
	cursors []downloadCursor

	// Testing hooks.
	testing struct {
		launchDownloadCompaction func(f *fileMetadata) bool
	}
}

func (d *DB) newDownloadSpanTask(vers *version, sp DownloadSpan) (_ *downloadSpanTask, ok bool) {
	bounds := base.UserKeyBoundsEndExclusive(sp.StartKey, sp.EndKey)
	// We are interested in all external sstables that *overlap* with
	// [sp.StartKey, sp.EndKey). Expand the bounds to the left so that we
	// include the start keys of any external sstables that overlap with
	// sp.StartKey.
	vers.IterAllLevelsAndSublevels(func(iter manifest.LevelIterator, level, sublevel int) {
		if f := iter.SeekGE(d.cmp, sp.StartKey); f != nil &&
			objstorage.IsExternalTable(d.objProvider, f.FileBacking.DiskFileNum) &&
			d.cmp(f.Smallest.UserKey, bounds.Start) < 0 {
			bounds.Start = f.Smallest.UserKey
		}
	})
	c := downloadCursor{
		level:  0,
		key:    bounds.Start,
		seqNum: 0,
	}
	_, c = c.NextExternalFile(d.cmp, d.objProvider, bounds, vers)
	if c.AtEnd() {
		// No external files in the given span.
		return nil, false
	}

	return &downloadSpanTask{
		downloadSpan:      sp,
		bounds:            bounds,
		taskCompletedChan: make(chan struct{}, 1),
		cursors:           []downloadCursor{c},
	}, true
}

// downloadCursor represents a position in the download process, which does not
// depend on a specific version.
//
// The Download process scans for external files level-by-level (starting with
// L0), and left-to-right (in terms of Smallest.UserKey) within each level. In
// L0, we break ties by the LargestSeqNum.
//
// A cursor can be thought of as a boundary between two files in a version
// (ordered by level, then by Smallest.UserKey, then by LargestSeqNum). A file
// is either "before" or "after" the cursor.
type downloadCursor struct {
	// LSM level (0 to NumLevels). When level=NumLevels, the cursor is at the end.
	level int
	// Inclusive lower bound for Smallest.UserKey for tables on level.
	key []byte
	// Inclusive lower bound for sequence number for tables on level with
	// Smallest.UserKey equaling key. Used to break ties within L0, and also used
	// to position a cursor immediately after a given file.
	seqNum uint64
}

// AtEnd returns true if the cursor is after all relevant files.
func (c downloadCursor) AtEnd() bool {
	return c.level >= manifest.NumLevels
}

func (c downloadCursor) String() string {
	return fmt.Sprintf("level=%d key=%q seqNum=%d", c.level, c.key, c.seqNum)
}

// makeCursorAtFile returns a downloadCursor that is immediately before the
// given file. Calling nextExternalFile on the resulting cursor (using the same
// version) should return f.
func makeCursorAtFile(f *fileMetadata, level int) downloadCursor {
	return downloadCursor{
		level:  level,
		key:    f.Smallest.UserKey,
		seqNum: f.LargestSeqNum,
	}
}

// makeCursorAfterFile returns a downloadCursor that is immediately
// after the given file.
func makeCursorAfterFile(f *fileMetadata, level int) downloadCursor {
	return downloadCursor{
		level:  level,
		key:    f.Smallest.UserKey,
		seqNum: f.LargestSeqNum + 1,
	}
}

func (c downloadCursor) FileIsAfterCursor(cmp base.Compare, f *fileMetadata, level int) bool {
	return c.Compare(cmp, makeCursorAfterFile(f, level)) < 0
}

func (c downloadCursor) Compare(keyCmp base.Compare, other downloadCursor) int {
	if c := cmp.Compare(c.level, other.level); c != 0 {
		return c
	}
	if c := keyCmp(c.key, other.key); c != 0 {
		return c
	}
	return cmp.Compare(c.seqNum, other.seqNum)
}

// NextExternalFile returns the first file after the cursor, returnin the file
// and a cursor that is immediately before the file. If no such file exists,
// returns a nil metadata and a cursor that is AtEnd.
func (c downloadCursor) NextExternalFile(
	cmp base.Compare, objProvider objstorage.Provider, bounds base.UserKeyBounds, v *version,
) (*fileMetadata, downloadCursor) {
	for !c.AtEnd() {
		if c.level == 0 {
			var first *fileMetadata
			var firstCursor downloadCursor
			for _, sublevel := range v.L0SublevelFiles {
				f := firstExternalFileInLevel(cmp, objProvider, c, sublevel.Iter(), bounds.End)
				if f != nil {
					c := makeCursorAtFile(f, c.level)
					if first == nil || c.Compare(cmp, firstCursor) < 0 {
						first = f
						firstCursor = c
					}
				}
			}
			if first != nil {
				return first, firstCursor
			}
		} else {
			it := v.Levels[c.level].Iter()
			f := firstExternalFileInLevel(cmp, objProvider, c, it, bounds.End)
			if f != nil {
				return f, makeCursorAtFile(f, c.level)
			}
		}
		c.key = bounds.Start
		c.seqNum = 0
		c.level++
	}
	return nil, c
}

// firstExternalFileInLevel finds the first external file after the cursor but
// which starts before the endBound. It is assumed that the iterator corresponds
// to cursor.level.
func firstExternalFileInLevel(
	cmp base.Compare,
	objProvider objstorage.Provider,
	cursor downloadCursor,
	it manifest.LevelIterator,
	endBound base.UserKeyBoundary,
) *fileMetadata {
	f := it.SeekGE(cmp, cursor.key)
	// Skip the file if it starts before cursor.key or is at that same key with lower
	// sequence number.
	for f != nil && !cursor.FileIsAfterCursor(cmp, f, cursor.level) {
		f = it.Next()
	}
	for ; f != nil && endBound.IsUpperBoundFor(cmp, f.Smallest.UserKey); f = it.Next() {
		if f.Virtual && objstorage.IsExternalTable(objProvider, f.FileBacking.DiskFileNum) {
			return f
		}
	}
	return nil
}

// tryLaunchDownloadForFile attempt to launch a download compaction for the
// given file. Returns true on success, or false if the file is already
// involved in a compaction.
func (d *DB) tryLaunchDownloadForFile(
	vers *version, env compactionEnv, download *downloadSpanTask, level int, f *fileMetadata,
) bool {
	if f.IsCompacting() {
		return false
	}
	if download.testing.launchDownloadCompaction != nil {
		return download.testing.launchDownloadCompaction(f)
	}
	kind := compactionKindRewrite
	if download.downloadSpan.ViaBackingFileDownload {
		kind = compactionKindCopy
	}
	pc := pickDownloadCompaction(vers, d.opts, env, d.mu.versions.picker.getBaseLevel(), kind, level, f)
	if pc == nil {
		// We are not able to run this download compaction at this time.
		return false
	}

	doneCh := make(chan error, 1)
	download.compactionDoneChans = append(download.compactionDoneChans, doneCh)
	c := newCompaction(pc, d.opts, d.timeNow(), d.objProvider)
	c.isDownload = true
	d.mu.compact.downloadingCount++
	d.addInProgressCompaction(c)
	go d.compact(c, doneCh)
	return true
}

type launchDownloadResult int8

const (
	launchedCompaction launchDownloadResult = iota
	didNotLaunchCompaction
	downloadTaskCompleted
)

func (d *DB) tryLaunchDownloadCompaction(
	download *downloadSpanTask, vers *manifest.Version, env compactionEnv, maxConcurrentDownloads int,
) launchDownloadResult {
	if len(download.cursors) == 0 {
		panic("download task already complete")
	}
	// We rebuild the cursors using the same slice. Note that we never append an
	// element without advancing oldCursors first.
	nextCursor := download.cursors[0]
	oldCursors := download.cursors[1:]
	download.cursors = download.cursors[:0]

	for len(oldCursors) > 0 || len(download.cursors) <= maxConcurrentDownloads {
		f, fCursor := nextCursor.NextExternalFile(d.cmp, d.objProvider, download.bounds, vers)
		if f == nil {
			// No more external files. Remaining old cursors can be discarded.
			if len(download.cursors) == 0 {
				download.taskCompletedChan <- struct{}{}
				return downloadTaskCompleted
			}
			download.cursors = append(download.cursors, fCursor)
			return didNotLaunchCompaction
		}
		nextCursor = makeCursorAfterFile(f, fCursor.level)

		// Discard old cursors that nextExternalFile just went over.
		for len(oldCursors) > 0 && oldCursors[0].Compare(d.cmp, nextCursor) <= 0 {
			oldCursors = oldCursors[1:]
		}

		if d.tryLaunchDownloadForFile(vers, env, download, fCursor.level, f) {
			// We launched a download for this file.
			if len(oldCursors) > 0 {
				download.cursors = append(download.cursors, oldCursors...)
			} else {
				download.cursors = append(download.cursors, nextCursor)
			}
			return launchedCompaction
		}

		// We were not able to launch a download for this file. Keep the cursor at
		// this file.
		download.cursors = append(download.cursors, fCursor)

		// Skip to the next existing cursor, if there is one.
		if len(oldCursors) > 0 {
			nextCursor = oldCursors[0]
			oldCursors = oldCursors[1:]
		}
	}

	return didNotLaunchCompaction
}

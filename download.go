// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
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
// The method returns once no external sstasbles overlap the given spans, the
// context is canceled, the db is closed, or an error is hit.
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
	for i := range spans {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.downloadSpan(ctx, spans[i]); err != nil {
			return err
		}
	}
	return nil
}

type downloadSpan struct {
	start []byte
	end   []byte
	// doneChans contains a list of channels passed into compactions as done
	// channels. Each channel has a buffer size of 1 and is only passed into
	// one compaction. This slice can grow over the lifetime of a downloadSpan.
	doneChans []chan error
	// compactionsStarted is the number of compactions started for this
	// downloadSpan. Must be equal to len(doneChans)-1, i.e. there's one spare
	// doneChan created each time a compaction starts up, for the next compaction.
	compactionsStarted int

	kind compactionKind
}

func (d *DB) downloadSpan(ctx context.Context, span DownloadSpan) error {
	dSpan := &downloadSpan{
		start: span.StartKey,
		end:   span.EndKey,
		// Protected by d.mu.
		doneChans: make([]chan error, 1),
		kind:      compactionKindRewrite,
	}
	if span.ViaBackingFileDownload {
		dSpan.kind = compactionKindCopy
	}
	dSpan.doneChans[0] = make(chan error, 1)
	doneChan := dSpan.doneChans[0]
	compactionIdx := 0

	func() {
		d.mu.Lock()
		defer d.mu.Unlock()

		d.mu.compact.downloads = append(d.mu.compact.downloads, dSpan)
		d.maybeScheduleCompaction()
	}()

	// Requires d.mu to be held.
	noExternalFilesInSpan := func() (noExternalFiles bool) {
		vers := d.mu.versions.currentVersion()

		for i := 0; i < len(vers.Levels); i++ {
			if vers.Levels[i].Empty() {
				continue
			}
			overlap := vers.Overlaps(i, base.UserKeyBoundsEndExclusive(span.StartKey, span.EndKey))
			foundExternalFile := false
			overlap.Each(func(metadata *manifest.FileMetadata) {
				objMeta, err := d.objProvider.Lookup(fileTypeTable, metadata.FileBacking.DiskFileNum)
				if err != nil {
					return
				}
				if objMeta.IsExternal() {
					foundExternalFile = true
				}
			})
			if foundExternalFile {
				return false
			}
		}
		return true
	}

	// Requires d.mu to be held.
	removeUsFromList := func() {
		// Check where we are in d.mu.compact.downloads. Remove us from the
		// list.
		for i := range d.mu.compact.downloads {
			if d.mu.compact.downloads[i] != dSpan {
				continue
			}
			copy(d.mu.compact.downloads[i:], d.mu.compact.downloads[i+1:])
			d.mu.compact.downloads = d.mu.compact.downloads[:len(d.mu.compact.downloads)-1]
			break
		}
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for {
		// Non-blocking select.
		select {
		case <-ctx.Done():
			removeUsFromList()
			return ctx.Err()

		case err := <-doneChan:
			if err != nil {
				removeUsFromList()
				return err
			}
			// Grab the next doneCh to wait on.
			compactionIdx++
			doneChan = dSpan.doneChans[compactionIdx]
			continue

		default:
		}

		// It's possible to have downloaded all files without getting notifications
		// on all doneChans. This is expected if there are a significant amount of
		// overlapping writes that schedule regular, non-download compactions.
		//
		// TODO(radu): if the span overlaps many files, checking this after every
		// compaction completes might be expensive.
		if noExternalFilesInSpan() {
			removeUsFromList()
			return nil
		}

		d.maybeScheduleCompaction()

		// If no compactions/download are running (and none were scheduled above),
		// we cannot wait on the condition.
		if d.mu.compact.compactingCount == 0 && d.mu.compact.downloadingCount == 0 {
			if d.closed.Load() != nil {
				return nil
			}
			// It is possible that the last compaction just updated the version but
			// did not yet update the compacting state on the external file(s). Sleep
			// for a bit and try again.
			// TODO(radu): investigate this case further.
			d.mu.Unlock()
			time.Sleep(100 * time.Microsecond)
			d.mu.Lock()
			continue
		}

		// Wait for a compaction/download to complete and recheck.
		d.mu.compact.cond.Wait()
	}
}

// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/private"
)

// flushExternalTable is installed as the private.FlushExternalTable hook.
// It's used by the internal/replay package to flush external tables into L0
// for supporting the pebble bench compact commands. Clients should use the
// replay package rather than calling this private hook directly.
func flushExternalTable(untypedDB interface{}, path string, originalMeta *fileMetadata) error {
	d := untypedDB.(*DB)
	if atomic.LoadInt32(&d.closed) != 0 {
		panic(ErrClosed)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}

	d.mu.Lock()
	fileNum := d.mu.versions.getNextFileNum()
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.mu.Unlock()

	m := &fileMetadata{
		FileNum:        fileNum,
		Size:           originalMeta.Size,
		CreationTime:   time.Now().Unix(),
		Smallest:       originalMeta.Smallest,
		Largest:        originalMeta.Largest,
		SmallestSeqNum: originalMeta.SmallestSeqNum,
		LargestSeqNum:  originalMeta.LargestSeqNum,
	}

	// Hard link the sstable into the DB directory.
	if err := ingestLink(jobID, d.opts, d.dirname, []string{path}, []*fileMetadata{m}); err != nil {
		return err
	}
	if err := d.dataDir.Sync(); err != nil {
		return err
	}

	d.commit.mu.Lock()
	defer d.commit.mu.Unlock()

	// Verify that the log sequence number hasn't exceeded the minimum
	// sequence number of the file.
	nextSeqNum := atomic.LoadUint64(d.commit.env.logSeqNum)
	if nextSeqNum > m.SmallestSeqNum {
		if err := ingestCleanup(d.opts.FS, d.dirname, []*fileMetadata{m}); err != nil {
			d.opts.Logger.Infof("flush external cleanup failed: %v", err)
		}
		return errors.Errorf("flush external %s: commit seqnum %d > file's smallest seqnum %d",
			m.FileNum, nextSeqNum, m.SmallestSeqNum)
	}

	// Assign sequence numbers from its current value through to the
	// external file's largest sequence number.
	count := m.LargestSeqNum - nextSeqNum + 1

	b := newBatch(nil)
	defer b.release()
	b.data = make([]byte, batchHeaderLen)
	b.setCount(uint32(count))
	b.commit.Add(1)
	d.commit.pending.enqueue(b)
	seqNum := atomic.AddUint64(d.commit.env.logSeqNum, uint64(count)) - uint64(count)
	b.setSeqNum(seqNum)

	// Apply the version edit.
	d.mu.Lock()
	d.mu.versions.logLock()
	ve := &versionEdit{
		NewFiles: []newFileEntry{newFileEntry{Level: 0, Meta: m}},
	}
	metrics := map[int]*LevelMetrics{
		0: &LevelMetrics{BytesIngested: m.Size, TablesIngested: 1},
	}
	err := d.mu.versions.logAndApply(jobID, ve, metrics, d.dataDir, func() []compactionInfo {
		return d.getInProgressCompactionInfoLocked(nil)
	})
	if err != nil {
		// NB: logAndApply will release d.mu.versions.logLock  unconditionally.
		d.mu.Unlock()
		if err2 := ingestCleanup(d.opts.FS, d.dirname, []*fileMetadata{m}); err2 != nil {
			d.opts.Logger.Infof("flush external cleanup failed: %v", err2)
		}
		return err
	}
	d.updateReadStateLocked(d.opts.DebugCheck)
	d.deleteObsoleteFiles(jobID)
	d.maybeScheduleCompaction()
	d.mu.Unlock()

	// Publish the sequence number.
	d.commit.publish(b)

	return nil
}

func init() {
	private.FlushExternalTable = flushExternalTable
}

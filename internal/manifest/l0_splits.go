// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"container/list"
	"context"
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/google/btree"
)

type splitConfig struct {
	splitBytes     int64
	sampleBytes    int64
	cmp            base.Compare
	sampleKeysFunc func(fileNum base.FileNum, bytesPerSample int64) ([][]byte, []int64, error)

	obsoleteMu *sync.Mutex
	obsoleteFn func(obsolete []base.FileNum)
}

// Callers should use newL0Splits, update, getSplits.
// Files specified by update are processed in the background.
// An added file queued for processing can be removed before
// it is processed -- this should help in preventing infinite
// queue build up when processing is slow. Removal cannot be
// synchronous since we may be in the middle of processing a
// file and we don't want removal (which is happening as part
// of creation of a new Version to block on sstable reads) --
// therefore the implementation takes a ref on the file and
// is responsible for passing it to the cleanup function if
// it is the last to unref.
//
// We could potentially generalize this structure for other
// background read operations, like reading range tombstones
// to decide whether to compute a compensated size.
type l0Splits struct {
	cfg         splitConfig
	btree       *btree.BTree
	fileKeysMap map[base.FileNum]fileKeys
	dirtyCount  int

	queueMu struct {
		sync.Mutex
		cond          *sync.Cond
		q             list.List
		qAddedFileMap map[base.FileNum]*list.Element
		stop          bool
		stopped       chan struct{}
	}
	splitMu struct {
		sync.Mutex
		keys [][]byte
	}
}

var l0SplitLabels = pprof.Labels("pebble", "l0-splits")

func newL0Splits(cfg splitConfig) *l0Splits {
	// Say 2GB in L0 and splitBytes = 100KB. That is 20K keys. Say 200 bytes
	// per key, so 4MB total, which is modest. With 100KB splitBytes and 5 2MB
	// files in L0, we have 100 keys, which should be a good enough sample for
	// splits.
	if cfg.splitBytes == 0 {
		cfg.splitBytes = 100 << 10
	}
	if cfg.sampleBytes == 0 {
		cfg.sampleBytes = 40 << 20
	}
	l := &l0Splits{
		cfg:         cfg,
		btree:       btree.New(16),
		fileKeysMap: make(map[base.FileNum]fileKeys),
	}
	l.queueMu.cond = sync.NewCond(&l.queueMu.Mutex)
	l.queueMu.q = list.List{}
	l.queueMu.qAddedFileMap = make(map[base.FileNum]*list.Element)
	l.queueMu.stopped = make(chan struct{})
	go func() {
		pprof.Do(context.Background(), l0SplitLabels, l.fileSampleLoop)
	}()
	return l
}

func (l *l0Splits) stop() {
	l.queueMu.Lock()
	l.queueMu.stop = true
	l.queueMu.cond.Signal()
	l.queueMu.Unlock()
	<-l.queueMu.stopped
}

// Should be called with removed files before the caller drops the ref.
func (l *l0Splits) update(addedFiles []*FileMetadata, removedFiles []base.FileNum) {
	l.queueMu.Lock()
	defer l.queueMu.Unlock()
	initialLen := l.queueMu.q.Len()
	for _, meta := range addedFiles {
		l.queueMu.qAddedFileMap[meta.FileNum] = l.queueMu.q.PushBack(&queueElement{addedFile: meta})
	}
	for _, f := range removedFiles {
		if e, ok := l.queueMu.qAddedFileMap[f]; ok {
			l.queueMu.q.Remove(e)
			delete(l.queueMu.qAddedFileMap, f)
		} else {
			l.queueMu.q.PushBack(&queueElement{removedFile: f})
		}
	}
	if initialLen == 0 && l.queueMu.q.Len() > 0 {
		l.queueMu.cond.Signal()
	}
}

func (l *l0Splits) getSplits() [][]byte {
	l.splitMu.Lock()
	defer l.splitMu.Unlock()
	return l.splitMu.keys
}

// ============== Internals =============
type btreeItem struct {
	key       []byte
	fileNum   base.FileNum
	byteCount int
	cmp       Compare
}

func (a *btreeItem) Less(than btree.Item) bool {
	b := than.(*btreeItem)
	c := a.cmp(a.key, b.key)
	if c == 0 {
		return a.fileNum < b.fileNum
	}
	return c < 0
}

type fileKeys [][]byte

type queueElement struct {
	// Exactly one is populated.
	addedFile   *FileMetadata
	removedFile base.FileNum
}

func (l *l0Splits) fileSampleLoop(context.Context) {
	l.queueMu.Lock()
	for {
		if l.queueMu.stop {
			l.queueMu.Unlock()
			close(l.queueMu.stopped)
			return
		}
		if l.queueMu.q.Len() > 0 {
			f := l.queueMu.q.Front()
			qElem := f.Value.(*queueElement)
			l.queueMu.q.Remove(f)
			if qElem.addedFile != nil {
				delete(l.queueMu.qAddedFileMap, qElem.addedFile.FileNum)
				atomic.AddInt32(&qElem.addedFile.refs, 1)
			}
			l.queueMu.Unlock()
			if qElem.addedFile != nil {
				l.processAddFile(qElem.addedFile)
			} else {
				l.processRemoveFile(qElem.removedFile)
			}
			l.queueMu.Lock()
			continue

		}
		l.queueMu.cond.Wait()
	}
}

func (l *l0Splits) processRemoveFile(num base.FileNum) {
	keys, ok := l.fileKeysMap[num]
	if !ok {
		panic(fmt.Sprintf("caller violated contract: removed file was never added: %d", num))
	}
	for i := range keys {
		if l.btree.Delete(&btreeItem{key: keys[i], fileNum: num, cmp: l.cfg.cmp}) == nil {
			panic("bug: key not found")
		}
	}
	l.dirtyCount += len(keys)
	delete(l.fileKeysMap, num)
	l.tryUpdateSplits()
}

func (l *l0Splits) processAddFile(f *FileMetadata) {
	fileNum := f.FileNum
	keys, keyBytes, err := l.cfg.sampleKeysFunc(fileNum, l.cfg.sampleBytes)
	// Add to fileKeysMap even if len(keys) == 0, which could happen if error
	// or otherwise, so that we can do detect that a file was not removed
	// without being added.
	l.fileKeysMap[fileNum] = keys
	if err != nil {
		// TODO(sumeer): log error
	}

	if atomic.AddInt32(&f.refs, -1) == 0 {
		obsolete := []base.FileNum{f.FileNum}
		l.cfg.obsoleteMu.Lock()
		l.cfg.obsoleteFn(obsolete)
		l.cfg.obsoleteMu.Unlock()
	}

	for i := range keys {
		prev := l.btree.ReplaceOrInsert(&btreeItem{
			key:       keys[i],
			fileNum:   fileNum,
			byteCount: int(keyBytes[i]),
			cmp:       l.cfg.cmp,
		})
		if prev != nil {
			panic("caller violated contract: file added more than once")
		}
	}
	l.dirtyCount += len(keys)
	l.tryUpdateSplits()
}

func (l *l0Splits) tryUpdateSplits() {
	if l.btree.Len() != 0 && float64(l.dirtyCount)/float64(l.btree.Len()) < 0.05 {
		return
	}
	l.dirtyCount = 0
	l.splitMu.Lock()
	defer l.splitMu.Unlock()
	l.splitMu.keys = l.splitMu.keys[:0]
	var byteCount int64
	processKeyFunc := func(i btree.Item) bool {
		item := i.(*btreeItem)
		if byteCount > l.cfg.splitBytes {
			l.splitMu.keys = append(l.splitMu.keys, item.key)
			byteCount = 0
		}
		byteCount += int64(item.byteCount)
		return true
	}
	l.btree.Ascend(processKeyFunc)
}

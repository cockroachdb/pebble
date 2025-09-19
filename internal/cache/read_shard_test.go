// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"cmp"
	"context"
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

type testReader struct {
	ctx context.Context
	key key
	re  *readEntry
	mu  struct {
		*sync.Mutex
		finishedWait     bool
		waitedNeedToRead bool
		waitedErr        error
		waitedValue      string

		cond *sync.Cond
	}
}

func newTestReader(
	ctx context.Context, id handleID, fileNum base.DiskFileNum, offset uint64, mu *sync.Mutex,
) *testReader {
	r := &testReader{
		ctx: ctx,
		key: makeKey(id, fileNum, offset),
	}
	r.mu.Mutex = mu
	r.mu.cond = sync.NewCond(mu)
	return r
}

func (r *testReader) getAsync(shard *shard) *string {
	v, re := shard.getWithMaybeReadEntry(r.key, CategorySSTableData, true /* desireReadEntry */)
	if v != nil {
		str := string(v.RawBuffer())
		v.Release()
		return &str
	}
	r.re = re
	go func() {
		v, _, err := re.waitForReadPermissionOrHandle(r.ctx)

		r.mu.Lock()
		defer r.mu.Unlock()

		switch {
		case v != nil:
			re.unrefAndTryRemoveFromMap()
			r.mu.waitedValue = string(v.RawBuffer())
			v.Release()

		case err != nil:
			re.unrefAndTryRemoveFromMap()
			r.mu.waitedErr = err

		default:
			r.mu.waitedNeedToRead = true
		}

		r.mu.finishedWait = true
		r.mu.cond.Signal()
	}()
	return nil
}

func (r *testReader) waitUntilFinishedWait() (*string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for !r.mu.finishedWait {
		r.mu.cond.Wait()
	}
	if r.mu.waitedNeedToRead {
		return nil, nil
	}
	if r.mu.waitedErr != nil {
		return nil, r.mu.waitedErr
	}
	return &r.mu.waitedValue, nil
}

func (r *testReader) setReadValue(t *testing.T, v string) {
	val := Alloc(len(v))
	copy(val.RawBuffer(), v)
	ReadHandle{entry: r.re}.SetReadValue(val)
	val.Release()
}

func (r *testReader) setError(err error) {
	ReadHandle{entry: r.re}.SetReadError(err)
}

func TestReadShard(t *testing.T) {
	var c *shard
	var readers map[string]*testReader
	var mu sync.Mutex
	freeShard := func() {
		if c != nil {
			c.Free()
			c = nil
		}
	}
	defer freeShard()
	datadriven.RunTest(t, "testdata/read_shard",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "init":
				freeShard()
				var maxSize int64
				td.ScanArgs(t, "max-size", &maxSize)
				c = &shard{}
				c.init(maxSize)
				if len(readers) > 0 {
					t.Fatalf("have %d readers that have not completed", len(readers))
				}
				readers = map[string]*testReader{}
				return ""

			case "get":
				var name string
				td.ScanArgs(t, "name", &name)
				if _, ok := readers[name]; ok {
					t.Fatalf("reader with name %s already exists", name)
				}
				var id, fileNum, offset int
				td.ScanArgs(t, "id", &id)
				td.ScanArgs(t, "file-num", &fileNum)
				td.ScanArgs(t, "offset", &offset)
				ctx := context.Background()
				if td.HasArg("cancelled-context") {
					var cancelFunc context.CancelFunc
					ctx, cancelFunc = context.WithCancel(ctx)
					cancelFunc()
				}
				r := newTestReader(ctx, handleID(id), base.DiskFileNum(fileNum), uint64(offset), &mu)
				val := r.getAsync(c)
				if val != nil {
					return fmt.Sprintf("val: %s", *val)
				}
				readers[name] = r
				time.Sleep(10 * time.Millisecond)
				return fmt.Sprintf("waiting\nmap-len: %d", c.readShard.lenForTesting())

			case "wait":
				var name string
				td.ScanArgs(t, "name", &name)
				val, err := readers[name].waitUntilFinishedWait()
				if val != nil || err != nil {
					delete(readers, name)
					if val != nil {
						return fmt.Sprintf("val: %s\nmap-len: %d", *val, c.readShard.lenForTesting())
					}
					if err != nil {
						return fmt.Sprintf("err: %s\nmap-len: %d", err.Error(), c.readShard.lenForTesting())
					}
				}
				return fmt.Sprintf("turn to read\nmap-len: %d", c.readShard.lenForTesting())

			case "set-read-value":
				var name string
				td.ScanArgs(t, "name", &name)
				var val string
				td.ScanArgs(t, "val", &val)
				readers[name].setReadValue(t, val)
				delete(readers, name)
				time.Sleep(10 * time.Millisecond)
				return fmt.Sprintf("map-len: %d", c.readShard.lenForTesting())

			case "set-error":
				var name string
				td.ScanArgs(t, "name", &name)
				readers[name].setError(errors.Errorf("read error: %s", name))
				delete(readers, name)
				time.Sleep(10 * time.Millisecond)
				return fmt.Sprintf("map-len: %d", c.readShard.lenForTesting())

			case "print-shard":
				return func() string {
					c.mu.RLock()
					defer c.mu.RUnlock()
					type shardEntry struct {
						k          key
						hasValue   bool
						referenced bool
					}
					var entries []shardEntry
					c.blocks.All(func(k key, e *entry) bool {
						entries = append(entries,
							shardEntry{k: k, hasValue: e.val != nil, referenced: e.referenced.Load()})
						return true
					})
					slices.SortFunc(entries, func(a, b shardEntry) int {
						return cmp.Or(
							cmp.Compare(a.k.id, b.k.id), cmp.Compare(a.k.fileNum, b.k.fileNum),
							cmp.Compare(a.k.offset, b.k.offset))
					})
					var b strings.Builder
					for _, e := range entries {
						fmt.Fprintf(&b, "id=%d file=%d offset=%d hasValue=%t referenced=%t\n",
							e.k.id, e.k.fileNum, e.k.offset, e.hasValue, e.referenced)
					}
					return b.String()
				}()

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)

			}
		})
}

// testSyncReaders is the config for multiple readers concurrently reading the
// same block.
type testSyncReaders struct {
	handle  *Handle
	fileNum base.DiskFileNum
	offset  uint64
	// val will be the value read, if not found in the cache.
	val []byte
	// numReaders is the number of concurrent readers.
	numReaders int
	// readerWithErrIndex is a reader that will have a read error and hand a
	// turn to another reader.
	readerWithErrIndex int
	// sleepDuration is the duration that the reader with the turns sleeps
	// before setting the value or error.
	sleepDuration time.Duration
	// wg is used to wait for all reader goroutines to be done.
	wg sync.WaitGroup
}

func TestReadShardConcurrent(t *testing.T) {
	cache := New(rand.Int63n(20 << 10))
	defer cache.Unref()
	c := cache.NewHandle()
	defer c.Close()

	handles := make([]*Handle, 50)
	for i := range handles {
		handles[i] = cache.NewHandle()
	}
	defer func() {
		for i := range handles {
			handles[i].Close()
		}
	}()

	var differentReaders []*testSyncReaders
	// 50 blocks are read.
	for i := 0; i < 50; i++ {
		valLen := rand.Intn(100) + 1
		val := make([]byte, valLen)
		crand.Read(val)
		readers := &testSyncReaders{
			handle:             handles[rand.Intn(len(handles))],
			fileNum:            base.DiskFileNum(rand.Uint64()),
			offset:             rand.Uint64(),
			val:                val,
			numReaders:         5,
			readerWithErrIndex: rand.Intn(5),
			sleepDuration:      time.Duration(rand.Intn(2)) * time.Millisecond,
		}
		readers.wg.Add(readers.numReaders)
		differentReaders = append(differentReaders, readers)
	}
	for _, r := range differentReaders {
		for j := 0; j < r.numReaders; j++ {
			go func(r *testSyncReaders, index int) {
				v, rh, _, _, _, err := r.handle.GetWithReadHandle(context.Background(), r.fileNum, r.offset, CategorySSTableData)
				require.NoError(t, err)
				if v != nil {
					require.Equal(t, r.val, v.RawBuffer())
					v.Release()
					r.wg.Done()
					return
				}
				if r.sleepDuration > 0 {
					time.Sleep(r.sleepDuration)
				}
				if r.readerWithErrIndex == index {
					rh.SetReadError(errors.Errorf("error"))
					r.wg.Done()
					return
				}
				v = Alloc(len(r.val))
				copy(v.RawBuffer(), r.val)
				rh.SetReadValue(v)
				v.Release()
				r.wg.Done()
			}(r, j)
		}
	}
	for _, r := range differentReaders {
		r.wg.Wait()
	}
}

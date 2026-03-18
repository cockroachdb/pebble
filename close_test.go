// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// TestCloseWithBlockedRemoteIO verifies that DB.Close() completes promptly when
// background operations (table stats loading, compactions) are blocked on
// remote storage I/O. The test creates a remote storage implementation that
// blocks reads until the context is cancelled, ingests external files backed by
// it, then closes the DB and verifies it doesn't hang.
func TestCloseWithBlockedRemoteIO(t *testing.T) {
	storage := newBlockingRemoteStorage()
	mem := vfs.NewMem()

	opts := &Options{
		FS:                    mem,
		FormatMajorVersion:    FormatNewest,
		L0CompactionThreshold: 100,
		L0StopWritesThreshold: 100,
	}
	opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		remote.NewLocator("blocking"): storage,
	})

	d, err := Open("", opts)
	require.NoError(t, err)

	// Write an SST into the blocking remote storage.
	writeOpts := d.opts.MakeWriterOptions(0, d.TableFormat())
	obj, err := storage.inner.CreateObject("ext1")
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewRemoteWritable(obj), writeOpts)
	require.NoError(t, w.Set([]byte("a"), []byte("val-a")))
	require.NoError(t, w.Set([]byte("z"), []byte("val-z")))
	require.NoError(t, w.Close())

	sz, err := storage.inner.Size("ext1")
	require.NoError(t, err)

	// Ingest the external file. This triggers table stats loading in the
	// background, which will try to read from the blocking storage.
	_, err = d.IngestExternalFiles(context.Background(), []ExternalFile{{
		Locator:           remote.NewLocator("blocking"),
		ObjName:           "ext1",
		Size:              uint64(sz),
		StartKey:          []byte("a"),
		EndKey:            []byte("z"),
		EndKeyIsInclusive: true,
		HasPointKey:       true,
	}})
	require.NoError(t, err)

	// Enable blocking on reads. Any background operation that tries to read
	// from the remote storage will now block until the context is cancelled.
	storage.block()

	// Close should cancel bgCtx, which unblocks any in-progress remote reads,
	// allowing background goroutines to finish.
	done := make(chan error, 1)
	go func() {
		done <- d.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DB.Close() hung; background operations likely blocked on remote I/O")
	}
}

// blockingRemoteStorage wraps an in-memory remote.Storage. When blocking is
// enabled, all ReadAt calls block until the context is cancelled.
type blockingRemoteStorage struct {
	inner remote.Storage

	mu       sync.Mutex
	blocking bool
	blockCh  chan struct{} // closed when blocking is enabled
}

func newBlockingRemoteStorage() *blockingRemoteStorage {
	return &blockingRemoteStorage{
		inner:   remote.NewInMem(),
		blockCh: make(chan struct{}),
	}
}

func (s *blockingRemoteStorage) block() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocking = true
	close(s.blockCh)
}

func (s *blockingRemoteStorage) isBlocking() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blocking
}

func (s *blockingRemoteStorage) Close() error { return s.inner.Close() }
func (s *blockingRemoteStorage) CreateObject(name string) (io.WriteCloser, error) {
	return s.inner.CreateObject(name)
}
func (s *blockingRemoteStorage) List(prefix, delimiter string) ([]string, error) {
	return s.inner.List(prefix, delimiter)
}
func (s *blockingRemoteStorage) Delete(name string) error        { return s.inner.Delete(name) }
func (s *blockingRemoteStorage) Size(name string) (int64, error) { return s.inner.Size(name) }
func (s *blockingRemoteStorage) IsNotExistError(err error) bool  { return s.inner.IsNotExistError(err) }

func (s *blockingRemoteStorage) ReadObject(
	ctx context.Context, objName string,
) (_ remote.ObjectReader, objSize int64, _ error) {
	reader, size, err := s.inner.ReadObject(ctx, objName)
	if err != nil {
		return nil, 0, err
	}
	return &blockingObjectReader{inner: reader, storage: s}, size, nil
}

type blockingObjectReader struct {
	inner   remote.ObjectReader
	storage *blockingRemoteStorage
}

func (r *blockingObjectReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	if r.storage.isBlocking() {
		// Block until the context is cancelled.
		<-ctx.Done()
		return ctx.Err()
	}
	return r.inner.ReadAt(ctx, p, offset)
}

func (r *blockingObjectReader) Close() error {
	return r.inner.Close()
}

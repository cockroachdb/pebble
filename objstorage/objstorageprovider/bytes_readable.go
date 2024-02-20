// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"

	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/objstorage"
)

// BytesReadable implements objstorage.Readable for a byte slice.
type BytesReadable []byte

var _ objstorage.Readable = (BytesReadable)(nil)

// ReadAt is part of the objstorage.Readable interface.
func (r BytesReadable) ReadAt(_ context.Context, p []byte, off int64) error {
	if n := copy(p, r[off:]); invariants.Enabled && n != len(p) {
		panic("short read")
	}
	return nil
}

// Close is part of the objstorage.Readable interface.
func (r BytesReadable) Close() error { return nil }

// Size is part of the objstorage.Readable interface.
func (r BytesReadable) Size() int64 { return int64(len(r)) }

// NewReadHandle is part of the objstorage.Readable interface.
func (r BytesReadable) NewReadHandle(_ context.Context) objstorage.ReadHandle { return r }

// SetupForCompaction is part of the objstorage.ReadHandle interface.
func (r BytesReadable) SetupForCompaction() {}

// RecordCacheHit is part of the objstorage.ReadHandle interface.
func (r BytesReadable) RecordCacheHit(ctx context.Context, offset, size int64) {}

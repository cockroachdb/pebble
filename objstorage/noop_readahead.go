// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import "io"

// NoopReadHandle can be used by Readable implementations that don't
// support read-ahead.
type NoopReadHandle struct {
	io.ReaderAt
}

// MakeNoopReadHandle initializes a NoopReadHandle.
func MakeNoopReadHandle(r io.ReaderAt) NoopReadHandle {
	return NoopReadHandle{ReaderAt: r}
}

var _ ReadHandle = (*NoopReadHandle)(nil)

// Close is part of the ReadHandle interface.
func (*NoopReadHandle) Close() error { return nil }

// MaxReadahead is part of the ReadHandle interface.
func (*NoopReadHandle) MaxReadahead() {}

// RecordCacheHit is part of the ReadHandle interface.
func (*NoopReadHandle) RecordCacheHit(offset, size int64) {}

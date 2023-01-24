// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import "io"

// NoopReadaheadHandle can be used by Readable implementations that don't
// support read-ahead.
type NoopReadaheadHandle struct {
	io.ReaderAt
}

// MakeNoopReadaheadHandle initializes a NoopReadaheadHandle.
func MakeNoopReadaheadHandle(r io.ReaderAt) NoopReadaheadHandle {
	return NoopReadaheadHandle{ReaderAt: r}
}

var _ ReadaheadHandle = (*NoopReadaheadHandle)(nil)

// Close is part of the ReadaheadHandle interface.
func (*NoopReadaheadHandle) Close() error { return nil }

// MaxReadahead is part of the ReadaheadHandle interface.
func (*NoopReadaheadHandle) MaxReadahead() {}

// RecordCacheHit is part of the ReadaheadHandle interface.
func (*NoopReadaheadHandle) RecordCacheHit(offset, size int64) {}

// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "io"

// CloseHelper wraps an io.Closer in a wrapper that ignores extra calls to
// Close. It is useful to ensure cleanup in error paths (using defer) without
// double-closing.
func CloseHelper(closer io.Closer) io.Closer {
	return &closeHelper{
		Closer: closer,
	}
}

type closeHelper struct {
	Closer io.Closer
}

// Close the underlying Closer, unless it was already closed.
func (h *closeHelper) Close() error {
	closer := h.Closer
	if closer == nil {
		return nil
	}
	h.Closer = nil
	return closer.Close()
}

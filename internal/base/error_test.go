// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCorruptBlockData(t *testing.T) {
	data := []byte("test block data")
	inner := errors.New("inner error")
	err := AttachCorruptBlockData(inner, data)
	// Wrap further.
	err = errors.Wrap(err, "outer")
	err = errors.WithStack(err)

	got := ExtractCorruptBlockData(err)
	require.Equal(t, data, got)

	// Original error message is preserved.
	require.Contains(t, err.Error(), "inner error")

	// No data when not attached.
	require.Nil(t, ExtractCorruptBlockData(errors.New("plain")))
}

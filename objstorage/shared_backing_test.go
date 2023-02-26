// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestSharedObjectBacking(t *testing.T) {
	meta := ObjectMetadata{
		FileNum:  1,
		FileType: base.FileTypeTable,
	}
	meta.Shared.CreatorID = 100
	meta.Shared.CreatorFileNum = 200

	buf, err := meta.SharedObjectBacking()
	require.NoError(t, err)

	meta1, err := fromSharedObjectBacking(meta.FileType, meta.FileNum, buf)
	require.NoError(t, err)
	require.Equal(t, meta, meta1)
}

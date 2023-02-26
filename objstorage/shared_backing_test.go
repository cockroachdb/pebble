// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"encoding/binary"
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

	t.Run("unknown-tags", func(t *testing.T) {
		// Append a tag that is safe to ignore.
		buf2 := buf
		buf2 = binary.AppendUvarint(buf2, 13)
		buf2 = binary.AppendUvarint(buf2, 2)
		buf2 = append(buf2, 1, 1)

		meta2, err := fromSharedObjectBacking(meta.FileType, meta.FileNum, buf2)
		require.NoError(t, err)
		require.Equal(t, meta, meta2)

		buf3 := buf2
		buf3 = binary.AppendUvarint(buf3, tagNotSafeToIgnoreMask+5)
		_, err = fromSharedObjectBacking(meta.FileType, meta.FileNum, buf3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown tag")
	})
}

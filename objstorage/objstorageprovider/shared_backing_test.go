// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/shared"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSharedObjectBacking(t *testing.T) {
	meta := objstorage.ObjectMetadata{
		FileNum:  1,
		FileType: base.FileTypeTable,
	}
	meta.Shared.CreatorID = 100
	meta.Shared.CreatorFileNum = 200
	meta.Shared.CleanupMethod = objstorage.SharedRefTracking

	st := DefaultSettings(vfs.NewMem(), "")
	st.Shared.Storage = shared.NewInMem()
	p, err := Open(st)
	require.NoError(t, err)
	defer p.Close()

	const creatorID = objstorage.CreatorID(99)
	require.NoError(t, p.SetCreatorID(creatorID))

	h, err := p.SharedObjectBacking(&meta)
	require.NoError(t, err)
	buf, err := h.Get()
	require.NoError(t, err)
	h.Close()
	_, err = h.Get()
	require.Error(t, err)

	meta1, refToCheck, err := fromSharedObjectBacking(meta.FileType, meta.FileNum, buf)
	require.NoError(t, err)
	require.Equal(t, meta, meta1)
	require.Equal(t, creatorID, refToCheck)

	t.Run("unknown-tags", func(t *testing.T) {
		// Append a tag that is safe to ignore.
		buf2 := buf
		buf2 = binary.AppendUvarint(buf2, 13)
		buf2 = binary.AppendUvarint(buf2, 2)
		buf2 = append(buf2, 1, 1)

		meta2, ref2, err := fromSharedObjectBacking(meta.FileType, meta.FileNum, buf2)
		require.NoError(t, err)
		require.Equal(t, meta, meta2)
		require.Equal(t, creatorID, ref2)

		buf3 := buf2
		buf3 = binary.AppendUvarint(buf3, tagNotSafeToIgnoreMask+5)
		_, _, err = fromSharedObjectBacking(meta.FileType, meta.FileNum, buf3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown tag")
	})
}

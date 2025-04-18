// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/stretchr/testify/require"
)

func TestSharedObjectNames(t *testing.T) {
	t.Run("crosscheck", func(t *testing.T) {
		for it := 0; it < 100; it++ {
			var meta objstorage.ObjectMetadata
			meta.DiskFileNum = base.DiskFileNum(rand.IntN(100000))
			meta.FileType = supportedFileTypes[rand.Int()%len(supportedFileTypes)]
			meta.Remote.CreatorID = objstorage.CreatorID(rand.Int64())
			meta.Remote.CreatorFileNum = base.DiskFileNum(rand.IntN(100000))
			if rand.IntN(4) == 0 {
				meta.Remote.CustomObjectName = fmt.Sprintf("foo-%d.sst", rand.IntN(10000))
			}

			obj := remoteObjectName(meta)
			// Cross-check against cleaner implementations.
			expObj := meta.Remote.CustomObjectName
			if expObj == "" {
				expObj = fmt.Sprintf("%04x-%s-%s", objHash(meta), meta.Remote.CreatorID, base.MakeFilename(meta.FileType, meta.Remote.CreatorFileNum))
			}
			require.Equal(t, expObj, obj)

			require.Equal(t, expObj+".ref.", sharedObjectRefPrefix(meta))

			refCreatorID := objstorage.CreatorID(rand.Int64())
			refObj := sharedObjectRefName(meta, refCreatorID, meta.DiskFileNum)
			expRefObj := fmt.Sprintf("%s.ref.%s.%s", expObj, refCreatorID, meta.DiskFileNum)
			require.Equal(t, refObj, expRefObj)
		}
	})

	t.Run("example", func(t *testing.T) {
		var meta objstorage.ObjectMetadata
		meta.DiskFileNum = base.DiskFileNum(123)
		meta.FileType = base.FileTypeTable
		meta.Remote.CreatorID = objstorage.CreatorID(456)
		meta.Remote.CreatorFileNum = base.DiskFileNum(789)
		require.Equal(t, remoteObjectName(meta), "0e17-456-000789.sst")
		require.Equal(t, sharedObjectRefPrefix(meta), "0e17-456-000789.sst.ref.")

		refCreatorID := objstorage.CreatorID(101112)
		require.Equal(
			t, sharedObjectRefName(meta, refCreatorID, meta.DiskFileNum),
			"0e17-456-000789.sst.ref.101112.000123",
		)
	})
	t.Run("example-blobfile", func(t *testing.T) {
		var meta objstorage.ObjectMetadata
		meta.DiskFileNum = base.DiskFileNum(123)
		meta.FileType = base.FileTypeBlob
		meta.Remote.CreatorID = objstorage.CreatorID(456)
		meta.Remote.CreatorFileNum = base.DiskFileNum(789)
		require.Equal(t, remoteObjectName(meta), "0e17-456-000789.blob")
		require.Equal(t, sharedObjectRefPrefix(meta), "0e17-456-000789.blob.ref.")

		refCreatorID := objstorage.CreatorID(101112)
		require.Equal(
			t, sharedObjectRefName(meta, refCreatorID, meta.DiskFileNum),
			"0e17-456-000789.blob.ref.101112.000123",
		)
	})
}

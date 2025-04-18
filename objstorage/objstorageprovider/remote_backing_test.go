// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var supportedFileTypes = []base.FileType{
	base.FileTypeTable,
	base.FileTypeBlob,
}

func TestSharedObjectBacking(t *testing.T) {
	for _, cleanup := range []objstorage.SharedCleanupMethod{objstorage.SharedRefTracking, objstorage.SharedNoCleanup} {
		name := "ref-tracking"
		if cleanup == objstorage.SharedNoCleanup {
			name = "no-cleanup"
		}
		for _, fileType := range supportedFileTypes {
			t.Run(fileType.String(), func(t *testing.T) {
				t.Run(name, func(t *testing.T) {
					st := DefaultSettings(vfs.NewMem(), "")
					sharedStorage := remote.NewInMem()
					st.Remote.StorageFactory = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
						"foo": sharedStorage,
					})
					p, err := Open(st)
					require.NoError(t, err)
					defer p.Close()

					const creatorID = objstorage.CreatorID(99)
					require.NoError(t, p.SetCreatorID(creatorID))
					meta := objstorage.ObjectMetadata{
						DiskFileNum: base.DiskFileNum(1),
						FileType:    fileType,
					}
					meta.Remote.CreatorID = 100
					meta.Remote.CreatorFileNum = base.DiskFileNum(200)
					meta.Remote.CleanupMethod = cleanup
					meta.Remote.Locator = "foo"
					meta.Remote.CustomObjectName = "obj-name"
					meta.Remote.Storage = sharedStorage

					h, err := p.RemoteObjectBacking(&meta)
					require.NoError(t, err)
					buf, err := h.Get()
					require.NoError(t, err)
					h.Close()
					_, err = h.Get()
					require.Error(t, err)

					d1, err := decodeRemoteObjectBacking(fileType, base.DiskFileNum(100), buf)
					require.NoError(t, err)
					require.Equal(t, uint64(100), uint64(d1.meta.DiskFileNum))
					require.Equal(t, fileType, d1.meta.FileType)
					d1.meta.Remote.Storage = sharedStorage
					require.Equal(t, meta.Remote, d1.meta.Remote)
					if cleanup == objstorage.SharedRefTracking {
						require.Equal(t, creatorID, d1.refToCheck.creatorID)
						require.Equal(t, base.DiskFileNum(1), d1.refToCheck.fileNum)
					} else {
						require.Equal(t, objstorage.CreatorID(0), d1.refToCheck.creatorID)
						require.Equal(t, base.DiskFileNum(0), d1.refToCheck.fileNum)
					}

					t.Run("unknown-tags", func(t *testing.T) {
						// Append a tag that is safe to ignore.
						buf2 := buf
						buf2 = binary.AppendUvarint(buf2, 13)
						buf2 = binary.AppendUvarint(buf2, 2)
						buf2 = append(buf2, 1, 1)

						d2, err := decodeRemoteObjectBacking(fileType, base.DiskFileNum(100), buf2)
						require.NoError(t, err)
						require.Equal(t, uint64(100), uint64(d2.meta.DiskFileNum))
						require.Equal(t, fileType, d2.meta.FileType)
						d2.meta.Remote.Storage = sharedStorage
						require.Equal(t, meta.Remote, d2.meta.Remote)
						if cleanup == objstorage.SharedRefTracking {
							require.Equal(t, creatorID, d2.refToCheck.creatorID)
							require.Equal(t, base.DiskFileNum(1), d2.refToCheck.fileNum)
						} else {
							require.Equal(t, objstorage.CreatorID(0), d2.refToCheck.creatorID)
							require.Equal(t, base.DiskFileNum(0), d2.refToCheck.fileNum)
						}

						buf3 := buf2
						buf3 = binary.AppendUvarint(buf3, tagNotSafeToIgnoreMask+5)
						_, err = decodeRemoteObjectBacking(meta.FileType, meta.DiskFileNum, buf3)
						require.Error(t, err)
						require.Contains(t, err.Error(), "unknown tag")
					})
				})
			})
		}
	}
}

func TestCreateSharedObjectBacking(t *testing.T) {
	for _, fileType := range supportedFileTypes {
		t.Run(fileType.String(), func(t *testing.T) {
			st := DefaultSettings(vfs.NewMem(), "")
			sharedStorage := remote.NewInMem()
			st.Remote.StorageFactory = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
				"foo": sharedStorage,
			})
			p, err := Open(st)
			require.NoError(t, err)
			defer p.Close()

			require.NoError(t, p.SetCreatorID(1))

			backing, err := p.CreateExternalObjectBacking("foo", "custom-obj-name")
			require.NoError(t, err)
			d, err := decodeRemoteObjectBacking(fileType, base.DiskFileNum(100), backing)
			require.NoError(t, err)
			require.Equal(t, uint64(100), uint64(d.meta.DiskFileNum))
			require.Equal(t, fileType, d.meta.FileType)
			require.Equal(t, remote.Locator("foo"), d.meta.Remote.Locator)
			require.Equal(t, "custom-obj-name", d.meta.Remote.CustomObjectName)
			require.Equal(t, objstorage.SharedNoCleanup, d.meta.Remote.CleanupMethod)
		})
	}
}

func TestAttachRemoteObjects(t *testing.T) {
	for _, fileType := range supportedFileTypes {
		t.Run(fileType.String(), func(t *testing.T) {
			st := DefaultSettings(vfs.NewMem(), "")
			sharedStorage := remote.NewInMem()
			st.Remote.StorageFactory = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
				"foo": sharedStorage,
			})
			p, err := Open(st)
			require.NoError(t, err)
			defer p.Close()
			require.NoError(t, p.SetCreatorID(1))
			backing, err := p.CreateExternalObjectBacking("foo", "custom-obj-name")
			require.NoError(t, err)
			_, err = p.AttachRemoteObjects([]objstorage.RemoteObjectToAttach{{
				FileType: fileType,
				FileNum:  100,
				Backing:  backing,
			}})
			require.NoError(t, err)

			// Sync, close, and reopen the provider and expect that we see
			// our object.
			require.NoError(t, p.Sync())
			require.NoError(t, p.Close())

			p, err = Open(st)
			require.NoError(t, err)
			defer p.Close()
			require.NoError(t, p.SetCreatorID(1))
			objs := p.List()
			require.Len(t, objs, 1)
			o := objs[0]
			require.Equal(t, remote.Locator("foo"), o.Remote.Locator)
			require.Equal(t, "custom-obj-name", o.Remote.CustomObjectName)
			require.Equal(t, uint64(100), uint64(o.DiskFileNum))
			require.Equal(t, fileType, o.FileType)
		})
	}
}

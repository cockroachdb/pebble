// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

type localSubsystem struct {
	fsDir vfs.File
}

type localLockedState struct {
	// objChangeCounter is incremented whenever non-remote objects are created.
	// The purpose of this counter is to avoid syncing the local filesystem when
	// only remote objects are changed.
	objChangeCounter uint64
	// objChangeCounterLastSync is the value of objChangeCounter at the time the
	// last completed sync was launched.
	objChangeCounterLastSync uint64
}

func (p *provider) localPath(fileType base.FileType, fileNum base.DiskFileNum) string {
	return base.MakeFilepath(p.st.Local.FS, p.st.Local.FSDirName, fileType, fileNum)
}

func (p *provider) localOpenForReading(
	ctx context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	opts objstorage.OpenOptions,
) (objstorage.Readable, error) {
	filename := p.localPath(fileType, fileNum)
	file, err := p.st.Local.FS.Open(filename, vfs.RandomReadsOption)
	if err != nil {
		if opts.MustExist && p.IsNotExistError(err) {
			err = base.AddDetailsToNotExistError(p.st.Local.FS, filename, err)
			err = base.MarkCorruptionError(err)
		}
		return nil, err
	}
	return newFileReadable(file, p.st.Local.FS, p.st.Local.ReadaheadConfig, filename)
}

func (p *provider) vfsCreate(
	_ context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	category vfs.DiskWriteCategory,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	filename := p.localPath(fileType, fileNum)
	file, err := p.st.Local.FS.Create(filename, category)
	if err != nil {
		return nil, objstorage.ObjectMetadata{}, err
	}
	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: p.st.Local.NoSyncOnClose,
		BytesPerSync:  p.st.Local.BytesPerSync,
	})
	meta := objstorage.ObjectMetadata{
		DiskFileNum: fileNum,
		FileType:    fileType,
	}
	return newFileBufferedWritable(file), meta, nil
}

func (p *provider) localRemove(fileType base.FileType, fileNum base.DiskFileNum) error {
	return p.st.Local.FSCleaner.Clean(p.st.Local.FS, fileType, p.localPath(fileType, fileNum))
}

// localInit finds any local FS objects.
func (p *provider) localInit() error {
	fsDir, err := p.st.Local.FS.OpenDir(p.st.Local.FSDirName)
	if err != nil {
		return err
	}
	p.local.fsDir = fsDir
	listing := p.st.Local.FSDirInitialListing
	if listing == nil {
		listing, err = p.st.Local.FS.List(p.st.Local.FSDirName)
		if err != nil {
			_ = p.localClose()
			return errors.Wrapf(err, "pebble: could not list store directory")
		}
	}

	for _, filename := range listing {
		fileType, fileNum, ok := base.ParseFilename(p.st.Local.FS, filename)
		if ok {
			switch fileType {
			case base.FileTypeTable, base.FileTypeBlob:
				o := objstorage.ObjectMetadata{
					FileType:    fileType,
					DiskFileNum: fileNum,
				}
				p.mu.knownObjects[o.DiskFileNum] = o
			}
		}
	}
	return nil
}

func (p *provider) localClose() error {
	var err error
	if p.local.fsDir != nil {
		err = p.local.fsDir.Close()
		p.local.fsDir = nil
	}
	return err
}

func (p *provider) localSync() error {
	p.mu.Lock()
	counterVal := p.mu.local.objChangeCounter
	lastSynced := p.mu.local.objChangeCounterLastSync
	p.mu.Unlock()

	if lastSynced >= counterVal {
		return nil
	}
	if err := p.local.fsDir.Sync(); err != nil {
		return err
	}

	p.mu.Lock()
	if p.mu.local.objChangeCounterLastSync < counterVal {
		p.mu.local.objChangeCounterLastSync = counterVal
	}
	p.mu.Unlock()

	return nil
}

func (p *provider) localSize(fileType base.FileType, fileNum base.DiskFileNum) (int64, error) {
	filename := p.localPath(fileType, fileNum)
	stat, err := p.st.Local.FS.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

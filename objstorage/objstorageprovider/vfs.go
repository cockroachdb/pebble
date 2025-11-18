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

	coldTier struct {
		fsDir vfs.File
	}
}

type localLockedState struct {
	hotTier, coldTier struct {
		// objChangeCounter is incremented whenever objects are created.
		// The purpose of this counter is to avoid syncing the local filesystem when
		// only remote objects are changed.
		objChangeCounter uint64
		// objChangeCounterLastSync is the value of objChangeCounter at the time the
		// last completed sync was launched.
		objChangeCounterLastSync uint64
	}
}

// objChanged is called after an object was created or deleted. It records the
// need to sync the relevant directory.
func (ls *localLockedState) objChanged(meta objstorage.ObjectMetadata) {
	if meta.Local.Tier == base.HotTier {
		ls.hotTier.objChangeCounter++
	} else {
		ls.coldTier.objChangeCounter++
	}
}

func (p *provider) localPath(
	fileType base.FileType, fileNum base.DiskFileNum, tier base.StorageTier,
) (vfs.FS, string) {
	if coldFS := p.st.Local.ColdTier.FS; tier == base.ColdTier && coldFS != nil {
		return coldFS, base.MakeFilepath(coldFS, p.st.Local.ColdTier.FSDirName, fileType, fileNum)
	}
	return p.st.Local.FS, base.MakeFilepath(p.st.Local.FS, p.st.Local.FSDirName, fileType, fileNum)
}

func (p *provider) localOpenForReading(
	ctx context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	tier base.StorageTier,
	opts objstorage.OpenOptions,
) (objstorage.Readable, error) {
	fs, filename := p.localPath(fileType, fileNum, tier)
	file, err := fs.Open(filename, vfs.RandomReadsOption)
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
	tier base.StorageTier,
	category vfs.DiskWriteCategory,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	fs, filename := p.localPath(fileType, fileNum, tier)
	file, err := fs.Create(filename, category)
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
	meta.Local.Tier = tier
	return newFileBufferedWritable(file), meta, nil
}

func (p *provider) localRemove(
	fileType base.FileType, fileNum base.DiskFileNum, tier base.StorageTier,
) error {
	fs, path := p.localPath(fileType, fileNum, tier)
	return p.st.Local.FSCleaner.Clean(fs, fileType, path)
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
		if fileType, fileNum, ok := base.ParseFilename(p.st.Local.FS, filename); ok {
			switch fileType {
			case base.FileTypeTable, base.FileTypeBlob:
				o := objstorage.ObjectMetadata{
					FileType:    fileType,
					DiskFileNum: fileNum,
				}
				o.Local.Tier = base.HotTier
				p.mu.knownObjects[o.DiskFileNum] = o
			}
		}
	}

	if p.st.Local.ColdTier.FS != nil {
		fsDir, err := p.st.Local.ColdTier.FS.OpenDir(p.st.Local.ColdTier.FSDirName)
		if err != nil {
			_ = p.localClose()
			return err
		}
		p.local.coldTier.fsDir = fsDir
		listing, err := p.st.Local.ColdTier.FS.List(p.st.Local.ColdTier.FSDirName)
		if err != nil {
			_ = p.localClose()
			return errors.Wrapf(err, "pebble: could not cold tier directory")
		}

		for _, filename := range listing {
			if fileType, fileNum, ok := base.ParseFilename(p.st.Local.FS, filename); ok {
				switch fileType {
				case base.FileTypeTable, base.FileTypeBlob:
					o := objstorage.ObjectMetadata{
						FileType:    fileType,
						DiskFileNum: fileNum,
					}
					o.Local.Tier = base.ColdTier
					if _, exists := p.mu.knownObjects[o.DiskFileNum]; exists {
						p.st.Logger.Errorf("object %s exists on both tiers; using hot tier version", o.DiskFileNum)
					} else {
						p.mu.knownObjects[o.DiskFileNum] = o
					}
				}
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
	if p.local.coldTier.fsDir != nil {
		err = firstError(err, p.local.coldTier.fsDir.Close())
		p.local.coldTier.fsDir = nil
	}
	return err
}

func (p *provider) localSync() error {
	p.mu.Lock()
	hot := p.mu.local.hotTier
	cold := p.mu.local.coldTier
	p.mu.Unlock()

	var hotSynced, coldSynced bool
	if hot.objChangeCounter > hot.objChangeCounterLastSync {
		if err := p.local.fsDir.Sync(); err != nil {
			return err
		}
		hotSynced = true
	}
	if cold.objChangeCounter > cold.objChangeCounterLastSync {
		if err := p.local.coldTier.fsDir.Sync(); err != nil {
			return err
		}
		coldSynced = true
	}

	if hotSynced || coldSynced {
		p.mu.Lock()
		if hotSynced && p.mu.local.hotTier.objChangeCounterLastSync < hot.objChangeCounter {
			p.mu.local.hotTier.objChangeCounterLastSync = hot.objChangeCounter
		}
		if coldSynced && p.mu.local.coldTier.objChangeCounterLastSync < cold.objChangeCounter {
			p.mu.local.coldTier.objChangeCounterLastSync = cold.objChangeCounter
		}
		p.mu.Unlock()
	}

	return nil
}

func (p *provider) localSize(
	fileType base.FileType, fileNum base.DiskFileNum, tier base.StorageTier,
) (int64, error) {
	fs, filename := p.localPath(fileType, fileNum, tier)
	stat, err := fs.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

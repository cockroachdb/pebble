// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
	hotTier struct {
		// objChangeCounter is incremented whenever objects are created.
		// The purpose of this counter is to avoid syncing the local filesystem when
		// only remote objects are changed.
		objChangeCounter uint64
		// objChangeCounterLastSync is the value of objChangeCounter at the time the
		// last completed sync was launched.
		objChangeCounterLastSync uint64
	}
	coldTier struct {
		// objChangeCounter is incremented whenever objects are created.
		// The purpose of this counter is to avoid syncing the local filesystem when
		// only remote objects are changed.
		objChangeCounter uint64
		// objChangeCounterLastSync is the value of objChangeCounter at the time the
		// last completed sync was launched.
		objChangeCounterLastSync uint64

		// metaFiles stores information about all cold tier objects that have a
		// known metadata file on the hot tier.
		metaFiles map[fileTypeAndNum]metaFileInfo
	}
}

type fileTypeAndNum struct {
	fileType base.FileType
	fileNum  base.DiskFileNum
}

type metaFileInfo struct {
	startOffset int64
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

// metaFileType returns the file type for a file that contains only the metadata
// for an object of the given type.
func metaFileType(fileType base.FileType) base.FileType {
	switch fileType {
	case base.FileTypeBlob:
		return base.FileTypeBlobMeta
	default:
		panic("unsupported file type for metaFileType")
	}
}

// metaPath returns the path to the metadata file for the given object in the
// cold tier. It is of the form "<file-num>.<file-type>meta.<start-offset>",
// where start-offset is the offset within the object where the metadata portion
// starts. For example: "000123.blobmeta.1048576".
func (p *provider) metaPath(
	fileType base.FileType, fileNum base.DiskFileNum, startOffset int64,
) string {
	metaFileType := metaFileType(fileType)
	prefix := base.MakeFilepath(p.st.Local.FS, p.st.Local.FSDirName, metaFileType, fileNum)
	return fmt.Sprintf("%s.%d", prefix, startOffset)
}

// offsetFromMetaPath parses the start offset from a metadata filename. For
// example, "000123.blobmeta.1048576" returns 1048576.
//
// Assumes the path was already validated by base.ParseFilename.
func offsetFromMetaPath(filename string) (startOffset int64, ok bool) {
	idx := strings.LastIndexByte(filename, '.')
	if idx == -1 {
		return 0, false
	}
	startOffset, err := strconv.ParseInt(filename[idx+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return startOffset, true
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
	r, err := newFileReadable(file, p.st.Local.FS, p.st.Local.ReadaheadConfig, filename)
	if err != nil {
		return nil, err
	}
	if tier == base.ColdTier {
		if startOffset, ok := p.getColdObjectMetaFile(fileType, fileNum); ok {
			metaPath := p.metaPath(fileType, fileNum, startOffset)
			return newColdReadable(r, p.st.Local.FS, metaPath, startOffset), nil
		}
	}
	return r, nil
}

func (p *provider) vfsCreate(
	_ context.Context,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	tier base.StorageTier,
	category vfs.DiskWriteCategory,
) (objstorage.Writable, objstorage.ObjectMetadata, error) {
	if tier == base.ColdTier && fileType != base.FileTypeBlob {
		return nil, objstorage.ObjectMetadata{}, errors.Errorf("cold tier not supported for file type %s", fileType)
	}

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
	w := objstorage.Writable(newFileBufferedWritable(file))
	if tier == base.ColdTier {
		w = newColdWritable(p, fileType, fileNum, w, category)
	}
	return w, meta, nil
}

func (p *provider) localRemove(
	fileType base.FileType, fileNum base.DiskFileNum, tier base.StorageTier,
) error {
	fs, path := p.localPath(fileType, fileNum, tier)
	err := p.st.Local.FSCleaner.Clean(fs, fileType, path)
	if tier == base.ColdTier {
		if startOffset, ok := p.popColdObjectMetaFile(fileType, fileNum); ok {
			metaPath := p.metaPath(fileType, fileNum, startOffset)
			metaFileType := metaFileType(fileType)
			err = firstError(err, p.st.Local.FSCleaner.Clean(p.st.Local.FS, metaFileType, metaPath))
		}
	}
	return err
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
		coldListing, err := p.st.Local.ColdTier.FS.List(p.st.Local.ColdTier.FSDirName)
		if err != nil {
			_ = p.localClose()
			return errors.Wrapf(err, "pebble: could not list cold tier directory")
		}

		for _, filename := range coldListing {
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
		p.mu.local.coldTier.metaFiles = make(map[fileTypeAndNum]metaFileInfo)
		// Look through the hot tier files to find any metadata files.
		for _, filename := range listing {
			if metaFileType, fileNum, ok := base.ParseFilename(p.st.Local.FS, filename); ok && metaFileType == base.FileTypeBlobMeta {
				startOffset, ok := offsetFromMetaPath(filename)
				if !ok {
					p.st.Logger.Errorf("could not parse offset component for %q", filename)
					continue
				}
				if _, ok := p.mu.knownObjects[fileNum]; !ok {
					// This is a stray file, remove it.
					filepath := p.st.Local.FS.PathJoin(p.st.Local.FSDirName, filename)
					_ = p.st.Local.FSCleaner.Clean(p.st.Local.FS, metaFileType, filepath)
					p.st.Logger.Infof("%q has no matching object, deleting", filepath)
					continue
				}
				p.mu.local.coldTier.metaFiles[fileTypeAndNum{
					fileType: base.FileTypeBlob,
					fileNum:  fileNum,
				}] = metaFileInfo{startOffset: startOffset}
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

// addColdObjectMetaFile adds an entry to coldTier.metaFiles.
func (p *provider) addColdObjectMetaFile(
	fileType base.FileType, fileNum base.DiskFileNum, startOffset int64,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.local.coldTier.metaFiles[fileTypeAndNum{
		fileType: fileType,
		fileNum:  fileNum,
	}] = metaFileInfo{
		startOffset: startOffset,
	}
}

// getColdObjectMetaFile returns ok=true if the object has a metadata file on
// the hot tier, along with the metadata start offset.
func (p *provider) getColdObjectMetaFile(
	fileType base.FileType, fileNum base.DiskFileNum,
) (startOffset int64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	info, ok := p.mu.local.coldTier.metaFiles[fileTypeAndNum{
		fileType: fileType,
		fileNum:  fileNum,
	}]
	return info.startOffset, ok
}

// popColdObjectMetaFile is like getColdObjectMetaFile but also removes the file
// from the internal map.
func (p *provider) popColdObjectMetaFile(
	fileType base.FileType, fileNum base.DiskFileNum,
) (startOffset int64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := fileTypeAndNum{fileType: fileType, fileNum: fileNum}
	info, ok := p.mu.local.coldTier.metaFiles[key]
	if !ok {
		return 0, false
	}
	delete(p.mu.local.coldTier.metaFiles, key)
	return info.startOffset, ok
}

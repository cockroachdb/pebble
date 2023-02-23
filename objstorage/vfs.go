// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
)

func (p *Provider) vfsPath(fileType base.FileType, fileNum base.FileNum) string {
	return base.MakeFilepath(p.st.FS, p.st.FSDirName, fileType, fileNum)
}

func (p *Provider) vfsOpenForReading(
	fileType base.FileType, fileNum base.FileNum, mustExist bool,
) (Readable, error) {
	filename := p.vfsPath(fileType, fileNum)
	file, err := p.st.FS.Open(filename, vfs.RandomReadsOption)
	if err != nil {
		if mustExist {
			base.MustExist(p.st.FS, filename, p.st.Logger, err)
		}
		return nil, err
	}
	// TODO(radu): we use the existence of the file descriptor as an indication
	// that the File might support Prefetch and SequentialReadsOption. We should
	// replace this with a cleaner way to obtain the capabilities of the FS / File.
	if fd := file.Fd(); fd != vfs.InvalidFd {
		return newFileReadable(file, p.st.FS, filename)
	}
	return newGenericFileReadable(file)
}

func (p *Provider) vfsCreate(
	fileType base.FileType, fileNum base.FileNum,
) (Writable, ObjectMetadata, error) {
	filename := p.vfsPath(fileType, fileNum)
	file, err := p.st.FS.Create(filename)
	if err != nil {
		return nil, ObjectMetadata{}, err
	}
	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: p.st.NoSyncOnClose,
		BytesPerSync:  p.st.BytesPerSync,
	})
	meta := ObjectMetadata{
		FileNum:  fileNum,
		FileType: fileType,
	}
	return newFileBufferedWritable(file), meta, nil
}

func (p *Provider) vfsRemove(fileType base.FileType, fileNum base.FileNum) error {
	return p.st.FSCleaner.Clean(p.st.FS, fileType, p.vfsPath(fileType, fileNum))
}

// vfsInit finds any local FS objects.
func (p *Provider) vfsInit() error {
	listing := p.st.FSDirInitialListing
	if listing == nil {
		var err error
		listing, err = p.st.FS.List(p.st.FSDirName)
		if err != nil {
			return errors.Wrapf(err, "pebble: could not list store directory")
		}
	}

	for _, filename := range listing {
		fileType, fileNum, ok := base.ParseFilename(p.st.FS, filename)
		if ok && fileType == base.FileTypeTable {
			o := ObjectMetadata{
				FileType: fileType,
				FileNum:  fileNum,
			}
			p.mu.knownObjects[o.FileNum] = o
		}
	}
	return nil
}

func (p *Provider) vfsSync() error {
	p.mu.Lock()
	shouldSync := p.mu.localObjectsChanged
	p.mu.localObjectsChanged = false
	p.mu.Unlock()

	if !shouldSync {
		return nil
	}
	if err := p.fsDir.Sync(); err != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.mu.localObjectsChanged = true
		return err
	}
	return nil
}

func (p *Provider) vfsSize(fileType base.FileType, fileNum base.FileNum) (int64, error) {
	filename := p.vfsPath(fileType, fileNum)
	stat, err := p.st.FS.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

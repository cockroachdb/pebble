// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
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

func (p *Provider) vfsCreate(fileType base.FileType, fileNum base.FileNum) (Writable, error) {
	filename := p.vfsPath(fileType, fileNum)
	file, err := p.st.FS.Create(filename)
	if err != nil {
		return nil, err
	}
	file = vfs.NewSyncingFile(file, vfs.SyncingFileOptions{
		NoSyncOnClose: p.st.NoSyncOnClose,
		BytesPerSync:  p.st.BytesPerSync,
	})
	return newFileBufferedWritable(file), nil
}

func (p *Provider) vfsRemove(fileType base.FileType, fileNum base.FileNum) error {
	return p.st.FSCleaner.Clean(p.st.FS, fileType, p.vfsPath(fileType, fileNum))
}

func (p *Provider) vfsFindExisting(ls []string) []ObjectMetadata {
	var res []ObjectMetadata
	for _, filename := range ls {
		fileType, fileNum, ok := base.ParseFilename(p.st.FS, filename)
		if ok && fileType == base.FileTypeTable {
			res = append(res, ObjectMetadata{
				FileType: fileType,
				FileNum:  fileNum,
			})
		}
	}
	return res
}

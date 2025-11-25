// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
)

// newColdWritable returns an objstorage.Writable that writes the main data to the
// wrapped "cold storage" writable, and all the metadata - i.e. everything after
// StartMetadataPortion() - to a separate file in a local filesystem. This
// allows reading metadata from the hot tier.
//
// When StartMetadataPortion() is called, coldWritable creates a new file with
// path produced by p.metaPath(). All subsequent writes are written to both
// files until Finish() or Abort() is called.
func newColdWritable(
	p *provider,
	fileType base.FileType,
	fileNum base.DiskFileNum,
	cold objstorage.Writable,
	diskWriteCategory vfs.DiskWriteCategory,
) *coldWritable {
	w := &coldWritable{
		p:                 p,
		fileType:          fileType,
		fileNum:           fileNum,
		cold:              cold,
		diskWriteCategory: diskWriteCategory,
	}
	return w
}

type coldWritable struct {
	p                 *provider
	fileType          base.FileType
	fileNum           base.DiskFileNum
	cold              objstorage.Writable
	diskWriteCategory vfs.DiskWriteCategory

	meta struct {
		started     bool
		startOffset int64
		file        vfs.File
		buf         []byte
	}

	err error
}

// metaBufSize is the same as the iobuf.Writer default size.
const metaBufSize = 4096

var _ objstorage.Writable = (*coldWritable)(nil)

// Write is part of the objstorage.Writable interface.
func (w *coldWritable) Write(p []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.meta.started {
		if w.meta.buf == nil {
			w.meta.buf = make([]byte, 0, metaBufSize)
		}
		// Because meta.file.Write() can mangle the buffer contents, we always have
		// to copy first.
		//
		// This code is similar to iobuf.Writer, except that we always issue the
		// write from a copy (since our writers can mangle the data).
		for ofs := 0; ofs < len(p); {
			n := copy(w.meta.buf[len(w.meta.buf):cap(w.meta.buf)], p[ofs:])
			w.meta.buf = w.meta.buf[:len(w.meta.buf)+n]
			ofs += n
			if len(w.meta.buf) == cap(w.meta.buf) {
				if w.err = w.flushMeta(); w.err != nil {
					return w.err
				}
			}
		}
	} else {
		w.meta.startOffset += int64(len(p))
	}
	w.err = w.cold.Write(p)
	return w.err
}

func (w *coldWritable) StartMetadataPortion() error {
	if w.err != nil {
		return w.err
	}
	if w.meta.started {
		return nil
	}
	w.meta.file, w.err = w.p.st.Local.FS.Create(w.metaPath(), w.diskWriteCategory)
	if w.err != nil {
		return w.err
	}
	w.meta.started = true
	return w.cold.StartMetadataPortion()
}

func (w *coldWritable) flushMeta() error {
	if _, err := w.meta.file.Write(w.meta.buf); err != nil {
		return err
	}
	w.meta.buf = w.meta.buf[:0]
	return nil
}

// Finish is part of the objstorage.Writable interface.
//
// Finish guarantees that on failure or crash, either the metadata file doesn't
// exist or it will be cleaned up when the object is deleted.
func (w *coldWritable) Finish() error {
	if w.err != nil {
		w.Abort()
		return w.err
	}
	if w.meta.started {
		if len(w.meta.buf) > 0 {
			if err := w.flushMeta(); err != nil {
				w.Abort()
				return err
			}
		}
		err := firstError(w.meta.file.Sync(), w.meta.file.Close())
		w.meta.file = nil
		if err != nil {
			w.Abort()
			return err
		}
	}
	// We finish the cold tier write first, then register the metadata file. If we
	// crash after cold.Finish() but before addColdObjectMetaFile(), the metadata
	// file will be discovered during startup.
	if err := w.cold.Finish(); err != nil {
		w.deleteMetaFile()
		return err
	}
	if w.meta.started {
		w.p.addColdObjectMetaFile(w.fileType, w.fileNum, w.meta.startOffset)
	}
	return nil
}

func (w *coldWritable) Abort() {
	if w.meta.started {
		if w.meta.file != nil {
			_ = w.meta.file.Close()
			w.meta.file = nil
		}
		w.deleteMetaFile()
	}
	w.cold.Abort()
}

func (w *coldWritable) deleteMetaFile() {
	_ = w.p.st.Local.FS.Remove(w.metaPath())
}

func (w *coldWritable) metaPath() string {
	return w.p.metaPath(w.fileType, w.fileNum, w.meta.startOffset)
}

// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat

import (
	"fmt"
	"io"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

// Catalog is used to manage the on-disk shared object catalog.
//
// The catalog file is a log of records, where each record is an encoded
// versionEdit.
type Catalog struct {
	fs      vfs.FS
	dirname string
	mu      struct {
		sync.Mutex

		objects CatalogContents

		marker *atomicfs.Marker

		catalogFile      vfs.File
		catalogRecWriter *record.Writer

		// catalogFilename is the filename of catalogFile when it is set, otherwise
		// it is the filename of the catalog file that was loaded when we opened the
		// catalog.
		catalogFilename string
	}
}

// CatalogContents contains the shared objects in the catalog.
type CatalogContents = map[base.FileNum]SharedObjectMetadata

// SharedObjectMetadata encapsulates the data stored in the catalog file for each object.
type SharedObjectMetadata struct {
	CreatorID      uint64
	CreatorFileNum base.FileNum
}

const (
	catalogFilenameBase = "SHARED-OBJ-CATALOG"
	catalogMarkerName   = "shared-obj-catalog"
)

// Open creates a Catalog and loads any existing catalog file, returning the contents.
func Open(fs vfs.FS, dirname string) (*Catalog, CatalogContents, error) {
	c := &Catalog{
		fs:      fs,
		dirname: dirname,
	}
	c.mu.objects = make(map[base.FileNum]SharedObjectMetadata)
	var err error
	c.mu.marker, c.mu.catalogFilename, err = atomicfs.LocateMarker(fs, dirname, catalogMarkerName)
	if err != nil {
		return nil, nil, err
	}
	// If the filename is empty, there is no existing catalog.
	if c.mu.catalogFilename != "" {
		if err := c.loadFromCatalogFile(c.mu.catalogFilename); err != nil {
			return nil, nil, err
		}
		if err := c.mu.marker.RemoveObsolete(); err != nil {
			return nil, nil, err
		}
		// TODO(radu): remove obsolete catalog files.
	}
	return c, c.mu.objects, nil
}

// Close any open files.
func (c *Catalog) Close() error {
	if c.mu.catalogFile != nil {
		err1 := c.mu.catalogRecWriter.Close()
		c.mu.catalogRecWriter = nil
		err2 := c.mu.catalogFile.Close()
		c.mu.catalogFile = nil
		if err1 != nil {
			return err1
		}
		return err2
	}
	return nil
}

// AddObject adds a shared object to the catalog; returns after the change is
// stably recorded on storage.
func (c *Catalog) AddObject(fileNum base.FileNum, meta SharedObjectMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ve := versionEdit{
		NewObjects: map[base.FileNum]SharedObjectMetadata{
			fileNum: meta,
		},
	}
	if err := c.writeToCatalogFileLocked(&ve); err != nil {
		panic(err)
	}
	c.mu.objects[fileNum] = meta
}

// DeleteObject deletes a shared object to the catalog; returns after the change
// is stably recorded on storage.
func (c *Catalog) DeleteObject(fileNum base.FileNum) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ve := versionEdit{
		DeletedObjects: map[base.FileNum]struct{}{
			fileNum: {},
		},
	}
	if err := c.writeToCatalogFileLocked(&ve); err != nil {
		panic(err)
	}
	delete(c.mu.objects, fileNum)
}

func (c *Catalog) loadFromCatalogFile(filename string) error {
	c.mu.objects = make(CatalogContents)
	catalogPath := c.fs.PathJoin(c.dirname, filename)
	f, err := c.fs.Open(catalogPath)
	if err != nil {
		return errors.Wrapf(
			err, "pebble: could not open shared object catalog file %q for DB %q",
			errors.Safe(filename), c.dirname,
		)
	}
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "pebble: error when loading shared object catalog file %q",
				errors.Safe(filename))
		}
		var ve versionEdit
		err = ve.Decode(r)
		if err != nil {
			return errors.Wrapf(err, "pebble: error when loading shared object catalog file %q",
				errors.Safe(filename))
		}
		// Apply the version edit to the current state.
		for fileNum := range ve.DeletedObjects {
			delete(c.mu.objects, fileNum)
		}
		for fileNum, meta := range ve.NewObjects {
			c.mu.objects[fileNum] = meta
		}
	}
	return nil
}

// writeToCatalogFileLocked writes a versionEdit to the catalog file.
// Creates a new file if this is the first write.
func (c *Catalog) writeToCatalogFileLocked(ve *versionEdit) error {
	if c.mu.catalogFile == nil {
		if err := c.createNewCatalogFileLocked(); err != nil {
			return err
		}
	}
	// TODO(radu): implement file rotation.
	return writeRecord(ve, c.mu.catalogFile, c.mu.catalogRecWriter)
}

func makeCatalogFilename(iter uint64) string {
	return fmt.Sprintf("%s-%06d", catalogFilenameBase, iter)
}

// createNewCatalogFileLocked creates a new catalog file, populates it with the
// current catalog and sets c.mu.catalogFile and c.mu.catalogRecWriter.
func (c *Catalog) createNewCatalogFileLocked() (outErr error) {
	if c.mu.catalogFile != nil {
		return errors.AssertionFailedf("catalogFile already open")
	}
	filename := makeCatalogFilename(c.mu.marker.NextIter())
	file, err := c.fs.Create(c.fs.PathJoin(c.dirname, filename))
	if err != nil {
		return err
	}
	recWriter := record.NewWriter(file)
	err = func() error {
		// Create a versionEdit that gets us from an empty catalog to the current state.
		ve := versionEdit{
			NewObjects: c.mu.objects,
		}
		if err := writeRecord(&ve, file, recWriter); err != nil {
			return err
		}

		// Move the marker to the new filename. Move handles syncing the data
		// directory as well.
		if err := c.mu.marker.Move(filename); err != nil {
			return errors.Wrap(err, "moving marker")
		}

		if c.mu.catalogFilename != "" {
			if err := c.fs.Remove(c.fs.PathJoin(c.dirname, c.mu.catalogFilename)); err != nil {
				return err
			}
		}

		return nil
	}()

	if err != nil {
		_ = recWriter.Close()
		_ = file.Close()
		return err
	}

	c.mu.catalogFile = file
	c.mu.catalogRecWriter = recWriter
	c.mu.catalogFilename = filename
	return nil
}

func writeRecord(ve *versionEdit, file vfs.File, recWriter *record.Writer) error {
	w, err := recWriter.Next()
	if err != nil {
		return err
	}
	if err := ve.Encode(w); err != nil {
		return err
	}
	if err := recWriter.Flush(); err != nil {
		return err
	}
	return file.Sync()
}

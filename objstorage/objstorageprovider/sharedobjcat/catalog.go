// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat

import (
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
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

		creatorID objstorage.CreatorID
		objects   map[base.DiskFileNum]SharedObjectMetadata

		marker *atomicfs.Marker

		catalogFile      vfs.File
		catalogRecWriter *record.Writer

		rotationHelper record.RotationHelper

		// catalogFilename is the filename of catalogFile when catalogFile != nil, otherwise
		// it is the filename of the last catalog file.
		catalogFilename string
	}
}

// SharedObjectMetadata encapsulates the data stored in the catalog file for each object.
type SharedObjectMetadata struct {
	// FileNum is the identifier for the object within the context of a single DB
	// instance.
	FileNum base.DiskFileNum
	// FileType is the type of the object. Only certain FileTypes are possible.
	FileType base.FileType
	// CreatorID identifies the DB instance that originally created the object.
	CreatorID objstorage.CreatorID
	// CreatorFileNum is the identifier for the object within the context of the
	// DB instance that originally created the object.
	CreatorFileNum base.DiskFileNum

	CleanupMethod objstorage.SharedCleanupMethod
}

const (
	catalogFilenameBase = "SHARED-CATALOG"
	catalogMarkerName   = "shared-catalog"

	// We create a new file when the size exceeds 1MB (and some other conditions
	// hold; see record.RotationHelper).
	rotateFileSize = 1024 * 1024 // 1MB
)

// CatalogContents contains the shared objects in the catalog.
type CatalogContents struct {
	// CreatorID, if it is set.
	CreatorID objstorage.CreatorID
	Objects   []SharedObjectMetadata
}

// Open creates a Catalog and loads any existing catalog file, returning the
// creator ID (if it is set) and the contents.
func Open(fs vfs.FS, dirname string) (*Catalog, CatalogContents, error) {
	c := &Catalog{
		fs:      fs,
		dirname: dirname,
	}
	c.mu.objects = make(map[base.DiskFileNum]SharedObjectMetadata)

	var err error
	c.mu.marker, c.mu.catalogFilename, err = atomicfs.LocateMarker(fs, dirname, catalogMarkerName)
	if err != nil {
		return nil, CatalogContents{}, err
	}
	// If the filename is empty, there is no existing catalog.
	if c.mu.catalogFilename != "" {
		if err := c.loadFromCatalogFile(c.mu.catalogFilename); err != nil {
			return nil, CatalogContents{}, err
		}
		if err := c.mu.marker.RemoveObsolete(); err != nil {
			return nil, CatalogContents{}, err
		}
		// TODO(radu): remove obsolete catalog files.
	}
	res := CatalogContents{
		CreatorID: c.mu.creatorID,
		Objects:   make([]SharedObjectMetadata, 0, len(c.mu.objects)),
	}
	for _, meta := range c.mu.objects {
		res.Objects = append(res.Objects, meta)
	}
	// Sort the objects so the function is deterministic.
	sort.Slice(res.Objects, func(i, j int) bool {
		return res.Objects[i].FileNum.FileNum() < res.Objects[j].FileNum.FileNum()
	})
	return c, res, nil
}

// SetCreatorID sets the creator ID. If it is already set, it must match.
func (c *Catalog) SetCreatorID(id objstorage.CreatorID) error {
	if !id.IsSet() {
		return errors.AssertionFailedf("attempt to unset CreatorID")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.creatorID.IsSet() {
		if c.mu.creatorID != id {
			return errors.AssertionFailedf("attempt to change CreatorID from %s to %s", c.mu.creatorID, id)
		}
		return nil
	}

	ve := versionEdit{CreatorID: id}
	if err := c.writeToCatalogFileLocked(&ve); err != nil {
		return errors.Wrapf(err, "pebble: could not write to shared object catalog: %v", err)
	}
	c.mu.creatorID = id
	return nil
}

// Close any open files.
func (c *Catalog) Close() error {
	return c.closeCatalogFile()
}

func (c *Catalog) closeCatalogFile() error {
	if c.mu.catalogFile == nil {
		return nil
	}
	err1 := c.mu.catalogRecWriter.Close()
	err2 := c.mu.catalogFile.Close()
	c.mu.catalogRecWriter = nil
	c.mu.catalogFile = nil
	if err1 != nil {
		return err1
	}
	return err2
}

// Batch is used to perform multiple object additions/deletions at once.
type Batch struct {
	ve versionEdit
}

// AddObject adds a new object to the batch.
//
// The given FileNum must be new - it must not match that of any object that was
// ever in the catalog.
func (b *Batch) AddObject(meta SharedObjectMetadata) {
	b.ve.NewObjects = append(b.ve.NewObjects, meta)
}

// DeleteObject adds an object removal to the batch.
func (b *Batch) DeleteObject(fileNum base.DiskFileNum) {
	b.ve.DeletedObjects = append(b.ve.DeletedObjects, fileNum)
}

// Reset clears the batch.
func (b *Batch) Reset() {
	b.ve.NewObjects = b.ve.NewObjects[:0]
	b.ve.DeletedObjects = b.ve.DeletedObjects[:0]
}

// IsEmpty returns true if the batch is empty.
func (b *Batch) IsEmpty() bool {
	return len(b.ve.NewObjects) == 0 && len(b.ve.DeletedObjects) == 0
}

// Copy returns a copy of the Batch.
func (b *Batch) Copy() Batch {
	var res Batch
	if len(b.ve.NewObjects) > 0 {
		res.ve.NewObjects = make([]SharedObjectMetadata, len(b.ve.NewObjects))
		copy(res.ve.NewObjects, b.ve.NewObjects)
	}
	if len(b.ve.DeletedObjects) > 0 {
		res.ve.DeletedObjects = make([]base.DiskFileNum, len(b.ve.DeletedObjects))
		copy(res.ve.DeletedObjects, b.ve.DeletedObjects)
	}
	return res
}

// Append merges two batches.
func (b *Batch) Append(other Batch) {
	b.ve.NewObjects = append(b.ve.NewObjects, other.ve.NewObjects...)
	b.ve.DeletedObjects = append(b.ve.DeletedObjects, other.ve.DeletedObjects...)
}

// ApplyBatch applies a batch of updates; returns after the change is stably
// recorded on storage.
func (c *Catalog) ApplyBatch(b Batch) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, n := range b.ve.DeletedObjects {
		if _, exists := c.mu.objects[n]; !exists {
			return errors.AssertionFailedf("deleting non-existent object %s", n)
		}
	}
	for _, meta := range b.ve.NewObjects {
		if _, exists := c.mu.objects[meta.FileNum]; exists {
			return errors.AssertionFailedf("adding existing object %s", meta.FileNum)
		}
	}

	if err := c.writeToCatalogFileLocked(&b.ve); err != nil {
		return errors.Wrapf(err, "pebble: could not write to shared object catalog: %v", err)
	}

	// Apply the batch to our current state.
	for _, n := range b.ve.DeletedObjects {
		delete(c.mu.objects, n)
	}
	for _, meta := range b.ve.NewObjects {
		c.mu.objects[meta.FileNum] = meta
	}
	b.Reset()
	return nil
}

func (c *Catalog) loadFromCatalogFile(filename string) error {
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
		if ve.CreatorID.IsSet() {
			c.mu.creatorID = ve.CreatorID
		}
		for _, fileNum := range ve.DeletedObjects {
			delete(c.mu.objects, fileNum)
		}
		for _, meta := range ve.NewObjects {
			c.mu.objects[meta.FileNum] = meta
		}
	}
	return nil
}

// writeToCatalogFileLocked writes a versionEdit to the catalog file.
// Creates a new file if this is the first write.
func (c *Catalog) writeToCatalogFileLocked(ve *versionEdit) error {
	c.mu.rotationHelper.AddRecord(int64(len(ve.NewObjects) + len(ve.DeletedObjects)))
	snapshotSize := int64(len(c.mu.objects))

	var shouldRotate bool
	if c.mu.catalogFile == nil {
		shouldRotate = true
	} else if c.mu.catalogRecWriter.Size() >= rotateFileSize {
		shouldRotate = c.mu.rotationHelper.ShouldRotate(snapshotSize)
	}

	if shouldRotate {
		if c.mu.catalogFile != nil {
			if err := c.closeCatalogFile(); err != nil {
				return err
			}
		}
		if err := c.createNewCatalogFileLocked(); err != nil {
			return err
		}
		c.mu.rotationHelper.Rotate(snapshotSize)
	}
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
	filepath := c.fs.PathJoin(c.dirname, filename)
	file, err := c.fs.Create(filepath)
	if err != nil {
		return err
	}
	recWriter := record.NewWriter(file)
	err = func() error {
		// Create a versionEdit that gets us from an empty catalog to the current state.
		var ve versionEdit
		ve.CreatorID = c.mu.creatorID
		ve.NewObjects = make([]SharedObjectMetadata, 0, len(c.mu.objects))
		for _, meta := range c.mu.objects {
			ve.NewObjects = append(ve.NewObjects, meta)
		}
		if err := writeRecord(&ve, file, recWriter); err != nil {
			return err
		}

		// Move the marker to the new filename. Move handles syncing the data
		// directory as well.
		if err := c.mu.marker.Move(filename); err != nil {
			return errors.Wrap(err, "moving marker")
		}

		return nil
	}()

	if err != nil {
		_ = recWriter.Close()
		_ = file.Close()
		_ = c.fs.Remove(filepath)
		return err
	}

	// Remove any previous file (ignoring any error).
	if c.mu.catalogFilename != "" {
		_ = c.fs.Remove(c.fs.PathJoin(c.dirname, c.mu.catalogFilename))
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

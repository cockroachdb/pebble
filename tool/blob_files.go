// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"fmt"
	"io"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

// blobFileMappings contains state for retrieving a value separated into a blob
// file.
//
// An sstable alone does not contain sufficient information to read a value from
// a blob file. The sstable encodes a BlobReferenceID which first must be translated
// to a BlobFileID and then from a BlobFileID to a DiskFileNum. The manifest
// contains this metadata. The blobFileMappings type contains this state, loaded
// from the manifest.
type blobFileMappings struct {
	stderr        io.Writer
	references    map[base.TableNum]*manifest.BlobReferences
	physicalFiles map[base.BlobFileID][]base.DiskFileNum
	fetcher       blob.ValueFetcher
	provider      debugReaderProvider
}

// LoadValueBlobContext returns a TableBlobContext that configures a sstable
// iterator to fetch the value stored in a blob file, using the mappings in this
// blobFileMappings.
func (m *blobFileMappings) LoadValueBlobContext(tableNum base.TableNum) sstable.TableBlobContext {
	return sstable.TableBlobContext{
		ValueFetcher: &m.fetcher,
		References:   m.references[tableNum],
	}
}

// Lookup implements base.BlobFileMapping.
func (m *blobFileMappings) Lookup(fileID base.BlobFileID) (base.ObjectInfo, bool) {
	files, ok := m.physicalFiles[fileID]
	if !ok || len(files) == 0 {
		return nil, false
	}
	// TODO(jackson): Consider checking for the existence of each file and using
	// the most recent existing file (and log the missing files).
	if len(files) > 1 {
		fmt.Fprintf(m.stderr, "warning: multiple physical files for blob file %s: %v\n", fileID, files)
	}
	return base.ObjectInfoLiteral{
		FileType:    base.FileTypeBlob,
		DiskFileNum: files[len(files)-1],
		// TODO(jackson): Add bounds for blob files.
		Bounds: base.UserKeyBounds{},
	}, true
}

// Close releases any resources held by the blobFileMappings.
func (m *blobFileMappings) Close() error {
	return errors.CombineErrors(m.fetcher.Close(), m.provider.objProvider.Close())
}

// newBlobFileMappings builds a blobFileMappings from a list of manifests.
//
// Possibly benign errors are printed to stderr. For example, if a process exits
// during a manifest rotation, we may encounter an error reading a manifest, but
// we'll still have sufficient information to read separated values from all
// tables in the current version of the LSM.
//
// The returned blobFileMappings must be closed to release resources.
func newBlobFileMappings(
	stderr io.Writer, fs vfs.FS, dir string, manifests []fileLoc,
) (*blobFileMappings, error) {
	settings := objstorageprovider.DefaultSettings(fs, dir)
	provider, err := objstorageprovider.Open(settings)
	if err != nil {
		return nil, err
	}
	mappings := &blobFileMappings{
		stderr:        stderr,
		references:    make(map[base.TableNum]*manifest.BlobReferences),
		physicalFiles: make(map[base.BlobFileID][]base.DiskFileNum),
		provider:      debugReaderProvider{objProvider: provider},
	}
	mappings.fetcher.Init(mappings, &mappings.provider, block.ReadEnv{})
	for _, fl := range manifests {
		err := func() error {
			mf, err := fs.Open(fl.path)
			if err != nil {
				return err
			}
			defer mf.Close()

			rr := record.NewReader(mf, fl.DiskFileNum)
			for {
				r, err := rr.Next()
				if err != nil {
					if err != io.EOF {
						return err
					}
					break
				}
				var ve manifest.VersionEdit
				if err = ve.Decode(r); err != nil {
					return err
				}
				for _, nf := range ve.NewTables {
					mappings.references[nf.Meta.TableNum] = &nf.Meta.BlobReferences
				}
				// Accumulate all the physical file numbers ever used for each
				// blob file ID. In the context of the debug tool, we don't want
				// to rely on reading strictly at the latest version.
				for _, nf := range ve.NewBlobFiles {
					existingFiles := mappings.physicalFiles[nf.FileID]
					if !slices.Contains(existingFiles, nf.Physical.FileNum) {
						mappings.physicalFiles[nf.FileID] = append(existingFiles, nf.Physical.FileNum)
					}
				}
			}
			return nil
		}()
		if err != nil {
			fmt.Fprintf(stderr, "error reading manifest %s: %v\n", fl.path, err)
		}
	}
	return mappings, nil
}

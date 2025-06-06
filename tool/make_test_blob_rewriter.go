// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build make_test_blob_rewriter

// Run using: go run -tags make_test_blob_rewriter ./tool/make_test_blob_rewriter.go
package main

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
)

func main() {
	dir := "tool/testdata/find-val-sep-db"
	manifestFilePath := filepath.Join(dir, "MANIFEST-000001")
	fs := vfs.Default

	d, err := pebble.Open(dir, &pebble.Options{FS: fs})
	log.Fatal(err)
	defer d.Close()

	manifestFile, err := fs.Open(manifestFilePath)
	log.Fatal(err)
	defer manifestFile.Close()

	rr := record.NewReader(manifestFile, 0 /* logNum */)
	var bve manifest.BulkVersionEdit
	bve.AllAddedTables = make(map[base.FileNum]*manifest.TableMetadata)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		log.Fatal(err)
		var ve manifest.VersionEdit
		log.Fatal(ve.Decode(r))
		log.Fatal(bve.Accumulate(&ve))
	}
	emptyVersion := manifest.NewInitialVersion(base.DefaultComparer)
	v, err := bve.Apply(emptyVersion, 32000)
	log.Fatal(err)

	var blobFileNum base.DiskFileNum
	var referencingSSTs []*manifest.TableMetadata
	for _, level := range v.Levels {
		iter := level.Iter()
		for sst := iter.First(); sst != nil; sst = iter.Next() {
			for _, ref := range sst.BlobReferences {
				blobFileNum = base.DiskFileNum(ref.FileID)
				referencingSSTs = append(referencingSSTs, sst)
			}
		}
	}

	blobFile, err := fs.Open(filepath.Join(dir, base.DiskFileNum(blobFileNum).String()))
	log.Fatal(err)
	defer blobFile.Close()

	tmpDir, err := os.MkdirTemp("", "tool/testdata/blob-rewrite-file")
	log.Fatal(err)
	defer os.RemoveAll(tmpDir)

	rewriter := pebble.NewBlobFileRewriter(fs, referencingSSTs, sstable.ReaderOptions{}, blob.FileReaderOptions{}, blobFileNum)

	outBlobFile, err := fs.Create(filepath.Join(tmpDir, "000021.blob"), vfs.WriteCategoryUnspecified)
	log.Fatal(err)
	defer outBlobFile.Close()

	writable := objstorageprovider.NewFileWritable(outBlobFile)
	writer := blob.NewFileWriter(base.DiskFileNum(1), writable, blob.FileWriterOptions{
		FlushGovernor: block.MakeFlushGovernor(base.DefaultBlockSize, base.DefaultBlockSizeThreshold, base.SizeClassAwareBlockSizeThreshold, nil),
	})

	err = rewriter.Rewrite()
	log.Fatal(err)

	_, err = writer.Close()
	log.Fatal(err)
}

// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build make_test_remotecat

// Run using: go run -tags make_test_remotecat ./tool/make_test_remotecat.go
package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider/remoteobjcat"
	"github.com/cockroachdb/pebble/v2/vfs"
)

func main() {
	dir, err := os.MkdirTemp("", "make-test-remotecat")
	if err != nil {
		log.Fatal(err)
	}
	catalog, _, err := remoteobjcat.Open(vfs.Default, dir)
	if err != nil {
		log.Fatal(err)
	}
	if err := catalog.SetCreatorID(3); err != nil {
		log.Fatal(err)
	}

	var b remoteobjcat.Batch
	b.AddObject(remoteobjcat.RemoteObjectMetadata{
		FileNum:        base.DiskFileNum(1),
		FileType:       base.FileTypeTable,
		CreatorID:      3,
		CreatorFileNum: base.DiskFileNum(1),
		CleanupMethod:  objstorage.SharedRefTracking,
		Locator:        "foo",
	})
	if err := catalog.ApplyBatch(b); err != nil {
		log.Fatal(err)
	}
	b.Reset()
	b.AddObject(remoteobjcat.RemoteObjectMetadata{
		FileNum:        base.DiskFileNum(2),
		FileType:       base.FileTypeTable,
		CreatorID:      5,
		CreatorFileNum: base.DiskFileNum(10),
		CleanupMethod:  objstorage.SharedRefTracking,
		Locator:        "foo",
	})
	b.DeleteObject(base.DiskFileNum(1))
	b.AddObject(remoteobjcat.RemoteObjectMetadata{
		FileNum:          base.DiskFileNum(3),
		FileType:         base.FileTypeTable,
		CleanupMethod:    objstorage.SharedRefTracking,
		Locator:          "bar",
		CustomObjectName: "external.sst",
	})
	if err := catalog.ApplyBatch(b); err != nil {
		log.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		log.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(dir, "REMOTE-OBJ-CATALOG-000001"))
	if err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile("tool/testdata/REMOTE-OBJ-CATALOG", contents, 0666); err != nil {
		log.Fatal(err)
	}
}

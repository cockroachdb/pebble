// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build make_test_scan_virtual_backing

// Run using: go run -tags make_test_scan_virtual_backing ./tool/make_test_scan_virtual_backing.go
//
// It builds a store in which an excise has left two virtual tables sharing a
// single physical backing sstable, and in which the manifests that originally
// listed that backing as a new table have been rotated away, so that the only
// surviving manifest references the backing via add-backing alone. The backing
// sstable holds blob references, so scanning it with `sstable scan
// --load-blobs-from` exercises the nil-reference path from issue #718.
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

const minSizeForValSep = 3

func main() {
	const dir = "tool/testdata/scan-virtual-backing-db"

	fs := vfs.Default
	if err := fs.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
	opts := &pebble.Options{
		FS:        fs,
		KeySchema: cockroachkvs.KeySchema.Name,
		KeySchemas: sstable.KeySchemas{
			cockroachkvs.KeySchema.Name: &cockroachkvs.KeySchema,
		},
		FormatMajorVersion:          pebble.FormatValueSeparation,
		DisableAutomaticCompactions: true,
		// Force a manifest rotation on every edit so that, by the time the
		// excise lands, the physical backing's new-table record is in an older
		// manifest that we can delete below.
		MaxManifestFileSize: 1,
	}
	opts.ValueSeparationPolicy = func() pebble.ValueSeparationPolicy {
		return pebble.ValueSeparationPolicy{
			Enabled:                true,
			MinimumSize:            minSizeForValSep,
			MinimumMVCCGarbageSize: 1,
			MaxBlobReferenceDepth:  10,
		}
	}
	d, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}

	set := func(k, v string) {
		if err := d.Set(cockroachkvs.ParseFormattedKey(k), []byte(v), nil); err != nil {
			log.Fatal(err)
		}
	}
	excise := func(start, end string) {
		if err := d.Excise(context.Background(), pebble.KeyRange{
			Start: cockroachkvs.ParseFormattedKey(start),
			End:   cockroachkvs.ParseFormattedKey(end),
		}); err != nil {
			log.Fatal(err)
		}
	}
	// Flush three separated values into a single physical sstable backed by a
	// blob file.
	set("key0", "value0")
	set("key1", "value1")
	set("key2", "value2")
	if err := d.Flush(); err != nil {
		log.Fatal(err)
	}
	// Excise the middle key so that a left (key0) and a right (key2) virtual
	// table remain, both backed by the original physical sstable.
	excise("key1", "key1\x00")
	// Excise the right virtual table. This second edit forces another rotation
	// (MaxManifestFileSize=1) whose fresh manifest begins with a snapshot of the
	// version left by the first excise, in which the backing appears only as
	// add-backing and never as a new-table.
	excise("key2", "key2\x00")
	if err := d.Close(); err != nil {
		log.Fatal(err)
	}

	stripToMinimalFixture(fs, dir)
}

// stripToMinimalFixture reduces dir to the files `sstable scan
// --load-blobs-from` actually reads: the newest manifest (so the backing is
// known only via add-backing, as after old manifests rotate away), and the
// sstable and blob files. Everything else -- WALs, OPTIONS, the LOCK and the
// markers -- is removed so the checked-in fixture stays small.
func stripToMinimalFixture(fs vfs.FS, dir string) {
	ls, err := fs.List(dir)
	if err != nil {
		log.Fatal(err)
	}
	var manifests []string
	for _, name := range ls {
		if strings.HasPrefix(name, "MANIFEST-") {
			manifests = append(manifests, name)
		}
	}
	if len(manifests) == 0 {
		log.Fatal("no manifests found")
	}
	// Manifest file numbers are zero-padded to a fixed width, so the
	// lexically-greatest name is the highest-numbered (newest) manifest.
	newestManifest := slices.Max(manifests)
	for _, name := range ls {
		keep := name == newestManifest ||
			strings.HasSuffix(name, ".sst") ||
			strings.HasSuffix(name, ".blob")
		if keep {
			continue
		}
		if err := fs.Remove(dir + "/" + name); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("kept manifest %s\n", newestManifest)
}

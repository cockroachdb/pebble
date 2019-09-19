// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"testing"

	"github.com/cockroachdb/pebble/vfs"
)

func TestParseFilename(t *testing.T) {
	testCases := map[string]bool{
		"000000.log":          true,
		"000000.log.zip":      false,
		"000000..log":         false,
		"a000000.log":         false,
		"abcdef.log":          false,
		"000001ldb":           false,
		"000001.sst":          true,
		"CURRENT":             true,
		"CURRaNT":             false,
		"LOCK":                true,
		"xLOCK":               false,
		"x.LOCK":              false,
		"MANIFEST":            false,
		"MANIFEST123456":      false,
		"MANIFEST-":           false,
		"MANIFEST-123456":     true,
		"MANIFEST-123456.doc": false,
		"OPTIONS":             false,
		"OPTIONS123456":       false,
		"OPTIONS-":            false,
		"OPTIONS-123456":      true,
		"OPTIONS-123456.doc":  false,
	}
	fs := vfs.NewMem()
	for tc, want := range testCases {
		_, _, got := ParseFilename(fs, fs.PathJoin("foo", tc))
		if got != want {
			t.Errorf("%q: got %v, want %v", tc, got, want)
		}
	}
}

func TestFilenameRoundTrip(t *testing.T) {
	testCases := map[FileType]bool{
		// CURRENT and LOCK files aren't numbered.
		FileTypeCurrent: false,
		FileTypeLock:    false,
		// The remaining file types are numbered.
		FileTypeLog:      true,
		FileTypeManifest: true,
		FileTypeTable:    true,
		FileTypeOptions:  true,
	}
	fs := vfs.NewMem()
	for fileType, numbered := range testCases {
		fileNums := []uint64{0}
		if numbered {
			fileNums = []uint64{0, 1, 2, 3, 10, 42, 99, 1001}
		}
		for _, fileNum := range fileNums {
			filename := MakeFilename(fs, "foo", fileType, fileNum)
			gotFT, gotFN, gotOK := ParseFilename(fs, filename)
			if !gotOK {
				t.Errorf("could not parse %q", filename)
				continue
			}
			if gotFT != fileType || gotFN != fileNum {
				t.Errorf("filename=%q: got %v, %v, want %v, %v", filename, gotFT, gotFN, fileType, fileNum)
				continue
			}
		}
	}
}

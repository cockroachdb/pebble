// Copyright 2013 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"path/filepath"
	"testing"
)

func TestParseDBFilename(t *testing.T) {
	testCases := map[string]bool{
		"000000.log":          true,
		"000000.log.zip":      false,
		"000000..log":         false,
		"a000000.log":         false,
		"abcdef.log":          false,
		"000001ldb":           false,
		"000001.ldb":          true,
		"000002.sst":          true,
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
	}
	for tc, want := range testCases {
		_, _, got := parseDBFilename(filepath.Join("foo", tc))
		if got != want {
			t.Errorf("%q: got %v, want %v", tc, got, want)
		}
	}
}

func TestFilenameRoundTrip(t *testing.T) {
	testCases := map[fileType]bool{
		// CURRENT and LOCK files aren't numbered.
		fileTypeCurrent: false,
		fileTypeLock:    false,
		// The remaining file types are numbered.
		fileTypeLog:               true,
		fileTypeManifest:          true,
		fileTypeOldFashionedTable: true,
		fileTypeTable:             true,
	}
	for fileType, numbered := range testCases {
		fileNums := []uint64{0}
		if numbered {
			fileNums = []uint64{0, 1, 2, 3, 10, 42, 99, 1001}
		}
		for _, fileNum := range fileNums {
			filename := dbFilename("foo", fileType, fileNum)
			gotFT, gotFN, gotOK := parseDBFilename(filename)
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

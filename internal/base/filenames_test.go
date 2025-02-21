// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestParseFilename(t *testing.T) {
	// NB: log files are not handled by ParseFilename, so valid log filenames
	// appear here with a false value. See the wal/ package.
	testCases := map[string]bool{
		"000000.log":             false,
		"000000.log.zip":         false,
		"000000..log":            false,
		"000001-002.log":         false,
		"a000000.log":            false,
		"abcdef.log":             false,
		"000001ldb":              false,
		"000001.sst":             true,
		"CURRENT":                false,
		"LOCK":                   true,
		"xLOCK":                  false,
		"x.LOCK":                 false,
		"MANIFEST":               false,
		"MANIFEST123456":         false,
		"MANIFEST-":              false,
		"MANIFEST-123456":        true,
		"MANIFEST-123456.doc":    false,
		"OPTIONS":                false,
		"OPTIONS123456":          false,
		"OPTIONS-":               false,
		"OPTIONS-123456":         true,
		"OPTIONS-123456.doc":     false,
		"CURRENT.123456":         false,
		"CURRENT.dbtmp":          false,
		"CURRENT.123456.dbtmp":   true,
		"temporary.123456.dbtmp": true,
		"foo.blob":               false,
		"000000.blob":            true,
		"000001.blob":            true,
		"935203523.blob":         true,
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
		// LOCK files aren't numbered.
		FileTypeLock: false,
		// The remaining file types are numbered.
		FileTypeManifest: true,
		FileTypeTable:    true,
		FileTypeOptions:  true,
		FileTypeOldTemp:  true,
		FileTypeTemp:     true,
		FileTypeBlob:     true,
		// NB: Log filenames are created and parsed elsewhere in the wal/
		// package.
		// FileTypeLog:      true,
	}
	fs := vfs.NewMem()
	for fileType, numbered := range testCases {
		fileNums := []DiskFileNum{0}
		if numbered {
			fileNums = []DiskFileNum{0, 1, 2, 3, 10, 42, 99, 1001}
		}
		for _, fileNum := range fileNums {
			filename := MakeFilepath(fs, "foo", fileType, fileNum)
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

type bufferFataler struct {
	buf bytes.Buffer
}

func (b *bufferFataler) Fatalf(msg string, args ...interface{}) {
	fmt.Fprintf(&b.buf, msg, args...)
}

func TestMustExist(t *testing.T) {
	err := os.ErrNotExist
	fs := vfs.Default
	var buf bufferFataler
	filename := fs.PathJoin("..", "..", "testdata", "db-stage-4", "000000.sst")

	MustExist(fs, filename, &buf, err)

	expected := fmt.Sprintf(`file does not exist
(1) filename: %s; directory contains 9 files, 3 unknown, 1 tables, 1 logs, 2 manifests
Wraps: (2) file does not exist
Error types: (1) *hintdetail.withDetail (2) *errors.errorString`, filename)
	require.Equal(t, expected, buf.buf.String())
}

func TestRedactFileNum(t *testing.T) {
	// Ensure that redaction never redacts file numbers.
	require.Equal(t, redact.RedactableString("000005"), redact.Sprint(FileNum(5)))
	require.Equal(t, redact.RedactableString("000005"), redact.Sprint(DiskFileNum(5)))
}

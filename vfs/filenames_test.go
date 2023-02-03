// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestParseFilepath(t *testing.T) {
	testCases := map[string]bool{
		"000000.log":             true,
		"000000.log.zip":         false,
		"000000..log":            false,
		"a000000.log":            false,
		"abcdef.log":             false,
		"000001ldb":              false,
		"000001.sst":             true,
		"CURRENT":                true,
		"CURRaNT":                false,
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
	}
	fs := NewMem()
	for tc, want := range testCases {
		_, _, got := ParseFilepath(fs, fs.PathJoin("foo", tc))
		if got != want {
			t.Errorf("%q: got %v, want %v", tc, got, want)
		}
	}
}

func TestFilepathRoundTrip(t *testing.T) {
	testCases := map[base.FileType]bool{
		// CURRENT and LOCK files aren't numbered.
		base.FileTypeCurrent: false,
		base.FileTypeLock:    false,
		// The remaining file types are numbered.
		base.FileTypeLog:      true,
		base.FileTypeManifest: true,
		base.FileTypeTable:    true,
		base.FileTypeOptions:  true,
		base.FileTypeOldTemp:  true,
		base.FileTypeTemp:     true,
	}
	fs := NewMem()
	for fileType, numbered := range testCases {
		fileNums := []base.FileNum{0}
		if numbered {
			fileNums = []base.FileNum{0, 1, 2, 3, 10, 42, 99, 1001}
		}
		for _, fileNum := range fileNums {
			filename := MakeFilepath(fs, "foo", fileType, fileNum)
			gotFT, gotFN, gotOK := ParseFilepath(fs, filename)
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
	fs := Default
	var buf bufferFataler
	filename := fs.PathJoin("..", "testdata", "db-stage-4", "000000.sst")

	MustExist(fs, filename, &buf, err)
	require.Equal(t, `000000.sst:
file does not exist
directory contains 10 files, 3 unknown, 1 tables, 1 logs, 1 manifests`, buf.buf.String())
}

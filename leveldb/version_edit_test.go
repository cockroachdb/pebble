// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/record"
)

func TestVersionEditDecode(t *testing.T) {
	testCases := []struct {
		filename     string
		encodedEdits []string
		edits        []versionEdit
	}{
		// db-stage-1 and db-stage-2 have the same manifest.
		{
			filename: "db-stage-1/MANIFEST-000002",
			encodedEdits: []string{
				"\x01\x1aleveldb.BytewiseComparator",
				"\x02\x03\x09\x00\x03\x04\x04\x00",
			},
			edits: []versionEdit{
				{
					comparatorName: "leveldb.BytewiseComparator",
				},
				{
					logNumber:      3,
					prevLogNumber:  0,
					nextFileNumber: 4,
					lastSequence:   0,
				},
			},
		},
		// db-stage-3 and db-stage-4 have the same manifest.
		{
			filename: "db-stage-3/MANIFEST-000004",
			encodedEdits: []string{
				"\x01\x1aleveldb.BytewiseComparator",
				"\x02\x06\x09\x00\x03\x07\x04\x05\x07\x00\x05\xa5\x01" +
					"\x0bbar\x00\x05\x00\x00\x00\x00\x00\x00" +
					"\x0bfoo\x01\x01\x00\x00\x00\x00\x00\x00",
			},
			edits: []versionEdit{
				{
					comparatorName: "leveldb.BytewiseComparator",
				},
				{
					logNumber:      6,
					prevLogNumber:  0,
					nextFileNumber: 7,
					lastSequence:   5,
					newFiles: []newFileEntry{
						{
							level: 0,
							meta: fileMetadata{
								fileNum:  5,
								size:     165,
								smallest: []byte("bar\x00\x05\x00\x00\x00\x00\x00\x00"),
								largest:  []byte("foo\x01\x01\x00\x00\x00\x00\x00\x00"),
							},
						},
					},
				},
			},
		},
	}

loop:
	for _, tc := range testCases {
		f, err := os.Open("../testdata/" + tc.filename)
		if err != nil {
			t.Errorf("filename=%q: open error: %v", tc.filename, err)
			continue
		}
		defer f.Close()
		i, r := 0, record.NewReader(f)
		for {
			rr, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("filename=%q i=%d: record reader error: %v", tc.filename, i, err)
				continue loop
			}
			if i >= len(tc.edits) {
				t.Errorf("filename=%q i=%d: too many version edits", tc.filename, i+1)
				continue loop
			}

			encodedEdit, err := ioutil.ReadAll(rr)
			if err != nil {
				t.Errorf("filename=%q i=%d: read error: %v", tc.filename, i, err)
				continue loop
			}
			if s := string(encodedEdit); s != tc.encodedEdits[i] {
				t.Errorf("filename=%q i=%d: got encoded %q, want %q", tc.filename, i, s, tc.encodedEdits[i])
				continue loop
			}

			var edit versionEdit
			err = edit.decode(bytes.NewReader(encodedEdit))
			if err != nil {
				t.Errorf("filename=%q i=%d: decode error: %v", tc.filename, i, err)
				continue loop
			}
			if !reflect.DeepEqual(edit, tc.edits[i]) {
				t.Errorf("filename=%q i=%d:\n\tgot  %#v\n\twant %#v", tc.filename, i, edit, tc.edits[i])
				continue loop
			}
			i++
		}
		if i != len(tc.edits) {
			t.Errorf("filename=%q: got %d edits, want %d", tc.filename, i, len(tc.edits))
			continue
		}
	}
}

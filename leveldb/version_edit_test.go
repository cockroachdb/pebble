// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/record"
)

func checkRoundTrip(e0 versionEdit) error {
	var e1 versionEdit
	buf := new(bytes.Buffer)
	if err := e0.encode(buf); err != nil {
		return fmt.Errorf("encode: %v", err)
	}
	if err := e1.decode(buf); err != nil {
		return fmt.Errorf("decode: %v", err)
	}
	if !reflect.DeepEqual(e1, e0) {
		return fmt.Errorf("\n\tgot  %#v\n\twant %#v", e1, e0)
	}
	return nil
}

func TestVersionEditRoundTrip(t *testing.T) {
	testCases := []versionEdit{
		// An empty version edit.
		{},
		// A complete version edit.
		{
			comparatorName: "11",
			logNumber:      22,
			prevLogNumber:  33,
			nextFileNumber: 44,
			lastSequence:   55,
			compactPointers: []compactPointerEntry{
				{
					level: 0,
					key:   internalKey("600"),
				},
				{
					level: 1,
					key:   internalKey("601"),
				},
				{
					level: 2,
					key:   internalKey("602"),
				},
			},
			deletedFiles: map[deletedFileEntry]bool{
				deletedFileEntry{
					level:   3,
					fileNum: 703,
				}: true,
				deletedFileEntry{
					level:   4,
					fileNum: 704,
				}: true,
			},
			newFiles: []newFileEntry{
				{
					level: 5,
					meta: fileMetadata{
						fileNum:  805,
						size:     8050,
						smallest: internalKey("abc\x00\x01\x02\x03\x04\x05\x06\x07"),
						largest:  internalKey("xyz\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9"),
					},
				},
				{
					level: 6,
					meta: fileMetadata{
						fileNum:  806,
						size:     8060,
						smallest: internalKey("A\x00\x01\x02\x03\x04\x05\x06\x07"),
						largest:  internalKey("Z\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9"),
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		if err := checkRoundTrip(tc); err != nil {
			t.Error(err)
		}
	}
}

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
								smallest: internalKey("bar\x00\x05\x00\x00\x00\x00\x00\x00"),
								largest:  internalKey("foo\x01\x01\x00\x00\x00\x00\x00\x00"),
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
				t.Errorf("filename=%q i=%d: decode\n\tgot  %#v\n\twant %#v", tc.filename, i, edit, tc.edits[i])
				continue loop
			}
			if err := checkRoundTrip(edit); err != nil {
				t.Errorf("filename=%q i=%d: round trip: %v", tc.filename, i, err)
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

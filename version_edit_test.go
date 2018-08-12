// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/kr/pretty"
	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/record"
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
						smallest: db.DecodeInternalKey([]byte("abc\x00\x01\x02\x03\x04\x05\x06\x07")),
						largest:  db.DecodeInternalKey([]byte("xyz\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
					},
				},
				{
					level: 6,
					meta: fileMetadata{
						fileNum:             806,
						size:                8060,
						smallest:            db.DecodeInternalKey([]byte("A\x00\x01\x02\x03\x04\x05\x06\x07")),
						largest:             db.DecodeInternalKey([]byte("Z\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
						markedForCompaction: true,
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
			filename: "db-stage-1/MANIFEST-000001",
			encodedEdits: []string{
				"\x02\x00\x03\x02\x04\x00",
			},
			edits: []versionEdit{
				{
					nextFileNumber: 2,
				},
			},
		},
		// db-stage-3 and db-stage-4 have the same manifest.
		{
			filename: "db-stage-3/MANIFEST-000005",
			encodedEdits: []string{
				"\x01\x1aleveldb.BytewiseComparator",
				"\x02\x00",
				"\x02\x04\t\x00\x03\x06\x04\x05d\x00\x04\xda\a\vbar" +
					"\x00\x05\x00\x00\x00\x00\x00\x00\vfoo\x01\x04\x00" +
					"\x00\x00\x00\x00\x00\x03\x05",
			},
			edits: []versionEdit{
				{
					comparatorName: "leveldb.BytewiseComparator",
				},
				{},
				{
					logNumber:      4,
					prevLogNumber:  0,
					nextFileNumber: 6,
					lastSequence:   5,
					newFiles: []newFileEntry{
						{
							level: 0,
							meta: fileMetadata{
								fileNum:        4,
								size:           986,
								smallest:       db.MakeInternalKey([]byte("bar"), 5, db.InternalKeyKindDelete),
								largest:        db.MakeInternalKey([]byte("foo"), 4, db.InternalKeyKindSet),
								smallestSeqNum: 3,
								largestSeqNum:  5,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			f, err := os.Open("testdata/" + tc.filename)
			if err != nil {
				t.Fatalf("filename=%q: open error: %v", tc.filename, err)
			}
			defer f.Close()
			i, r := 0, record.NewReader(f)
			for {
				rr, err := r.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("filename=%q i=%d: record reader error: %v", tc.filename, i, err)
				}
				if i >= len(tc.edits) {
					t.Fatalf("filename=%q i=%d: too many version edits", tc.filename, i+1)
				}

				encodedEdit, err := ioutil.ReadAll(rr)
				if err != nil {
					t.Fatalf("filename=%q i=%d: read error: %v", tc.filename, i, err)
				}
				if s := string(encodedEdit); s != tc.encodedEdits[i] {
					t.Fatalf("filename=%q i=%d: got encoded %q, want %q", tc.filename, i, s, tc.encodedEdits[i])
				}

				var edit versionEdit
				err = edit.decode(bytes.NewReader(encodedEdit))
				if err != nil {
					t.Fatalf("filename=%q i=%d: decode error: %v", tc.filename, i, err)
				}
				if !reflect.DeepEqual(edit, tc.edits[i]) {
					t.Fatalf("filename=%q i=%d: decode\n\tgot  %#v\n\twant %#v\n%s", tc.filename, i, edit, tc.edits[i],
						strings.Join(pretty.Diff(edit, tc.edits[i]), "\n"))
				}
				if err := checkRoundTrip(edit); err != nil {
					t.Fatalf("filename=%q i=%d: round trip: %v", tc.filename, i, err)
				}

				i++
			}
			if i != len(tc.edits) {
				t.Fatalf("filename=%q: got %d edits, want %d", tc.filename, i, len(tc.edits))
			}
		})
	}
}

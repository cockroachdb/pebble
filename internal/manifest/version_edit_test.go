// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/kr/pretty"
)

func checkRoundTrip(e0 VersionEdit) error {
	var e1 VersionEdit
	buf := new(bytes.Buffer)
	if err := e0.Encode(buf); err != nil {
		return fmt.Errorf("encode: %v", err)
	}
	if err := e1.Decode(buf); err != nil {
		return fmt.Errorf("decode: %v", err)
	}
	if !reflect.DeepEqual(e1, e0) {
		return fmt.Errorf("\n\tgot  %#v\n\twant %#v", e1, e0)
	}
	return nil
}

func TestVersionEditRoundTrip(t *testing.T) {
	testCases := []VersionEdit{
		// An empty version edit.
		{},
		// A complete version edit.
		{
			ComparerName: "11",
			LogNum:       22,
			PrevLogNum:   33,
			NextFileNum:  44,
			LastSeqNum:   55,
			DeletedFiles: map[DeletedFileEntry]bool{
				DeletedFileEntry{
					Level:   3,
					FileNum: 703,
				}: true,
				DeletedFileEntry{
					Level:   4,
					FileNum: 704,
				}: true,
			},
			NewFiles: []NewFileEntry{
				{
					Level: 5,
					Meta: FileMetadata{
						FileNum:  805,
						Size:     8050,
						Smallest: base.DecodeInternalKey([]byte("abc\x00\x01\x02\x03\x04\x05\x06\x07")),
						Largest:  base.DecodeInternalKey([]byte("xyz\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
					},
				},
				{
					Level: 6,
					Meta: FileMetadata{
						FileNum:             806,
						Size:                8060,
						Smallest:            base.DecodeInternalKey([]byte("A\x00\x01\x02\x03\x04\x05\x06\x07")),
						Largest:             base.DecodeInternalKey([]byte("Z\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
						SmallestSeqNum:      3,
						LargestSeqNum:       5,
						MarkedForCompaction: true,
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
		edits        []VersionEdit
	}{
		// db-stage-1 and db-stage-2 have the same manifest.
		{
			filename: "db-stage-1/MANIFEST-000001",
			encodedEdits: []string{
				"\x02\x00\x03\x02\x04\x00",
			},
			edits: []VersionEdit{
				{
					NextFileNum: 2,
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
			edits: []VersionEdit{
				{
					ComparerName: "leveldb.BytewiseComparator",
				},
				{},
				{
					LogNum:      4,
					PrevLogNum:  0,
					NextFileNum: 6,
					LastSeqNum:  5,
					NewFiles: []NewFileEntry{
						{
							Level: 0,
							Meta: FileMetadata{
								FileNum:        4,
								Size:           986,
								Smallest:       base.MakeInternalKey([]byte("bar"), 5, base.InternalKeyKindDelete),
								Largest:        base.MakeInternalKey([]byte("foo"), 4, base.InternalKeyKindSet),
								SmallestSeqNum: 3,
								LargestSeqNum:  5,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			f, err := os.Open("../../testdata/" + tc.filename)
			if err != nil {
				t.Fatalf("filename=%q: open error: %v", tc.filename, err)
			}
			defer f.Close()
			i, r := 0, record.NewReader(f, 0 /* logNum */)
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

				var edit VersionEdit
				err = edit.Decode(bytes.NewReader(encodedEdit))
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

func TestVersionEditEncodeLastSeqNum(t *testing.T) {
	testCases := []struct {
		edit    VersionEdit
		encoded string
	}{
		// If ComparerName is unset, LastSeqNum is only encoded if non-zero.
		{VersionEdit{LastSeqNum: 0}, ""},
		{VersionEdit{LastSeqNum: 1}, "\x04\x01"},
		// For compatibility with RocksDB, if ComparerName is set we always encode
		// LastSeqNum.
		{VersionEdit{ComparerName: "foo", LastSeqNum: 0}, "\x01\x03\x66\x6f\x6f\x04\x00"},
		{VersionEdit{ComparerName: "foo", LastSeqNum: 1}, "\x01\x03\x66\x6f\x6f\x04\x01"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var buf bytes.Buffer
			if err := c.edit.Encode(&buf); err != nil {
				t.Fatal(err)
			}
			if result := buf.String(); c.encoded != result {
				t.Fatalf("expected %x, but found %x", c.encoded, result)
			}

			if c.edit.ComparerName != "" {
				// Manually decode the version edit so that we can verify the contents
				// even if the LastSeqNum decodes to 0.
				d := versionEditDecoder{strings.NewReader(c.encoded)}

				// Decode ComparerName.
				tag, err := d.readUvarint()
				if err != nil {
					t.Fatal()
				}
				if tag != tagComparator {
					t.Fatalf("expected %d, but found %d", tagComparator, tag)
				}
				s, err := d.readBytes()
				if err != nil {
					t.Fatal(err)
				}
				if c.edit.ComparerName != string(s) {
					t.Fatalf("expected %q, but found %q", c.edit.ComparerName, s)
				}

				// Decode LastSeqNum.
				tag, err = d.readUvarint()
				if err != nil {
					t.Fatal(err)
				}
				if tag != tagLastSequence {
					t.Fatalf("expected %d, but found %d", tagLastSequence, tag)
				}
				val, err := d.readUvarint()
				if err != nil {
					t.Fatal(err)
				}
				if c.edit.LastSeqNum != val {
					t.Fatalf("expected %d, but found %d", c.edit.LastSeqNum, val)
				}
			}
		})
	}
}

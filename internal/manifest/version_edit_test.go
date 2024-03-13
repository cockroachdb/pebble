// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/record"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func checkRoundTrip(e0 VersionEdit) error {
	var e1 VersionEdit
	buf := new(bytes.Buffer)
	if err := e0.Encode(buf); err != nil {
		return errors.Wrap(err, "encode")
	}
	if err := e1.Decode(buf); err != nil {
		return errors.Wrap(err, "decode")
	}
	if diff := pretty.Diff(e0, e1); diff != nil {
		return errors.Errorf("%s", strings.Join(diff, "\n"))
	}
	return nil
}

// Version edits with virtual sstables will not be the same after a round trip
// as the Decode function will not set the FileBacking for a virtual sstable.
// We test round trip + bve accumulation here, after which the virtual sstable
// FileBacking should be set.
func TestVERoundTripAndAccumulate(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	m1 := (&FileMetadata{
		FileNum:         810,
		Size:            8090,
		CreationTime:    809060,
		SmallestSeqNum:  9,
		LargestSeqNum:   11,
		SyntheticPrefix: []byte("after"),
		SyntheticSuffix: []byte("foo"),
	}).ExtendPointKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("a"), 0, base.InternalKeyKindSet),
		base.MakeInternalKey([]byte("m"), 0, base.InternalKeyKindSet),
	).ExtendRangeKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("l"), 0, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("z")),
	)
	m1.InitPhysicalBacking()

	m2 := (&FileMetadata{
		FileNum:        812,
		Size:           8090,
		CreationTime:   809060,
		SmallestSeqNum: 9,
		LargestSeqNum:  11,
		Virtual:        true,
		FileBacking:    m1.FileBacking,
	}).ExtendPointKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("a"), 0, base.InternalKeyKindSet),
		base.MakeInternalKey([]byte("c"), 0, base.InternalKeyKindSet),
	)

	ve1 := VersionEdit{
		ComparerName:         "11",
		MinUnflushedLogNum:   22,
		ObsoletePrevLogNum:   33,
		NextFileNum:          44,
		LastSeqNum:           55,
		CreatedBackingTables: []*FileBacking{m1.FileBacking},
		NewFiles: []NewFileEntry{
			{
				Level: 4,
				Meta:  m2,
				// Only set for the test.
				BackingFileNum: m2.FileBacking.DiskFileNum,
			},
		},
	}
	var err error
	buf := new(bytes.Buffer)
	if err = ve1.Encode(buf); err != nil {
		t.Error(err)
	}
	var ve2 VersionEdit
	if err = ve2.Decode(buf); err != nil {
		t.Error(err)
	}
	// Perform accumulation to set the FileBacking on the files in the Decoded
	// version edit.
	var bve BulkVersionEdit
	require.NoError(t, bve.Accumulate(&ve2))
	if diff := pretty.Diff(ve1, ve2); diff != nil {
		t.Error(errors.Errorf("%s", strings.Join(diff, "\n")))
	}
}

func TestVersionEditRoundTrip(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	m1 := (&FileMetadata{
		FileNum:      805,
		Size:         8050,
		CreationTime: 805030,
	}).ExtendPointKeyBounds(
		cmp,
		base.DecodeInternalKey([]byte("abc\x00\x01\x02\x03\x04\x05\x06\x07")),
		base.DecodeInternalKey([]byte("xyz\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
	)
	m1.InitPhysicalBacking()

	m2 := (&FileMetadata{
		FileNum:             806,
		Size:                8060,
		CreationTime:        806040,
		SmallestSeqNum:      3,
		LargestSeqNum:       5,
		MarkedForCompaction: true,
		SyntheticSuffix:     []byte("foo"),
	}).ExtendPointKeyBounds(
		cmp,
		base.DecodeInternalKey([]byte("A\x00\x01\x02\x03\x04\x05\x06\x07")),
		base.DecodeInternalKey([]byte("Z\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
	)
	m2.InitPhysicalBacking()

	m3 := (&FileMetadata{
		FileNum:      807,
		Size:         8070,
		CreationTime: 807050,
	}).ExtendRangeKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("aaa"), 0, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("zzz")),
	)
	m3.InitPhysicalBacking()

	m4 := (&FileMetadata{
		FileNum:        809,
		Size:           8090,
		CreationTime:   809060,
		SmallestSeqNum: 9,
		LargestSeqNum:  11,
	}).ExtendPointKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("a"), 0, base.InternalKeyKindSet),
		base.MakeInternalKey([]byte("m"), 0, base.InternalKeyKindSet),
	).ExtendRangeKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("l"), 0, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("z")),
	)
	m4.InitPhysicalBacking()

	m5 := (&FileMetadata{
		FileNum:        810,
		Size:           8090,
		CreationTime:   809060,
		SmallestSeqNum: 9,
		LargestSeqNum:  11,
	}).ExtendPointKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("a"), 0, base.InternalKeyKindSet),
		base.MakeInternalKey([]byte("m"), 0, base.InternalKeyKindSet),
	).ExtendRangeKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("l"), 0, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("z")),
	)
	m5.InitPhysicalBacking()

	m6 := (&FileMetadata{
		FileNum:        811,
		Size:           8090,
		CreationTime:   809060,
		SmallestSeqNum: 9,
		LargestSeqNum:  11,
	}).ExtendPointKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("a"), 0, base.InternalKeyKindSet),
		base.MakeInternalKey([]byte("m"), 0, base.InternalKeyKindSet),
	).ExtendRangeKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("l"), 0, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("z")),
	)
	m6.InitPhysicalBacking()

	testCases := []VersionEdit{
		// An empty version edit.
		{},
		// A complete version edit.
		{
			ComparerName:         "11",
			MinUnflushedLogNum:   22,
			ObsoletePrevLogNum:   33,
			NextFileNum:          44,
			LastSeqNum:           55,
			RemovedBackingTables: []base.DiskFileNum{10, 11},
			CreatedBackingTables: []*FileBacking{m5.FileBacking, m6.FileBacking},
			DeletedFiles: map[DeletedFileEntry]*FileMetadata{
				{
					Level:   3,
					FileNum: 703,
				}: nil,
				{
					Level:   4,
					FileNum: 704,
				}: nil,
			},
			NewFiles: []NewFileEntry{
				{
					Level: 4,
					Meta:  m1,
				},
				{
					Level: 5,
					Meta:  m2,
				},
				{
					Level: 6,
					Meta:  m3,
				},
				{
					Level: 6,
					Meta:  m4,
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
	// TODO(radu): these should be datadriven tests that output the encoded and
	// decoded edits.
	cmp := base.DefaultComparer.Compare
	m := (&FileMetadata{
		FileNum:        4,
		Size:           709,
		SmallestSeqNum: 12,
		LargestSeqNum:  14,
		CreationTime:   1701712644,
	}).ExtendPointKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("bar"), 14, base.InternalKeyKindDelete),
		base.MakeInternalKey([]byte("foo"), 13, base.InternalKeyKindSet),
	)
	m.InitPhysicalBacking()

	testCases := []struct {
		filename     string
		encodedEdits []string
		edits        []VersionEdit
	}{
		// db-stage-1 and db-stage-2 have the same manifest.
		{
			filename: "db-stage-1/MANIFEST-000001",
			encodedEdits: []string{
				"\x01\x1aleveldb.BytewiseComparator\x03\x02\x04\x00",
				"\x02\x02\x03\x03\x04\t",
			},
			edits: []VersionEdit{
				{
					ComparerName: "leveldb.BytewiseComparator",
					NextFileNum:  2,
				},
				{
					MinUnflushedLogNum: 0x2,
					NextFileNum:        0x3,
					LastSeqNum:         0x9,
				},
			},
		},
		// db-stage-3 and db-stage-4 have the same manifest.
		{
			filename: "db-stage-3/MANIFEST-000006",
			encodedEdits: []string{
				"\x01\x1aleveldb.BytewiseComparator\x02\x02\x03\a\x04\x00",
				"\x02\x05\x03\x06\x04\x0eg\x00\x04\xc5\x05\vbar\x00\x0e\x00\x00\x00\x00\x00\x00\vfoo\x01\r\x00\x00\x00\x00\x00\x00\f\x0e\x06\x05\x84\xa6\xb8\xab\x06\x01",
			},
			edits: []VersionEdit{
				{
					ComparerName:       "leveldb.BytewiseComparator",
					MinUnflushedLogNum: 0x2,
					NextFileNum:        0x7,
				},
				{
					MinUnflushedLogNum: 0x5,
					NextFileNum:        0x6,
					LastSeqNum:         0xe,
					NewFiles: []NewFileEntry{
						{
							Level: 0,
							Meta:  m,
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

				encodedEdit, err := io.ReadAll(rr)
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
			require.NoError(t, c.edit.Encode(&buf))
			if result := buf.String(); c.encoded != result {
				t.Fatalf("expected %x, but found %x", c.encoded, result)
			}

			if c.edit.ComparerName != "" {
				// Manually decode the version edit so that we can verify the contents
				// even if the LastSeqNum decodes to 0.
				d := versionEditDecoder{strings.NewReader(c.encoded)}

				// Decode ComparerName.
				tag, err := d.readUvarint()
				require.NoError(t, err)
				if tag != tagComparator {
					t.Fatalf("expected %d, but found %d", tagComparator, tag)
				}
				s, err := d.readBytes()
				require.NoError(t, err)
				if c.edit.ComparerName != string(s) {
					t.Fatalf("expected %q, but found %q", c.edit.ComparerName, s)
				}

				// Decode LastSeqNum.
				tag, err = d.readUvarint()
				require.NoError(t, err)
				if tag != tagLastSequence {
					t.Fatalf("expected %d, but found %d", tagLastSequence, tag)
				}
				val, err := d.readUvarint()
				require.NoError(t, err)
				if c.edit.LastSeqNum != val {
					t.Fatalf("expected %d, but found %d", c.edit.LastSeqNum, val)
				}
			}
		})
	}
}

func TestVersionEditApply(t *testing.T) {
	const flushSplitBytes = 10 * 1024 * 1024
	const readCompactionRate = 32000

	versions := make(map[string]*Version)
	datadriven.RunTest(t, "testdata/version_edit_apply",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				// Define a version.
				name := d.CmdArgs[0].String()
				v, err := ParseVersionDebug(base.DefaultComparer, flushSplitBytes, d.Input)
				if err != nil {
					d.Fatalf(t, "%v", err)
				}
				versions[name] = v
				return v.DebugString()

			case "apply":
				// Apply an edit to the given version.
				name := d.CmdArgs[0].String()
				v := versions[name]
				if v == nil {
					d.Fatalf(t, "unknown version %q", name)
					return ""
				}

				bve := BulkVersionEdit{}
				bve.AddedByFileNum = make(map[base.FileNum]*FileMetadata)
				for _, l := range v.Levels {
					it := l.Iter()
					for f := it.First(); f != nil; f = it.Next() {
						bve.AddedByFileNum[f.FileNum] = f
					}
				}

				lines := strings.Split(d.Input, "\n")
				for len(lines) > 0 {
					// We allow multiple version edits with a special separator.
					var linesForOneVE []string
					if nextSplit := slices.Index(lines, "new version edit"); nextSplit != -1 {
						linesForOneVE = lines[:nextSplit]
						lines = lines[nextSplit+1:]
					} else {
						linesForOneVE = lines
						lines = nil
					}
					ve, err := ParseVersionEditDebug(strings.Join(linesForOneVE, "\n"))
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					if err := bve.Accumulate(ve); err != nil {
						return fmt.Sprintf("error during Accumulate: %v", err)
					}
				}

				newv, err := bve.Apply(v, base.DefaultComparer, flushSplitBytes, readCompactionRate)
				if err != nil {
					return err.Error()
				}

				// Reinitialize the L0 sublevels in the original version; otherwise we
				// will get "AddL0Files called twice on the same receiver" panics.
				if err := v.InitL0Sublevels(flushSplitBytes); err != nil {
					panic(err)
				}
				return newv.DebugString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestParseVersionEditDebugRoundTrip(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{
			input: `  add-table:     L1 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL]`,
		},
		{
			input:  `add-table: L0 1:[a#0,SET-z#0,DEL]`,
			output: `  add-table:     L0 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL]`,
		},
		{
			input: `  del-table:     L1 000001`,
		},
		{
			input: strings.Join([]string{
				`  del-table:     L1 000002`,
				`  del-table:     L3 000003`,
				`  add-table:     L1 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL]`,
				`  add-table:     L2 000002:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL]`,
			}, "\n"),
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			ve, err := ParseVersionEditDebug(tc.input)
			require.NoError(t, err)
			got := ve.DebugString(base.DefaultFormatter)
			got = strings.TrimSuffix(got, "\n")
			want := tc.input
			if tc.output != "" {
				want = tc.output
			}
			require.Equal(t, want, got)
		})
	}
}

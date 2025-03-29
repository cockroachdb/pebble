// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
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
	diff := pretty.Diff(e0, e1)
	// SyntheticPrefixAndSuffix can't be correctly compared with pretty.Diff.
	diff = slices.DeleteFunc(diff, func(s string) bool {
		return strings.Contains(s, "SyntheticPrefixAndSuffix")
	})
	if len(diff) > 0 {
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
	m1 := (&TableMetadata{
		FileNum:                  810,
		Size:                     8090,
		CreationTime:             809060,
		SmallestSeqNum:           9,
		LargestSeqNum:            11,
		LargestSeqNumAbsolute:    11,
		SyntheticPrefixAndSuffix: sstable.MakeSyntheticPrefixAndSuffix([]byte("after"), []byte("foo")),
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

	m2 := (&TableMetadata{
		FileNum:               812,
		Size:                  8090,
		CreationTime:          809060,
		SmallestSeqNum:        9,
		LargestSeqNum:         11,
		LargestSeqNumAbsolute: 11,
		Virtual:               true,
		FileBacking:           m1.FileBacking,
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
		NewTables: []NewTableEntry{
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
	m1 := (&TableMetadata{
		FileNum:      805,
		Size:         8050,
		CreationTime: 805030,
	}).ExtendPointKeyBounds(
		cmp,
		base.DecodeInternalKey([]byte("abc\x00\x01\x02\x03\x04\x05\x06\x07")),
		base.DecodeInternalKey([]byte("xyz\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
	)
	m1.InitPhysicalBacking()

	m2 := (&TableMetadata{
		FileNum:                  806,
		Size:                     8060,
		CreationTime:             806040,
		SmallestSeqNum:           3,
		LargestSeqNum:            5,
		LargestSeqNumAbsolute:    5,
		MarkedForCompaction:      true,
		SyntheticPrefixAndSuffix: sstable.MakeSyntheticPrefixAndSuffix(nil, []byte("foo")),
	}).ExtendPointKeyBounds(
		cmp,
		base.DecodeInternalKey([]byte("A\x00\x01\x02\x03\x04\x05\x06\x07")),
		base.DecodeInternalKey([]byte("Z\x01\xff\xfe\xfd\xfc\xfb\xfa\xf9")),
	)
	m2.InitPhysicalBacking()

	m3 := (&TableMetadata{
		FileNum:      807,
		Size:         8070,
		CreationTime: 807050,
	}).ExtendRangeKeyBounds(
		cmp,
		base.MakeInternalKey([]byte("aaa"), 0, base.InternalKeyKindRangeKeySet),
		base.MakeExclusiveSentinelKey(base.InternalKeyKindRangeKeySet, []byte("zzz")),
	)
	m3.InitPhysicalBacking()

	m4 := (&TableMetadata{
		FileNum:               809,
		Size:                  8090,
		CreationTime:          809060,
		SmallestSeqNum:        9,
		LargestSeqNum:         11,
		LargestSeqNumAbsolute: 11,
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

	m5 := (&TableMetadata{
		FileNum:               810,
		Size:                  8090,
		CreationTime:          809060,
		SmallestSeqNum:        9,
		LargestSeqNum:         11,
		LargestSeqNumAbsolute: 11,
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

	m6 := (&TableMetadata{
		FileNum:               811,
		Size:                  8090,
		CreationTime:          809060,
		SmallestSeqNum:        9,
		LargestSeqNum:         11,
		LargestSeqNumAbsolute: 11,
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
			DeletedTables: map[DeletedTableEntry]*TableMetadata{
				{
					Level:   3,
					FileNum: 703,
				}: nil,
				{
					Level:   4,
					FileNum: 704,
				}: nil,
			},
			NewTables: []NewTableEntry{
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
	var inputBuf, outputBuf bytes.Buffer
	datadriven.RunTest(t, "testdata/version_edit_decode",
		func(t *testing.T, d *datadriven.TestData) string {
			inputBuf.Reset()
			outputBuf.Reset()
			switch d.Cmd {
			case "encode":
				ve, err := ParseVersionEditDebug(d.Input)
				require.NoError(t, err)
				require.NoError(t, ve.Encode(&inputBuf))
				serialized := inputBuf.Bytes()
				for len(serialized) > 0 {
					line := serialized[:min(30, len(serialized))]
					serialized = serialized[len(line):]
					outputBuf.WriteString(hex.EncodeToString(line))
					outputBuf.WriteByte('\n')
				}
				fmt.Fprint(&outputBuf, ve.DebugString(base.DefaultFormatter))
				return outputBuf.String()

			case "decode":
				for _, l := range crstrings.Lines(d.Input) {
					i := strings.IndexByte(l, '#')
					if i == -1 {
						i = len(l)
					}
					s := strings.Map(func(r rune) rune {
						if unicode.IsSpace(r) {
							return -1
						}
						return r
					}, l[:i])
					if strings.HasPrefix(s, "\"") {
						unquoted, err := strconv.Unquote(s)
						require.NoError(t, err)
						io.WriteString(&inputBuf, unquoted)
					} else {
						b, err := hex.DecodeString(s)
						require.NoError(t, err)
						inputBuf.Write(b)
					}
				}

				r := bytes.NewReader(inputBuf.Bytes())
				var ve VersionEdit
				if err := ve.Decode(r); err != nil {
					return fmt.Sprintf("err: %v", err)
				}

				// Ensure the version edit roundtrips.
				if err := checkRoundTrip(ve); err != nil {
					t.Fatal(err)
				}

				serialized := inputBuf.Bytes()
				for len(serialized) > 0 {
					line := serialized[:min(30, len(serialized))]
					serialized = serialized[len(line):]
					outputBuf.WriteString(hex.EncodeToString(line))
					outputBuf.WriteByte('\n')
				}
				fmt.Fprint(&outputBuf, ve.String())
				return outputBuf.String()
			default:
				panic(fmt.Sprintf("unknown command: %s", d.Cmd))
			}
		})
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
				if c.edit.LastSeqNum != base.SeqNum(val) {
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
				var saveName string
				d.MaybeScanArgs(t, "name", &saveName)
				// Apply an edit to the given version.
				name := d.CmdArgs[0].String()
				v := versions[name]
				if v == nil {
					d.Fatalf(t, "unknown version %q", name)
					return ""
				}

				bve := BulkVersionEdit{}
				bve.AddedTablesByFileNum = make(map[base.FileNum]*TableMetadata)
				for _, l := range v.Levels {
					for f := range l.All() {
						bve.AddedTablesByFileNum[f.FileNum] = f
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
				if saveName != "" {
					versions[saveName] = newv
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
			input: `  del-blob-file: 000001`,
		},
		{
			input: `  add-blob-file: 000005 size:[20535 (20KB)] vals:[25935 (25KB)]`,
		},
		{
			input: `  add-table:     L1 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:1`,
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
				`  add-table:     L1 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:1`,
				`  add-table:     L2 000002:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:2`,
			}, "\n"),
		},
		{
			input: strings.Join([]string{
				`  del-table:     L1 000002`,
				`  del-table:     L3 000003`,
				`  add-table:     L1 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:1`,
				`  add-table:     L2 000002:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:2`,
				`  add-blob-file: 000005 size:[20535 (20KB)] vals:[25935 (25KB)]`,
				`  del-blob-file: 000004`,
				`  del-blob-file: 000006`,
			}, "\n"),
		},
		{
			input: strings.Join([]string{
				`  add-table:     L1 000001:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:1`,
				`  add-table:     L2 000002:[a#0,SET-z#0,DEL] seqnums:[0-0] points:[a#0,SET-z#0,DEL] size:2 blobrefs:[(002431: 3008533), (002432: 10534); depth:2]`,
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

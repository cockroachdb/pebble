// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/bytealloc"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestLevelIterator(t *testing.T) {
	var level LevelSlice
	datadriven.RunTest(t, "testdata/level_iterator",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				var files []*TableMetadata
				var startReslice int
				var endReslice int
				for _, metaStr := range strings.Split(d.Input, " ") {
					switch metaStr {
					case "[":
						startReslice = len(files)
						continue
					case "]":
						endReslice = len(files)
						continue
					case " ", "":
						continue
					default:
						parts := strings.Split(metaStr, "-")
						if len(parts) != 2 {
							t.Fatalf("malformed table spec: %q", metaStr)
						}
						m := &TableMetadata{FileNum: base.FileNum(len(files) + 1)}
						m.ExtendPointKeyBounds(
							base.DefaultComparer.Compare,
							base.ParseInternalKey(strings.TrimSpace(parts[0])),
							base.ParseInternalKey(strings.TrimSpace(parts[1])),
						)
						m.SmallestSeqNum = m.Smallest().SeqNum()
						m.LargestSeqNum = m.Largest().SeqNum()
						m.LargestSeqNumAbsolute = m.LargestSeqNum
						m.InitPhysicalBacking()
						files = append(files, m)
					}
				}
				level = NewLevelSliceKeySorted(base.DefaultComparer.Compare, files)
				level = level.Reslice(func(start, end *LevelIterator) {
					for i := 0; i < startReslice; i++ {
						start.Next()
					}
					for i := len(files); i > endReslice; i-- {
						end.Prev()
					}
				})
				return ""

			case "iter":
				return runIterCmd(t, d, level.Iter(), false /* verbose */)

			default:
				return fmt.Sprintf("unknown command %q", d.Cmd)
			}
		})
}

func TestLevelIteratorFiltered(t *testing.T) {
	var level LevelSlice
	datadriven.RunTest(t, "testdata/level_iterator_filtered",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				var files []*TableMetadata
				for _, metaStr := range strings.Split(d.Input, "\n") {
					m, err := ParseTableMetadataDebug(metaStr)
					require.NoError(t, err)
					files = append(files, m)
				}
				level = NewLevelSliceKeySorted(base.DefaultComparer.Compare, files)
				return ""

			case "iter":
				var keyType string
				d.ScanArgs(t, "key-type", &keyType)
				iter := level.Iter()
				switch keyType {
				case "both":
					// noop
				case "points":
					iter = iter.Filter(KeyTypePoint)
				case "ranges":
					iter = iter.Filter(KeyTypeRange)
				}
				return runIterCmd(t, d, iter, true /* verbose */)

			default:
				return fmt.Sprintf("unknown command %q", d.Cmd)
			}
		})
}

func runIterCmd(t *testing.T, d *datadriven.TestData, iter LevelIterator, verbose bool) string {
	var buf bytes.Buffer
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var m *TableMetadata
		switch parts[0] {
		case "first":
			m = iter.First()
		case "last":
			m = iter.Last()
		case "next":
			m = iter.Next()
		case "prev":
			m = iter.Prev()
		case "seek-ge":
			m = iter.SeekGE(base.DefaultComparer.Compare, []byte(parts[1]))
		case "seek-lt":
			m = iter.SeekLT(base.DefaultComparer.Compare, []byte(parts[1]))
		default:
			return fmt.Sprintf("unknown command %q", parts[0])
		}
		buf.WriteString(line)
		buf.WriteString(": ")
		if m == nil {
			fmt.Fprintln(&buf, ".")
		} else {
			if verbose {
				fmt.Fprintln(&buf, m.DebugString(base.DefaultComparer.FormatKey, verbose))
			} else {
				fmt.Fprintln(&buf, m)
			}
		}
	}
	return buf.String()
}

func makeTestTableMetadata() (tables []*TableMetadata, keys [][]byte) {
	const (
		countTables  = 10_000
		maxKeyLength = 5
	)
	var (
		ks          = testkeys.Alpha(maxKeyLength)
		buf         = make([]byte, ks.MaxLen()+testkeys.MaxSuffixLen)
		tablesAlloc = make([]TableMetadata, countTables)
		balloc      = bytealloc.A(make([]byte, 0, 4096))
	)
	tables = make([]*TableMetadata, countTables)
	keys = make([][]byte, 0, 2*countTables)

	// Create 2*countTables keys in order, while constructing a LevelMetadata
	// with `countTables` tables. Even-indexed keys are appended to keys, but
	// are not used to construct a table. Odd-indexed keys are used to
	// synthesize a TableMetadata with a file number matching the key's index.
	// The smallest and largest boundary keys of synthesized TableMetadata are
	// identical, using the odd-indexed key as the user key and the index itself
	// as the sequence number.
	for i := int64(0); i < countTables; i++ {
		var userKey []byte
		var n int
		// Generate a key. The first key is appended to `keys`, but doesn't
		// appear within any table metadata.
		n = testkeys.WriteKey(buf, ks, 2*i)
		balloc, userKey = balloc.Copy(buf[:n])
		keys = append(keys, userKey)
		// Generate the next key. This key is also appended to `keys`, but does
		// appear within the table metadata as both smallest and largest bounds.
		n = testkeys.WriteKey(buf, ks, 2*i+1)
		balloc, userKey = balloc.Copy(buf[:n])
		keys = append(keys, userKey)

		tablesAlloc[i] = TableMetadata{FileNum: base.FileNum(2*i + 1)}
		tablesAlloc[i].ExtendPointKeyBounds(testkeys.Comparer.Compare,
			base.MakeInternalKey(userKey, base.SeqNum(i), base.InternalKeyKindSet),
			base.MakeInternalKey(userKey, base.SeqNum(i), base.InternalKeyKindSet))
		tablesAlloc[i].InitPhysicalBacking()
		tables[i] = &tablesAlloc[i]
	}
	return tables, keys
}

// TestLevelIteratorSeek tests LevelIterator.{SeekGE,SeekLT}.
func TestLevelIteratorSeek(t *testing.T) {
	tables, keys := makeTestTableMetadata()
	lm := MakeLevelMetadata(testkeys.Comparer.Compare, 6 /* L6 */, tables)

	// Test seeking using SeekGE to every key in `keys`.
	t.Run("SeekGE", func(t *testing.T) {
		iter := lm.Iter()
		for i := range keys {
			m := iter.SeekGE(testkeys.Comparer.Compare, keys[i])

			// Seeking to a key with an odd index should find a table
			// metadata with a file number of i (and a largest boundary
			// exactly equal to the seek key).
			// Seeking to a key with an even index should find a table
			// metadata with a file number of i+1 (and a largest boundary
			// exactly equal to the key at index i+1).
			want := i + (i+1)%2
			if m == nil {
				t.Fatalf("SeekGE(%q [%d]) = nil", keys[i], i)
			} else if int(m.FileNum) != want {
				t.Fatalf("SeekGE(%q [%d]) = %s", keys[i], i, m.FileNum)
			}
		}
	})

	// Test seeking using SeekLT to every key in `keys`.
	t.Run("SeekLT", func(t *testing.T) {
		iter := lm.Iter()
		// The table with the smallest key has smallest = keys[1]. Neither
		// keys[0] nor keys[1] is strictly less than keys[1], so SeekLT(keys[0])
		// and SeekLT(keys[1]) should both return nil.
		require.Nil(t, iter.SeekLT(testkeys.Comparer.Compare, keys[0]))
		require.Nil(t, iter.SeekLT(testkeys.Comparer.Compare, keys[1]))

		for i := 2; i < len(keys); i++ {
			m := iter.SeekLT(testkeys.Comparer.Compare, keys[i])

			// Seeking to a key with an odd index should find a table
			// metadata with a file number of i-2.
			// Seeking to a key with an even index should find a table
			// metadata with a file number of i-1.
			want := i - 1 - i%2
			if m == nil {
				t.Fatalf("SeekLT(%q [%d]) = nil", keys[i], i)
			} else if int(m.FileNum) != want {
				t.Fatalf("SeekLT(%q [%d]) = %s", keys[i], i, m.FileNum)
			}
		}
	})
}

func TestLevelIteratorFind(t *testing.T) {
	tables, _ := makeTestTableMetadata()
	for _, level := range []int{0, 1, 6} {
		t.Run(fmt.Sprintf("level%d", level), func(t *testing.T) {
			lm := MakeLevelMetadata(testkeys.Comparer.Compare, level, tables)
			for _, m := range tables {
				got := slices.Collect(lm.Find(testkeys.Comparer.Compare, m).All())
				if len(got) != 1 {
					t.Fatalf("Find(%s) = %s", m, got)
				} else if got[0].FileNum != m.FileNum {
					t.Fatalf("Find(%s) = %s", m, got)
				}
			}
		})
	}
}

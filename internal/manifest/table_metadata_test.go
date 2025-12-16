// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestExtendBounds(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	parseBounds := func(line string) (lower, upper InternalKey) {
		parts := strings.Split(line, "-")
		if len(parts) == 1 {
			parts = strings.Split(parts[0], ":")
			start, end := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			lower = base.ParseInternalKey(start)
			switch k := lower.Kind(); k {
			case base.InternalKeyKindRangeDelete:
				upper = base.MakeRangeDeleteSentinelKey([]byte(end))
			case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
				upper = base.MakeExclusiveSentinelKey(k, []byte(end))
			default:
				panic(fmt.Sprintf("unknown kind %s with end key", k))
			}
		} else {
			l, u := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			lower, upper = base.ParseInternalKey(l), base.ParseInternalKey(u)
		}
		return
	}
	format := func(m *TableMetadata) string {
		var b bytes.Buffer
		var smallest, largest string
		switch m.boundTypeSmallest {
		case boundTypePointKey:
			smallest = "point"
		case boundTypeRangeKey:
			smallest = "range"
		default:
			return fmt.Sprintf("unknown bound type %d", m.boundTypeSmallest)
		}
		switch m.boundTypeLargest {
		case boundTypePointKey:
			largest = "point"
		case boundTypeRangeKey:
			largest = "range"
		default:
			return fmt.Sprintf("unknown bound type %d", m.boundTypeLargest)
		}
		bounds, err := m.boundsMarker()
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(&b, "%s\n", m.DebugString(base.DefaultFormatter, true))
		fmt.Fprintf(&b, "  bounds: (smallest=%s,largest=%s) (0x%08b)\n", smallest, largest, bounds)
		return b.String()
	}
	m := &TableMetadata{}
	datadriven.RunTest(t, "testdata/file_metadata_bounds", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "reset":
			m = &TableMetadata{}
			return ""
		case "extend-point-key-bounds":
			u, l := parseBounds(d.Input)
			m.ExtendPointKeyBounds(cmp, u, l)
			return format(m)
		case "extend-range-key-bounds":
			u, l := parseBounds(d.Input)
			m.ExtendRangeKeyBounds(cmp, u, l)
			return format(m)
		default:
			return fmt.Sprintf("unknown command %s\n", d.Cmd)
		}
	})
}

func TestTableMetadata_ParseRoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		input  string
		output string
	}{
		{
			name:  "point keys only",
			input: "000001:[a#0,SET-z#0,DEL] seqnums:[#0-#0] points:[a#0,SET-z#0,DEL]",
		},
		{
			name:  "range keys only",
			input: "000001:[a#0,RANGEKEYSET-z#0,RANGEKEYDEL] seqnums:[#0-#0] ranges:[a#0,RANGEKEYSET-z#0,RANGEKEYDEL]",
		},
		{
			name:  "point and range keys",
			input: "000001:[a#0,RANGEKEYSET-d#0,DEL] seqnums:[#0-#0] points:[b#0,SET-d#0,DEL] ranges:[a#0,RANGEKEYSET-c#0,RANGEKEYDEL]",
		},
		{
			name:  "point and range keys with nonzero senums",
			input: "000001:[a#3,RANGEKEYSET-d#4,DEL] seqnums:[#3-#7] points:[b#3,SET-d#4,DEL] ranges:[a#3,RANGEKEYSET-c#5,RANGEKEYDEL]",
		},
		{
			name:   "whitespace",
			input:  " 000001 : [ a#0,SET - z#0,DEL] points : [ a#0,SET - z#0,DEL] ",
			output: "000001:[a#0,SET-z#0,DEL] seqnums:[#0-#0] points:[a#0,SET-z#0,DEL]",
		},
		{
			name:  "virtual",
			input: "000001(000008):[a#0,SET-z#0,DEL] seqnums:[#0-#0] points:[a#0,SET-z#0,DEL]",
		},
		{
			name:  "blobrefs",
			input: "000196:[bar#0,SET-foo#0,SET] seqnums:[#0-#0] points:[bar#0,SET-foo#0,SET] blobrefs:[(B000191: 2952), (B000075: 108520); depth:2]",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := ParseTableMetadataDebug(tc.input)
			require.NoError(t, err)
			err = m.Validate(base.DefaultComparer.Compare, base.DefaultFormatter)
			require.NoError(t, err)
			got := m.DebugString(base.DefaultFormatter, true)
			want := tc.input
			if tc.output != "" {
				want = tc.output
			}
			require.Equal(t, want, got)
		})
	}
}

func TestTableMetadata_ScaleStatistic(t *testing.T) {
	var meta TableMetadata
	meta.TableBacking = &TableBacking{}
	meta.Virtual = true

	meta.Size = 50
	meta.TableBacking.Size = 100
	require.Equal(t, uint64(0), meta.ScaleStatistic(0))
	require.Equal(t, uint64(1), meta.ScaleStatistic(1))
	require.Equal(t, uint64(1), meta.ScaleStatistic(2))
	require.Equal(t, uint64(2), meta.ScaleStatistic(3))
	require.Equal(t, uint64(50_000), meta.ScaleStatistic(100_000))

	t.Run("invalid sizes", func(t *testing.T) {
		meta.Size = 0
		meta.TableBacking.Size = 100
		require.Equal(t, uint64(0), meta.ScaleStatistic(0))
		require.Equal(t, uint64(1), meta.ScaleStatistic(1))
		require.Equal(t, uint64(1), meta.ScaleStatistic(10))

		meta.Size = 1000
		meta.TableBacking.Size = 1000
		require.Equal(t, uint64(0), meta.ScaleStatistic(0))
		require.Equal(t, uint64(1), meta.ScaleStatistic(1))
		require.Equal(t, uint64(10), meta.ScaleStatistic(10))
	})

	t.Run("overflow", func(t *testing.T) {
		meta.Size = 1 << 60
		meta.TableBacking.Size = 1 << 60
		require.Equal(t, uint64(0), meta.ScaleStatistic(0))
		require.Equal(t, uint64(1), meta.ScaleStatistic(1))
		require.Equal(t, uint64(12345), meta.ScaleStatistic(12345))
		require.Equal(t, uint64(1<<40), meta.ScaleStatistic(1<<40))
		require.Equal(t, uint64(1<<50), meta.ScaleStatistic(1<<50))

		meta.Size = 1 << 40
		meta.TableBacking.Size = 1 << 50
		require.Equal(t, uint64(1<<30), meta.ScaleStatistic(1<<40))
		require.Equal(t, uint64(1<<40), meta.ScaleStatistic(1<<50))
	})
}

// TestTableMetadataSize tests the expected size of our TableMetadata and
// TableBacking structs.
//
// This test exists as a callout for whoever changes these structs to be mindful
// of their size increasing -- as the cost of these structs is proportional to
// the amount of files that exist.
func TestTableMetadataSize(t *testing.T) {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Skip("Test only supported on amd64 and arm64 architectures")
	}

	const tableMetadataSize = 240
	if structSize := unsafe.Sizeof(TableMetadata{}); structSize != tableMetadataSize {
		t.Errorf("TableMetadata struct size (%d bytes) is not expected size (%d bytes)",
			structSize, tableMetadataSize)
	}

	const tableBackingSize = 176
	if structSize := unsafe.Sizeof(TableBacking{}); structSize != tableBackingSize {
		t.Errorf("TableBacking struct size (%d bytes) is not expected size (%d bytes)",
			structSize, tableBackingSize)
	}
}

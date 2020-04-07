// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/record"
)

func readManifest(filename string) (*Version, error) {
	f, err := os.Open("testdata/MANIFEST_import")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rr := record.NewReader(f, 0 /* logNum */)
	var v *Version
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var ve VersionEdit
		if err = ve.Decode(r); err != nil {
			return nil, err
		}
		var bve BulkVersionEdit
		bve.Accumulate(&ve)
		if v, _, err = bve.Apply(v, base.DefaultComparer.Compare, base.DefaultFormatter); err != nil {
			return nil, err
		}
	}
	fmt.Printf("L0 filecount: %d\n", len(v.Files[0]))
	return v, nil
}

func TestL0SubLevels(t *testing.T) {
	parseMeta := func(s string) (*FileMetadata, error) {
		parts := strings.Split(s, ":")
		if len(parts) != 2 {
			t.Fatalf("malformed table spec: %s", s)
		}
		fileNum, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, err
		}
		fields := strings.Fields(parts[1])
		keyRange := strings.Split(strings.TrimSpace(fields[0]), "-")
		m := FileMetadata{
			Smallest: base.ParseInternalKey(strings.TrimSpace(keyRange[0])),
			Largest:  base.ParseInternalKey(strings.TrimSpace(keyRange[1])),
		}
		m.SmallestSeqNum = m.Smallest.SeqNum()
		m.LargestSeqNum = m.Largest.SeqNum()
		m.FileNum = base.FileNum(fileNum)
		m.Size = uint64(256)

		if len(fields) > 1 {
			for _, field := range fields[1:] {
				parts := strings.Split(field, "=")
				switch parts[0] {
				case "base_compacting":
					m.IsIntraL0Compacting = false
					m.Compacting = true
				case "intra_l0_compacting":
					m.IsIntraL0Compacting = true
					m.Compacting = true
				case "compacting":
					m.Compacting = true
				case "size":
					sizeInt, err := strconv.Atoi(parts[1])
					if err != nil {
						return nil, err
					}
					m.Size = uint64(sizeInt)
				}
			}
		}

		return &m, nil
	}

	var level int
	var err error
	var fileMetas [NumLevels][]*FileMetadata
	var sublevels *L0SubLevels
	baseLevel := NumLevels - 1

	datadriven.RunTest(t, "testdata/l0_sublevels", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			fileMetas = [NumLevels][]*FileMetadata{}
			baseLevel = NumLevels - 1
			for _, data := range strings.Split(td.Input, "\n") {
				data = strings.TrimSpace(data)
				switch data {
				case "L0", "L1", "L2", "L3", "L4", "L5", "L6":
					level, err = strconv.Atoi(data[1:])
					if err != nil {
						return err.Error()
					}
				default:
					meta, err := parseMeta(data)
					if err != nil {
						return err.Error()
					}
					if level != 0 && level < baseLevel {
						baseLevel = level
					}
					fileMetas[level] = append(fileMetas[level], meta)
				}
			}

			flushSplitMaxBytes := 64
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "flush_split_max_bytes":
					flushSplitMaxBytes, err = strconv.Atoi(arg.Vals[0])
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			for i := 0; i < NumLevels; i++ {
				SortBySeqNum(fileMetas[i])
			}

			sublevels, err = NewL0SubLevels(
				fileMetas[0],
				base.DefaultComparer.Compare,
				base.DefaultFormatter,
				uint64(flushSplitMaxBytes))

			if err != nil {
				t.Fatal(err)
			}

			var builder strings.Builder
			builder.WriteString(sublevels.describe(true))
			return builder.String()
		case "read-amp":
			return strconv.Itoa(sublevels.ReadAmplification())
		case "flush-split-keys":
			var builder strings.Builder
			builder.WriteString("flush user split keys: ")
			flushSplitKeys := sublevels.FlushSplitKeys()
			for i, key := range flushSplitKeys {
				builder.Write(key)
				if i < len(flushSplitKeys) - 1 {
					builder.WriteString(", ")
				}
			}
			return builder.String()
		case "max-depth-after-ongoing-compactions":
			return strconv.Itoa(sublevels.MaxDepthAfterOngoingCompactions())
		}
		return fmt.Sprintf("unrecognized command: %s", td.Cmd)
	})
}

func BenchmarkL0SubLevelsInit(b *testing.B) {
	v, err := readManifest("testdata/MANIFEST_import")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sl, err := NewL0SubLevels(v.Files[0], base.DefaultComparer.Compare, base.DefaultFormatter, 5<<20)
		if err != nil {
			b.Fatal(err)
		} else if sl == nil {
			b.Fatal("expected non-nil L0SubLevels to be generated")
		}
	}
}

package crdbtest

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/binfmt"
	"github.com/cockroachdb/pebble/internal/treeprinter"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/stretchr/testify/require"
)

// TestKeySchema tests the cockroachKeyWriter and cockroachKeySeeker.
func TestKeySchema(t *testing.T) {
	for _, file := range []string{"suffix_types", "block_encoding", "seek"} {
		t.Run(file, func(t *testing.T) {
			runDataDrivenTest(t, fmt.Sprintf("testdata/%s", file))
		})
	}
}

func runDataDrivenTest(t *testing.T, path string) {
	var blockData []byte
	var e colblk.DataBlockEncoder
	e.Init(&KeySchema)
	var iter colblk.DataBlockIter
	iter.InitOnce(&KeySchema, Comparer.Compare, Comparer.Split, nil)

	datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			e.Reset()
			var buf []byte
			for _, l := range crstrings.Lines(td.Input) {
				key, value := parseKV(l)
				kcmp := e.KeyWriter.ComparePrev(key.UserKey)
				e.Add(key, value, 0, kcmp, false /* isObsolete */)
				buf = e.MaterializeLastUserKey(buf[:0])
				if !Comparer.Equal(key.UserKey, buf) {
					td.Fatalf(t, "incorect MaterializeLastKey: %s instead of %s", formatUserKey(buf), formatUserKey(key.UserKey))
				}
			}
			numRows := e.Rows()
			size := e.Size()
			blockData, _ = e.Finish(numRows, size)
			require.Equal(t, size, len(blockData))
			return fmt.Sprintf("%d rows, total size %dB", numRows, size)

		case "describe":
			var d colblk.DataBlockDecoder
			d.Init(&KeySchema, blockData)
			f := binfmt.New(blockData)
			tp := treeprinter.New()
			d.Describe(f, tp)
			return tp.String()

		case "suffix-types":
			var d colblk.DataBlockDecoder
			d.Init(&KeySchema, blockData)
			var ks cockroachKeySeeker
			ks.init(&d)
			return fmt.Sprintf("suffix-types: %s", ks.suffixTypes)

		case "keys":
			var d colblk.DataBlockDecoder
			d.Init(&KeySchema, blockData)
			require.NoError(t, iter.Init(&d, block.IterTransforms{}))
			defer iter.Close()
			var buf bytes.Buffer
			var prevKey base.InternalKey
			for kv := iter.First(); kv != nil; kv = iter.Next() {
				fmt.Fprintf(&buf, "%s", formatKV(*kv))
				if prevKey.UserKey != nil && base.InternalCompare(Comparer.Compare, prevKey, kv.K) != -1 {
					buf.WriteString(" !!! OUT OF ORDER KEY !!!")
				}
				buf.WriteString("\n")
				prevKey = kv.K.Clone()
			}
			return buf.String()

		case "seek":
			var d colblk.DataBlockDecoder
			d.Init(&KeySchema, blockData)
			require.NoError(t, iter.Init(&d, block.IterTransforms{}))
			defer iter.Close()
			var buf strings.Builder
			for _, l := range crstrings.Lines(td.Input) {
				key := parseUserKey(l)
				fmt.Fprintf(&buf, "%s: ", formatUserKey(key))
				kv := iter.SeekGE(key, base.SeekGEFlagsNone)
				require.NoError(t, iter.Error())
				if kv == nil {
					buf.WriteString(".\n")
				} else {
					fmt.Fprintf(&buf, "%s\n", formatKV(*kv))
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

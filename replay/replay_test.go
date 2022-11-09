package replay

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/datatest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/rangekey"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestLoadFlushedSSTableKeys(t *testing.T) {
	var buf bytes.Buffer
	var fileNums []base.FileNum
	opts := &pebble.Options{
		EventListener: pebble.EventListener{
			FlushEnd: func(info pebble.FlushInfo) {
				// Fix the durations to 1s for determinism.
				info.Duration, info.TotalDuration = time.Second, time.Second
				for _, tbl := range info.Output {
					fileNums = append(fileNums, tbl.FileNum)
				}
				fmt.Fprintln(&buf, info.String())
			},
		},
		FS:                 vfs.NewMem(),
		Comparer:           testkeys.Comparer,
		FormatMajorVersion: pebble.FormatRangeKeys,
	}
	d, err := pebble.Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	datadriven.RunTest(t, "testdata/flushed_sstable_keys", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "commit":
			b := d.NewIndexedBatch()
			if err := datatest.DefineBatch(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}

			b := d.NewBatch()
			err := loadFlushedSSTableKeys(b, opts.FS, "", fileNums, opts.MakeReaderOptions())
			if err != nil {
				b.Close()
				return err.Error()
			}

			br, _ := pebble.ReadBatch(b.Repr())
			for kind, ukey, v, ok := br.Next(); ok; kind, ukey, v, ok = br.Next() {
				fmt.Fprintf(&buf, "%s.%s", ukey, kind)
				switch kind {
				case base.InternalKeyKindRangeDelete,
					base.InternalKeyKindRangeKeyDelete:
					fmt.Fprintf(&buf, "-%s", v)
				case base.InternalKeyKindSet,
					base.InternalKeyKindMerge:
					fmt.Fprintf(&buf, ": %s", v)
				case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset:
					s, err := rangekey.Decode(base.MakeInternalKey(ukey, 0, kind), v, nil)
					if err != nil {
						return err.Error()
					}
					if kind == base.InternalKeyKindRangeKeySet {
						fmt.Fprintf(&buf, "-%s: %s → %s", s.End, s.Keys[0].Suffix, s.Keys[0].Value)
					} else {
						fmt.Fprintf(&buf, "-%s: %s", s.End, s.Keys[0].Suffix)
					}
				case base.InternalKeyKindDelete, base.InternalKeyKindSingleDelete:
				default:
					fmt.Fprintf(&buf, ": %x", v)
				}
				fmt.Fprintln(&buf)
			}

			s := buf.String()
			buf.Reset()
			require.NoError(t, b.Close())

			fileNums = fileNums[:0]
			return s
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
		return ""
	})
}

type bufferLogger struct {
	bytes.Buffer
}

func (l *bufferLogger) Infof(format string, args ...interface{}) {
	fmt.Fprintf(&l.Buffer, format, args...)
	fmt.Fprintln(&l.Buffer)
}

func (l *bufferLogger) Fatalf(format string, args ...interface{}) {
	fmt.Fprintf(&l.Buffer, format, args...)
	fmt.Fprintln(&l.Buffer)
}

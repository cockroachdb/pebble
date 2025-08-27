package sstable

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable/colblk"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

func TestWriter_RangeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	cmp := testkeys.Comparer
	keySchema := colblk.DefaultKeySchema(cmp, 16)
	buildFn := func(td *datadriven.TestData, format TableFormat) (*Reader, error) {
		mem := vfs.NewMem()
		f, err := mem.Create("test", vfs.WriteCategoryUnspecified)
		if err != nil {
			return nil, err
		}

		// Use a "suffix-aware" Comparer, that will sort suffix-values in
		// descending order of timestamp, rather than in lexical order.
		w := NewWriter(objstorageprovider.NewFileWritable(f), WriterOptions{
			Comparer:    cmp,
			TableFormat: format,
			KeySchema:   &keySchema,
		})
		defer func() {
			if w != nil {
				_ = w.Close()
			}
		}()
		for _, data := range strings.Split(td.Input, "\n") {
			// Format. One of:
			// - SET $START-$END $SUFFIX=$VALUE
			// - UNSET $START-$END $SUFFIX
			// - DEL $START-$END
			parts := strings.Split(data, " ")
			kind, startEnd := parts[0], parts[1]

			startEndSplit := bytes.Split([]byte(startEnd), []byte("-"))

			var start, end, suffix, value []byte
			start, end = startEndSplit[0], startEndSplit[1]

			switch kind {
			case "SET":
				sv := bytes.Split([]byte(parts[2]), []byte("="))
				suffix, value = sv[0], sv[1]
				err = w.RangeKeySet(start, end, suffix, value)
			case "UNSET":
				suffix = []byte(parts[2])
				err = w.RangeKeyUnset(start, end, suffix)
			case "DEL":
				err = w.RangeKeyDelete(start, end)
			default:
				return nil, errors.Newf("unexpected key kind: %s", kind)
			}
			if err != nil {
				return nil, err
			}

			// Scramble the bytes in each of the input arrays. This helps with
			// flushing out subtle bugs due to byte slice re-use.
			for _, slice := range [][]byte{start, end, suffix, value} {
				_, _ = rand.Read(slice)
			}
		}

		if err = w.Close(); err != nil {
			return nil, err
		}
		w = nil

		f, err = mem.Open("test")
		if err != nil {
			return nil, err
		}

		r, err = newReader(f, ReaderOptions{
			Comparer:   cmp,
			KeySchemas: MakeKeySchemas(&keySchema),
		})
		if err != nil {
			return nil, err
		}

		return r, nil
	}

	for format := TableFormatPebblev2; format <= TableFormatMax; format++ {
		t.Run(format.String(), func(t *testing.T) {
			datadriven.RunTest(t, "testdata/writer_range_keys", func(t *testing.T, td *datadriven.TestData) string {
				switch td.Cmd {
				case "build":
					if r != nil {
						_ = r.Close()
						r = nil
					}
					var err error
					r, err = buildFn(td, format)
					if err != nil {
						return err.Error()
					}

					iter, err := r.NewRawRangeKeyIter(context.Background(), NoFragmentTransforms, NoReadEnv)
					if err != nil {
						return err.Error()
					}
					defer iter.Close()

					var buf bytes.Buffer
					s, err := iter.First()
					for ; s != nil; s, err = iter.Next() {
						_, _ = fmt.Fprintf(&buf, "%s\n", s)
					}
					if err != nil {
						return err.Error()
					}
					return buf.String()

				default:
					return fmt.Sprintf("unknown command: %s", td.Cmd)
				}
			})
		})
	}
}

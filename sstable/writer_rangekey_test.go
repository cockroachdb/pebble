package sstable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWriter_RangeKeys(t *testing.T) {
	var r *Reader
	defer func() {
		if r != nil {
			require.NoError(t, r.Close())
		}
	}()

	buildFn := func(td *datadriven.TestData) (*Reader, error) {
		mem := vfs.NewMem()
		f, err := mem.Create("test")
		if err != nil {
			return nil, err
		}

		// Use a "suffix-aware" Comparer, that will sort suffix-values in
		// descending order of timestamp, rather than in lexical order.
		cmp := testkeys.Comparer
		w := NewWriter(f, WriterOptions{Comparer: cmp})
		for _, data := range strings.Split(td.Input, "\n") {

			// Format. One of:
			// - SET $START-$END $SUFFIX=$VALUE
			// - UNSET $START-$END $SUFFIX
			// - DEL $START-$END
			parts := strings.Split(data, " ")
			kind, startEnd := parts[0], parts[1]

			startEndSplit := bytes.Split([]byte(startEnd), []byte("-"))
			start, end := startEndSplit[0], startEndSplit[1]

			switch kind {
			case "SET":
				sv := bytes.Split([]byte(parts[2]), []byte("="))
				err = w.RangeKeySet(start, end, sv[0], sv[1])
			case "UNSET":
				err = w.RangeKeyUnset(start, end, []byte(parts[2]))
			case "DEL":
				err = w.RangeKeyDelete(start, end)
			default:
				return nil, errors.Newf("unexpected key kind: %s", kind)
			}

			if err != nil {
				return nil, err
			}
		}

		if err = w.Close(); err != nil {
			return nil, err
		}

		f, err = mem.Open("test")
		if err != nil {
			return nil, err
		}

		r, err = NewReader(f, ReaderOptions{Comparer: cmp})
		if err != nil {
			return nil, err
		}

		return r, nil
	}

	datadriven.RunTest(t, "testdata/writer_range_keys", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "build":
			if r != nil {
				_ = r.Close()
				r = nil
			}

			var err error
			r, err = buildFn(td)
			if err != nil {
				return err.Error()
			}

			iter, err := r.NewRawRangeKeyIter()
			if err != nil {
				return err.Error()
			}
			defer iter.Close()

			var buf bytes.Buffer
			for key, val := iter.First(); key != nil; key, val = iter.Next() {
				if !rangekey.IsRangeKey(key.Kind()) {
					return fmt.Sprintf("expected range key kind; got %s", key.Kind())
				}
				end, rkVal, ok := rangekey.DecodeEndKey(key.Kind(), val)
				if !ok {
					return "could not decode end key"
				}

				span := keyspan.Span{
					Start: *key,
					End:   end,
					Value: rkVal,
				}
				_, _ = fmt.Fprintf(&buf, "%s\n", rangekey.Format(base.DefaultComparer.FormatKey, span))
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

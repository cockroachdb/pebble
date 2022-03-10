// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/rangekey"
	"github.com/cockroachdb/pebble/vfs"
)

func optsFromArgs(td *datadriven.TestData, writerOpts *WriterOptions) error {
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "leveldb":
			if len(arg.Vals) != 0 {
				return errors.Errorf("%s: arg %s expects 0 values", td.Cmd, arg.Key)
			}
			writerOpts.TableFormat = TableFormatLevelDB
		case "block-size":
			if len(arg.Vals) != 1 {
				return errors.Errorf("%s: arg %s expects 1 value", td.Cmd, arg.Key)
			}
			var err error
			writerOpts.BlockSize, err = strconv.Atoi(arg.Vals[0])
			if err != nil {
				return err
			}
		case "index-block-size":
			if len(arg.Vals) != 1 {
				return errors.Errorf("%s: arg %s expects 1 value", td.Cmd, arg.Key)
			}
			var err error
			writerOpts.IndexBlockSize, err = strconv.Atoi(arg.Vals[0])
			if err != nil {
				return err
			}
		case "filter":
			writerOpts.FilterPolicy = bloom.FilterPolicy(10)
		case "comparer-split-4b-suffix":
			writerOpts.Comparer = test4bSuffixComparer
		}
	}
	return nil
}

func runBuildCmd(
	td *datadriven.TestData, writerOpts *WriterOptions, cacheSize int,
) (*WriterMetadata, *Reader, error) {

	f0 := &memFile{}
	if err := optsFromArgs(td, writerOpts); err != nil {
		return nil, nil, err
	}

	w := NewWriter(f0, *writerOpts)
	var rangeDels []keyspan.Span
	rangeDelFrag := keyspan.Fragmenter{
		Cmp:    DefaultComparer.Compare,
		Format: DefaultComparer.FormatKey,
		Emit: func(fragmented []keyspan.Span) {
			rangeDels = append(rangeDels, fragmented...)
		},
	}
	var rangeKeys []keyspan.Span
	rangeKeyFrag := keyspan.Fragmenter{
		Cmp:    DefaultComparer.Compare,
		Format: DefaultComparer.FormatKey,
		Emit: func(fragmented []keyspan.Span) {
			rangeKeys = append(rangeKeys, fragmented...)
		},
	}
	for _, data := range strings.Split(td.Input, "\n") {
		j := strings.Index(data, ":")
		key := base.ParseInternalKey(data[:j])
		value := []byte(data[j+1:])
		switch key.Kind() {
		case InternalKeyKindRangeDelete:
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.Errorf("%v", r)
					}
				}()
				rangeDelFrag.Add(keyspan.Span{Start: key, End: value})
			}()
			if err != nil {
				return nil, nil, err
			}
		case base.InternalKeyKindRangeKeyDelete,
			base.InternalKeyKindRangeKeyUnset,
			base.InternalKeyKindRangeKeySet:
			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						err = errors.Errorf("%v", r)
					}
				}()
				key, value := rangekey.Parse(data)
				endKey, value, ok := rangekey.DecodeEndKey(key.Kind(), value)
				if !ok {
					err = errors.New("could not decode end key")
					return
				}
				rangeKeyFrag.Add(keyspan.Span{Start: key, End: endKey, Value: value})
			}()
			if err != nil {
				return nil, nil, err
			}
		default:
			if err := w.Add(key, value); err != nil {
				return nil, nil, err
			}
		}
	}
	rangeDelFrag.Finish()
	for _, v := range rangeDels {
		if err := w.Add(v.Start, v.End); err != nil {
			return nil, nil, err
		}
	}
	rangeKeyFrag.Finish()
	for _, v := range rangeKeys {
		// Reconstitute the value from the end key and the user value.
		n := rangekey.RecombinedValueLen(v.Start.Kind(), v.End, v.Value)
		b := make([]byte, n)
		_ = rangekey.RecombineValue(v.Start.Kind(), b, v.End, v.Value)
		if err := w.AddRangeKey(v.Start, b); err != nil {
			return nil, nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	meta, err := w.Metadata()
	if err != nil {
		return nil, nil, err
	}

	readerOpts := ReaderOptions{Comparer: writerOpts.Comparer}
	if writerOpts.FilterPolicy != nil {
		readerOpts.Filters = map[string]FilterPolicy{
			writerOpts.FilterPolicy.Name(): writerOpts.FilterPolicy,
		}
	}
	if cacheSize > 0 {
		readerOpts.Cache = cache.New(int64(cacheSize))
		defer readerOpts.Cache.Unref()
	}
	r, err := NewMemReader(f0.Data(), readerOpts)
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

func runBuildRawCmd(
	td *datadriven.TestData, opts *WriterOptions,
) (*WriterMetadata, *Reader, error) {
	mem := vfs.NewMem()
	f0, err := mem.Create("test")
	if err != nil {
		return nil, nil, err
	}

	w := NewWriter(f0, *opts)
	for i := range td.CmdArgs {
		arg := &td.CmdArgs[i]
		if arg.Key == "range-del-v1" {
			w.rangeDelV1Format = true
			break
		}
	}

	for _, data := range strings.Split(td.Input, "\n") {
		j := strings.Index(data, ":")
		key := base.ParseInternalKey(data[:j])

		switch key.Kind() {
		case base.InternalKeyKindRangeKeyDelete,
			base.InternalKeyKindRangeKeyUnset,
			base.InternalKeyKindRangeKeySet:
			// Values for range keys must be converted into their "packed" form before
			// being added to the Writer.
			_, value := rangekey.Parse(data)
			if err := w.AddRangeKey(key, value); err != nil {
				return nil, nil, err
			}
		default:
			value := []byte(data[j+1:])
			if err := w.Add(key, value); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	meta, err := w.Metadata()
	if err != nil {
		return nil, nil, err
	}

	f1, err := mem.Open("test")
	if err != nil {
		return nil, nil, err
	}
	r, err := NewReader(f1, ReaderOptions{})
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

func runIterCmd(td *datadriven.TestData, r *Reader) string {
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "globalSeqNum":
			if len(arg.Vals) != 1 {
				return fmt.Sprintf("%s: arg %s expects 1 value", td.Cmd, arg.Key)
			}
			v, err := strconv.Atoi(arg.Vals[0])
			if err != nil {
				return err.Error()
			}
			r.Properties.GlobalSeqNum = uint64(v)
		default:
			return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
		}
	}
	origIter, err := r.NewIter(nil /* lower */, nil /* upper */)
	if err != nil {
		return err.Error()
	}
	iter := newIterAdapter(origIter)
	defer iter.Close()

	var b bytes.Buffer
	var prefix []byte
	for _, line := range strings.Split(td.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "seek-ge":
			if len(parts) < 2 || len(parts) > 3 {
				return "seek-ge <key> [<try-seek-using-next]\n"
			}
			prefix = nil
			trySeekUsingNext := false
			if len(parts) == 3 {
				var err error
				trySeekUsingNext, err = strconv.ParseBool(parts[2])
				if err != nil {
					return err.Error()
				}
			}
			iter.SeekGE([]byte(strings.TrimSpace(parts[1])), trySeekUsingNext)
		case "seek-prefix-ge":
			if len(parts) != 2 && len(parts) != 3 {
				return "seek-prefix-ge <key> [<try-seek-using-next>]\n"
			}
			prefix = []byte(strings.TrimSpace(parts[1]))
			trySeekUsingNext := false
			if len(parts) == 3 {
				var err error
				trySeekUsingNext, err = strconv.ParseBool(parts[2])
				if err != nil {
					return err.Error()
				}
			}
			iter.SeekPrefixGE(prefix, prefix /* key */, trySeekUsingNext)
		case "seek-lt":
			if len(parts) != 2 {
				return "seek-lt <key>\n"
			}
			prefix = nil
			iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			prefix = nil
			iter.First()
		case "last":
			prefix = nil
			iter.Last()
		case "next":
			iter.Next()
		case "next-ignore-result":
			iter.NextIgnoreResult()
		case "prev":
			iter.Prev()
		case "set-bounds":
			if len(parts) <= 1 || len(parts) > 3 {
				return "set-bounds lower=<lower> upper=<upper>\n"
			}
			var lower []byte
			var upper []byte
			for _, part := range parts[1:] {
				arg := strings.Split(strings.TrimSpace(part), "=")
				switch arg[0] {
				case "lower":
					lower = []byte(arg[1])
					if len(lower) == 0 {
						lower = nil
					}
				case "upper":
					upper = []byte(arg[1])
					if len(upper) == 0 {
						upper = nil
					}
				default:
					return fmt.Sprintf("set-bounds: unknown arg: %s", arg)
				}
			}
			iter.SetBounds(lower, upper)
		case "stats":
			fmt.Fprintf(&b, "%+v\n", iter.Stats())
			continue
		case "reset-stats":
			iter.ResetStats()
			continue
		}
		if iter.Valid() && checkValidPrefix(prefix, iter.Key().UserKey) {
			fmt.Fprintf(&b, "<%s:%d>", iter.Key().UserKey, iter.Key().SeqNum())
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "<err=%v>", err)
		} else {
			fmt.Fprintf(&b, ".")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func runRewriteCmd(
	td *datadriven.TestData, r *Reader, writerOpts WriterOptions,
) (*WriterMetadata, *Reader, error) {
	var from, to []byte
	for _, arg := range td.CmdArgs {
		switch arg.Key {
		case "from":
			from = []byte(arg.Vals[0])
		case "to":
			to = []byte(arg.Vals[0])
		}
	}
	if from == nil || to == nil {
		return nil, r, errors.New("missing from/to")
	}

	opts := writerOpts
	if err := optsFromArgs(td, &opts); err != nil {
		return nil, r, err
	}

	f := &memFile{}
	meta, err := rewriteKeySuffixesInBlocks(r, f, opts, from, to, 2)
	if err != nil {
		return nil, r, errors.Wrap(err, "rewrite failed")
	}
	readerOpts := ReaderOptions{Comparer: opts.Comparer}
	if opts.FilterPolicy != nil {
		readerOpts.Filters = map[string]FilterPolicy{
			opts.FilterPolicy.Name(): opts.FilterPolicy,
		}
	}
	r.Close()

	r, err = NewMemReader(f.Data(), readerOpts)
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/colblk"
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
		case "comparer":
			var err error
			if writerOpts.Comparer, err = comparerFromCmdArg(arg); err != nil {
				return err
			}
		case "writing-to-lowest-level":
			writerOpts.WritingToLowestLevel = true
		case "is-strict-obsolete":
			writerOpts.IsStrictObsolete = true
		}
	}
	if writerOpts.Comparer == nil {
		writerOpts.Comparer = testkeys.Comparer
	}
	if writerOpts.KeySchema == nil {
		s := colblk.DefaultKeySchema(writerOpts.Comparer, 16)
		writerOpts.KeySchema = &s
	}
	return nil
}

func comparerFromCmdArg(arg datadriven.CmdArg) (*Comparer, error) {
	switch arg.Vals[0] {
	case "split-4b-suffix":
		return test4bSuffixComparer, nil
	case "testkeys":
		return testkeys.Comparer, nil
	case "default":
		return base.DefaultComparer, nil
	default:
		return nil, errors.Errorf("unknown comparer: %s", arg.Vals[0])
	}
}

func runBuildMemObjCmd(
	td *datadriven.TestData, writerOpts *WriterOptions,
) (*WriterMetadata, *objstorage.MemObj, error) {
	obj := &objstorage.MemObj{}
	if err := optsFromArgs(td, writerOpts); err != nil {
		return nil, nil, err
	}

	w := NewRawWriter(obj, *writerOpts)
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	if err := writeKVs(w, td.Input); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	meta, err := w.Metadata()
	w = nil
	if err != nil {
		return nil, nil, err
	}
	return meta, obj, nil
}

func writeKVs(w RawWriter, input string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("%v", r)
		}
	}()
	for _, data := range strings.Split(input, "\n") {
		switch {
		case strings.HasPrefix(data, "Span:"):
			err = w.EncodeSpan(keyspan.ParseSpan(strings.TrimPrefix(data, "Span:")))
		default:
			forceObsolete := strings.HasPrefix(data, "force-obsolete:")
			if forceObsolete {
				data = strings.TrimSpace(strings.TrimPrefix(data, "force-obsolete:"))
			}
			j := strings.Index(data, ":")
			key := base.ParseInternalKey(data[:j])
			value := []byte(data[j+1:])

			switch key.Kind() {
			case InternalKeyKindRangeDelete:
				if forceObsolete {
					return errors.Errorf("force-obsolete is not allowed for RANGEDEL")
				}
				err = w.Add(key, value, false /* forceObsolete */)
			default:
				if bytes.HasPrefix(value, []byte("blobInlineHandle(")) {
					var handle blob.InlineHandle
					var attr base.ShortAttribute
					handle, attr, err = decodeBlobInlineHandleAndAttribute(string(value))
					if err != nil {
						return err
					}
					err = w.AddWithBlobHandle(key, handle, attr, forceObsolete)
				} else {
					err = w.Add(key, value, forceObsolete)
				}
			}
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}
	return err
}

// decodeBlobInlineHandleAndAttribute decodes a blob handle (in its inline form)
// and its short attribute from a debug string. It expects a value of the form:
// blobInlineHandle(<refIndex>, blk<blocknum>, <offset>, <valLen>, <attr>). For example:
//
//	blobInlineHandle(24, blk255, 10, 9235, 0x07)
func decodeBlobInlineHandleAndAttribute(
	ref string,
) (blob.InlineHandle, base.ShortAttribute, error) {
	fields := strings.FieldsFunc(strings.TrimSuffix(strings.TrimPrefix(ref, "blobInlineHandle("), ")"),
		func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
	if len(fields) != 5 {
		return blob.InlineHandle{}, base.ShortAttribute(0), errors.New("expected 5 fields")
	}
	refIdx, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return blob.InlineHandle{}, base.ShortAttribute(0), errors.Wrap(err, "failed to parse file offset")
	}
	blockNum, err := strconv.ParseUint(strings.TrimPrefix(fields[1], "blk"), 10, 32)
	if err != nil {
		return blob.InlineHandle{}, base.ShortAttribute(0), errors.Wrap(err, "failed to parse block number")
	}
	off, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		return blob.InlineHandle{}, base.ShortAttribute(0), errors.Wrap(err, "failed to parse offset")
	}
	valLen, err := strconv.ParseUint(fields[3], 10, 32)
	if err != nil {
		return blob.InlineHandle{}, base.ShortAttribute(0), errors.Wrap(err, "failed to parse value length")
	}
	attr, err := hex.DecodeString(strings.TrimPrefix(fields[4], "0x"))
	if err != nil {
		return blob.InlineHandle{}, base.ShortAttribute(0), errors.Wrap(err, "failed to parse attribute")
	}
	return blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			ReferenceID: blob.ReferenceID(refIdx),
			ValueLen:    uint32(valLen),
		},
		HandleSuffix: blob.HandleSuffix{
			BlockNum:      uint32(blockNum),
			OffsetInBlock: uint32(off),
		},
	}, base.ShortAttribute(attr[0]), nil
}

func runBuildCmd(
	td *datadriven.TestData, writerOpts *WriterOptions, cacheHandle *cache.Handle,
) (*WriterMetadata, *Reader, error) {
	meta, obj, err := runBuildMemObjCmd(td, writerOpts)
	if err != nil {
		return nil, nil, err
	}
	r, err := openReader(obj, writerOpts, cacheHandle)
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

func openReader(
	obj *objstorage.MemObj, writerOpts *WriterOptions, cacheHandle *cache.Handle,
) (*Reader, error) {
	readerOpts := ReaderOptions{
		Comparer:   writerOpts.Comparer,
		KeySchemas: KeySchemas{writerOpts.KeySchema.Name: writerOpts.KeySchema},
	}
	if writerOpts.FilterPolicy != nil {
		readerOpts.Filters = map[string]FilterPolicy{
			writerOpts.FilterPolicy.Name(): writerOpts.FilterPolicy,
		}
	}
	readerOpts.CacheOpts = sstableinternal.CacheOptions{CacheHandle: cacheHandle}
	r, err := NewMemReader(obj.Data(), readerOpts)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func runBuildRawCmd(
	td *datadriven.TestData, opts *WriterOptions,
) (*WriterMetadata, *Reader, error) {
	if err := optsFromArgs(td, opts); err != nil {
		return nil, nil, err
	}

	mem := vfs.NewMem()
	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(mem, "" /* dirName */))
	if err != nil {
		return nil, nil, err
	}
	defer provider.Close()

	f0, _, err := provider.Create(context.Background(), base.FileTypeTable, base.DiskFileNum(0), objstorage.CreateOptions{})
	if err != nil {
		return nil, nil, err
	}

	w := NewRawWriter(f0, *opts)
	defer func() {
		if w != nil {
			_ = w.Close()
		}
	}()
	for _, data := range strings.Split(td.Input, "\n") {
		if strings.HasPrefix(data, "Span:") {
			data = strings.TrimPrefix(data, "Span:")
			if err := w.EncodeSpan(keyspan.ParseSpan(data)); err != nil {
				return nil, nil, err
			}
			continue
		}

		j := strings.Index(data, ":")
		key := base.ParseInternalKey(data[:j])
		value := []byte(data[j+1:])
		if err := w.Add(key, value, false); err != nil {
			return nil, nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}
	meta, err := w.Metadata()
	w = nil
	if err != nil {
		return nil, nil, err
	}

	f1, err := provider.OpenForReading(context.Background(), base.FileTypeTable, base.DiskFileNum(0), objstorage.OpenOptions{})
	if err != nil {
		return nil, nil, err
	}
	r, err := NewReader(context.Background(), f1, ReaderOptions{
		Comparer:   opts.Comparer,
		KeySchemas: KeySchemas{opts.KeySchema.Name: opts.KeySchema},
	})
	if err != nil {
		return nil, nil, err
	}
	return meta, r, nil
}

type runIterCmdOption func(*runIterCmdOptions)

type runIterCmdOptions struct {
	showCommands  bool
	everyOp       func(io.Writer)
	stats         *base.InternalIteratorStats
	maskingFilter TestKeysMaskingFilter
}

func runIterCmdEveryOp(everyOp func(io.Writer)) runIterCmdOption {
	return func(opts *runIterCmdOptions) { opts.everyOp = everyOp }
}

func runIterCmdStats(stats *base.InternalIteratorStats) runIterCmdOption {
	return func(opts *runIterCmdOptions) { opts.stats = stats }
}

var runIterCmdShowCommands = func(opts *runIterCmdOptions) { opts.showCommands = true }

// runIterCmdMaskingFilter associates a masking filter and enables use of the
// "with-masking" command.
func runIterCmdMaskingFilter(maskingFilter TestKeysMaskingFilter) runIterCmdOption {
	return func(opts *runIterCmdOptions) { opts.maskingFilter = maskingFilter }
}

func runIterCmd(
	td *datadriven.TestData, iter Iterator, printValue bool, opt ...runIterCmdOption,
) string {
	var opts runIterCmdOptions
	for _, o := range opt {
		o(&opts)
	}

	defer iter.Close()

	var b bytes.Buffer
	var prefix []byte
	var maskingSuffix []byte
	var kv *base.InternalKV
	skipMaskedKeys := func(direction int, kv *base.InternalKV) *base.InternalKV {
		if len(maskingSuffix) == 0 {
			return kv
		}
		for kv != nil {
			k := kv.K.UserKey
			suffix := k[testkeys.Comparer.Split(k):]
			if len(suffix) == 0 || testkeys.Comparer.CompareRangeSuffixes(suffix, maskingSuffix) <= 0 {
				return kv
			}
			if direction > 0 {
				kv = iter.Next()
			} else {
				kv = iter.Prev()
			}
		}
		return kv
	}
	lines := crstrings.Lines(td.Input)
	maxCmdLen := 1
	for _, line := range lines {
		maxCmdLen = max(maxCmdLen, len(line))
	}
	for _, line := range lines {
		parts := strings.Fields(line)
		switch parts[0] {
		case "seek-ge":
			if len(parts) < 2 || len(parts) > 3 {
				return "seek-ge <key> [<try-seek-using-next]\n"
			}
			prefix = nil
			var flags base.SeekGEFlags
			if len(parts) == 3 {
				if trySeekUsingNext, err := strconv.ParseBool(parts[2]); err != nil {
					return err.Error()
				} else if trySeekUsingNext {
					flags = flags.EnableTrySeekUsingNext()
				}
			}
			kv = iter.SeekGE([]byte(strings.TrimSpace(parts[1])), flags)
			kv = skipMaskedKeys(+1, kv)
		case "seek-prefix-ge":
			if len(parts) != 2 && len(parts) != 3 {
				return "seek-prefix-ge <key> [<try-seek-using-next>]\n"
			}
			prefix = []byte(strings.TrimSpace(parts[1]))
			var flags base.SeekGEFlags
			if len(parts) == 3 {
				if trySeekUsingNext, err := strconv.ParseBool(parts[2]); err != nil {
					return err.Error()
				} else if trySeekUsingNext {
					flags = flags.EnableTrySeekUsingNext()
				}
			}
			kv = iter.SeekPrefixGE(prefix, prefix /* key */, flags)
			kv = skipMaskedKeys(+1, kv)
		case "seek-lt":
			if len(parts) != 2 {
				return "seek-lt <key>\n"
			}
			prefix = nil
			kv = iter.SeekLT([]byte(strings.TrimSpace(parts[1])), base.SeekLTFlagsNone)
			kv = skipMaskedKeys(-1, kv)
		case "first":
			prefix = nil
			kv = iter.First()
			kv = skipMaskedKeys(+1, kv)
		case "last":
			prefix = nil
			kv = iter.Last()
			kv = skipMaskedKeys(-1, kv)
		case "next":
			kv = iter.Next()
			kv = skipMaskedKeys(+1, kv)
		case "next-ignore-result":
			_ = iter.Next()
		case "prev":
			kv = iter.Prev()
			kv = skipMaskedKeys(-1, kv)
		case "next-prefix":
			if len(parts) != 1 {
				return "next-prefix should have no parameter\n"
			}
			if kv == nil {
				return "next-prefix cannot be called on exhauster iterator\n"
			}
			k := kv.K.UserKey
			prefixLen := testkeys.Comparer.Split(k)
			k = k[:prefixLen]
			kSucc := testkeys.Comparer.ImmediateSuccessor(nil, k)
			kv = iter.NextPrefix(kSucc)
			kv = skipMaskedKeys(+1, kv)
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
			kv = nil
		case "stats":
			// The timing is non-deterministic, so set to 0.
			opts.stats.BlockReadDuration = 0
			fmt.Fprintf(&b, "%+v\n", *opts.stats)
			continue
		case "reset-stats":
			*opts.stats = base.InternalIteratorStats{}
			continue
		case "mask-suffix":
			maskingSuffix = []byte(parts[1])
			if err := opts.maskingFilter.SetSuffix(maskingSuffix); err != nil {
				return fmt.Sprintf("set-suffix error: %s", err)
			}
			continue
		case "internal-iter-state":
			fmt.Fprintf(&b, "| %T:\n", iter)
			si, _ := iter.(*singleLevelIteratorRowBlocks)
			if twoLevelIter, ok := iter.(*twoLevelIteratorRowBlocks); ok {
				si = &twoLevelIter.secondLevel
				if twoLevelIter.topLevelIndex.Valid() {
					fmt.Fprintf(&b, "|  topLevelIndex.Key() = %q\n", twoLevelIter.topLevelIndex.Separator())
					bhp, err := twoLevelIter.topLevelIndex.BlockHandleWithProperties()
					if err != nil {
						fmt.Fprintf(&b, "|  topLevelIndex entry failed to decode as BHP: %s\n", err)
					} else {
						fmt.Fprintf(&b, "|  topLevelIndex.BlockHandleWithProperties() = (Offset: %d, Length: %d, Props: %x)\n",
							bhp.Offset, bhp.Length, bhp.Props)
					}
				} else {
					fmt.Fprintf(&b, "|  topLevelIndex iter invalid\n")
				}
				fmt.Fprintf(&b, "|  topLevelIndex.isDataInvalidated()=%t\n", twoLevelIter.topLevelIndex.IsDataInvalidated())
			}
			if si.index.Valid() {
				fmt.Fprintf(&b, "|  index.Separator() = %q\n", si.index.Separator())
				bhp, err := si.index.BlockHandleWithProperties()
				if err != nil {
					fmt.Fprintf(&b, "|  index entry failed to decode as BHP: %s\n", err)
				} else {
					fmt.Fprintf(&b, "|  index.BlockHandleWithProperties() = (Offset: %d, Length: %d, Props: %x)\n",
						bhp.Offset, bhp.Length, bhp.Props)
				}
			} else {
				fmt.Fprintf(&b, "|  index iter invalid\n")
			}
			fmt.Fprintf(&b, "|  index.isDataInvalidated()=%t\n", si.index.IsDataInvalidated())
			fmt.Fprintf(&b, "|  data.isDataInvalidated()=%t\n", si.data.IsDataInvalidated())
			fmt.Fprintf(&b, "|  hideObsoletePoints = %t\n", si.transforms.HideObsoletePoints)
			fmt.Fprintf(&b, "|  dataBH = (Offset: %d, Length: %d)\n", si.dataBH.Offset, si.dataBH.Length)
			fmt.Fprintf(&b, "|  (boundsCmp,positionedUsingLatestBounds) = (%d,%t)\n", si.boundsCmp, si.positionedUsingLatestBounds)
			fmt.Fprintf(&b, "|  exhaustedBounds = %d\n", si.exhaustedBounds)

			continue
		}
		if opts.everyOp != nil {
			opts.everyOp(&b)
		}
		var v []byte
		err := iter.Error()
		if err == nil && kv != nil {
			v, _, err = kv.Value(nil)
		}

		if opts.showCommands {
			fmt.Fprintf(&b, "%*s: ", min(maxCmdLen, 40), line)
		}
		if err != nil {
			fmt.Fprintf(&b, "<err=%v>", err)
		} else if kv != nil && checkValidPrefix(prefix, kv.K.UserKey) {
			fmt.Fprintf(&b, "<%s:%d>", kv.K.UserKey, kv.K.SeqNum())
			if printValue {
				fmt.Fprintf(&b, ":%s", string(v))
			}
		} else {
			fmt.Fprintf(&b, ".")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func runRewriteCmd(
	td *datadriven.TestData, r *Reader, sst []byte, writerOpts WriterOptions,
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

	f := &objstorage.MemObj{}
	meta, _, err := rewriteKeySuffixesInBlocks(r, sst, f, opts, from, to, 2)
	if err != nil {
		return nil, r, errors.Wrap(err, "rewrite failed")
	}
	readerOpts := ReaderOptions{
		Comparer:   opts.Comparer,
		KeySchemas: KeySchemas{opts.KeySchema.Name: opts.KeySchema},
	}
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

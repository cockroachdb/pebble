// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"context"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
)

// ReadAll returns all point keys, range del spans, and range key spans from an
// sstable. Closes the Readable. Panics on errors.
func ReadAll(
	r objstorage.Readable, ro ReaderOptions, blobValueFetcher base.ValueFetcher,
) (points []base.InternalKV, rangeDels, rangeKeys []keyspan.Span, err error) {
	reader, err := NewReader(context.Background(), r, ro)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() { _ = reader.Close() }()
	pointIter, err := reader.NewIter(NoTransforms, nil /* lower */, nil /* upper */, AssertNoBlobHandles)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() { _ = pointIter.Close() }()

	for kv := pointIter.First(); kv != nil; kv = pointIter.Next() {
		val, _, err := kv.Value(nil)
		if err != nil {
			return nil, nil, nil, err
		}
		points = append(points, base.InternalKV{
			K: kv.K.Clone(),
			V: base.MakeInPlaceValue(val),
		})
	}

	ctx := context.Background()
	rangeDelIter, err := reader.NewRawRangeDelIter(ctx, NoFragmentTransforms, block.NoReadEnv)
	if err != nil {
		return nil, nil, nil, err
	}
	if rangeDelIter != nil {
		defer rangeDelIter.Close()
		s, err := rangeDelIter.First()
		if err != nil {
			return nil, nil, nil, err
		}
		for s != nil {
			rangeDels = append(rangeDels, s.Clone())
			s, err = rangeDelIter.Next()
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}

	rangeKeyIter, err := reader.NewRawRangeKeyIter(ctx, NoFragmentTransforms, block.NoReadEnv)
	if err != nil {
		return nil, nil, nil, err
	}
	if rangeKeyIter != nil {
		defer rangeKeyIter.Close()
		s, err := rangeKeyIter.First()
		if err != nil {
			return nil, nil, nil, err
		}
		for s != nil {
			rangeKeys = append(rangeKeys, s.Clone())
			s, err = rangeKeyIter.Next()
			if err != nil {
				return nil, nil, nil, err
			}
		}
	}
	return points, rangeDels, rangeKeys, nil
}

// ParsedKVOrSpan represents a KV or a key span produced by ParseTestKVsAndSpans.
//
// There are three possibilities:
//   - key span: only the Span field is set.
//   - KV without blob value: only the Key and Value fields are set (and
//     optionally ForceObsolete).
//   - KV with blob value: only the Key, BlobHandle, and Attr fields are set
//     (and optionally ForceObsolete).
type ParsedKVOrSpan struct {
	// If Span is not nil, the rest of the fields are unset.
	Span          *keyspan.Span
	Key           base.InternalKey
	ForceObsolete bool
	// Either Value is set, or BlobHandle and Attr are set.
	Value      []byte
	BlobHandle blob.InlineHandle
	Attr       base.ShortAttribute
}

func (kv ParsedKVOrSpan) IsKeySpan() bool {
	return kv.Span != nil
}

func (kv ParsedKVOrSpan) HasBlobValue() bool {
	return kv.Span == nil && kv.Value == nil
}

func (kv ParsedKVOrSpan) String() string {
	if kv.IsKeySpan() {
		return fmt.Sprintf("Span: %s", kv.Span)
	}
	prefix := crstrings.If(kv.ForceObsolete, "force-obsolete: ")
	if !kv.HasBlobValue() {
		return fmt.Sprintf("%s%s = %s", prefix, kv.Key, kv.Value)
	}
	return fmt.Sprintf("%s%s = blobInlineHandle(%d, blk%d, %d, %d, 0x%02x)", prefix, kv.Key,
		kv.BlobHandle.ReferenceID, kv.BlobHandle.BlockNum, kv.BlobHandle.OffsetInBlock, kv.BlobHandle.ValueLen, kv.Attr,
	)
}

// ParseTestKVsAndSpans parses a multi-line string that defines SSTable contents.
// The lines can be either key-value pairs or key spans.
// Sample input showing the format:
//
//	a#1,SET = a
//	force-obsolete: d#2,SET = d
//	f#3,SET = blobInlineHandle(0, blk1, 10, 100, 0x07)
//	Span: d-e:{(#4,RANGEDEL)}
//	Span: a-d:{(#11,RANGEKEYSET,@10,foo)}
//	Span: g-l:{(#5,RANGEDEL)}
//	Span: y-z:{(#12,RANGEKEYSET,@11,foo)}
//
// Note that the older KV format "<user-key>.<kind>.<seq-num> : <value>" is also supported
// (for now).
func ParseTestKVsAndSpans(input string) (_ []ParsedKVOrSpan, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Newf("%v\n%s", r, debug.Stack())
		}
	}()
	var result []ParsedKVOrSpan
	for _, line := range crstrings.Lines(input) {
		if strings.HasPrefix(line, "Span:") {
			span := keyspan.ParseSpan(strings.TrimPrefix(line, "Span:"))
			result = append(result, ParsedKVOrSpan{Span: &span})
			continue
		}

		var kv ParsedKVOrSpan
		line, kv.ForceObsolete = strings.CutPrefix(line, "force-obsolete:")
		// There should be exactly one "=" or ":" in the remaining line.
		keyStr, valStr, ok := strings.Cut(line, "=")
		if !ok {
			keyStr, valStr, ok = strings.Cut(line, ":")
		}
		if !ok {
			return nil, errors.Newf("KV format is [force-obsolete:] <key>=<value> (or <key>:<value>): %q", line)
		}
		kv.Key = base.ParseInternalKey(strings.TrimSpace(keyStr))
		valStr = strings.TrimSpace(valStr)

		if kv.ForceObsolete && kv.Key.Kind() == InternalKeyKindRangeDelete {
			return nil, errors.Errorf("force-obsolete is not allowed for RANGEDEL")
		}

		if strings.HasPrefix(valStr, "blobInlineHandle(") {
			handle, attr, err := decodeBlobInlineHandleAndAttribute(valStr)
			if err != nil {
				return nil, err
			}
			kv.BlobHandle = handle
			kv.Attr = attr
		} else {
			kv.Value = []byte(valStr)
		}
		result = append(result, kv)
	}
	return result, nil
}

// ParseTestSST parses the KVs and spans in the input (see ParseTestKVAndSpans)
// and writes them to an sstable.
func ParseTestSST(w RawWriter, input string) error {
	kvs, err := ParseTestKVsAndSpans(input)
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		var err error
		switch {
		case kv.IsKeySpan():
			err = w.EncodeSpan(*kv.Span)
		case kv.HasBlobValue():
			err = w.AddWithBlobHandle(kv.Key, kv.BlobHandle, kv.Attr, kv.ForceObsolete)
		default:
			err = w.Add(kv.Key, kv.Value, kv.ForceObsolete)
		}
		if err != nil {
			return errors.Wrapf(err, "failed to write %s", kv)
		}
	}
	return nil
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

// ParseWriterOptions modifies WriterOptions based on the given arguments. Each
// argument is a string or a fmt.Stringer (like datadriven.TestData.CmdArg) with
// format either "<key>" or "<key>=<value>".
//
// Note that the test can specify a table format. If a format is already
// specified in WriterOptions, the test format must not be newer than that.
func ParseWriterOptions[StringOrStringer any](o *WriterOptions, args ...StringOrStringer) error {
	for _, arg := range args {
		str, ok := any(arg).(string)
		if !ok {
			str = any(arg).(fmt.Stringer).String()
		}
		key, value, _ := strings.Cut(str, "=")
		var err error
		switch key {
		case "table-format":
			var tableFormat TableFormat
			tableFormat, err = ParseTableFormatString("(" + value + ")")
			if err != nil {
				return err
			}
			if o.TableFormat != 0 && o.TableFormat < tableFormat {
				return errors.Errorf("table format %s is newer than default format %s", tableFormat, o.TableFormat)
			}
			o.TableFormat = tableFormat

		case "block-size":
			o.BlockSize, err = strconv.Atoi(value)

		case "index-block-size":
			o.IndexBlockSize, err = strconv.Atoi(value)

		case "filter":
			o.FilterPolicy = bloom.FilterPolicy(10)

		case "comparer":
			o.Comparer, err = comparerFromCmdArg(value)

		case "writing-to-lowest-level":
			o.WritingToLowestLevel = true

		case "is-strict-obsolete":
			o.IsStrictObsolete = true

		case "format", "leveldb":
			return errors.Errorf("%q is deprecated", key)

		default:
			// TODO(radu): ignoring unknown keys is error-prone; we need to find an
			// easy way for the upper layer to extract its own arguments.
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func comparerFromCmdArg(value string) (*Comparer, error) {
	switch value {
	case "split-4b-suffix":
		return test4bSuffixComparer, nil
	case "testkeys":
		return testkeys.Comparer, nil
	case "default":
		return base.DefaultComparer, nil
	default:
		return nil, errors.Errorf("unknown comparer: %s", value)
	}
}

var test4bSuffixComparer = func() *base.Comparer {
	c := new(base.Comparer)
	*c = *base.DefaultComparer
	c.Split = func(key []byte) int {
		if len(key) > 4 {
			return len(key) - 4
		}
		return len(key)
	}
	c.Name = "comparer-split-4b-suffix"
	return c
}()

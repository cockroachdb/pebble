// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package blobtest contains helpers for interacting with value separation and
// blob files in tests.
package blobtest

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
)

// Values is a helper for using blob handles in tests. It supports parsing a
// human-readable string describing a blob handle, synthesizing unspecified
// fields, and tracking the blob handle to support future fetches.
type Values struct {
	mostRecentFileNum base.DiskFileNum
	mostRecentHandles map[base.DiskFileNum]blob.Handle
	// trackedHandles maps from a blob handle to its value. The value may be nil
	// if the value was not specified (in which case Fetch will
	// deterministically derive a random value from the handle itself.)
	trackedHandles map[blob.Handle]string
}

// Fetch returns the value corresponding to the given handle.
func (bv *Values) Fetch(
	ctx context.Context, handleSuffix []byte, blobFileNum base.DiskFileNum, valLen uint32, _ []byte,
) (val []byte, callerOwned bool, err error) {
	if bv.trackedHandles == nil {
		return nil, false, errors.New("no tracked handles")
	}

	decodedHandleSuffix := blob.DecodeHandleSuffix(handleSuffix)
	decodedHandle := blob.Handle{
		FileNum:       blobFileNum,
		BlockNum:      decodedHandleSuffix.BlockNum,
		OffsetInBlock: decodedHandleSuffix.OffsetInBlock,
		ValueLen:      valLen,
	}

	value, ok := bv.trackedHandles[decodedHandle]
	if !ok {
		return nil, false, errors.Newf("unknown handle %s", decodedHandle)
	}

	// If there was not an explicitly specified value, generate a random one
	// deterministically from the file number, block number and offset in block.
	if len(value) == 0 {
		return deriveValueFromHandle(decodedHandle), false, nil
	}
	return []byte(value), false, nil
}

func deriveValueFromHandle(handle blob.Handle) []byte {
	rng := rand.New(rand.NewPCG((uint64(handle.FileNum)<<32)|uint64(handle.BlockNum), uint64(handle.OffsetInBlock)))
	return testutils.RandBytes(rng, int(handle.ValueLen))
}

// ParseInternalValue parses a debug blob handle from the string, returning the
// handle as an InternalValue and recording the handle's corresponding value.
func (bv *Values) ParseInternalValue(input string) (base.InternalValue, error) {
	h, err := bv.Parse(input)
	if err != nil {
		return base.InternalValue{}, err
	}

	// Encode the handle suffix to be the 'ValueOrHandle' of the InternalValue.
	handleSuffix := blob.HandleSuffix{
		BlockNum:      h.BlockNum,
		OffsetInBlock: h.OffsetInBlock,
	}
	handleSuffixBytes := make([]byte, blob.MaxInlineHandleLength)
	i := handleSuffix.Encode(handleSuffixBytes)

	return base.MakeLazyValue(base.LazyValue{
		ValueOrHandle: handleSuffixBytes[:i],
		Fetcher: &base.LazyFetcher{
			Fetcher: bv,
			Attribute: base.AttributeAndLen{
				ValueLen: h.ValueLen,
				// TODO(jackson): Support user-specified short attributes.
				ShortAttribute: base.ShortAttribute(h.ValueLen & 0x07),
			},
			BlobFileNum: h.FileNum,
		},
	}), nil
}

// IsBlobHandle returns true if the input string looks like it's a debug blob
// handle.
func (bv *Values) IsBlobHandle(input string) bool {
	return strings.HasPrefix(input, "blob{")
}

// Parse parses a debug blob handle from the string, returning the handle and
// recording the handle's corresponding value.
func (bv *Values) Parse(input string) (h blob.Handle, err error) {
	if bv.trackedHandles == nil {
		bv.trackedHandles = make(map[blob.Handle]string)
		bv.mostRecentHandles = make(map[base.DiskFileNum]blob.Handle)
	}

	defer func() {
		if r := recover(); r != nil {
			h, err = blob.Handle{}, errFromPanic(r)
		}
	}()
	const debugParserSeparators = `(){};=`
	p := strparse.MakeParser(debugParserSeparators, input)
	p.Expect("blob")
	p.Expect("{")
	var value string
	var fileNumSet, blockNumSet, offsetSet, valueLenSet bool
	for done := false; !done; {
		if p.Done() {
			return blob.Handle{}, errors.New("unexpected end of input")
		}
		switch x := p.Next(); x {
		case "}":
			done = true
		case "fileNum":
			p.Expect("=")
			h.FileNum = p.DiskFileNum()
			fileNumSet = true
		case "blockNum":
			p.Expect("=")
			h.BlockNum = p.Uint32()
			blockNumSet = true
		case "offset":
			p.Expect("=")
			h.OffsetInBlock = p.Uint32()
			offsetSet = true
		case "valueLen":
			p.Expect("=")
			h.ValueLen = p.Uint32()
			valueLenSet = true
		case "value":
			p.Expect("=")
			value = p.Next()
			if valueLenSet && h.ValueLen != uint32(len(value)) {
				return blob.Handle{}, errors.Newf("valueLen mismatch: %d != %d", h.ValueLen, len(value))
			}
		default:
			return blob.Handle{}, errors.Newf("unknown field: %q", x)
		}
	}

	if !fileNumSet {
		h.FileNum = bv.mostRecentFileNum
	}
	if !blockNumSet {
		h.BlockNum = bv.mostRecentHandles[h.FileNum].BlockNum
	}
	if !offsetSet {
		h.OffsetInBlock = bv.mostRecentHandles[h.FileNum].OffsetInBlock + bv.mostRecentHandles[h.FileNum].ValueLen
	}
	if !valueLenSet {
		if len(value) > 0 {
			h.ValueLen = uint32(len(value))
		} else {
			h.ValueLen = 12
		}
	}
	bv.mostRecentFileNum = h.FileNum
	bv.mostRecentHandles[h.FileNum] = h
	bv.trackedHandles[h] = value
	return h, nil
}

// ParseInlineHandle parses a debug blob handle from the string. It maps the
// file number to a reference index using the provided *BlobReferences,
// returning an inline handle.
//
// It's intended for tests that must manually construct inline blob references.
func (bv *Values) ParseInlineHandle(
	input string, references *References,
) (h blob.InlineHandle, err error) {
	fullHandle, err := bv.Parse(input)
	if err != nil {
		return blob.InlineHandle{}, err
	}
	return blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			ReferenceID: references.MapToReferenceID(fullHandle.FileNum),
			ValueLen:    fullHandle.ValueLen,
		},
		HandleSuffix: blob.HandleSuffix{
			BlockNum:      fullHandle.BlockNum,
			OffsetInBlock: fullHandle.OffsetInBlock,
		},
	}, nil
}

// WriteFiles writes all the blob files referenced by Values, using
// newBlobObject to construct new objects. Additionally, it updates the
// BlobFileMetadatas contained within metas with the resulting physical and
// logical sizes of the blob files.
func WriteFiles(
	bv *Values,
	newBlobObject func(fileNum base.DiskFileNum) (objstorage.Writable, error),
	writerOpts blob.FileWriterOptions,
	metas map[base.DiskFileNum]*manifest.BlobFileMetadata,
) error {
	// Organize the handles by file number.
	files := make(map[base.DiskFileNum][]blob.Handle)
	for handle := range bv.trackedHandles {
		files[handle.FileNum] = append(files[handle.FileNum], handle)
	}

	for fileNum, handles := range files {
		slices.SortFunc(handles, func(a, b blob.Handle) int {
			if v := cmp.Compare(a.BlockNum, b.BlockNum); v != 0 {
				return v
			}
			return cmp.Compare(a.OffsetInBlock, b.OffsetInBlock)
		})
		writable, err := newBlobObject(fileNum)
		if err != nil {
			return err
		}
		writer := blob.NewFileWriter(fileNum, writable, writerOpts)
		for i, handle := range handles {
			if i > 0 && handles[i-1].BlockNum != handle.BlockNum {
				writer.Flush()
			}
			if value, ok := bv.trackedHandles[handle]; ok {
				writer.AddValue([]byte(value))
			} else {
				writer.AddValue(deriveValueFromHandle(handle))
			}
		}
		stats, err := writer.Close()
		if err != nil {
			return err
		}
		metas[fileNum].Size = stats.FileLen
		metas[fileNum].ValueSize = stats.UncompressedValueBytes
	}
	return nil
}

// errFromPanic can be used in a recover block to convert panics into errors.
func errFromPanic(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return errors.Errorf("%v", r)
}

// References is a helper for tests that manually construct inline blob
// references. It tracks the set of file numbers used within a sstable, and maps
// each file number to a reference index (encoded within the
// blob.InlineHandlePreface).
type References struct {
	fileNums []base.DiskFileNum
}

// MapToReferenceID maps the given file number to a reference ID.
func (b *References) MapToReferenceID(fileNum base.DiskFileNum) blob.ReferenceID {
	for i, fn := range b.fileNums {
		if fn == fileNum {
			return blob.ReferenceID(i)
		}
	}
	i := uint32(len(b.fileNums))
	b.fileNums = append(b.fileNums, fileNum)
	return blob.ReferenceID(i)
}

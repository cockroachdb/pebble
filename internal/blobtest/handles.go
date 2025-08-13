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
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable/blob"
)

// Values is a helper for using blob handles in tests. It supports parsing a
// human-readable string describing a blob handle, synthesizing unspecified
// fields, and tracking the blob handle to support future fetches.
type Values struct {
	References References

	mostRecentBlobFileID base.BlobFileID
	mostRecentHandles    map[base.BlobFileID]blob.Handle
	// trackedHandles maps from a blob handle to its value. The value may be nil
	// if the value was not specified (in which case Fetch will
	// deterministically derive a random value from the handle itself.)
	trackedHandles map[blob.Handle]string
}

// FetchHandle returns the value corresponding to the given handle.
func (bv *Values) FetchHandle(
	ctx context.Context, handleSuffix []byte, blobFileID base.BlobFileID, valLen uint32, _ []byte,
) (val []byte, callerOwned bool, err error) {
	if bv.trackedHandles == nil {
		return nil, false, errors.New("no tracked handles")
	}

	decodedHandleSuffix := blob.DecodeHandleSuffix(handleSuffix)
	decodedHandle := blob.Handle{
		BlobFileID: blobFileID,
		ValueLen:   valLen,
		BlockID:    decodedHandleSuffix.BlockID,
		ValueID:    decodedHandleSuffix.ValueID,
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
	rng := rand.New(rand.NewPCG((uint64(handle.BlobFileID)<<32)|uint64(handle.BlockID), uint64(handle.ValueID)))
	b := make([]byte, handle.ValueLen)
	for i := range b {
		b[i] = 'a' + byte(rng.IntN(26))
	}
	return b
}

// ParseInternalValue parses a debug blob handle from the string, returning the
// handle as an InternalValue and recording the handle's corresponding value.
func (bv *Values) ParseInternalValue(input string) (base.InternalValue, error) {
	h, _, err := bv.Parse(input)
	if err != nil {
		return base.InternalValue{}, err
	}

	// Encode the handle suffix to be the 'ValueOrHandle' of the InternalValue.
	handleSuffix := blob.HandleSuffix{
		BlockID: h.BlockID,
		ValueID: h.ValueID,
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
			BlobFileID: h.BlobFileID,
		},
	}), nil
}

// IsBlobHandle returns true if the input string looks like it's a debug blob
// handle.
func IsBlobHandle(input string) bool {
	return strings.HasPrefix(input, "blob{")
}

// Parse parses a debug blob handle from the string, returning the handle and
// recording the handle's corresponding value.
func (bv *Values) Parse(input string) (h blob.Handle, remaining string, err error) {
	if bv.trackedHandles == nil {
		bv.trackedHandles = make(map[blob.Handle]string)
		bv.mostRecentHandles = make(map[base.BlobFileID]blob.Handle)
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
	var fileNumSet, blockIDSet, valueLenSet, valueIDSet bool
	for done := false; !done; {
		if p.Done() {
			return blob.Handle{}, "", errors.New("unexpected end of input")
		}
		switch x := p.Next(); x {
		case "}":
			done = true
		case "fileNum":
			p.Expect("=")
			h.BlobFileID = base.BlobFileID(p.Uint64())
			fileNumSet = true
		case "blockID":
			p.Expect("=")
			h.BlockID = blob.BlockID(p.Uint32())
			blockIDSet = true
		case "valueID":
			p.Expect("=")
			h.ValueID = blob.BlockValueID(p.Uint32())
			valueIDSet = true
		case "valueLen":
			p.Expect("=")
			h.ValueLen = p.Uint32()
			valueLenSet = true
		case "value":
			p.Expect("=")
			value = p.Next()
			if valueLenSet && h.ValueLen != uint32(len(value)) {
				return blob.Handle{}, "", errors.Newf("valueLen mismatch: %d != %d", h.ValueLen, len(value))
			}
		default:
			return blob.Handle{}, "", errors.Newf("unknown field: %q", x)
		}
	}

	if !fileNumSet {
		h.BlobFileID = bv.mostRecentBlobFileID
	}
	if !blockIDSet {
		h.BlockID = bv.mostRecentHandles[h.BlobFileID].BlockID
	}
	if !valueIDSet {
		if recentHandle, ok := bv.mostRecentHandles[h.BlobFileID]; ok {
			h.ValueID = recentHandle.ValueID + 1
		} else {
			h.ValueID = 0
		}
	}
	if !valueLenSet {
		if len(value) > 0 {
			h.ValueLen = uint32(len(value))
		} else {
			h.ValueLen = 12
		}
	}
	bv.mostRecentBlobFileID = h.BlobFileID
	bv.mostRecentHandles[h.BlobFileID] = h
	bv.trackedHandles[h] = value
	return h, p.Remaining(), nil
}

// ParseInlineHandle parses a debug blob handle from the string. It maps the
// file number to a reference index using the provided *BlobReferences,
// returning an inline handle.
//
// It's intended for tests that must manually construct inline blob references.
func (bv *Values) ParseInlineHandle(
	input string,
) (h blob.InlineHandle, remaining string, err error) {
	fullHandle, remaining, err := bv.Parse(input)
	if err != nil {
		return blob.InlineHandle{}, "", err
	}
	h = blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			ReferenceID: bv.References.MapToReferenceID(fullHandle.BlobFileID),
			ValueLen:    fullHandle.ValueLen,
		},
		HandleSuffix: blob.HandleSuffix{
			BlockID: fullHandle.BlockID,
			ValueID: fullHandle.ValueID,
		},
	}
	return h, remaining, nil
}

// IsEmpty returns true if the Values has no tracked handles.
func (bv *Values) IsEmpty() bool {
	return len(bv.trackedHandles) == 0
}

// WriteFiles writes all the blob files referenced by Values, using
// newBlobObject to construct new objects.
//
// Return the FileWriterStats for the written blob files.
func (bv *Values) WriteFiles(
	newBlobObject func(fileNum base.DiskFileNum) (objstorage.Writable, error),
	writerOpts blob.FileWriterOptions,
) (map[base.DiskFileNum]blob.FileWriterStats, error) {
	// Organize the handles by file number.
	files := make(map[base.DiskFileNum][]blob.Handle)
	for handle := range bv.trackedHandles {
		diskFileNum := base.DiskFileNum(handle.BlobFileID)
		files[diskFileNum] = append(files[diskFileNum], handle)
	}

	stats := make(map[base.DiskFileNum]blob.FileWriterStats)
	for fileNum, handles := range files {
		slices.SortFunc(handles, func(a, b blob.Handle) int {
			if v := cmp.Compare(a.BlockID, b.BlockID); v != 0 {
				return v
			}
			return cmp.Compare(a.ValueID, b.ValueID)
		})
		writable, err := newBlobObject(fileNum)
		if err != nil {
			return nil, err
		}
		writer := blob.NewFileWriter(fileNum, writable, writerOpts)
		prevID := -1
		for i, handle := range handles {
			if i > 0 && handles[i-1].BlockID != handle.BlockID {
				writer.FlushForTesting()
				prevID = -1
			}
			// The user of a blobtest.Values may specify a value ID for a handle. If
			// there's a gap in the value IDs, we need to fill in the missing values
			// with synthesized values.
			prevID++
			for prevID < int(handle.ValueID) {
				writer.AddValue(deriveValueFromHandle(blob.Handle{
					BlobFileID: base.BlobFileID(fileNum),
					BlockID:    handle.BlockID,
					ValueID:    blob.BlockValueID(prevID),
					ValueLen:   12,
				}), base.TieringMeta{})
				prevID++
			}

			if value, ok := bv.trackedHandles[handle]; ok {
				writer.AddValue([]byte(value), base.TieringMeta{})
			} else {
				writer.AddValue(deriveValueFromHandle(handle), base.TieringMeta{})
			}
		}
		fileStats, err := writer.Close()
		if err != nil {
			return nil, err
		}
		stats[fileNum] = fileStats
	}
	return stats, nil
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
	fileIDs []base.BlobFileID
}

// MapToReferenceID maps the given file number to a reference ID.
func (b *References) MapToReferenceID(fileID base.BlobFileID) base.BlobReferenceID {
	for i, fn := range b.fileIDs {
		if fn == fileID {
			return base.BlobReferenceID(i)
		}
	}
	i := uint32(len(b.fileIDs))
	b.fileIDs = append(b.fileIDs, fileID)
	return base.BlobReferenceID(i)
}

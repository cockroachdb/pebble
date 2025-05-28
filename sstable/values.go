// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/valblk"
)

// AssertNoBlobHandles is a TableBlobContext that configures a sstable iterator
// to panic if it ever encounters a value that references an external blob file.
var AssertNoBlobHandles = TableBlobContext{
	ValueFetcher: base.NoBlobFetches,
	// Passing a nil BlobReferences will cause any attempt to construct an
	// InternalValue from a blob handle to panic.
	References: nil,
}

// DebugHandlesBlobContext is a TableBlobContext that configures a sstable
// iterator to return the partially decoded inline blob handle upon encountering
// a value that references an external blob file.
var DebugHandlesBlobContext = TableBlobContext{
	ValueFetcher: nil,
	References:   nil,
	// Passing a non-nil BlobHandleFn will return the partially decoded inline
	// handle before panicking due to missing BlobReferences.
	BlobHandleFn: func(preface blob.InlineHandlePreface, remainder []byte) base.InternalValue {
		handleSuffix := blob.DecodeHandleSuffix(remainder)
		ih := blob.InlineHandle{
			InlineHandlePreface: preface,
			HandleSuffix:        handleSuffix,
		}
		return base.MakeInPlaceValue([]byte(ih.String()))
	},
}

// LoadValBlobContext returns a TableBlobContext that configures a
// sstable iterator to fetch the value stored in a blob file. It is the
// caller's responsibility to close the ValueFetcher returned.
func LoadValBlobContext(
	rp blob.ReaderProvider, blobRefs BlobReferences,
) (*blob.ValueFetcher, TableBlobContext) {
	vf := &blob.ValueFetcher{}
	vf.Init(rp, block.ReadEnv{})
	return vf, TableBlobContext{
		ValueFetcher: vf,
		References:   blobRefs,
	}
}

// BlobReferences provides a mapping from an index to a file number for a
// sstable's blob references. In practice, this is implemented by
// manifest.BlobReferences.
type BlobReferences interface {
	// FileNumByID returns the FileNum for the identified BlobReference.
	FileNumByID(i blob.ReferenceID) base.DiskFileNum
	// IDByFileNum returns the reference ID for the given FileNum. If the file
	// number is not found, the second return value is false.
	IDByFileNum(fileNum base.DiskFileNum) (blob.ReferenceID, bool)
}

// TableBlobContext configures how values that reference external blob files
// should be retrieved and handled.
type TableBlobContext struct {
	// ValueFetcher is used as the base.ValueFetcher for values returned that
	// reference external blob files.
	ValueFetcher base.ValueFetcher
	// References provides a mapping from an index to a file number for a
	// sstable's blob references.
	References BlobReferences
	// BlobHandleFn is an optional function that is invoked after an inline blob
	// handle has had its preface decoded. Note that if this function is set,
	// we will not do any work in making a lazy value.
	BlobHandleFn func(preface blob.InlineHandlePreface, remainder []byte) base.InternalValue
}

// defaultInternalValueConstructor is the default implementation of the
// block.GetInternalValueForPrefixAndValueHandler interface.
type defaultInternalValueConstructor struct {
	blobContext TableBlobContext
	env         *block.ReadEnv
	vbReader    valblk.Reader

	// lazyFetcher is the LazyFetcher value embedded in any LazyValue that we
	// return. It is used to avoid having a separate allocation for that.
	lazyFetcher base.LazyFetcher
}

// Assert that defaultInternalValueConstructor implements the
// block.GetInternalValueForPrefixAndValueHandler interface.
var _ block.GetInternalValueForPrefixAndValueHandler = (*defaultInternalValueConstructor)(nil)

// GetInternalValueForPrefixAndValueHandle returns a InternalValue for the
// given value prefix and value.
//
// The result is only valid until the next call to
// GetInternalValueForPrefixAndValueHandle. Use InternalValue.Clone if the
// lifetime of the InternalValue needs to be extended. For more details, see
// the "memory management" comment where LazyValue is declared.
func (i *defaultInternalValueConstructor) GetInternalValueForPrefixAndValueHandle(
	handle []byte,
) base.InternalValue {
	vp := block.ValuePrefix(handle[0])
	if vp.IsValueBlockHandle() {
		return i.vbReader.GetInternalValueForPrefixAndValueHandle(handle)
	} else if !vp.IsBlobValueHandle() {
		panic(errors.AssertionFailedf("block: %x is neither a valblk or blob handle prefix", vp))
	}

	// The first byte of [handle] is the valuePrefix byte.
	//
	// After that, is the inline-handle preface encoding a) the length of the
	// value and b) the blob reference index. We need to map the blob reference
	// index into a file number,
	//
	// The remainder of the handle (the suffix) encodes the value's location
	// within the blob file. We defer parsing of it until the user retrieves the
	// value. We propagate it as LazyValue.ValueOrHandle.
	preface, remainder := blob.DecodeInlineHandlePreface(handle[1:])

	// If BlobHandleFn is specified, we don't care about our value at all; we
	// just need to return what we have already decoded for our inline blob
	// handle.
	if i.blobContext.BlobHandleFn != nil {
		return i.blobContext.BlobHandleFn(preface, remainder)
	}

	// We can't convert a blob handle into an InternalValue without
	// BlobReferences providing the mapping of a reference index to a blob file
	// number.
	if i.blobContext.References == nil {
		panic(errors.AssertionFailedf("blob references not configured"))
	}

	if i.env.Stats != nil {
		// TODO(jackson): Add stats to differentiate between blob values and
		// value-block values.
		i.env.Stats.SeparatedPointValue.Count++
		i.env.Stats.SeparatedPointValue.ValueBytes += uint64(preface.ValueLen)
	}

	fetcher := i.blobContext.ValueFetcher
	if fetcher == nil {
		fetcher = base.NoBlobFetches
	}

	i.lazyFetcher = base.LazyFetcher{
		Fetcher: fetcher,
		Attribute: base.AttributeAndLen{
			ValueLen:       preface.ValueLen,
			ShortAttribute: vp.ShortAttribute(),
		},
		BlobFileNum: i.blobContext.References.FileNumByID(preface.ReferenceID),
	}
	return base.MakeLazyValue(base.LazyValue{
		ValueOrHandle: remainder,
		Fetcher:       &i.lazyFetcher,
	})
}

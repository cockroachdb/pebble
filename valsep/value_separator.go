// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package valsep

import (
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
)

type valueSeparationMode uint8

const (
	// preserveAllHotBlobReferences preserves existing blob references and
	// does not write any new blob files. Input references to existing blob
	// references will be preserved and metadata about the table's blob
	// references will be collected.
	preserveAllHotBlobReferences valueSeparationMode = iota
	// rewriteAllHotBlobReferences rewrites all existing blob references.
	// When writing new blob files, we will always separate potential MVCC garbage
	// values into external blob files. MVCC garbage values are determined on a
	// best-effort basis; see comments in sstable.IsLikelyMVCCGarbage for the
	// exact criteria we use.
	rewriteAllHotBlobReferences
)

// ValueSeparator can function in any of the valueSeparationModes to write
// new or preserve blob references when writing an sstable. FinishOutput
// should be called when the output sstable is complete. The ValueSeparator
// can then be reused for the next output sstable.
// This will be extended in the future to support the writing of hot and cold
// blob files. All blob references are currently written to the hot tier.
type ValueSeparator struct {
	mode valueSeparationMode
	// inputBlobPhysicalFiles holds the *PhysicalBlobFile for every unique blob
	// file referenced by input sstables.
	inputBlobPhysicalFiles   map[base.BlobFileID]*manifest.PhysicalBlobFile
	outputBlobReferenceDepth manifest.BlobReferenceDepth
	comparer                 *base.Comparer
	// newBlobObject constructs a new blob object for use in the compaction.
	newBlobObject      func() (objstorage.Writable, objstorage.ObjectMetadata, error)
	shortAttrExtractor base.ShortAttributeExtractor
	// writerOpts is used to configure all constructed blob writers.
	writerOpts blob.FileWriterOptions
	//
	// currentConfig holds the configuration for the current output sstable.
	// It is set to globalConfig by default and on every call to FinishOutput.
	// It may be overridden by SetNextOutputConfig (i.e, if a SpanPolicy
	// dictates a different minimum size for a span of the keyspace).
	currentConfig ValueSeparationOutputConfig
	// globalConfig holds settings that are applied globally across all outputs.
	globalConfig ValueSeparationOutputConfig
	// invalidValueCallback is called when a value is encountered for which the
	// short attribute extractor returns an error.
	invalidValueCallback func(userKey []byte, value []byte, err error)

	// state.
	buf []byte
	// disableValueSeparationBySuffix indicates whether value separation should be
	// disabled for values with certain suffixes. This is only applicable when
	// rewriting all hot blob references.
	disableValueSeparationBySuffix bool

	// currPendingReferences holds the pending references that have been referenced by
	// the current output sstable. The index of a reference with a given blob
	// file ID is the value of the base.BlobReferenceID used by its value handles
	// within the output sstable.
	currPendingReferences []pendingReference
	// blobTiers holds the state for each storage tier of blob files being written.
	//
	// INVARIANT: sum(blobTiers.totalPreservedValueSize) == sum(currPendingReferencesValueSizes[preserved]=true)
	blobTiers [base.NumStorageTiers]blobTierState
}

type pendingReference struct {
	blobFileID base.BlobFileID
	valueSize  uint64
	// preserved indicates whether this reference is being carried over
	// from an existing blob reference, or is a new reference being
	// written to a new blob file.
	preserved bool
}

type blobWriterAndMeta struct {
	fileWriter *blob.FileWriter
	objMeta    objstorage.ObjectMetadata
	// refID is the BlobReferenceID assigned to this blob file
	// within the output sstable. This is assigned when the fileWriter
	// is created.
	refID base.BlobReferenceID
}

type blobTierState struct {
	writer blobWriterAndMeta
	// totalPreservedValueSize is the sum of the sizes of all values
	// from preserved blob references written to this tier.
	totalPreservedValueSize uint64
}

var _ ValueSeparation = &ValueSeparator{}

func NewPreserveAllHotBlobReferences(
	inputBlobPhysicalFiles map[base.BlobFileID]*manifest.PhysicalBlobFile,
	outputBlobReferenceDepth manifest.BlobReferenceDepth,
	globalMinimumSize int,
) *ValueSeparator {
	config := ValueSeparationOutputConfig{
		MinimumSize: globalMinimumSize,
	}
	return &ValueSeparator{
		mode:                     preserveAllHotBlobReferences,
		inputBlobPhysicalFiles:   inputBlobPhysicalFiles,
		outputBlobReferenceDepth: outputBlobReferenceDepth,
		globalConfig:             config,
		currentConfig:            config,
	}
}

type WriteNewBlobFilesOptions struct {
	// InputBlobPhysicalFiles holds the *PhysicalBlobFile for every unique blob
	// file referenced by input sstables. This may be nil if there are no input
	// blob files to preserve.
	InputBlobPhysicalFiles         map[base.BlobFileID]*manifest.PhysicalBlobFile
	ShortAttrExtractor             base.ShortAttributeExtractor
	InvalidValueCallback           func(userKey []byte, value []byte, err error)
	DisableValueSeparationBySuffix bool
}

func NewWriteNewBlobFiles(
	comparer *base.Comparer,
	newBlobObject func() (objstorage.Writable, objstorage.ObjectMetadata, error),
	writerOpts blob.FileWriterOptions,
	globalMinimumSize int,
	opts WriteNewBlobFilesOptions,
) *ValueSeparator {
	inputBlobPhysicalFiles := opts.InputBlobPhysicalFiles
	if inputBlobPhysicalFiles == nil {
		inputBlobPhysicalFiles = make(map[base.BlobFileID]*manifest.PhysicalBlobFile)
	}
	config := ValueSeparationOutputConfig{
		MinimumSize:                    globalMinimumSize,
		DisableValueSeparationBySuffix: opts.DisableValueSeparationBySuffix,
	}
	return &ValueSeparator{
		mode:                           rewriteAllHotBlobReferences,
		inputBlobPhysicalFiles:         inputBlobPhysicalFiles,
		outputBlobReferenceDepth:       1,
		comparer:                       comparer,
		newBlobObject:                  newBlobObject,
		shortAttrExtractor:             opts.ShortAttrExtractor,
		writerOpts:                     writerOpts,
		globalConfig:                   config,
		currentConfig:                  config,
		invalidValueCallback:           opts.InvalidValueCallback,
		disableValueSeparationBySuffix: opts.DisableValueSeparationBySuffix,
	}
}

// SetNextOutputConfig implements the ValueSeparation interface.
func (vs *ValueSeparator) SetNextOutputConfig(config ValueSeparationOutputConfig) {
	vs.currentConfig = config
}

func (vs *ValueSeparator) Kind() sstable.ValueSeparationKind {
	if vs.currentConfig.MinimumSize != vs.globalConfig.MinimumSize {
		return sstable.ValueSeparationSpanPolicy
	}
	return sstable.ValueSeparationDefault
}

func (vs *ValueSeparator) MinimumSize() int {
	return vs.currentConfig.MinimumSize
}

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob files if they were closed now.
func (vs *ValueSeparator) EstimatedFileSize() uint64 {
	if vs.mode == preserveAllHotBlobReferences {
		return 0
	}
	var size uint64
	for i := range vs.blobTiers {
		writer := vs.blobTiers[i].writer
		if writer.fileWriter != nil {
			size += writer.fileWriter.EstimatedSize()
		}
	}
	return size
}

// EstimatedReferenceSize implements ValueSeparation.
func (vs *ValueSeparator) EstimatedReferenceSize() uint64 {
	// When we're writing to new blob files, the size of the blob file itself is
	// a better estimate of the disk space consumed than the uncompressed value
	// sizes, so we use vs.EstimatedFileSize.
	//
	// TODO(jackson): The totalValueSize is the uncompressed value sizes. With
	// compressible data, it overestimates the disk space consumed by the blob
	// references. It also does not include the blob file's index block or
	// footer, so it can underestimate if values are completely incompressible.
	//
	// Should we compute a compression ratio per blob file and scale the
	// references appropriately?
	size := vs.EstimatedFileSize()
	for i := range vs.blobTiers {
		size += vs.blobTiers[i].totalPreservedValueSize
	}
	return size
}

// Add implements ValueSeparation.
func (vs *ValueSeparator) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool, isLikelyMVCCGarbage bool,
) error {
	if kv.V.IsBlobValueHandle() && vs.mode == preserveAllHotBlobReferences {
		return vs.preserveBlobReference(tw, kv, forceObsolete)
	}

	// Fetch the value since either not a reference, or not preserving the reference.
	v, callerOwned, err := kv.Value(vs.buf)
	if err != nil {
		return err
	}
	if callerOwned {
		vs.buf = v[:0]
	}

	if vs.mode == preserveAllHotBlobReferences {
		// If the value is not already a blob handle (either it's in-place or in
		// a value block), we retrieve the value and write it through Add. The
		// sstable fileWriter may still decide to put the value in a value block,
		// but regardless the value will be written to the sstable itself and
		// not a blob file.
		return tw.Add(kv.K, v, forceObsolete)
	}
	// We are rewriting all hot blob references. Check that the value meets the criteria
	// for separation.

	// Only values of certain key kinds and sizes are eligible for separation.
	keyKind := kv.K.Kind()
	if keyKind != base.InternalKeyKindSet && keyKind != base.InternalKeyKindSetWithDelete {
		// Only SET and SETWITHDEL can be separated.
		return tw.Add(kv.K, v, forceObsolete)
	}

	// Values that are too small are never separated; however, MVCC keys are
	// separated if they are a SET key kind, as long as the value is not empty.
	if len(v) < vs.currentConfig.MinimumSize && (vs.disableValueSeparationBySuffix || !isLikelyMVCCGarbage) {
		return tw.Add(kv.K, v, forceObsolete)
	}

	// This KV met all the criteria and its value will be separated.
	return vs.separateValue(tw, kv, v, forceObsolete, isLikelyMVCCGarbage)
}

// separateValue separates the value into a blob file and writes a blob handle
// into the sstable. The provided value is presumed to meet the criteria for
// separation.
func (vs *ValueSeparator) separateValue(
	tw sstable.RawWriter,
	kv *base.InternalKV,
	rawValue []byte,
	forceObsolete bool,
	isLikelyMVCCGarbage bool,
) (err error) {
	if vs.mode == preserveAllHotBlobReferences {
		return errors.AssertionFailedf("separateValue called in preserveAllHotBlobReferences mode")
	}

	// If there's a configured short attribute extractor, extract the value's
	// short attribute.
	var shortAttr base.ShortAttribute
	if vs.shortAttrExtractor != nil {
		keyPrefixLen := vs.comparer.Split(kv.K.UserKey)
		shortAttr, err = vs.shortAttrExtractor(kv.K.UserKey, keyPrefixLen, rawValue)
		if err != nil {
			// Report that there was a value for which the short attribute
			// extractor was unable to extract a short attribute.
			if vs.invalidValueCallback != nil {
				vs.invalidValueCallback(kv.K.UserKey, rawValue, err)
			}

			// Rather than erroring out and aborting the flush or compaction, we
			// fallback to writing the value verbatim to the sstable. Otherwise
			// a flush could busy loop, repeatedly attempting to write the same
			// memtable and repeatedly unable to extract a key's short attribute.
			return tw.Add(kv.K, rawValue, forceObsolete)
		}
	}

	wnm, err := vs.getWriter(base.HotTier)
	if err != nil {
		return err
	}
	// Append the value to the blob file.
	handle := wnm.fileWriter.AddValue(rawValue, isLikelyMVCCGarbage)
	// Write the key and the handle to the sstable. We need to map the
	// blob.Handle into a blob.InlineHandle. Everything is copied verbatim,
	// except the FileNum is translated into a reference index.
	inlineHandle := blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			ReferenceID: wnm.refID,
			ValueLen:    handle.ValueLen,
		},
		HandleSuffix: blob.HandleSuffix{
			BlockID: handle.BlockID,
			ValueID: handle.ValueID,
		},
	}
	return tw.AddWithBlobHandle(kv.K, inlineHandle, shortAttr, forceObsolete)
}

// preserveBlobReference preserves an existing blob reference by copying it
// into the output sstable. The provided kv must have a value that is an
// existing blob handle.
func (vs *ValueSeparator) preserveBlobReference(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool,
) error {
	// We are preserving blob references and the value is an existing blob handle.
	// We can copy it into the output sstable, taking note of the reference for
	// the table metadata.
	lv := kv.V.LazyValue()
	fileID := lv.Fetcher.BlobFileID

	var refID base.BlobReferenceID
	if refIdx := slices.IndexFunc(vs.currPendingReferences, func(ref pendingReference) bool {
		return ref.blobFileID == fileID
	}); refIdx != -1 {
		refID = base.BlobReferenceID(refIdx)
	} else {
		refID = base.BlobReferenceID(len(vs.currPendingReferences))
		vs.currPendingReferences = append(vs.currPendingReferences, pendingReference{
			blobFileID: fileID,
			valueSize:  0,
			preserved:  true,
		})
	}
	if invariants.Enabled && vs.currPendingReferences[refID].blobFileID != fileID {
		panic("wrong reference index")
	}

	handleSuffix := blob.DecodeHandleSuffix(lv.ValueOrHandle)
	inlineHandle := blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			ReferenceID: refID,
			ValueLen:    lv.Fetcher.Attribute.ValueLen,
		},
		HandleSuffix: handleSuffix,
	}
	err := tw.AddWithBlobHandle(kv.K, inlineHandle, lv.Fetcher.Attribute.ShortAttribute, forceObsolete)
	if err != nil {
		return err
	}
	vs.currPendingReferences[refID].valueSize += uint64(lv.Fetcher.Attribute.ValueLen)
	vs.blobTiers[base.HotTier].totalPreservedValueSize += uint64(lv.Fetcher.Attribute.ValueLen)
	return nil
}

// closeWriters closes any open blob file writers, returning metadata about
// the newly written blob files.
func (vs *ValueSeparator) closeWriters(
	outputBlobRefs manifest.BlobReferences,
) (newBlobFiles []NewBlobFileInfo, referencedValueSize uint64, err error) {
	for i := range vs.blobTiers {
		writer := vs.blobTiers[i].writer
		if writer.fileWriter == nil {
			continue
		}
		stats, err := writer.fileWriter.Close()
		if err != nil {
			return nil, 0, err
		}
		vs.blobTiers[i].writer.fileWriter = nil
		meta := &manifest.PhysicalBlobFile{
			FileNum:      writer.objMeta.DiskFileNum,
			Size:         stats.FileLen,
			ValueSize:    stats.UncompressedValueBytes,
			CreationTime: uint64(time.Now().Unix()),
		}
		meta.PopulateProperties(&stats.Properties)
		outputBlobRefs[writer.refID] = manifest.MakeBlobReference(
			base.BlobFileID(writer.objMeta.DiskFileNum),
			stats.UncompressedValueBytes,
			stats.UncompressedValueBytes,
			meta,
		)
		referencedValueSize += stats.UncompressedValueBytes
		newBlobFiles = append(newBlobFiles, NewBlobFileInfo{
			FileStats:    stats,
			FileObject:   writer.objMeta,
			FileMetadata: meta,
		})
	}

	return newBlobFiles, referencedValueSize, nil
}

func (vs *ValueSeparator) getWriter(blobTier base.StorageTier) (*blobWriterAndMeta, error) {
	wnm := &vs.blobTiers[blobTier].writer
	if wnm.fileWriter != nil {
		return wnm, nil
	}
	writable, objMeta, err := vs.newBlobObject()
	if err != nil {
		return nil, err
	}
	wnm.objMeta = objMeta
	wnm.fileWriter = blob.NewFileWriter(objMeta.DiskFileNum, writable, vs.writerOpts)
	wnm.refID = base.BlobReferenceID(len(vs.currPendingReferences))
	vs.currPendingReferences = append(vs.currPendingReferences, pendingReference{
		blobFileID: base.BlobFileID(objMeta.DiskFileNum),
	})
	return wnm, nil
}

func (vs *ValueSeparator) maybeCheckInvariants() {
	if invariants.Enabled {
		if vs.mode == preserveAllHotBlobReferences {
			// Assert that the incrementally-maintained totalValueSize matches the
			// sum of all the reference value sizes.
			totalValueSize := uint64(0)
			for _, ref := range vs.currPendingReferences {
				if ref.preserved {
					totalValueSize += ref.valueSize
				} else {
					panic("no new references should exist in preserveAllHotBlobReferences mode")
				}
			}
			accumulatedPreservedValueSize := vs.blobTiers[base.HotTier].totalPreservedValueSize
			if totalValueSize != accumulatedPreservedValueSize {
				panic(errors.AssertionFailedf("totalPreservedValueSize mismatch: %d != %d", totalValueSize, accumulatedPreservedValueSize))
			}
		}
	}
}

// FinishOutput closes the current blob file (if any). It returns the stats
// and metadata of the now completed blob file. The state is reset to be
// ready for the next output sstable.
func (vs *ValueSeparator) FinishOutput() (ValueSeparationMetadata, error) {
	vs.maybeCheckInvariants()

	references := make(manifest.BlobReferences, len(vs.currPendingReferences))
	// Make blob references for preserved hot references. Physical files for
	// these references must exist in the input files.
	for i := range vs.currPendingReferences {
		ref := vs.currPendingReferences[i]
		if !ref.preserved {
			continue
		}
		phys, ok := vs.inputBlobPhysicalFiles[ref.blobFileID]
		if !ok {
			return ValueSeparationMetadata{},
				errors.AssertionFailedf("pebble: blob file %s not found among input sstables", ref.blobFileID)
		}
		references[i] = manifest.MakeBlobReference(ref.blobFileID, ref.valueSize, ref.valueSize, phys)
	}

	newBlobFiles, newReferencedValueSize, err := vs.closeWriters(references)
	if err != nil {
		return ValueSeparationMetadata{}, err
	}

	referenceSize := vs.blobTiers[base.HotTier].totalPreservedValueSize + newReferencedValueSize

	// Reset the minimum size for the next output.
	vs.currentConfig = vs.globalConfig
	// Reset the remaining state.
	vs.currPendingReferences = vs.currPendingReferences[:0]
	for i := range vs.blobTiers {
		vs.blobTiers[i].totalPreservedValueSize = 0
		vs.blobTiers[i].writer = blobWriterAndMeta{}
	}
	return ValueSeparationMetadata{
		BlobReferences:    references,
		BlobReferenceSize: referenceSize,
		// The outputBlobReferenceDepth is computed from the input sstables,
		// reflecting the worst-case overlap of referenced blob files. If this
		// sstable references fewer unique blob files, reduce its depth to the
		// count of unique files.
		BlobReferenceDepth: min(vs.outputBlobReferenceDepth, manifest.BlobReferenceDepth(len(references))),
		NewBlobFiles:       newBlobFiles,
	}, nil
}

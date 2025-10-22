// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/valsep"
	"github.com/cockroachdb/redact"
)

// latencyTolerantMinimumSize is the minimum size, in bytes, of a value that
// will be separated into a blob file when the value storage policy is
// ValueStorageLatencyTolerant.
const latencyTolerantMinimumSize = 10

var neverSeparateValues getValueSeparation = func(JobID, *tableCompaction, ValueStoragePolicy) valsep.ValueSeparation {
	return valsep.NeverSeparateValues{}
}

// determineCompactionValueSeparation determines whether a compaction should
// separate values into blob files. It returns a compact.ValueSeparation
// implementation that should be used for the compaction.
//
// It assumes that the compaction will write tables at d.TableFormat() or above.
func (d *DB) determineCompactionValueSeparation(
	jobID JobID, c *tableCompaction, valueStorage ValueStoragePolicy,
) valsep.ValueSeparation {
	if d.FormatMajorVersion() < FormatValueSeparation ||
		d.opts.Experimental.ValueSeparationPolicy == nil {
		return valsep.NeverSeparateValues{}
	}
	policy := d.opts.Experimental.ValueSeparationPolicy()
	if !policy.Enabled {
		return valsep.NeverSeparateValues{}
	}

	// We're allowed to write blob references. Determine whether we should carry
	// forward existing blob references, or write new ones.

	var blobFileSet map[base.BlobFileID]*manifest.PhysicalBlobFile
	if c.version != nil {
		// For flushes, c.version is nil.
		blobFileSet = uniqueInputBlobMetadatas(&c.version.BlobFiles, c.inputs)
	}
	minSize := uint64(policy.MinimumSize)
	switch valueStorage {
	case ValueStorageLowReadLatency:
		return valsep.NeverSeparateValues{}
	case ValueStorageLatencyTolerant:
		minSize = latencyTolerantMinimumSize
	default:
	}
	if writeBlobs, outputBlobReferenceDepth := shouldWriteBlobFiles(c, policy, minSize); !writeBlobs {
		// This compaction should preserve existing blob references.
		kind := sstable.ValueSeparationDefault
		if valueStorage != ValueStorageDefault {
			kind = sstable.ValueSeparationSpanPolicy
		}
		return &preserveBlobReferences{
			inputBlobPhysicalFiles:      blobFileSet,
			outputBlobReferenceDepth:    outputBlobReferenceDepth,
			minimumValueSize:            int(minSize),
			originalValueSeparationKind: kind,
		}
	}

	// This compaction should write values to new blob files.
	return valsep.NewWriteNewBlobFiles(
		d.opts.Comparer,
		func() (objstorage.Writable, objstorage.ObjectMetadata, error) {
			return d.newCompactionOutputBlob(
				jobID, c.kind, c.outputLevel.level, &c.metrics.bytesWritten, c.objCreateOpts)
		},
		d.makeBlobWriterOptions(c.outputLevel.level),
		policy.MinimumSize,
		valsep.WriteNewBlobFilesOptions{
			InputBlobPhysicalFiles: blobFileSet,
			ShortAttrExtractor:     d.opts.Experimental.ShortAttributeExtractor,
			InvalidValueCallback: func(userKey []byte, value []byte, err error) {
				// The value may not be safe, so it will be redacted when redaction
				// is enabled.
				d.opts.EventListener.PossibleAPIMisuse(PossibleAPIMisuseInfo{
					Kind:    InvalidValue,
					UserKey: userKey,
					ExtraInfo: redact.Sprintf("callback=ShortAttributeExtractor,value=%x,err=%q",
						value, err),
				})
			},
		},
	)
}

// shouldWriteBlobFiles returns true if the compaction should write new blob
// files. If it returns false, the referenceDepth return value contains the
// maximum blob reference depth to assign to output sstables (the actual value
// may be lower iff the output table references fewer distinct blob files).
func shouldWriteBlobFiles(
	c *tableCompaction, policy ValueSeparationPolicy, minimumValueSizeForCompaction uint64,
) (writeBlobs bool, referenceDepth manifest.BlobReferenceDepth) {
	// Flushes will have no existing references to blob files and should write
	// their values to new blob files.
	if c.kind == compactionKindFlush {
		return true, 0
	}

	inputReferenceDepth := compactionBlobReferenceDepth(c.inputs)

	if c.kind == compactionKindVirtualRewrite {
		// A virtual rewrite is a compaction that just materializes a
		// virtual table. No new blob files should be written, and the
		// reference depth is unchanged.
		return false, inputReferenceDepth
	}

	if inputReferenceDepth == 0 {
		// None of the input sstables reference blob files. It may be the case
		// that these sstables were created before value separation was enabled.
		// We should try to write to new blob files.
		return true, 0
	}
	// If the compaction's output blob reference depth would be greater than the
	// configured max, we should rewrite the values into new blob files to
	// restore locality.
	if inputReferenceDepth > manifest.BlobReferenceDepth(policy.MaxBlobReferenceDepth) {
		return true, 0
	}
	// Compare policies used by each input file. If all input files have the
	// same policy characteristics as the current one, then we can preserve
	// existing blob references. This ensures that tables that were not written
	// with value separation enabled will have their values written to new blob files.
	for _, level := range c.inputs {
		for t := range level.files.All() {
			backingProps, backingPropsValid := t.TableBacking.Properties()
			if !backingPropsValid {
				continue
			}
			if backingProps.ValueSeparationMinSize != minimumValueSizeForCompaction {
				return true, 0
			}
		}
	}

	// Otherwise, we won't write any new blob files but will carry forward
	// existing references.
	return false, inputReferenceDepth
}

// compactionBlobReferenceDepth computes the blob reference depth for a
// compaction. It's computed by finding the maximum blob reference depth of
// input sstables in each level. These per-level depths are then summed to
// produce a worst-case approximation of the blob reference locality of the
// compaction's output sstables.
//
// The intuition is that as compactions combine files referencing distinct blob
// files, outputted sstables begin to reference more and more distinct blob
// files. In the worst case, these references are evenly distributed across the
// keyspace and there is very little locality.
func compactionBlobReferenceDepth(levels []compactionLevel) manifest.BlobReferenceDepth {
	// TODO(jackson): Consider using a range tree to precisely compute the
	// depth. This would require maintaining minimum and maximum keys.
	var depth manifest.BlobReferenceDepth
	for _, level := range levels {
		// L0 allows files to overlap one another, so it's not sufficient to
		// just take the maximum within the level. Instead, we need to sum the
		// max of each sublevel.
		//
		// TODO(jackson): This and other compaction logic would likely be
		// cleaner if we modeled each sublevel as its own `compactionLevel`.
		if level.level == 0 {
			for _, sublevel := range level.l0SublevelInfo {
				var sublevelDepth int
				for t := range sublevel.LevelSlice.All() {
					sublevelDepth = max(sublevelDepth, int(t.BlobReferenceDepth))
				}
				depth += manifest.BlobReferenceDepth(sublevelDepth)
			}
			continue
		}

		var levelDepth manifest.BlobReferenceDepth
		for t := range level.files.All() {
			levelDepth = max(levelDepth, t.BlobReferenceDepth)
		}
		depth += levelDepth
	}
	return depth
}

// uniqueInputBlobMetadatas returns a slice of all unique blob file metadata
// objects referenced by tables in levels.
func uniqueInputBlobMetadatas(
	blobFileSet *manifest.BlobFileSet, levels []compactionLevel,
) map[base.BlobFileID]*manifest.PhysicalBlobFile {
	m := make(map[base.BlobFileID]*manifest.PhysicalBlobFile)
	for _, level := range levels {
		for t := range level.files.All() {
			for _, ref := range t.BlobReferences {
				if _, ok := m[ref.FileID]; ok {
					continue
				}
				phys, ok := blobFileSet.LookupPhysical(ref.FileID)
				if !ok {
					panic(errors.AssertionFailedf("pebble: blob file %s not found", ref.FileID))
				}
				m[ref.FileID] = phys
			}
		}
	}
	return m
}

// preserveBlobReferences implements the compact.ValueSeparation interface. When
// a compaction is configured with preserveBlobReferences, the compaction will
// not create any new blob files. However, input references to existing blob
// references will be preserved and metadata about the table's blob references
// will be collected.
type preserveBlobReferences struct {
	// inputBlobPhysicalFiles holds the *PhysicalBlobFile for every unique blob
	// file referenced by input sstables.
	inputBlobPhysicalFiles   map[base.BlobFileID]*manifest.PhysicalBlobFile
	outputBlobReferenceDepth manifest.BlobReferenceDepth

	// minimumValueSize is the minimum size of values used by the value separation
	// policy that was originally used to write the input sstables.
	minimumValueSize int
	// originalValueSeparationKind is the value separation policy that was originally used to
	// write the input sstables.
	originalValueSeparationKind sstable.ValueSeparationKind

	// state
	buf []byte
	// currReferences holds the pending references that have been referenced by
	// the current output sstable. The index of a reference with a given blob
	// file ID is the value of the base.BlobReferenceID used by its value handles
	// within the output sstable.
	currReferences []pendingReference
	// totalValueSize is the sum of currReferenceValueSizes.
	//
	// INVARIANT: totalValueSize == sum(currReferenceValueSizes)
	totalValueSize uint64
}

type pendingReference struct {
	blobFileID base.BlobFileID
	valueSize  uint64
}

// Assert that *preserveBlobReferences implements the compact.ValueSeparation
// interface.
var _ valsep.ValueSeparation = (*preserveBlobReferences)(nil)

// SetNextOutputConfig implements the ValueSeparation interface.
func (vs *preserveBlobReferences) SetNextOutputConfig(config valsep.ValueSeparationOutputConfig) {}

// Kind implements the ValueSeparation interface.
func (vs *preserveBlobReferences) Kind() sstable.ValueSeparationKind {
	return vs.originalValueSeparationKind
}

// MinimumSize implements the ValueSeparation interface.
func (vs *preserveBlobReferences) MinimumSize() int { return vs.minimumValueSize }

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob file if it were closed now.
func (vs *preserveBlobReferences) EstimatedFileSize() uint64 {
	return 0
}

// EstimatedReferenceSize returns an estimate of the disk space consumed by the
// current output sstable's blob references so far.
func (vs *preserveBlobReferences) EstimatedReferenceSize() uint64 {
	// TODO(jackson): The totalValueSize is the uncompressed value sizes. With
	// compressible data, it overestimates the disk space consumed by the blob
	// references. It also does not include the blob file's index block or
	// footer, so it can underestimate if values are completely incompressible.
	//
	// Should we compute a compression ratio per blob file and scale the
	// references appropriately?
	return vs.totalValueSize
}

// Add implements compact.ValueSeparation. This implementation will write
// existing blob references to the output table.
func (vs *preserveBlobReferences) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool, _ bool,
) error {
	if !kv.V.IsBlobValueHandle() {
		// If the value is not already a blob handle (either it's in-place or in
		// a value block), we retrieve the value and write it through Add. The
		// sstable writer may still decide to put the value in a value block,
		// but regardless the value will be written to the sstable itself and
		// not a blob file.
		v, callerOwned, err := kv.Value(vs.buf)
		if err != nil {
			return err
		}
		if callerOwned {
			vs.buf = v[:0]
		}
		return tw.Add(kv.K, v, forceObsolete)
	}

	// The value is an existing blob handle. We can copy it into the output
	// sstable, taking note of the reference for the table metadata.
	lv := kv.V.LazyValue()
	fileID := lv.Fetcher.BlobFileID

	var refID base.BlobReferenceID
	if refIdx := slices.IndexFunc(vs.currReferences, func(ref pendingReference) bool {
		return ref.blobFileID == fileID
	}); refIdx != -1 {
		refID = base.BlobReferenceID(refIdx)
	} else {
		refID = base.BlobReferenceID(len(vs.currReferences))
		vs.currReferences = append(vs.currReferences, pendingReference{
			blobFileID: fileID,
			valueSize:  0,
		})
	}

	if invariants.Enabled && vs.currReferences[refID].blobFileID != fileID {
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
	vs.currReferences[refID].valueSize += uint64(lv.Fetcher.Attribute.ValueLen)
	vs.totalValueSize += uint64(lv.Fetcher.Attribute.ValueLen)
	return nil
}

// FinishOutput implements compact.ValueSeparation.
func (vs *preserveBlobReferences) FinishOutput() (valsep.ValueSeparationMetadata, error) {
	if invariants.Enabled {
		// Assert that the incrementally-maintained totalValueSize matches the
		// sum of all the reference value sizes.
		totalValueSize := uint64(0)
		for _, ref := range vs.currReferences {
			totalValueSize += ref.valueSize
		}
		if totalValueSize != vs.totalValueSize {
			return valsep.ValueSeparationMetadata{},
				errors.AssertionFailedf("totalValueSize mismatch: %d != %d", totalValueSize, vs.totalValueSize)
		}
	}

	references := make(manifest.BlobReferences, len(vs.currReferences))
	for i := range vs.currReferences {
		ref := vs.currReferences[i]
		phys, ok := vs.inputBlobPhysicalFiles[ref.blobFileID]
		if !ok {
			return valsep.ValueSeparationMetadata{},
				errors.AssertionFailedf("pebble: blob file %s not found among input sstables", ref.blobFileID)
		}
		references[i] = manifest.MakeBlobReference(ref.blobFileID, ref.valueSize, ref.valueSize, phys)
	}
	referenceSize := vs.totalValueSize
	vs.currReferences = vs.currReferences[:0]
	vs.totalValueSize = 0
	return valsep.ValueSeparationMetadata{
		BlobReferences:    references,
		BlobReferenceSize: referenceSize,
		// The outputBlobReferenceDepth is computed from the input sstables,
		// reflecting the worst-case overlap of referenced blob files. If this
		// sstable references fewer unique blob files, reduce its depth to the
		// count of unique files.
		BlobReferenceDepth: min(vs.outputBlobReferenceDepth, manifest.BlobReferenceDepth(len(references))),
	}, nil
}

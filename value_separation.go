// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/compact"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/redact"
)

// latencyTolerantMinimumSize is the minimum size, in bytes, of a value that
// will be separated into a blob file when the value storage policy is
// ValueStorageLatencyTolerant.
const latencyTolerantMinimumSize = 10

var neverSeparateValues getValueSeparation = func(JobID, *tableCompaction, sstable.TableFormat) compact.ValueSeparation {
	return compact.NeverSeparateValues{}
}

// determineCompactionValueSeparation determines whether a compaction should
// separate values into blob files. It returns a compact.ValueSeparation
// implementation that should be used for the compaction.
func (d *DB) determineCompactionValueSeparation(
	jobID JobID, c *tableCompaction, tableFormat sstable.TableFormat,
) compact.ValueSeparation {
	if tableFormat < sstable.TableFormatPebblev7 || d.FormatMajorVersion() < FormatValueSeparation ||
		d.opts.Experimental.ValueSeparationPolicy == nil {
		return compact.NeverSeparateValues{}
	}
	policy := d.opts.Experimental.ValueSeparationPolicy()
	if !policy.Enabled {
		return compact.NeverSeparateValues{}
	}

	// We're allowed to write blob references. Determine whether we should carry
	// forward existing blob references, or write new ones.
	if writeBlobs, outputBlobReferenceDepth := shouldWriteBlobFiles(c, policy); !writeBlobs {
		// This compaction should preserve existing blob references.
		return &preserveBlobReferences{
			inputBlobPhysicalFiles:   uniqueInputBlobMetadatas(&c.version.BlobFiles, c.inputs),
			outputBlobReferenceDepth: outputBlobReferenceDepth,
		}
	}

	// This compaction should write values to new blob files.
	return &writeNewBlobFiles{
		comparer: d.opts.Comparer,
		newBlobObject: func() (objstorage.Writable, objstorage.ObjectMetadata, error) {
			return d.newCompactionOutputBlob(
				jobID, c.kind, c.outputLevel.level, &c.metrics.bytesWritten, c.objCreateOpts)
		},
		shortAttrExtractor: d.opts.Experimental.ShortAttributeExtractor,
		writerOpts:         d.opts.MakeBlobWriterOptions(c.outputLevel.level),
		minimumSize:        policy.MinimumSize,
		globalMinimumSize:  policy.MinimumSize,
		invalidValueCallback: func(userKey []byte, value []byte, err error) {
			// The value may not be safe, so it will be redacted when redaction
			// is enabled.
			d.opts.EventListener.PossibleAPIMisuse(PossibleAPIMisuseInfo{
				Kind:    InvalidValue,
				UserKey: userKey,
				ExtraInfo: redact.Sprintf("callback=ShortAttributeExtractor,value=%x,err=%q",
					value, err),
			})
		},
	}
}

// shouldWriteBlobFiles returns true if the compaction should write new blob
// files. If it returns false, the referenceDepth return value contains the
// maximum blob reference depth to assign to output sstables (the actual value
// may be lower iff the output table references fewer distinct blob files).
func shouldWriteBlobFiles(
	c *tableCompaction, policy ValueSeparationPolicy,
) (writeBlobs bool, referenceDepth manifest.BlobReferenceDepth) {
	// Flushes will have no existing references to blob files and should write
	// their values to new blob files.
	if c.kind == compactionKindFlush {
		return true, 0
	}
	inputReferenceDepth := compactionBlobReferenceDepth(c.inputs)
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

// writeNewBlobFiles implements the strategy and mechanics for separating values
// into external blob files.
type writeNewBlobFiles struct {
	comparer *base.Comparer
	// newBlobObject constructs a new blob object for use in the compaction.
	newBlobObject      func() (objstorage.Writable, objstorage.ObjectMetadata, error)
	shortAttrExtractor ShortAttributeExtractor
	// writerOpts is used to configure all constructed blob writers.
	writerOpts blob.FileWriterOptions
	// minimumSize imposes a lower bound on the size of values that can be
	// separated into a blob file. Values smaller than this are always written
	// to the sstable (but may still be written to a value block within the
	// sstable).
	//
	// minimumSize is set to globalMinimumSize by default and on every call to
	// FinishOutput. It may be overriden by SetNextOutputConfig (i.e, if a
	// SpanPolicy dictates a different minimum size for a span of the keyspace).
	minimumSize int
	// globalMinimumSize is the size threshold for separating values into blob
	// files globally across the keyspace. It may be overridden per-output by
	// SetNextOutputConfig.
	globalMinimumSize int
	// invalidValueCallback is called when a value is encountered for which the
	// short attribute extractor returns an error.
	invalidValueCallback func(userKey []byte, value []byte, err error)

	// Current blob writer state
	writer  *blob.FileWriter
	objMeta objstorage.ObjectMetadata

	buf []byte
}

// Assert that *writeNewBlobFiles implements the compact.ValueSeparation interface.
var _ compact.ValueSeparation = (*writeNewBlobFiles)(nil)

// SetNextOutputConfig implements the ValueSeparation interface.
func (vs *writeNewBlobFiles) SetNextOutputConfig(config compact.ValueSeparationOutputConfig) {
	vs.minimumSize = config.MinimumSize
}

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob file if it were closed now.
func (vs *writeNewBlobFiles) EstimatedFileSize() uint64 {
	if vs.writer == nil {
		return 0
	}
	return vs.writer.EstimatedSize()
}

// EstimatedReferenceSize returns an estimate of the disk space consumed by the
// current output sstable's blob references so far.
func (vs *writeNewBlobFiles) EstimatedReferenceSize() uint64 {
	// When we're writing to new blob files, the size of the blob file itself is
	// a better estimate of the disk space consumed than the uncompressed value
	// sizes.
	return vs.EstimatedFileSize()
}

// Add adds the provided key-value pair to the sstable, possibly separating the
// value into a blob file.
func (vs *writeNewBlobFiles) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool,
) error {
	// We always fetch the value if we're rewriting blob files. We want to
	// replace any references to existing blob files with references to new blob
	// files that we write during the compaction.
	v, callerOwned, err := kv.Value(vs.buf)
	if err != nil {
		return err
	}
	if callerOwned {
		vs.buf = v[:0]
	}

	// Values that are too small are never separated.
	if len(v) < vs.minimumSize {
		return tw.Add(kv.K, v, forceObsolete)
	}
	// Merge and deletesized keys are never separated.
	switch kv.K.Kind() {
	case base.InternalKeyKindMerge, base.InternalKeyKindDeleteSized:
		return tw.Add(kv.K, v, forceObsolete)
	}

	// This KV met all the criteria and its value will be separated.
	// If there's a configured short attribute extractor, extract the value's
	// short attribute.
	var shortAttr base.ShortAttribute
	if vs.shortAttrExtractor != nil {
		keyPrefixLen := vs.comparer.Split(kv.K.UserKey)
		shortAttr, err = vs.shortAttrExtractor(kv.K.UserKey, keyPrefixLen, v)
		if err != nil {
			// Report that there was a value for which the short attribute
			// extractor was unable to extract a short attribute.
			vs.invalidValueCallback(kv.K.UserKey, v, err)

			// Rather than erroring out and aborting the flush or compaction, we
			// fallback to writing the value verbatim to the sstable. Otherwise
			// a flush could busy loop, repeatedly attempting to write the same
			// memtable and repeatedly unable to extract a key's short attribute.
			return tw.Add(kv.K, v, forceObsolete)
		}
	}

	// If we don't have an open blob writer, create one. We create blob objects
	// lazily so that we don't create them unless a compaction will actually
	// write to a blob file. This avoids creating and deleting empty blob files
	// on every compaction in parts of the keyspace that a) are required to be
	// in-place or b) have small values.
	if vs.writer == nil {
		writable, objMeta, err := vs.newBlobObject()
		if err != nil {
			return err
		}
		vs.objMeta = objMeta
		vs.writer = blob.NewFileWriter(objMeta.DiskFileNum, writable, vs.writerOpts)
	}

	// Append the value to the blob file.
	handle := vs.writer.AddValue(v)

	// Write the key and the handle to the sstable. We need to map the
	// blob.Handle into a blob.InlineHandle. Everything is copied verbatim,
	// except the FileNum is translated into a reference index.
	inlineHandle := blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			// Since we're writing blob files and maintaining a 1:1 mapping
			// between sstables and blob files, the reference index is always 0
			// here. Only compactions that don't rewrite blob files will produce
			// handles with nonzero reference indices.
			ReferenceID: 0,
			ValueLen:    handle.ValueLen,
		},
		HandleSuffix: blob.HandleSuffix{
			BlockID: handle.BlockID,
			ValueID: handle.ValueID,
		},
	}
	return tw.AddWithBlobHandle(kv.K, inlineHandle, shortAttr, forceObsolete)
}

// FinishOutput closes the current blob file (if any). It returns the stats
// and metadata of the now completed blob file.
func (vs *writeNewBlobFiles) FinishOutput() (compact.ValueSeparationMetadata, error) {
	if vs.writer == nil {
		return compact.ValueSeparationMetadata{}, nil
	}
	stats, err := vs.writer.Close()
	if err != nil {
		return compact.ValueSeparationMetadata{}, err
	}
	vs.writer = nil
	meta := &manifest.PhysicalBlobFile{
		FileNum:      vs.objMeta.DiskFileNum,
		Size:         stats.FileLen,
		ValueSize:    stats.UncompressedValueBytes,
		CreationTime: uint64(time.Now().Unix()),
	}
	// Reset the minimum size for the next output.
	vs.minimumSize = vs.globalMinimumSize

	return compact.ValueSeparationMetadata{
		BlobReferences: manifest.BlobReferences{manifest.MakeBlobReference(
			base.BlobFileID(vs.objMeta.DiskFileNum),
			stats.UncompressedValueBytes,
			meta,
		)},
		BlobReferenceSize:  stats.UncompressedValueBytes,
		BlobReferenceDepth: 1,
		BlobFileStats:      stats,
		BlobFileObject:     vs.objMeta,
		BlobFileMetadata:   meta,
	}, nil
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

	// state
	buf []byte
	// currReferences holds the pending references that have been referenced by
	// the current output sstable. The index of a reference with a given blob
	// file ID is the value of the blob.ReferenceID used by its value handles
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
var _ compact.ValueSeparation = (*preserveBlobReferences)(nil)

// SetNextOutputConfig implements the ValueSeparation interface.
func (vs *preserveBlobReferences) SetNextOutputConfig(config compact.ValueSeparationOutputConfig) {}

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
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool,
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

	var refID blob.ReferenceID
	if refIdx := slices.IndexFunc(vs.currReferences, func(ref pendingReference) bool {
		return ref.blobFileID == fileID
	}); refIdx != -1 {
		refID = blob.ReferenceID(refIdx)
	} else {
		refID = blob.ReferenceID(len(vs.currReferences))
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
func (vs *preserveBlobReferences) FinishOutput() (compact.ValueSeparationMetadata, error) {
	if invariants.Enabled {
		// Assert that the incrementally-maintained totalValueSize matches the
		// sum of all the reference value sizes.
		totalValueSize := uint64(0)
		for _, ref := range vs.currReferences {
			totalValueSize += ref.valueSize
		}
		if totalValueSize != vs.totalValueSize {
			return compact.ValueSeparationMetadata{},
				errors.AssertionFailedf("totalValueSize mismatch: %d != %d", totalValueSize, vs.totalValueSize)
		}
	}

	references := make(manifest.BlobReferences, len(vs.currReferences))
	for i := range vs.currReferences {
		ref := vs.currReferences[i]
		phys, ok := vs.inputBlobPhysicalFiles[ref.blobFileID]
		if !ok {
			return compact.ValueSeparationMetadata{},
				errors.AssertionFailedf("pebble: blob file %s not found among input sstables", ref.blobFileID)
		}
		references[i] = manifest.MakeBlobReference(ref.blobFileID, ref.valueSize, phys)
	}
	referenceSize := vs.totalValueSize
	vs.currReferences = vs.currReferences[:0]
	vs.totalValueSize = 0
	return compact.ValueSeparationMetadata{
		BlobReferences:    references,
		BlobReferenceSize: referenceSize,
		// The outputBlobReferenceDepth is computed from the input sstables,
		// reflecting the worst-case overlap of referenced blob files. If this
		// sstable references fewer unique blob files, reduce its depth to the
		// count of unique files.
		BlobReferenceDepth: min(vs.outputBlobReferenceDepth, manifest.BlobReferenceDepth(len(references))),
	}, nil
}

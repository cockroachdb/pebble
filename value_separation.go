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
	"github.com/cockroachdb/pebble/sstable/tieredmeta"
	"github.com/cockroachdb/redact"
)

// latencyTolerantMinimumSize is the minimum size, in bytes, of a value that
// will be separated into a blob file when the value storage policy is
// ValueStorageLatencyTolerant.
const latencyTolerantMinimumSize = 10

var neverSeparateValues getValueSeparation = func(
	JobID, *tableCompaction, sstable.TableFormat, tieredmeta.ColdTierThresholdRetriever) compact.ValueSeparation {
	return compact.NeverSeparateValues{}
}

// determineCompactionValueSeparation determines whether a compaction should
// separate values into blob files. It returns a compact.ValueSeparation
// implementation that should be used for the compaction.
func (d *DB) determineCompactionValueSeparation(
	jobID JobID,
	c *tableCompaction,
	tableFormat sstable.TableFormat,
	cttRetriever tieredmeta.ColdTierThresholdRetriever,
) compact.ValueSeparation {
	if tableFormat < sstable.TableFormatPebblev7 || d.FormatMajorVersion() < FormatValueSeparation ||
		d.opts.Experimental.ValueSeparationPolicy == nil {
		return compact.NeverSeparateValues{}
	}
	policy := d.opts.Experimental.ValueSeparationPolicy()
	if !policy.Enabled {
		return compact.NeverSeparateValues{}
	}

	var inputBlobPhysicalFiles map[base.BlobFileID]*manifest.PhysicalBlobFile
	if c.version != nil {
		inputBlobPhysicalFiles = uniqueInputBlobMetadatas(&c.version.BlobFiles, c.inputs)
	}
	// We're allowed to write blob references. Determine whether we should carry
	// forward existing blob references, or write new ones.
	writeBlobs, outputBlobReferenceDepth, coldWriteSpans :=
		shouldWriteBlobFiles(c, policy, cttRetriever)
	if !writeBlobs {
		// This compaction should preserve existing blob references.
		return newPreserveAllHotBlobReferences(
			inputBlobPhysicalFiles, outputBlobReferenceDepth)
	}
	_ = coldWriteSpans
	// This compaction should write values to new blob files.
	writerOpts := d.opts.MakeBlobWriterOptions(c.outputLevel.level, d.BlobFileFormat())
	writerOpts.CTTRetriever = cttRetriever
	return newWriteNewBlobFiles(
		inputBlobPhysicalFiles,
		d.opts.Comparer,
		func() (objstorage.Writable, objstorage.ObjectMetadata, error) {
			return d.newCompactionOutputBlob(
				jobID, c.kind, c.outputLevel.level, &c.metrics.bytesWritten, c.objCreateOpts)
		},
		d.opts.Experimental.ShortAttributeExtractor,
		writerOpts,
		policy.MinimumSize,
		func(userKey []byte, value []byte, err error) {
			// The value may not be safe, so it will be redacted when redaction
			// is enabled.
			d.opts.EventListener.PossibleAPIMisuse(PossibleAPIMisuseInfo{
				Kind:    InvalidValue,
				UserKey: userKey,
				ExtraInfo: redact.Sprintf("callback=ShortAttributeExtractor,value=%x,err=%q",
					value, err),
			})
		},
		nil,
	)
}

const minLevelForColdBlobFiles = 5

// TODO(sumeer): make this configurable.
const coldTierValSizeThreshold = 50

// shouldWriteBlobFiles returns true if the compaction should write new blob
// files.
//
// If it returns false, the referenceDepth return value contains the maximum
// blob reference depth to assign to output sstables (the actual value may be
// lower iff the output table references fewer distinct blob files).
//
// If it returns true, existing hot tier blob references will get rewritten,
// and existing inline values can get separated. For this case, we also need
// to decide whether to write cold tier blob files or not, since we don't want
// to move small number of bytes with each compaction to cold tier blob files
// (which will be tiny). Since the size threshold for a value to be separated
// into a cold tier blob file is low, we will see transitions from both inline
// values and hot tier blob files to cold tier blob files. We make this
// decision of whether to write new cold blob files per TieringSpanID, by
// approximating the bytes that will get written. Any cold tier reference that
// is becoming hot will get rewritten, but other cold tier references will be
// preserved in their existing blob files.
func shouldWriteBlobFiles(
	c *tableCompaction,
	policy ValueSeparationPolicy,
	cttRetriever tieredmeta.ColdTierThresholdRetriever,
) (
	writeBlobs bool,
	referenceDepth manifest.BlobReferenceDepth,
	coldWriteSpans map[base.TieringSpanID]struct{},
) {
	// Flushes will have no existing references to blob files and should write
	// their values to new blob files.
	if c.kind == compactionKindFlush {
		return true, 0, nil
	}
	inputReferenceDepth := compactionBlobReferenceDepth(c.inputs)
	// If the compaction's output blob reference depth would be greater than the
	// configured max, we should rewrite the values into new blob files to
	// restore locality.
	if inputReferenceDepth > 0 &&
		inputReferenceDepth <= manifest.BlobReferenceDepth(policy.MaxBlobReferenceDepth) {
		return false, inputReferenceDepth, nil
	}
	// Else, either the compaction's input blob reference depth is greater than
	// the configured max, so we should rewrite the values into new blob files
	// to restore locality, or the inputs have no hot blob references. In the
	// latter case, either these sstables were created before value separation
	// was enabled, or they only have cold blob references. In either case, we
	// should be willing to write new blob references.

	// TODO(sumeer): completely skip if tiering is disabled.
	if c.outputLevel != nil && c.outputLevel.level >= minLevelForColdBlobFiles {
		hist := compactionTieringHistograms(c.inputs)
		type coldAttrAndBytesEstimates struct {
			coldLTThreshold base.TieringAttribute
			valueBytes      uint64
		}
		var tieringSpansLTThreshold map[base.TieringSpanID]coldAttrAndBytesEstimates
		for k := range hist.Histograms {
			if k.TieringSpanID == 0 {
				continue
			}
			if tieringSpansLTThreshold == nil {
				tieringSpansLTThreshold = make(map[base.TieringSpanID]coldAttrAndBytesEstimates)
			}
			tieringSpansLTThreshold[k.TieringSpanID] = coldAttrAndBytesEstimates{}
		}
		for spanID := range tieringSpansLTThreshold {
			tieringSpansLTThreshold[spanID] =
				coldAttrAndBytesEstimates{coldLTThreshold: cttRetriever.GetColdTierLTThreshold(spanID)}
		}
		for k, h := range hist.Histograms {
			if k.TieringSpanID == 0 {
				continue
			}
			if k.KindAndTier == base.SSTableKeyBytes ||
				k.KindAndTier == base.SSTableBlobReferenceColdBytes {
				continue
			}
			if k.KindAndTier == base.SSTableValueBytes && h.MeanSize() < coldTierValSizeThreshold {
				continue
			}
			v := tieringSpansLTThreshold[k.TieringSpanID]
			v.valueBytes += h.ColdBytes(v.coldLTThreshold)
			tieringSpansLTThreshold[k.TieringSpanID] = v
		}
		for spanID, v := range tieringSpansLTThreshold {
			// TODO(sumeer): make this byte threshold a function of the level, and the
			// number of input bytes in the compaction.
			if v.valueBytes > 1e6 {
				if coldWriteSpans == nil {
					coldWriteSpans = make(map[base.TieringSpanID]struct{})
				}
				coldWriteSpans[spanID] = struct{}{}
			}
		}
	}
	return true, 0, coldWriteSpans
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

func compactionTieringHistograms(
	levels []compactionLevel,
) tieredmeta.TieringHistogramBlockContents {
	var hist tieredmeta.TieringHistogramBlockContents
	for _, level := range levels {
		for t := range level.files.All() {
			stats, valid := t.Stats()
			if !valid {
				continue
			}
			hist.Merge(&stats.TieringHistograms)
		}
	}
	return hist
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

// MayWriteColdBlobFiles implements the ValueSeparation interface.
func (vs *writeNewBlobFiles) MayWriteColdBlobFiles() bool {
	return false
}

// MayWriteColdBlobFilesForTieringSpanID implements the ValueSeparation interface.
func (vs *writeNewBlobFiles) MayWriteColdBlobFilesForTieringSpanID(_ base.TieringSpanID) bool {
	return false
}

// OverrideNextOutputConfig implements the ValueSeparation interface.
func (vs *writeNewBlobFiles) OverrideNextOutputConfig(
	config compact.ValueSeparationOverrideConfig,
) {
	vs.minimumSize = config.MinimumSize
}

// StartOutput implements the ValueSeparation interface.
func (vs *writeNewBlobFiles) StartOutput(config compact.ValueSeparationOutputConfig) {}

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob file if it were closed now.
func (vs *writeNewBlobFiles) EstimatedFileSize() uint64 {
	if vs.writer == nil {
		return 0
	}
	return vs.writer.EstimatedSize()
}

// EstimatedHotReferenceSize implements compact.ValueSeparation.
func (vs *writeNewBlobFiles) EstimatedHotReferenceSize() uint64 {
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
		return tw.Add(kv.K, v, forceObsolete, kv.M)
	}
	// Merge and deletesized keys are never separated.
	switch kv.K.Kind() {
	case base.InternalKeyKindMerge, base.InternalKeyKindDeleteSized:
		return tw.Add(kv.K, v, forceObsolete, kv.M)
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
			return tw.Add(kv.K, v, forceObsolete, kv.M)
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
	handle := vs.writer.AddValue(v, kv.M.Tiering)

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
	return tw.AddWithBlobHandle(kv.K, inlineHandle, shortAttr, forceObsolete, kv.M, base.HotTier)
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
		NewBlobFiles: []compact.NewBlobFileInfo{{
			FileStats:    stats,
			FileObject:   vs.objMeta,
			FileMetadata: meta,
		}},
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
var _ compact.ValueSeparation = (*preserveBlobReferences)(nil)

// MayWriteColdBlobFiles implements the ValueSeparation interface.
func (vs *preserveBlobReferences) MayWriteColdBlobFiles() bool {
	return false
}

// MayWriteColdBlobFilesForTieringSpanID implements the ValueSeparation interface.
func (vs *preserveBlobReferences) MayWriteColdBlobFilesForTieringSpanID(_ base.TieringSpanID) bool {
	return false
}

// OverrideNextOutputConfig implements the ValueSeparation interface.
func (vs *preserveBlobReferences) OverrideNextOutputConfig(
	config compact.ValueSeparationOverrideConfig,
) {
}

// StartOutput implements the ValueSeparation interface.
func (vs *preserveBlobReferences) StartOutput(config compact.ValueSeparationOutputConfig) {}

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob file if it were closed now.
func (vs *preserveBlobReferences) EstimatedFileSize() uint64 {
	return 0
}

// EstimatedHotReferenceSize implements compact.ValueSeparation.
func (vs *preserveBlobReferences) EstimatedHotReferenceSize() uint64 {
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
		return tw.Add(kv.K, v, forceObsolete, kv.M)
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
	err := tw.AddWithBlobHandle(
		kv.K, inlineHandle, lv.Fetcher.Attribute.ShortAttribute, forceObsolete, kv.M, base.HotTier)
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

type valueSeparationMode uint8

const (
	// preserveAllHotBlobReferences preserves existing hot blob references and
	// does not write any new (hot or cold) blob files. Existing cold blob
	// references that are now hot, will be inlined.
	preserveAllHotBlobReferences valueSeparationMode = iota
	// rewriteAllHotBlobReferences rewrites all existing hot blob references,
	// and preserves existing cold blob references, and may write new cold blob
	// files, depending on other configuration.
	rewriteAllHotBlobReferences
)

// generalizedValueSeparation can function in any of the valueSeparationModes,
// and can write cold blob files.
//
// TODO(sumeer): fully integrate generalizedValueSeparation and remove the
// other implementations.
type generalizedValueSeparation struct {
	// Initial state.

	mode valueSeparationMode
	// inputBlobPhysicalFiles holds the *PhysicalBlobFile for every unique blob
	// file referenced by input sstables.
	inputBlobPhysicalFiles   map[base.BlobFileID]*manifest.PhysicalBlobFile
	outputBlobReferenceDepth manifest.BlobReferenceDepth

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
	// coldWriteSpans is the set of spans for which new cold blob files can be
	// written for the compaction as a whole. For a particular output, the call
	// to StartOutput will select 0 or 1 element from this set.
	coldWriteSpans map[base.TieringSpanID]struct{}

	// state.
	buf                           []byte
	preservedReferenceIndexOffset base.BlobReferenceID
	tieringSpanForNewColdBlobFile base.TieringSpanID
	// currPreservedReferences holds the pending references that have been referenced by
	// the current output sstable. The index of a reference with a given blob
	// file ID is the value of the base.BlobReferenceID used by its value handles
	// within the output sstable.
	currPreservedReferences []pendingReference
	// totalPreservedValueSize is the sum of currPreservedReferenceValueSizes.
	//
	// INVARIANT: sum(totalPreservedValueSize) == sum(currReferenceValueSizes)
	totalPreservedValueSize [base.NumStorageTiers]uint64
	writers                 [base.NumStorageTiers]blobWriterAndMeta
}

var _ compact.ValueSeparation = &generalizedValueSeparation{}

func newPreserveAllHotBlobReferences(
	inputBlobPhysicalFiles map[base.BlobFileID]*manifest.PhysicalBlobFile,
	outputBlobReferenceDepth manifest.BlobReferenceDepth,
) *generalizedValueSeparation {
	return &generalizedValueSeparation{
		mode:                     preserveAllHotBlobReferences,
		inputBlobPhysicalFiles:   inputBlobPhysicalFiles,
		outputBlobReferenceDepth: outputBlobReferenceDepth,
	}
}

func newWriteNewBlobFiles(
	inputBlobPhysicalFiles map[base.BlobFileID]*manifest.PhysicalBlobFile,
	comparer *base.Comparer,
	newBlobObject func() (objstorage.Writable, objstorage.ObjectMetadata, error),
	shortAttrExtractor ShortAttributeExtractor,
	writerOpts blob.FileWriterOptions,
	globalMinimumSize int,
	invalidValueCallback func(userKey []byte, value []byte, err error),
	coldWriteSpans map[base.TieringSpanID]struct{},
) *generalizedValueSeparation {
	return &generalizedValueSeparation{
		mode:                     rewriteAllHotBlobReferences,
		inputBlobPhysicalFiles:   inputBlobPhysicalFiles,
		outputBlobReferenceDepth: 1,
		comparer:                 comparer,
		newBlobObject:            newBlobObject,
		shortAttrExtractor:       shortAttrExtractor,
		writerOpts:               writerOpts,
		minimumSize:              globalMinimumSize,
		globalMinimumSize:        globalMinimumSize,
		invalidValueCallback:     invalidValueCallback,
		coldWriteSpans:           coldWriteSpans,
	}
}

type blobWriterAndMeta struct {
	writer  *blob.FileWriter
	objMeta objstorage.ObjectMetadata
}

// MayWriteColdBlobFiles implements the ValueSeparation interface.
func (vs *generalizedValueSeparation) MayWriteColdBlobFiles() bool {
	return vs.mode == rewriteAllHotBlobReferences && len(vs.coldWriteSpans) > 0
}

// MayWriteColdBlobFilesForTieringSpanID implements the ValueSeparation interface.
func (vs *generalizedValueSeparation) MayWriteColdBlobFilesForTieringSpanID(
	tieringSpanID base.TieringSpanID,
) bool {
	if vs.mode != rewriteAllHotBlobReferences {
		return false
	}
	_, ok := vs.coldWriteSpans[tieringSpanID]
	return ok
}

// OverrideNextOutputConfig implements the ValueSeparation interface.
func (vs *generalizedValueSeparation) OverrideNextOutputConfig(
	config compact.ValueSeparationOverrideConfig,
) {
	vs.minimumSize = config.MinimumSize
}

// StartOutput implements the ValueSeparation interface.
func (vs *generalizedValueSeparation) StartOutput(config compact.ValueSeparationOutputConfig) {
	if vs.mode == preserveAllHotBlobReferences {
		return
	}
	// Index 0 is used for the new hot blob file, if any.
	preservedOffset := base.BlobReferenceID(1)
	if config.TieringSpanIDForNewColdBlobFile != 0 {
		_, ok := vs.coldWriteSpans[config.TieringSpanIDForNewColdBlobFile]
		if ok {
			// Index 1 is used for the new cold blob file, if any.
			preservedOffset++
			vs.tieringSpanForNewColdBlobFile = config.TieringSpanIDForNewColdBlobFile
		}
	}
	vs.preservedReferenceIndexOffset = preservedOffset
}

// EstimatedFileSize returns an estimate of the disk space consumed by the current
// blob files if they were closed now.
func (vs *generalizedValueSeparation) EstimatedFileSize() uint64 {
	if vs.mode == preserveAllHotBlobReferences {
		return 0
	}
	var size uint64
	for i := range vs.writers {
		if vs.writers[i].writer != nil {
			size += vs.writers[i].writer.EstimatedSize()
		}
	}
	return size
}

// EstimatedHotReferenceSize implements compact.ValueSeparation.
func (vs *generalizedValueSeparation) EstimatedHotReferenceSize() uint64 {
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
	return vs.EstimatedFileSize() + vs.totalPreservedValueSize[base.HotTier]
}

// Add implements compact.ValueSeparation.
func (vs *generalizedValueSeparation) Add(
	tw sstable.RawWriter, kv *base.InternalKV, forceObsolete bool,
) error {
	shouldBeCold := false
	if kv.M.Tiering.SpanID != 0 {
		ctLTThreshold := vs.writerOpts.CTTRetriever.GetColdTierLTThreshold(kv.M.Tiering.SpanID)
		if kv.M.Tiering.Attribute < ctLTThreshold {
			shouldBeCold = true
		}
	}
	// Only set to true if it is already a reference.
	preserveReference := false
	// Only set if it is currently a reference, and represents the tier of the
	// reference.
	var referenceTier base.StorageTier
	if kv.V.IsBlobValueHandle() {
		lv := kv.V.LazyValue()
		blobFileID := lv.Fetcher.BlobFileID
		phys, ok := vs.inputBlobPhysicalFiles[blobFileID]
		if !ok {
			return errors.AssertionFailedf("pebble: blob file %s not found among input sstables", blobFileID)
		}
		referenceTier = phys.Tier
		if phys.Tier == base.HotTier {
			// hot => {hot, cold}.
			if vs.mode == preserveAllHotBlobReferences {
				// Can't do hot => cold transition, even if desired.
				preserveReference = true
			}
			// Else will be rewritten, anyway, so we can decide whether
			// to do a transition or not below.
		} else {
			if shouldBeCold {
				// cold => cold.
				preserveReference = true
			} else {
				// cold => hot. We do this even if no blob files are being written --
				// i.e., we will inline the value.
				preserveReference = false
			}
		}
	}
	if preserveReference {
		// The value is an existing blob handle. We can copy it into the output
		// sstable, taking note of the reference for the table metadata.
		lv := kv.V.LazyValue()
		fileID := lv.Fetcher.BlobFileID
		var refID base.BlobReferenceID
		if refIdx := slices.IndexFunc(vs.currPreservedReferences, func(ref pendingReference) bool {
			return ref.blobFileID == fileID
		}); refIdx != -1 {
			refID = base.BlobReferenceID(refIdx) + vs.preservedReferenceIndexOffset
		} else {
			refID = base.BlobReferenceID(len(vs.currPreservedReferences)) + vs.preservedReferenceIndexOffset
			vs.currPreservedReferences = append(vs.currPreservedReferences, pendingReference{
				blobFileID: fileID,
				valueSize:  0,
			})
		}
		if invariants.Enabled && vs.currPreservedReferences[refID].blobFileID != fileID {
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
		err := tw.AddWithBlobHandle(
			kv.K, inlineHandle, lv.Fetcher.Attribute.ShortAttribute, forceObsolete, kv.M, referenceTier)
		if err != nil {
			return err
		}
		vs.currPreservedReferences[refID].valueSize += uint64(lv.Fetcher.Attribute.ValueLen)
		vs.totalPreservedValueSize[referenceTier] += uint64(lv.Fetcher.Attribute.ValueLen)
		return nil
	}
	// Fetch the value since either not a reference, or not preserving the reference.
	v, callerOwned, err := kv.Value(vs.buf)
	if err != nil {
		return err
	}
	if callerOwned {
		vs.buf = v[:0]
	}
	keyKind := kv.K.Kind()
	if keyKind != base.InternalKeyKindSet && keyKind != base.InternalKeyKindSetWithDelete {
		// Only SET and SETWITHDEL can be separated.
		return tw.Add(kv.K, v, forceObsolete, kv.M)
	}
	// Key is SET or SETWITHDEL.
	writeToColdBlob :=
		vs.mode == rewriteAllHotBlobReferences && shouldBeCold && len(v) >= coldTierValSizeThreshold &&
			vs.tieringSpanForNewColdBlobFile == kv.M.Tiering.SpanID
	writeToHotBlob := !writeToColdBlob && vs.mode == rewriteAllHotBlobReferences
	if writeToHotBlob {
		// Values that are too small are never separated.
		if len(v) < vs.minimumSize {
			writeToHotBlob = false
		}
	}
	if !writeToHotBlob && !writeToColdBlob {
		return tw.Add(kv.K, v, forceObsolete, kv.M)
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
			return tw.Add(kv.K, v, forceObsolete, kv.M)
		}
	}
	blobTier := base.HotTier
	if writeToColdBlob {
		blobTier = base.ColdTier
	}
	wnm, err := vs.getWriter(blobTier, kv.M.Tiering.SpanID)
	if err != nil {
		return err
	}
	// Append the value to the blob file.
	handle := wnm.writer.AddValue(v, kv.M.Tiering)
	// The referenceID is 0 for hot blob file, 1 for cold blob file (if any).
	var referenceID base.BlobReferenceID
	if writeToColdBlob {
		referenceID = 1
	}
	// Write the key and the handle to the sstable. We need to map the
	// blob.Handle into a blob.InlineHandle. Everything is copied verbatim,
	// except the FileNum is translated into a reference index.
	inlineHandle := blob.InlineHandle{
		InlineHandlePreface: blob.InlineHandlePreface{
			ReferenceID: referenceID,
			ValueLen:    handle.ValueLen,
		},
		HandleSuffix: blob.HandleSuffix{
			BlockID: handle.BlockID,
			ValueID: handle.ValueID,
		},
	}
	return tw.AddWithBlobHandle(kv.K, inlineHandle, shortAttr, forceObsolete, kv.M, blobTier)
}

func (vs *generalizedValueSeparation) getWriter(
	blobTier base.StorageTier, spanID base.TieringSpanID,
) (*blobWriterAndMeta, error) {
	wnm := &vs.writers[blobTier]
	if wnm.writer != nil {
		return wnm, nil
	}
	// TODO(sumeer): use blobTier to create in the appropriate tier.
	writable, objMeta, err := vs.newBlobObject()
	if err != nil {
		return nil, err
	}
	wnm.objMeta = objMeta
	wnm.writer = blob.NewFileWriter(objMeta.DiskFileNum, writable, vs.writerOpts)
	return wnm, nil
}

// FinishOutput closes the current blob file (if any). It returns the stats
// and metadata of the now completed blob file.
func (vs *generalizedValueSeparation) FinishOutput() (compact.ValueSeparationMetadata, error) {
	if invariants.Enabled {
		for i := base.StorageTier(0); i < base.NumStorageTiers; i++ {
			// Assert that the incrementally-maintained totalValueSize matches the
			// sum of all the reference value sizes.
			totalValueSize := uint64(0)
			for _, ref := range vs.currPreservedReferences {
				if phys, ok := vs.inputBlobPhysicalFiles[ref.blobFileID]; ok && phys.Tier == i {
					totalValueSize += ref.valueSize
				}
			}
			if totalValueSize != vs.totalPreservedValueSize[i] {
				return compact.ValueSeparationMetadata{},
					errors.AssertionFailedf("totalValueSize mismatch for tier %d: %d != %d",
						i, totalValueSize, vs.totalPreservedValueSize[i])
			}
		}
	}
	var references manifest.BlobReferences
	if len(vs.currPreservedReferences) > 0 || vs.writers[base.ColdTier].writer != nil {
		// vs.preservedReferenceIndexOffset leaves space in the prefix for the new
		// blob files.
		references = make(manifest.BlobReferences,
			len(vs.currPreservedReferences)+int(vs.preservedReferenceIndexOffset))
	} else {
		// No preserved references and no new cold blob file, so tighten up the
		// references slice.
		if vs.writers[base.HotTier].writer != nil {
			references = make(manifest.BlobReferences, 1)
		}
		// Else no references.
	}
	numPopulatedHotReferences := 0
	for i := range vs.currPreservedReferences {
		ref := vs.currPreservedReferences[i]
		phys, ok := vs.inputBlobPhysicalFiles[ref.blobFileID]
		if !ok {
			return compact.ValueSeparationMetadata{},
				errors.AssertionFailedf("pebble: blob file %s not found among input sstables", ref.blobFileID)
		}
		if phys.Tier == base.HotTier {
			numPopulatedHotReferences++
		}
		references[i+int(vs.preservedReferenceIndexOffset)] =
			manifest.MakeBlobReference(ref.blobFileID, ref.valueSize, phys)
	}
	var referenceSize uint64
	for i := range vs.totalPreservedValueSize {
		referenceSize += vs.totalPreservedValueSize[i]
	}

	var newBlobFiles []compact.NewBlobFileInfo
	for i := range vs.writers {
		if vs.writers[i].writer == nil {
			continue
		}
		stats, err := vs.writers[i].writer.Close()
		if err != nil {
			return compact.ValueSeparationMetadata{}, err
		}
		vs.writers[i].writer = nil
		meta := &manifest.PhysicalBlobFile{
			FileNum:      vs.writers[i].objMeta.DiskFileNum,
			Size:         stats.FileLen,
			ValueSize:    stats.UncompressedValueBytes,
			CreationTime: uint64(time.Now().Unix()),
			Tier:         base.StorageTier(i),
		}
		references[i] = manifest.MakeBlobReference(
			base.BlobFileID(vs.writers[i].objMeta.DiskFileNum),
			stats.UncompressedValueBytes,
			meta,
		)
		if i == int(base.HotTier) {
			numPopulatedHotReferences++
		}
		referenceSize += stats.UncompressedValueBytes
		newBlobFiles = append(newBlobFiles, compact.NewBlobFileInfo{
			FileStats:    stats,
			FileObject:   vs.writers[i].objMeta,
			FileMetadata: meta,
		})
	}
	// The outputBlobReferenceDepth is computed from the input sstables,
	// reflecting the worst-case overlap of referenced blob files. If this
	// sstable references fewer unique hot blob files, reduce its depth to the
	// count of unique hot blob files.
	blobReferenceDepth := min(vs.outputBlobReferenceDepth,
		manifest.BlobReferenceDepth(numPopulatedHotReferences))

	// Reset the minimum size for the next output.
	vs.minimumSize = vs.globalMinimumSize
	// Reset the remaining state.
	vs.preservedReferenceIndexOffset = 0
	vs.tieringSpanForNewColdBlobFile = 0
	vs.currPreservedReferences = vs.currPreservedReferences[:0]
	for i := range vs.totalPreservedValueSize {
		vs.totalPreservedValueSize[i] = 0
		vs.writers[i] = blobWriterAndMeta{}
	}

	return compact.ValueSeparationMetadata{
		BlobReferences:     references,
		BlobReferenceSize:  referenceSize,
		BlobReferenceDepth: blobReferenceDepth,
		NewBlobFiles:       newBlobFiles,
	}, nil
}

// Lint
var _ = &generalizedValueSeparation{}

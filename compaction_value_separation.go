// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/valsep"
	"github.com/cockroachdb/redact"
)

var neverSeparateValues getValueSeparation = func(JobID, *tableCompaction) valsep.ValueSeparation {
	return valsep.NeverSeparateValues{}
}

// determineCompactionValueSeparation determines whether a compaction should
// separate values into blob files. It returns a valsep.ValueSeparation
// implementation that should be used for the compaction.
//
// It assumes that the compaction will write tables at d.TableFormat() or above.
func (d *DB) determineCompactionValueSeparation(
	jobID JobID, c *tableCompaction,
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
	if writeBlobs, outputBlobReferenceDepth := shouldWriteBlobFiles(c, policy, d.opts.Experimental.SpanPolicyFunc, d.cmp); !writeBlobs {
		// This compaction should preserve existing blob references.
		return valsep.NewPreserveAllHotBlobReferences(
			blobFileSet,
			outputBlobReferenceDepth,
			policy.MinimumSize,
			policy.MinimumMVCCGarbageSize,
		)
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
		policy.MinimumMVCCGarbageSize,
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
	c *tableCompaction, policy ValueSeparationPolicy, spanPolicyFunc SpanPolicyFunc, cmp Compare,
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

			// Set expected policy to global policy values to start, and
			// extract the expected values from the span policy.
			expectedMinSize := policy.MinimumSize
			expectedValSepBySuffixDisabled := false
			bounds := t.UserKeyBounds()
			spanPolicy, err := spanPolicyFunc(t.UserKeyBounds())
			// For now, if we can't determine the span policy, we should just assume
			// the default policy is in effect for this table.
			if err == nil {
				if !spanPolicy.StillCovers(cmp, bounds.End.Key) {
					// The table's key range now uses multiple span policies. Rewrite to new
					// blob files so values are stored according to the current policy.
					return true, 0
				}
				if spanPolicy.ValueStoragePolicy.DisableBlobSeparation {
					expectedMinSize = 0
				} else if spanPolicy.ValueStoragePolicy.ContainsOverrides() {
					expectedMinSize = spanPolicy.ValueStoragePolicy.OverrideBlobSeparationMinimumSize
					if expectedMinSize == 0 {
						// A 0 minimum value size on the span policy indicates the field
						// was unset, but other parts of value separation are being
						// overridden. Use the default min size.
						expectedMinSize = policy.MinimumSize
					}
					expectedValSepBySuffixDisabled = spanPolicy.ValueStoragePolicy.DisableSeparationBySuffix
				}
			}

			if int(backingProps.ValueSeparationMinSize) != expectedMinSize ||
				(expectedMinSize > 0 && backingProps.ValueSeparationBySuffixDisabled != expectedValSepBySuffixDisabled) {
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

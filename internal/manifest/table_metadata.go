// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"bytes"
	stdcmp "cmp"
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/cockroachdb/crlib/crmath"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/sstable/virtual"
)

// TableMetadata is maintained for leveled-ssts, i.e., they belong to a level of
// some version. TableMetadata does not contain the actual level of the sst,
// since such leveled-ssts can move across levels in different versions, while
// sharing the same TableMetadata. There are two kinds of leveled-ssts, physical
// and virtual. Underlying both leveled-ssts is a backing-sst, for which the
// only state is TableBacking. A backing-sst is level-less. It is possible for a
// backing-sst to be referred to by a physical sst in one version and by one or
// more virtual ssts in one or more versions. A backing-sst becomes obsolete and
// can be deleted once it is no longer required by any physical or virtual sst
// in any version.
//
// We maintain some invariants:
//
//  1. Each physical and virtual sst will have a unique TableMetadata.TableNum,
//     and there will be exactly one TableMetadata associated with the TableNum.
//
//  2. Within a version, a backing-sst is either only referred to by one
//     physical sst or one or more virtual ssts.
//
//  3. Once a backing-sst is referred to by a virtual sst in the latest version,
//     it cannot go back to being referred to by a physical sst in any future
//     version.
//
// Once a physical sst is no longer needed by any version, we will no longer
// maintain the table metadata associated with it. We will still maintain the
// TableBacking associated with the physical sst if the backing sst is required
// by any virtual ssts in any version.
//
// When using these fields in the context of a Virtual Table, These fields
// have additional invariants imposed on them, and/or slightly varying meanings:
//   - boundTypeSmallest and boundTypeLargest (and their counterparts
//     {Point,Range}KeyBounds.{Smallest(), Largest()}) remain tight bounds that represent a
//     key at that exact bound. We make the effort to determine the next smallest
//     or largest key in an sstable after virtualizing it, to maintain this
//     tightness. If the largest is a sentinel key (IsExclusiveSentinel()), it
//     could mean that a rangedel or range key ends at that user key, or has been
//     truncated to that user key.
//   - One invariant is that if a rangedel or range key is truncated on its
//     upper bound, the virtual sstable *must* have a rangedel or range key
//     sentinel key as its upper bound. This is because truncation yields
//     an exclusive upper bound for the rangedel/rangekey, and if there are
//     any points at that exclusive upper bound within the same virtual
//     sstable, those could get uncovered by this truncation. We enforce this
//     invariant in calls to keyspan.Truncate.
//   - Size is an estimate of the size of the virtualized portion of this sstable.
//     The underlying file's size is stored in TableBacking.Size, though it could
//     also be estimated or could correspond to just the referenced portion of
//     a file (eg. if the file originated on another node).
//   - Size must be > 0.
//   - SmallestSeqNum and LargestSeqNum are loose bounds for virtual sstables.
//     This means that all keys in the virtual sstable must have seqnums within
//     [SmallestSeqNum, LargestSeqNum], however there's no guarantee that there's
//     a key with a seqnum at either of the bounds. Calculating tight seqnum
//     bounds would be too expensive and deliver little value.
//   - Note: These properties do not apply to external sstables, whose bounds are
//     loose rather than tight, as we do not open them on ingest.
type TableMetadata struct {
	// AllowedSeeks is used to determine if a file should be picked for
	// a read triggered compaction. It is decremented when read sampling
	// in pebble.Iterator after every after every positioning operation
	// that returns a user key (eg. Next, Prev, SeekGE, SeekLT, etc).
	AllowedSeeks atomic.Int64

	// TableBacking is the physical file that backs either physical or virtual
	// sstables.
	TableBacking *TableBacking

	// InitAllowedSeeks is the inital value of allowed seeks. This is used
	// to re-set allowed seeks on a file once it hits 0.
	InitAllowedSeeks int64
	// TableNum is the table number, unique across the lifetime of a DB.
	//
	// INVARIANT: when !TableMetadata.Virtual, TableNum == TableBacking.DiskFileNum.
	TableNum base.TableNum
	// Size is the size of the file, in bytes. Size is an approximate value for
	// virtual sstables.
	//
	// INVARIANTS:
	// - When !TableMetadata.Virtual, Size == TableBacking.Size.
	// - Size should be non-zero. Size 0 virtual sstables must not be created.
	Size uint64
	// File creation time in seconds since the epoch (1970-01-01 00:00:00
	// UTC). For ingested sstables, this corresponds to the time the file was
	// ingested. For virtual sstables, this corresponds to the wall clock time
	// when the TableMetadata for the virtual sstable was first created.
	CreationTime int64
	// LargestSeqNumAbsolute is an upper bound for the largest sequence number
	// in the table. This upper bound is guaranteed to be higher than any
	// sequence number any of the table's keys have held at any point in time
	// while the database has been open. Specifically, if the table contains
	// keys that have had their sequence numbers zeroed during a compaction,
	// LargestSeqNumAbsolute will be at least as high as the pre-zeroing
	// sequence number. LargestSeqNumAbsolute is NOT durably persisted, so after
	// a database restart it takes on the value of LargestSeqNum.
	LargestSeqNumAbsolute base.SeqNum
	// Lower and upper bounds for the smallest and largest sequence numbers in
	// the table, across both point and range keys. For physical sstables, these
	// values are tight bounds. For virtual sstables, there is no guarantee that
	// there will be keys with SmallestSeqNum or LargestSeqNum within virtual
	// sstable bounds.
	SmallestSeqNum base.SeqNum
	LargestSeqNum  base.SeqNum
	// PointKeyBounds.Smallest() and PointKeyBounds.Largest() are the inclusive bounds for the
	// internal point keys stored in the table. This includes RANGEDELs, which
	// alter point keys.
	// NB: these field should be set using ExtendPointKeyBounds. They are left
	// exported for reads as an optimization.
	PointKeyBounds InternalKeyBounds
	// RangeKeyBounds.Smallest() and RangeKeyBounds.Largest() are the bounds for the
	// internal range keys stored in the table.
	// NB: these field should be set using ExtendRangeKeyBounds. They are left
	// exported for reads as an optimization.
	RangeKeyBounds *InternalKeyBounds
	// BlobReferences is a list of blob files containing values that are
	// referenced by this sstable.
	BlobReferences BlobReferences
	// BlobReferenceDepth is the stack depth of blob files referenced by this
	// sstable. See the comment on the BlobReferenceDepth type for more details.
	//
	// INVARIANT: BlobReferenceDepth == 0 iff len(BlobReferences) == 0
	// INVARIANT: BlobReferenceDepth <= len(BlobReferences)
	BlobReferenceDepth BlobReferenceDepth

	// refs is the reference count for the table, used to determine when a table
	// is obsolete. When a table's reference count falls to zero, the table is
	// considered obsolete and the table's references on its associated files
	// (backing file, blob references) are released.
	//
	// The tables in each version are maintained in a copy-on-write B-tree and
	// each B-tree node keeps a reference on the contained tables.
	refs atomic.Int32

	// statsValid indicates if stats have been loaded for the table. The
	// TableStats structure is populated only if valid is true.
	statsValid atomic.Bool
	// stats describe table statistics. Written only once, by the same process
	//
	stats TableStats

	// For L0 files only. Protected by DB.mu. Used to generate L0 sublevels and
	// pick L0 compactions. Only accurate for the most recent Version.
	// TODO(radu): this is very hacky and fragile. This information should live
	// inside l0Sublevels.
	SubLevel         int
	L0Index          int
	minIntervalIndex int
	maxIntervalIndex int

	// NB: the alignment of this struct is 8 bytes. We pack all the bools to
	// ensure an optimal packing.

	// IsIntraL0Compacting is set to True if this file is part of an intra-L0
	// compaction. When it's true, IsCompacting must also return true. If
	// Compacting is true and IsIntraL0Compacting is false for an L0 file, the
	// file must be part of a compaction to Lbase.
	IsIntraL0Compacting bool
	CompactionState     CompactionState
	// True if compaction of this file has been explicitly requested.
	// Previously, RocksDB and earlier versions of Pebble allowed this
	// flag to be set by a user table property collector. Some earlier
	// versions of Pebble respected this flag, while other more recent
	// versions ignored this flag.
	//
	// More recently this flag has been repurposed to facilitate the
	// compaction of 'atomic compaction units'. Files marked for
	// compaction are compacted in a rewrite compaction at the lowest
	// possible compaction priority.
	//
	// NB: A count of files marked for compaction is maintained on
	// Version, and compaction picking reads cached annotations
	// determined by this field.
	//
	// Protected by DB.mu.
	MarkedForCompaction bool
	// HasPointKeys tracks whether the table contains point keys (including
	// RANGEDELs). If a table contains only range deletions, HasPointsKeys is
	// still true.
	HasPointKeys bool
	// HasRangeKeys tracks whether the table contains any range keys.
	HasRangeKeys bool
	// Virtual is true if the TableMetadata belongs to a virtual sstable.
	Virtual bool
	// boundsSet track whether the overall bounds have been set.
	boundsSet bool
	// boundTypeSmallest and boundTypeLargest provide an indication as to which
	// key type (point or range) corresponds to the smallest and largest overall
	// table bounds.
	boundTypeSmallest, boundTypeLargest boundType
	// VirtualParams are set only when Virtual is true.
	VirtualParams *virtual.VirtualReaderParams

	// SyntheticPrefix is used to prepend a prefix to all keys and/or override all
	// suffixes in a table; used for some virtual tables.
	SyntheticPrefixAndSuffix sstable.SyntheticPrefixAndSuffix
}

// Ref increments the table's ref count. If this is the table's first reference,
// Ref will increment the reference of the table's TableBacking.
func (m *TableMetadata) Ref() {
	if v := m.refs.Add(1); v == 1 {
		m.TableBacking.Ref()
	}
}

// Unref decrements the table's reference count. If the count reaches zero, the
// table releases its references on associated files. If the table's backing
// file becomes obsolete, it's inserted into the provided ObsoleteFiles.
func (m *TableMetadata) Unref(obsoleteFiles ObsoleteFilesSet) {
	v := m.refs.Add(-1)
	if invariants.Enabled && v < 0 {
		panic(errors.AssertionFailedf("pebble: invalid TableMetadata refcounting for table %s", m.TableNum))
	}
	// When the reference count reaches zero, release the table's references.
	if v == 0 {
		if m.TableBacking.Unref() == 0 {
			obsoleteFiles.AddBacking(m.TableBacking)
		}
	}
}

// InternalKeyBounds returns the set of overall table bounds.
func (m *TableMetadata) InternalKeyBounds() (InternalKey, InternalKey) {
	return m.Smallest(), m.Largest()
}

// UserKeyBounds returns the user key bounds that correspond to m.Smallest and
// Largest. Because we do not allow split user keys, the user key bounds of
// files within a level do not overlap.
func (m *TableMetadata) UserKeyBounds() base.UserKeyBounds {
	return base.UserKeyBoundsFromInternal(m.Smallest(), m.Largest())
}

// UserKeyBoundsByType returns the user key bounds for the given key types.
// Note that the returned bounds are invalid when requesting KeyTypePoint but
// HasPointKeys is false, or when requesting KeyTypeRange and HasRangeKeys is
// false.
func (m *TableMetadata) UserKeyBoundsByType(keyType KeyType) base.UserKeyBounds {
	switch keyType {
	case KeyTypePoint:
		return base.UserKeyBoundsFromInternal(m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest())
	case KeyTypeRange:
		if !m.HasRangeKeys {
			return base.UserKeyBounds{}
		}
		return base.UserKeyBoundsFromInternal(m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest())
	default:
		return base.UserKeyBoundsFromInternal(m.Smallest(), m.Largest())
	}
}

// SyntheticSeqNum returns a SyntheticSeqNum which is set when SmallestSeqNum
// equals LargestSeqNum.
func (m *TableMetadata) SyntheticSeqNum() sstable.SyntheticSeqNum {
	if m.SmallestSeqNum == m.LargestSeqNum {
		return sstable.SyntheticSeqNum(m.SmallestSeqNum)
	}
	return sstable.NoSyntheticSeqNum
}

// IterTransforms returns an sstable.IterTransforms populated according to the
// file.
func (m *TableMetadata) IterTransforms() sstable.IterTransforms {
	return sstable.IterTransforms{
		SyntheticSeqNum:          m.SyntheticSeqNum(),
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
	}
}

// FragmentIterTransforms returns an sstable.FragmentIterTransforms populated
// according to the file.
func (m *TableMetadata) FragmentIterTransforms() sstable.FragmentIterTransforms {
	return sstable.FragmentIterTransforms{
		SyntheticSeqNum:          m.SyntheticSeqNum(),
		SyntheticPrefixAndSuffix: m.SyntheticPrefixAndSuffix,
	}
}

func (m *TableMetadata) PhysicalMeta() *TableMetadata {
	if m.Virtual {
		panic("pebble: table metadata does not belong to a physical sstable")
	}
	return m
}

func (m *TableMetadata) VirtualMeta() *TableMetadata {
	if !m.Virtual {
		panic("pebble: table metadata does not belong to a virtual sstable")
	}
	return m
}

// EstimatedReferenceSize returns the estimated physical size of all the file's
// blob references in the table. This sum, added to the sstable's size, yields
// an approximation of the overall size of the data represented by the table.
//
// EstimatedReferenceSize is an estimate, but it's guaranteed to be stable over
// the lifetime of the table. This is necessary to correctly maintain
// incrementally-updated metrics.
func (m *TableMetadata) EstimatedReferenceSize() uint64 {
	var size uint64
	for i := range m.BlobReferences {
		size += m.BlobReferences[i].EstimatedPhysicalSize
	}
	return size
}

// ObjectInfo implements the base.ObjectInfo interface.
func (m *TableMetadata) FileInfo() (base.FileType, base.DiskFileNum) {
	return base.FileTypeTable, m.TableBacking.DiskFileNum
}

// TableBacking either backs a single physical sstable, or one or more virtual
// sstables.
//
// See the comment above the TableMetadata type for sstable terminology.
type TableBacking struct {
	DiskFileNum base.DiskFileNum
	Size        uint64

	// Reference count for the backing file, used to determine when a backing file
	// is obsolete and can be removed.
	//
	// The reference count is at least the number of distinct tables that use this
	// backing across all versions that have a non-zero reference count. The tables
	// in each version are maintained in a copy-on-write B-tree and each B-tree node
	// keeps a reference on the respective backings.
	//
	// In addition, a reference count is taken for every backing in the latest
	// version's VirtualBackings (necessary to support Protect/Unprotect).
	refs atomic.Int32

	propsValid atomic.Bool
	// stats are populated exactly once.
	stats TableBackingProperties
}

// MustHaveRefs asserts that the backing has a positive refcount.
func (b *TableBacking) MustHaveRefs() {
	if refs := b.refs.Load(); refs <= 0 {
		panic(errors.AssertionFailedf("backing %s must have positive refcount (refs=%d)",
			b.DiskFileNum, refs))
	}
}

// Ref increments the backing's ref count.
func (b *TableBacking) Ref() {
	b.refs.Add(1)
}

// IsUnused returns if the backing is not being used by any tables in a version
// or btree.
func (b *TableBacking) IsUnused() bool {
	return b.refs.Load() == 0
}

// Unref decrements the backing's ref count (and returns the new count).
func (b *TableBacking) Unref() int32 {
	v := b.refs.Add(-1)
	if invariants.Enabled && v < 0 {
		panic(errors.AssertionFailedf("pebble: invalid TableBacking refcounting: file %s has refcount %d", b.DiskFileNum, v))
	}
	return v
}

// TableBackingProperties are properties of the physical backing; they are
// directly derived from the sstable.Properties of the physical table.
type TableBackingProperties struct {
	sstable.CommonProperties
	CompressionStats block.CompressionStats
}

// Properties returns the backing properties if they have been populated, or nil and
// ok=false if they were not.
//
// The caller must not modify the returned stats.
func (b *TableBacking) Properties() (_ *TableBackingProperties, ok bool) {
	if !b.propsValid.Load() {
		return nil, false
	}
	return &b.stats, true
}

// PopulateProperties populates the table stats. Can be called at most once for a
// TableBacking.
func (b *TableBacking) PopulateProperties(props *sstable.Properties) *TableBackingProperties {
	b.stats.CommonProperties = props.CommonProperties
	var err error
	// TODO(radu): store block.CompressionStats directly in props, to avoid having
	// to parse them back.
	b.stats.CompressionStats, err = block.ParseCompressionStats(props.CompressionStats)
	if invariants.Enabled && err != nil {
		panic(errors.AssertionFailedf("pebble: error parsing compression stats %q for table %s: %v", b.stats.CompressionStats, b.DiskFileNum, err))
	}
	oldStatsValid := b.propsValid.Swap(true)
	if invariants.Enabled && oldStatsValid {
		panic("stats set twice")
	}
	return &b.stats
}

// InitPhysicalBacking allocates and sets the TableBacking which is required by a
// physical sstable TableMetadata.
//
// Ensure that the state required by TableBacking, such as the TableNum, is
// already set on the TableMetadata before InitPhysicalBacking is called.
// Calling InitPhysicalBacking only after the relevant state has been set in the
// TableMetadata is not necessary in tests which don't rely on TableBacking.
func (m *TableMetadata) InitPhysicalBacking() {
	if m.Virtual {
		panic("pebble: virtual sstables should use a pre-existing TableBacking")
	}
	if m.TableBacking != nil {
		panic("backing already initialized")
	}
	m.TableBacking = &TableBacking{
		DiskFileNum: base.PhysicalTableDiskFileNum(m.TableNum),
		Size:        m.Size,
	}
}

// InitVirtualBacking creates a new TableBacking for a virtual table.
//
// The Smallest/Largest bounds must already be set to their final values.
func (m *TableMetadata) InitVirtualBacking(fileNum base.DiskFileNum, size uint64) {
	m.AttachVirtualBacking(&TableBacking{
		DiskFileNum: fileNum,
		Size:        size,
	})
}

// AttachVirtualBacking attaches an existing TableBacking for a virtual table.
//
// The Smallest/Largest bounds must already be set to their final values.
func (m *TableMetadata) AttachVirtualBacking(backing *TableBacking) {
	if !m.Virtual {
		panic("pebble: provider-backed sstables must be virtual")
	}
	if m.TableBacking != nil {
		panic("backing already initialized")
	}
	m.TableBacking = backing
	if m.Smallest().UserKey == nil || m.Largest().UserKey == nil {
		panic("bounds must be set before attaching backing")
	}
	m.VirtualParams = &virtual.VirtualReaderParams{
		Lower:   m.Smallest(),
		Upper:   m.Largest(),
		FileNum: m.TableNum,
	}
}

// ValidateVirtual should be called once the TableMetadata for a virtual sstable
// is created to verify that the fields of the virtual sstable are sound.
func (m *TableMetadata) ValidateVirtual(createdFrom *TableMetadata) {
	switch {
	case !m.Virtual:
		panic("pebble: invalid virtual sstable")
	case createdFrom.SmallestSeqNum != m.SmallestSeqNum:
		panic("pebble: invalid smallest sequence number for virtual sstable")
	case createdFrom.LargestSeqNum != m.LargestSeqNum:
		panic("pebble: invalid largest sequence number for virtual sstable")
	case createdFrom.LargestSeqNumAbsolute != m.LargestSeqNumAbsolute:
		panic("pebble: invalid largest absolute sequence number for virtual sstable")
	case createdFrom.TableBacking != nil && createdFrom.TableBacking != m.TableBacking:
		panic("pebble: invalid physical sstable state for virtual sstable")
	case m.Size == 0:
		panic("pebble: virtual sstable size must be set upon creation")
	}
}

// SetCompactionState transitions this file's compaction state to the given
// state. Protected by DB.mu.
func (m *TableMetadata) SetCompactionState(to CompactionState) {
	if invariants.Enabled {
		transitionErr := func() error {
			return errors.Newf("pebble: invalid compaction state transition: %s -> %s", m.CompactionState, to)
		}
		switch m.CompactionState {
		case CompactionStateNotCompacting:
			if to != CompactionStateCompacting {
				panic(transitionErr())
			}
		case CompactionStateCompacting:
			if to != CompactionStateCompacted && to != CompactionStateNotCompacting {
				panic(transitionErr())
			}
		case CompactionStateCompacted:
			panic(transitionErr())
		default:
			panic(fmt.Sprintf("pebble: unknown compaction state: %d", m.CompactionState))
		}
	}
	m.CompactionState = to
}

// IsCompacting returns true if this file's compaction state is
// CompactionStateCompacting. Protected by DB.mu.
func (m *TableMetadata) IsCompacting() bool {
	return m.CompactionState == CompactionStateCompacting
}

// Stats returns the table statistics if they have been populated, or nil and
// ok=false if they were not.
//
// The caller must not modify the returned stats.
func (m *TableMetadata) Stats() (_ *TableStats, ok bool) {
	if !m.statsValid.Load() {
		return nil, false
	}
	return &m.stats, true
}

// PopulateStats populates the table stats. Can be called at most once for a
// TableMetadata.
func (m *TableMetadata) PopulateStats(stats *TableStats) {
	m.stats = *stats
	oldStatsValid := m.statsValid.Swap(true)
	if invariants.Enabled && oldStatsValid {
		panic("stats set twice")
	}
}

// ExtendPointKeyBounds attempts to extend the lower and upper point key bounds
// and overall table bounds with the given smallest and largest keys. The
// smallest and largest bounds may not be extended if the table already has a
// bound that is smaller or larger, respectively. The receiver is returned.
// NB: calling this method should be preferred to manually setting the bounds by
// manipulating the fields directly, to maintain certain invariants.
func (m *TableMetadata) ExtendPointKeyBounds(
	cmp Compare, smallest, largest InternalKey,
) *TableMetadata {
	// Update the point key bounds.
	if !m.HasPointKeys {
		m.PointKeyBounds.SetInternalKeyBounds(smallest, largest)
		m.HasPointKeys = true
	} else {
		isSmallestPoint := base.InternalCompare(cmp, smallest, m.PointKeyBounds.Smallest()) < 0
		isLargestPoint := base.InternalCompare(cmp, largest, m.PointKeyBounds.Largest()) > 0
		if isSmallestPoint && isLargestPoint {
			m.PointKeyBounds.SetInternalKeyBounds(smallest, largest)
		} else if isSmallestPoint {
			m.PointKeyBounds.SetSmallest(smallest)
		} else if isLargestPoint {
			m.PointKeyBounds.SetLargest(largest)
		}
	}
	// Update the overall bounds.
	m.extendOverallBounds(cmp, m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest(), boundTypePointKey)
	return m
}

// ExtendRangeKeyBounds attempts to extend the lower and upper range key bounds
// and overall table bounds with the given smallest and largest keys. The
// smallest and largest bounds may not be extended if the table already has a
// bound that is smaller or larger, respectively. The receiver is returned.
// NB: calling this method should be preferred to manually setting the bounds by
// manipulating the fields directly, to maintain certain invariants.
func (m *TableMetadata) ExtendRangeKeyBounds(
	cmp Compare, smallest, largest InternalKey,
) *TableMetadata {
	// Update the range key bounds.
	if !m.HasRangeKeys {
		m.RangeKeyBounds = &InternalKeyBounds{}
		m.RangeKeyBounds.SetInternalKeyBounds(smallest, largest)
		m.HasRangeKeys = true
	} else {
		isSmallestRange := base.InternalCompare(cmp, smallest, m.RangeKeyBounds.Smallest()) < 0
		isLargestRange := base.InternalCompare(cmp, largest, m.RangeKeyBounds.Largest()) > 0
		if isSmallestRange && isLargestRange {
			m.RangeKeyBounds.SetInternalKeyBounds(smallest, largest)
		} else if isSmallestRange {
			m.RangeKeyBounds.SetSmallest(smallest)
		} else if isLargestRange {
			m.RangeKeyBounds.SetLargest(largest)
		}
	}
	// Update the overall bounds.
	m.extendOverallBounds(cmp, m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest(), boundTypeRangeKey)
	return m
}

// extendOverallBounds attempts to extend the overall table lower and upper
// bounds. The given bounds may not be used if a lower or upper bound already
// exists that is smaller or larger than the given keys, respectively. The given
// boundType will be used if the bounds are updated.
func (m *TableMetadata) extendOverallBounds(
	cmp Compare, smallest, largest InternalKey, bTyp boundType,
) {
	if !m.boundsSet {
		m.boundsSet = true
		m.boundTypeSmallest, m.boundTypeLargest = bTyp, bTyp
	} else {
		if base.InternalCompare(cmp, smallest, m.Smallest()) < 0 {
			m.boundTypeSmallest = bTyp
		}
		if base.InternalCompare(cmp, largest, m.Largest()) > 0 {
			m.boundTypeLargest = bTyp
		}
	}
}

// Overlaps returns true if the file key range overlaps with the given user key bounds.
func (m *TableMetadata) Overlaps(cmp Compare, bounds *base.UserKeyBounds) bool {
	b := m.UserKeyBounds()
	return b.Overlaps(cmp, bounds)
}

// ContainedWithinSpan returns true if the file key range completely overlaps with the
// given range ("end" is assumed to exclusive).
func (m *TableMetadata) ContainedWithinSpan(cmp Compare, start, end []byte) bool {
	lowerCmp, upperCmp := cmp(m.Smallest().UserKey, start), cmp(m.Largest().UserKey, end)
	return lowerCmp >= 0 && (upperCmp < 0 || (upperCmp == 0 && m.Largest().IsExclusiveSentinel()))
}

// ContainsKeyType returns whether or not the file contains keys of the provided
// type.
func (m *TableMetadata) ContainsKeyType(kt KeyType) bool {
	switch kt {
	case KeyTypePointAndRange:
		return true
	case KeyTypePoint:
		return m.HasPointKeys
	case KeyTypeRange:
		return m.HasRangeKeys
	default:
		panic("unrecognized key type")
	}
}

// SmallestBound returns the file's smallest bound of the key type. It returns a
// false second return value if the file does not contain any keys of the key
// type.
func (m *TableMetadata) SmallestBound(kt KeyType) (InternalKey, bool) {
	switch kt {
	case KeyTypePointAndRange:
		return m.Smallest(), true
	case KeyTypePoint:
		return m.PointKeyBounds.Smallest(), m.HasPointKeys
	case KeyTypeRange:
		if !m.HasRangeKeys {
			return InternalKey{}, m.HasRangeKeys
		}
		return m.RangeKeyBounds.Smallest(), m.HasRangeKeys
	default:
		panic("unrecognized key type")
	}
}

// LargestBound returns the file's largest bound of the key type. It returns a
// false second return value if the file does not contain any keys of the key
// type.
func (m *TableMetadata) LargestBound(kt KeyType) (InternalKey, bool) {
	switch kt {
	case KeyTypePointAndRange:
		ik := m.Largest()
		return ik, true
	case KeyTypePoint:
		return m.PointKeyBounds.Largest(), m.HasPointKeys
	case KeyTypeRange:
		if !m.HasRangeKeys {
			return InternalKey{}, m.HasRangeKeys
		}
		return m.RangeKeyBounds.Largest(), m.HasRangeKeys
	default:
		panic("unrecognized key type")
	}
}

const (
	maskContainsPointKeys = 1 << 0
	maskSmallest          = 1 << 1
	maskLargest           = 1 << 2
)

// boundsMarker returns a marker byte whose bits encode the following
// information (in order from least significant bit):
// - if the table contains point keys
// - if the table's smallest key is a point key
// - if the table's largest key is a point key
func (m *TableMetadata) boundsMarker() (sentinel uint8, err error) {
	if m.HasPointKeys {
		sentinel |= maskContainsPointKeys
	}
	switch m.boundTypeSmallest {
	case boundTypePointKey:
		sentinel |= maskSmallest
	case boundTypeRangeKey:
		// No op - leave bit unset.
	default:
		return 0, base.CorruptionErrorf("file %s has neither point nor range key as smallest key", m.TableNum)
	}
	switch m.boundTypeLargest {
	case boundTypePointKey:
		sentinel |= maskLargest
	case boundTypeRangeKey:
		// No op - leave bit unset.
	default:
		return 0, base.CorruptionErrorf("file %s has neither point nor range key as largest key", m.TableNum)
	}
	return
}

// ScaleStatistic scales the given value by the table-size:backing-size ratio if
// the table is virtual. Returns the unchanged value when the table is not
// virtual.
//
// The scaling rounds up so any non-zero value stays non-zero.
func (m *TableMetadata) ScaleStatistic(value uint64) uint64 {
	if !m.Virtual {
		return value
	}
	// Make sure the sizes are sane, just in case.
	size := max(m.Size, 1)
	backingSize := max(m.TableBacking.Size, size)
	return crmath.ScaleUint64(value, size, backingSize)
}

// String implements fmt.Stringer, printing the file number and the overall
// table bounds.
func (m *TableMetadata) String() string {
	return fmt.Sprintf("%s:[%s-%s]", m.TableNum, m.Smallest(), m.Largest())
}

// DebugString returns a verbose representation of TableMetadata, typically for
// use in tests and debugging, returning the file number and the point, range
// and overall bounds for the table.
func (m *TableMetadata) DebugString(format base.FormatKey, verbose bool) string {
	var b bytes.Buffer
	if m.Virtual {
		fmt.Fprintf(&b, "%s(%s):[%s-%s]",
			m.TableNum, m.TableBacking.DiskFileNum, m.Smallest().Pretty(format), m.Largest().Pretty(format))
	} else {
		fmt.Fprintf(&b, "%s:[%s-%s]",
			m.TableNum, m.Smallest().Pretty(format), m.Largest().Pretty(format))
	}
	if !verbose {
		return b.String()
	}
	fmt.Fprintf(&b, " seqnums:[%d-%d]", m.SmallestSeqNum, m.LargestSeqNum)
	if m.HasPointKeys {
		fmt.Fprintf(&b, " points:[%s-%s]",
			m.PointKeyBounds.Smallest().Pretty(format), m.PointKeyBounds.Largest().Pretty(format))
	}
	if m.HasRangeKeys {
		fmt.Fprintf(&b, " ranges:[%s-%s]",
			m.RangeKeyBounds.Smallest().Pretty(format), m.RangeKeyBounds.Largest().Pretty(format))
	}
	if m.Size != 0 {
		fmt.Fprintf(&b, " size:%d", m.Size)
		if m.Virtual && m.TableBacking != nil {
			fmt.Fprintf(&b, "(%d)", m.TableBacking.Size)
		}
	}
	if len(m.BlobReferences) > 0 {
		fmt.Fprint(&b, " blobrefs:[")
		for i, r := range m.BlobReferences {
			if i > 0 {
				fmt.Fprint(&b, ", ")
			}
			fmt.Fprintf(&b, "(%s: %d)", r.FileID, r.ValueSize)
		}
		fmt.Fprintf(&b, "; depth:%d]", m.BlobReferenceDepth)
	}
	return b.String()
}

const debugParserSeparators = ":-[]();{}"

// errFromPanic can be used in a recover block to convert panics into errors.
func errFromPanic(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return errors.Errorf("%v", r)
}

// ParseTableMetadataDebug parses a TableMetadata from its DebugString
// representation.
func ParseTableMetadataDebug(s string) (_ *TableMetadata, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.CombineErrors(err, errFromPanic(r))
		}
	}()

	// Input format:
	//	000000:[a#0,SET-z#0,SET] seqnums:[5-5] points:[...] ranges:[...] size:5
	m := &TableMetadata{}
	p := strparse.MakeParser(debugParserSeparators, s)
	m.TableNum = p.FileNum()
	var backingNum base.DiskFileNum
	if p.Peek() == "(" {
		p.Expect("(")
		backingNum = p.DiskFileNum()
		p.Expect(")")
	}
	p.Expect(":", "[")

	smallest := p.InternalKey()
	p.Expect("-")
	largest := p.InternalKey()
	p.Expect("]")

	for !p.Done() {
		field := p.Next()
		p.Expect(":")
		switch field {
		case "seqnums":
			p.Expect("[")
			m.SmallestSeqNum = p.SeqNum()
			p.Expect("-")
			m.LargestSeqNum = p.SeqNum()
			p.Expect("]")
			m.LargestSeqNumAbsolute = m.LargestSeqNum

		case "points":
			p.Expect("[")
			smallestPoint := p.InternalKey()
			p.Expect("-")
			m.PointKeyBounds.SetInternalKeyBounds(smallestPoint, p.InternalKey())
			m.HasPointKeys = true
			p.Expect("]")

		case "ranges":
			m.RangeKeyBounds = &InternalKeyBounds{}
			p.Expect("[")
			smallest := p.InternalKey()
			p.Expect("-")
			m.RangeKeyBounds.SetInternalKeyBounds(smallest, p.InternalKey())
			m.HasRangeKeys = true
			p.Expect("]")

		case "size":
			m.Size = p.Uint64()

		case "blobrefs":
			p.Expect("[")
			for p.Peek() != ";" {
				if p.Peek() == "," {
					p.Expect(",")
				}
				p.Expect("(")
				var ref BlobReference
				ref.FileID = p.BlobFileID()
				p.Expect(":")
				ref.ValueSize = p.Uint64()
				m.BlobReferences = append(m.BlobReferences, ref)
				p.Expect(")")
			}
			p.Expect(";")
			p.Expect("depth")
			p.Expect(":")
			m.BlobReferenceDepth = BlobReferenceDepth(p.Uint64())
			p.Expect("]")

		default:
			p.Errf("unknown field %q", field)
		}
	}

	cmp := base.DefaultComparer.Compare
	if base.InternalCompare(cmp, smallest, m.PointKeyBounds.Smallest()) == 0 {
		m.boundTypeSmallest = boundTypePointKey
	} else if m.HasRangeKeys && base.InternalCompare(cmp, smallest, m.RangeKeyBounds.Smallest()) == 0 {
		m.boundTypeSmallest = boundTypeRangeKey
	}
	if base.InternalCompare(cmp, largest, m.PointKeyBounds.Largest()) == 0 {
		m.boundTypeLargest = boundTypePointKey
	} else if m.HasRangeKeys && base.InternalCompare(cmp, largest, m.RangeKeyBounds.Largest()) == 0 {
		m.boundTypeLargest = boundTypeRangeKey
	}

	// By default, when the parser sees just the overall bounds, we set the point
	// keys. This preserves backwards compatability with existing test cases that
	// specify only the overall bounds.
	if !m.HasPointKeys && !m.HasRangeKeys {
		m.PointKeyBounds.SetInternalKeyBounds(smallest, largest)
		m.HasPointKeys = true
		m.boundTypeSmallest, m.boundTypeLargest = boundTypePointKey, boundTypePointKey
	}
	if backingNum == 0 {
		m.InitPhysicalBacking()
	} else {
		m.Virtual = true
		m.InitVirtualBacking(backingNum, 0 /* size */)
	}
	return m, nil
}

// Validate validates the metadata for consistency with itself, returning an
// error if inconsistent.
func (m *TableMetadata) Validate(cmp Compare, formatKey base.FormatKey) error {
	// Combined range and point key validation.

	if !m.HasPointKeys && !m.HasRangeKeys {
		return base.CorruptionErrorf("file %s has neither point nor range keys",
			errors.Safe(m.TableNum))
	}
	if base.InternalCompare(cmp, m.Smallest(), m.Largest()) > 0 {
		return base.CorruptionErrorf("file %s has inconsistent bounds: %s vs %s",
			errors.Safe(m.TableNum), m.Smallest().Pretty(formatKey),
			m.Largest().Pretty(formatKey))
	}
	if m.SmallestSeqNum > m.LargestSeqNum {
		return base.CorruptionErrorf("file %s has inconsistent seqnum bounds: %d vs %d",
			errors.Safe(m.TableNum), m.SmallestSeqNum, m.LargestSeqNum)
	}
	if m.LargestSeqNumAbsolute < m.LargestSeqNum {
		return base.CorruptionErrorf("file %s has inconsistent absolute largest seqnum bounds: %d vs %d",
			errors.Safe(m.TableNum), m.LargestSeqNumAbsolute, m.LargestSeqNum)
	}

	// Point key validation.

	if m.HasPointKeys {
		if base.InternalCompare(cmp, m.PointKeyBounds.Smallest(), m.PointKeyBounds.Largest()) > 0 {
			return base.CorruptionErrorf("file %s has inconsistent point key bounds: %s vs %s",
				errors.Safe(m.TableNum), m.PointKeyBounds.Smallest().Pretty(formatKey),
				m.PointKeyBounds.Largest().Pretty(formatKey))
		}
		if base.InternalCompare(cmp, m.PointKeyBounds.Smallest(), m.Smallest()) < 0 ||
			base.InternalCompare(cmp, m.PointKeyBounds.Largest(), m.Largest()) > 0 {
			return base.CorruptionErrorf(
				"file %s has inconsistent point key bounds relative to overall bounds: "+
					"overall = [%s-%s], point keys = [%s-%s]",
				errors.Safe(m.TableNum),
				m.Smallest().Pretty(formatKey), m.Largest().Pretty(formatKey),
				m.PointKeyBounds.Smallest().Pretty(formatKey), m.PointKeyBounds.Largest().Pretty(formatKey),
			)
		}
		if !isValidPointBoundKeyKind[m.PointKeyBounds.Smallest().Kind()] {
			return base.CorruptionErrorf("file %s has invalid smallest point key kind", m)
		}
		if !isValidPointBoundKeyKind[m.PointKeyBounds.Largest().Kind()] {
			return base.CorruptionErrorf("file %s has invalid largest point key kind", m)
		}
	}

	// Range key validation.

	if m.HasRangeKeys {
		if base.InternalCompare(cmp, m.RangeKeyBounds.Smallest(), m.RangeKeyBounds.Largest()) > 0 {
			return base.CorruptionErrorf("file %s has inconsistent range key bounds: %s vs %s",
				errors.Safe(m.TableNum), m.RangeKeyBounds.Smallest().Pretty(formatKey),
				m.RangeKeyBounds.Largest().Pretty(formatKey))
		}
		if base.InternalCompare(cmp, m.RangeKeyBounds.Smallest(), m.Smallest()) < 0 ||
			base.InternalCompare(cmp, m.RangeKeyBounds.Largest(), m.Largest()) > 0 {
			return base.CorruptionErrorf(
				"file %s has inconsistent range key bounds relative to overall bounds: "+
					"overall = [%s-%s], range keys = [%s-%s]",
				errors.Safe(m.TableNum),
				m.Smallest().Pretty(formatKey), m.Largest().Pretty(formatKey),
				m.RangeKeyBounds.Smallest().Pretty(formatKey), m.RangeKeyBounds.Largest().Pretty(formatKey),
			)
		}
		if !isValidRangeKeyBoundKeyKind[m.RangeKeyBounds.Smallest().Kind()] {
			return base.CorruptionErrorf("file %s has invalid smallest range key kind", m)
		}
		if !isValidRangeKeyBoundKeyKind[m.RangeKeyBounds.Largest().Kind()] {
			return base.CorruptionErrorf("file %s has invalid largest range key kind", m)
		}
	}

	// Ensure that TableMetadata.Init was called.
	if m.TableBacking == nil {
		return base.CorruptionErrorf("table metadata TableBacking not set")
	}
	// Assert that there's a nonzero blob reference depth if and only if the
	// table has a nonzero count of blob references. Additionally, the file's
	// blob reference depth should be bounded by the number of blob references.
	if (len(m.BlobReferences) == 0) != (m.BlobReferenceDepth == 0) || m.BlobReferenceDepth > BlobReferenceDepth(len(m.BlobReferences)) {
		return base.CorruptionErrorf("table %s with %d blob refs but %d blob ref depth",
			m.TableNum, len(m.BlobReferences), m.BlobReferenceDepth)
	}
	if m.SyntheticPrefixAndSuffix.HasPrefix() {
		if !m.Virtual {
			return base.CorruptionErrorf("non-virtual file with synthetic prefix")
		}
		if !bytes.HasPrefix(m.Smallest().UserKey, m.SyntheticPrefixAndSuffix.Prefix()) {
			return base.CorruptionErrorf("virtual file with synthetic prefix has smallest key with a different prefix: %s", m.Smallest().Pretty(formatKey))
		}
		if !bytes.HasPrefix(m.Largest().UserKey, m.SyntheticPrefixAndSuffix.Prefix()) {
			return base.CorruptionErrorf("virtual file with synthetic prefix has largest key with a different prefix: %s", m.Largest().Pretty(formatKey))
		}
	}
	if m.SyntheticPrefixAndSuffix.HasSuffix() {
		if !m.Virtual {
			return base.CorruptionErrorf("non-virtual file with synthetic suffix")
		}
	}

	return nil
}

var (
	isValidPointBoundKeyKind = [base.InternalKeyKindMax + 1]bool{
		base.InternalKeyKindDelete:        true,
		base.InternalKeyKindSet:           true,
		base.InternalKeyKindMerge:         true,
		base.InternalKeyKindSingleDelete:  true,
		base.InternalKeyKindRangeDelete:   true,
		base.InternalKeyKindSetWithDelete: true,
		base.InternalKeyKindDeleteSized:   true,
	}
	isValidRangeKeyBoundKeyKind = [base.InternalKeyKindMax + 1]bool{
		base.InternalKeyKindRangeKeySet:    true,
		base.InternalKeyKindRangeKeyUnset:  true,
		base.InternalKeyKindRangeKeyDelete: true,
	}
)

// TableInfo returns a subset of the TableMetadata state formatted as a
// TableInfo.
func (m *TableMetadata) TableInfo() TableInfo {
	return TableInfo{
		FileNum:        m.TableNum,
		Size:           m.Size,
		Smallest:       m.Smallest(),
		Largest:        m.Largest(),
		SmallestSeqNum: m.SmallestSeqNum,
		LargestSeqNum:  m.LargestSeqNum,
		blobReferences: m.BlobReferences,
	}
}

func (m *TableMetadata) cmpSeqNum(b *TableMetadata) int {
	// NB: This is the same ordering that RocksDB uses for L0 files.

	// Sort first by largest sequence number.
	if v := stdcmp.Compare(m.LargestSeqNum, b.LargestSeqNum); v != 0 {
		return v
	}
	// Then by smallest sequence number.
	if v := stdcmp.Compare(m.SmallestSeqNum, b.SmallestSeqNum); v != 0 {
		return v
	}
	// Break ties by file number.
	return stdcmp.Compare(m.TableNum, b.TableNum)
}

func (m *TableMetadata) cmpSmallestKey(b *TableMetadata, cmp Compare) int {
	return base.InternalCompare(cmp, m.Smallest(), b.Smallest())
}

// boundType represents the type of key (point or range) present as the smallest
// and largest keys.
type boundType uint8

const (
	boundTypePointKey boundType = iota + 1
	boundTypeRangeKey
)

// Smallest returns the smallest key based on the bound type of
// boundTypeSmallest.
//
//gcassert:inline
func (m *TableMetadata) Smallest() InternalKey {
	x := &m.PointKeyBounds
	if m.boundTypeSmallest == boundTypeRangeKey {
		x = m.RangeKeyBounds
	}
	return x.Smallest()
}

// Largest returns the largest key based on the bound type of
// boundTypeLargest.
//
//gcassert:inline
func (m *TableMetadata) Largest() InternalKey {
	x := &m.PointKeyBounds
	if m.boundTypeLargest == boundTypeRangeKey {
		x = m.RangeKeyBounds
	}
	return x.Largest()
}

// InternalKeyBounds represents set of keys (smallest, largest) used for the
// in-memory and on-disk partial DBs that make up a pebble DB.
//
// It consists of the smallest, largest keys and their respective trailers.
// The keys are represented as a single string; their individual representations
// are given by the userKeySeparatorIdx as:
//   - smallest: [0, userKeySeparatorIdx)
//   - largest: [userKeySeparatorIdx, len(userKeyData))
//
// This format allows us to save a couple of bytes that will add up
// proportionally to the amount of sstables we have.
type InternalKeyBounds struct {
	userKeyData         string
	userKeySeparatorIdx int
	smallestTrailer     base.InternalKeyTrailer
	largestTrailer      base.InternalKeyTrailer
}

func (ikr *InternalKeyBounds) SetInternalKeyBounds(smallest, largest InternalKey) {
	ikr.userKeyData = string(smallest.UserKey) + string(largest.UserKey)
	ikr.smallestTrailer = smallest.Trailer
	ikr.largestTrailer = largest.Trailer
	ikr.userKeySeparatorIdx = len(smallest.UserKey)
}

//gcassert:inline
func (ikr *InternalKeyBounds) SmallestUserKey() []byte {
	return unsafe.Slice(unsafe.StringData(ikr.userKeyData), ikr.userKeySeparatorIdx)
}

//gcassert:inline
func (ikr *InternalKeyBounds) Smallest() InternalKey {
	return InternalKey{
		UserKey: ikr.SmallestUserKey(),
		Trailer: ikr.smallestTrailer,
	}
}

//gcassert:inline
func (ikr *InternalKeyBounds) LargestUserKey() []byte {
	largestStart := unsafe.StringData(ikr.userKeyData[ikr.userKeySeparatorIdx:])
	return unsafe.Slice(largestStart, len(ikr.userKeyData)-ikr.userKeySeparatorIdx)
}

//gcassert:inline
func (ikr *InternalKeyBounds) Largest() InternalKey {
	ik := InternalKey{
		UserKey: ikr.LargestUserKey(),
		Trailer: ikr.largestTrailer,
	}
	return ik
}

func (ikr *InternalKeyBounds) SetSmallest(ik InternalKey) {
	ikr.userKeyData = string(ik.UserKey) + string(ikr.LargestUserKey())
	ikr.smallestTrailer = ik.Trailer
	ikr.userKeySeparatorIdx = len(ik.UserKey)
}

func (ikr *InternalKeyBounds) SetLargest(ik InternalKey) {
	smallestUserKey := ikr.SmallestUserKey()
	ikr.userKeyData = string(smallestUserKey) + string(ik.UserKey)
	ikr.largestTrailer = ik.Trailer
	ikr.userKeySeparatorIdx = len(smallestUserKey)
}

// TableInfo contains the common information for table related events.
type TableInfo struct {
	// FileNum is the internal DB identifier for the table.
	FileNum base.FileNum
	// Size is the size of the file in bytes.
	Size uint64
	// Smallest is the smallest internal key in the table.
	Smallest InternalKey
	// Largest is the largest internal key in the table.
	Largest InternalKey
	// SmallestSeqNum is the smallest sequence number in the table.
	SmallestSeqNum base.SeqNum
	// LargestSeqNum is the largest sequence number in the table.
	LargestSeqNum base.SeqNum
	// blobReferences is the list of blob files referenced by the table.
	blobReferences BlobReferences
}

// GetBlobReferenceFiles returns the list of blob file numbers referenced by
// the table.
func (t *TableInfo) GetBlobReferenceFiles() []base.BlobFileID {
	files := make([]base.BlobFileID, 0, len(t.blobReferences))
	for _, blob := range t.blobReferences {
		files = append(files, blob.FileID)
	}
	return files
}

// TableStats contains statistics on a table used for compaction heuristics,
// and export via Metrics.
type TableStats struct {
	// Estimate of the total disk space that may be dropped by this table's
	// point deletions by compacting them.
	PointDeletionsBytesEstimate uint64
	// Estimate of the total disk space that may be dropped by this table's
	// range deletions by compacting them. This estimate is at data-block
	// granularity and is not updated if compactions beneath the table reduce
	// the amount of reclaimable disk space. It also does not account for
	// overlapping data in L0 and ignores L0 sublevels, but the error that
	// introduces is expected to be small. Similarly, multiple overlapping
	// RANGEDELs can in different levels can count the same data to be deleted
	// multiple times.
	//
	// Tables in the bottommost level of the LSM may have a nonzero estimate if
	// snapshots or move compactions prevented the elision of their range
	// tombstones. A table in the bottommost level that was ingested into L6
	// will have a zero estimate, because the file's sequence numbers indicate
	// that the tombstone cannot drop any data contained within the file itself.
	RangeDeletionsBytesEstimate uint64
	// TombstoneDenseBlocksRatio is the ratio of data blocks in this table that
	// fulfills at least one of the following:
	// 1. The block contains at least options.Experimental.NumDeletionsThreshold
	//    point tombstones.
	// 2. The ratio of the uncompressed size of point tombstones to the
	//    uncompressed size of the block is at least
	//    options.Experimental.DeletionSizeRatioThreshold.
	// This statistic is used to determine eligibility for a tombstone density
	// compaction.
	TombstoneDenseBlocksRatio float64
}

// CompactionState is the compaction state of a file.
//
// The following shows the valid state transitions:
//
//	NotCompacting --> Compacting --> Compacted
//	      ^               |
//	      |               |
//	      +-------<-------+
//
// Input files to a compaction transition to Compacting when a compaction is
// picked. A file that has finished compacting typically transitions into the
// Compacted state, at which point it is effectively obsolete ("zombied") and
// will eventually be removed from the LSM. A file that has been move-compacted
// will transition from Compacting back into the NotCompacting state, signaling
// that the file may be selected for a subsequent compaction. A failed
// compaction will result in all input tables transitioning from Compacting to
// NotCompacting.
//
// This state is in-memory only. It is not persisted to the manifest.
type CompactionState uint8

// CompactionStates.
const (
	CompactionStateNotCompacting CompactionState = iota
	CompactionStateCompacting
	CompactionStateCompacted
)

// String implements fmt.Stringer.
func (s CompactionState) String() string {
	switch s {
	case CompactionStateNotCompacting:
		return "NotCompacting"
	case CompactionStateCompacting:
		return "Compacting"
	case CompactionStateCompacted:
		return "Compacted"
	default:
		panic(fmt.Sprintf("pebble: unknown compaction state %d", s))
	}
}

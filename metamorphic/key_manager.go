// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"slices"
	"strings"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/internal/base"
)

// keyMeta is metadata associated with an (objID, key) pair, where objID is
// a writer containing the key.
type keyMeta struct {
	objID objID
	key   []byte
	// history provides the history of writer operations applied against this
	// key on this object. history is always ordered in chronological order.
	history keyHistory
}

// all returns true if all the operations in the history are of the provided op
// types.
func (m *keyMeta) all(typs ...OpType) bool {
	containsOtherTypes := slices.ContainsFunc(m.history, func(i keyHistoryItem) bool {
		notOfProvidedType := !slices.Contains(typs, i.opType)
		return notOfProvidedType
	})
	return !containsOtherTypes
}

func (m *keyMeta) clear() {
	m.history = m.history[:0]
}

// mergeInto merges this metadata into the metadata for other, appending all of
// its individual operations to dst at the provided timestamp.
func (m *keyMeta) mergeInto(dst *keyMeta) {
	for _, op := range m.history {
		// If the key is being merged into a database object and the operation
		// is a delete, we can clear the destination history. Database objects
		// are end points in the merging of keys and won't be the source of a
		// future merge. Deletions cause all other operations to behave as
		// though the key was never written to the database at all, so we don't
		// need to consider it for maintaining single delete invariants.
		//
		// NB: There's a subtlety here in that isDelete() will return true if
		// opType is a writerSingleDelete, but single deletes are capable of
		// leaking information about the history of writes. However, that's
		// okay, because as long as we're properly generating single deletes
		// according to the W1 invariant described in keyManager's comment, a
		// single delete is equivalent to delete for the current history.
		if dst.objID.tag() == dbTag && op.opType.isDelete() {
			dst.clear()
			continue
		}
		dst.history = append(dst.history, keyHistoryItem{
			opType: op.opType,
		})
	}
}

type bounds struct {
	smallest    []byte
	largest     []byte
	largestExcl bool // is largest exclusive?
}

func (b bounds) checkValid(cmp base.Compare) {
	if c := cmp(b.smallest, b.largest); c > 0 {
		panic(fmt.Sprintf("invalid bound [%q, %q]", b.smallest, b.largest))
	} else if c == 0 && b.largestExcl {
		panic(fmt.Sprintf("invalid bound [%q, %q)", b.smallest, b.largest))
	}
}

func (b bounds) String() string {
	if b.largestExcl {
		return fmt.Sprintf("[%q,%q)", b.smallest, b.largest)
	}
	return fmt.Sprintf("[%q,%q]", b.smallest, b.largest)
}

// Overlaps returns true iff the bounds intersect.
func (b *bounds) Overlaps(cmp base.Compare, other bounds) bool {
	if b.IsUnset() || other.IsUnset() {
		return false
	}
	// Is b strictly before other?
	if v := cmp(b.largest, other.smallest); v < 0 || (v == 0 && b.largestExcl) {
		return false
	}
	// Is b strictly after other?
	if v := cmp(b.smallest, other.largest); v > 0 || (v == 0 && other.largestExcl) {
		return false
	}
	return true
}

// IsUnset returns true if the bounds haven't been set.
func (b bounds) IsUnset() bool {
	return b.smallest == nil && b.largest == nil
}

// Expand potentially expands the receiver bounds to include the other given
// bounds. If the receiver is unset, the other bounds are copied.
func (b *bounds) Expand(cmp base.Compare, other bounds) {
	if other.IsUnset() {
		return
	}
	other.checkValid(cmp)
	if b.IsUnset() {
		*b = other
		return
	}
	if cmp(b.smallest, other.smallest) > 0 {
		b.smallest = other.smallest
	}
	if v := cmp(b.largest, other.largest); v < 0 || (v == 0 && b.largestExcl) {
		b.largest = other.largest
		b.largestExcl = other.largestExcl
	}
}

// keyManager tracks the write operations performed on keys in the generation
// phase of the metamorphic test. It maintains histories of operations performed
// against every unique user key on every writer object. These histories inform
// operation generation in order to maintain invariants that Pebble requires of
// end users, mostly around single deletions.
//
// A single deletion has a subtle requirement of the writer:
//
//	W1: The writer may only single delete a key `k` if `k` has been Set once
//	    (and never MergeD) since the last delete.
//
// When a SINGLEDEL key deletes a SET key within a compaction, both the SET and
// the SINGLEDEL keys are elided. If multiple SETs of the key exist within the
// LSM, the SINGLEDEL reveals the lower SET. This behavior is dependent on the
// internal LSM state and nondeterministic. To ensure determinism, the end user
// must satisfy W1 and use single delete only when they can guarantee that the
// key has been set at most once since the last delete, preventing this rollback
// to a previous value.
//
// This W1 invariant requires a delicate dance during operation generation,
// because independent batches may be independently built and committed. With
// multi-instance variants of the metamorphic tests, keys in batches may
// ultimately be committed to any of several DB instances. To satisfy these
// requirements, the key manager tracks the history of every key on every
// writable object. When generating a new single deletion operation, the
// generator asks the key manager for a set of keys for which a single delete
// maintains the W1 invariant within the object itself. This object-local W1
// invariant (OLW1) is equivalent to W1 if one only ever performs write
// operations directly against individual DB objects.
//
// However with the existence of batches that receive writes independent of DB
// objects, W1 may be violated by appending the histories of two objects that
// independently satisfy OLW1. Consider a sequence such as:
//
//  1. db1.Set("foo")
//  2. batch1.Set("foo")
//  3. batch1.SingleDelete("foo")
//  4. db1.Apply(batch1)
//
// Both db1 and batch1 satisfy the object-local invariant OLW1. However the
// composition of the histories created by appending batch1's operations to db1
// creates a history that now violates W1 on db1. To detect this violation,
// batch applications/commits and ingestions examine the tail of the destination
// object's history and the head of the source batch's history. When a violation
// is detected, these operations insert additional Delete operations to clear
// the conflicting keys before proceeding with the conflicting operation. These
// deletes reset the key history.
//
// Note that this generation-time key tracking requires that operations be
// infallible, because a runtime failure would cause the key manager's state to
// diverge from the runtime object state. Ingestion operations pose an obstacle,
// because the generator may generate ingestions that fail due to overlapping
// sstables. The key manager tracks the bounds of keys written to each object,
// and uses this to infer at generation time which ingestions will fail,
// maintaining key histories accordingly.
type keyManager struct {
	kf KeyFormat

	byObj map[objID]*objKeyMeta
	// globalKeys represents all the keys that have been generated so far. Not
	// all these keys have been written to. globalKeys is sorted.
	globalKeys [][]byte
	// globalKeysMap contains the same keys as globalKeys but in a map. It
	// ensures no duplication.
	globalKeysMap map[string]bool
	// globalKeyPrefixes contains all the key prefixes (as defined by the
	// comparer's Split) generated so far. globalKeyPrefixes is sorted.
	globalKeyPrefixes [][]byte
	// globalKeyPrefixesMap contains the same keys as globalKeyPrefixes. It
	// ensures no duplication.
	globalKeyPrefixesMap map[string]struct{}
}

type objKeyMeta struct {
	id objID
	// List of keys, and what has happened to each in this object.
	// Will be transferred when needed.
	keys map[string]*keyMeta
	// bounds holds user key bounds encompassing all the keys set within an
	// object. It's updated within `update` when a new op is generated.
	bounds bounds
	// These flags are true if the object has had range del or range key operations.
	hasRangeDels     bool
	hasRangeKeys     bool
	hasRangeKeyUnset bool
	// List of RangeKeySets for this object. Used to check for overlapping
	// RangeKeySets in external files.
	rangeKeySets []pebble.KeyRange
}

// MergeKey adds the given key, merging the histories as needed.
func (okm *objKeyMeta) MergeKey(k *keyMeta) {
	meta, ok := okm.keys[string(k.key)]
	if !ok {
		meta = &keyMeta{
			objID: okm.id,
			key:   k.key,
		}
		okm.keys[string(k.key)] = meta
	}
	k.mergeInto(meta)
}

// CollapseKeys collapses the history of all keys. Used with ingestion operation
// which only use the last value of any given key.
func (okm *objKeyMeta) CollapseKeys() {
	for _, keyMeta := range okm.keys {
		keyMeta.history = keyMeta.history.collapsed()
	}
}

// MergeFrom merges the `from` metadata into this one, appending all of its
// individual operations at the provided timestamp.
func (okm *objKeyMeta) MergeFrom(from *objKeyMeta, cmp base.Compare) {
	// The result should be the same regardless of the ordering of the keys.
	for _, k := range from.keys {
		okm.MergeKey(k)
	}
	okm.bounds.Expand(cmp, from.bounds)
	okm.hasRangeDels = okm.hasRangeDels || from.hasRangeDels
	okm.hasRangeKeys = okm.hasRangeKeys || from.hasRangeKeys
	okm.hasRangeKeyUnset = okm.hasRangeKeyUnset || from.hasRangeKeyUnset
	okm.rangeKeySets = append(okm.rangeKeySets, from.rangeKeySets...)
}

// objKeyMeta looks up the objKeyMeta for a given object, creating it if necessary.
func (k *keyManager) objKeyMeta(o objID) *objKeyMeta {
	m, ok := k.byObj[o]
	if !ok {
		m = &objKeyMeta{
			id:   o,
			keys: make(map[string]*keyMeta),
		}
		k.byObj[o] = m
	}
	return m
}

// SortedKeysForObj returns all the entries in objKeyMeta(o).keys, in sorted
// order.
func (k *keyManager) SortedKeysForObj(o objID) []keyMeta {
	okm := k.objKeyMeta(o)
	res := make([]keyMeta, 0, len(okm.keys))
	for _, m := range okm.keys {
		res = append(res, *m)
	}
	slices.SortFunc(res, func(a, b keyMeta) int {
		cmp := k.kf.Comparer.Compare(a.key, b.key)
		if cmp == 0 {
			panic(fmt.Sprintf("distinct keys %q and %q compared as equal", a.key, b.key))
		}
		return cmp
	})
	return res
}

// InRangeKeysForObj returns all keys in the range [lower, upper) associated with the
// given object, in sorted order. If either of the bounds is nil, it is ignored.
func (k *keyManager) InRangeKeysForObj(o objID, lower, upper []byte) []keyMeta {
	var inRangeKeys []keyMeta
	for _, km := range k.SortedKeysForObj(o) {
		if (lower == nil || k.kf.Comparer.Compare(km.key, lower) >= 0) &&
			(upper == nil || k.kf.Comparer.Compare(km.key, upper) < 0) {
			inRangeKeys = append(inRangeKeys, km)
		}
	}
	return inRangeKeys

}

// KeysForExternalIngest returns the keys that will be ingested with an external
// object (taking into consideration the bounds, synthetic suffix, etc).
func (k *keyManager) KeysForExternalIngest(obj externalObjWithBounds) []keyMeta {
	var res []keyMeta
	var lastPrefix []byte
	for _, km := range k.SortedKeysForObj(obj.externalObjID) {
		// Apply prefix and suffix changes, then check the bounds.
		if obj.syntheticPrefix.IsSet() {
			km.key = obj.syntheticPrefix.Apply(km.key)
		}
		if obj.syntheticSuffix.IsSet() {
			n := k.kf.Comparer.Split(km.key)
			km.key = append(km.key[:n:n], obj.syntheticSuffix...)
		}
		// We only keep the first of every unique prefix for external ingests.
		// See the use of uniquePrefixes in newExternalObjOp. However, we must
		// ignore keys that only exist as a discretized range deletion for this
		// purpose, so we only update the lastPrefix if the key contains any
		// non-range delete keys.
		// TODO(jackson): Track range deletions separately.
		if lastPrefix != nil && k.kf.Comparer.Equal(lastPrefix, km.key[:k.kf.Comparer.Split(km.key)]) {
			continue
		}
		if !km.all(OpWriterDeleteRange) {
			lastPrefix = append(lastPrefix[:0], km.key[:k.kf.Comparer.Split(km.key)]...)
		}
		if k.kf.Comparer.Compare(km.key, obj.bounds.Start) >= 0 &&
			k.kf.Comparer.Compare(km.key, obj.bounds.End) < 0 {
			res = append(res, km)
		}
	}
	// Check for duplicate resulting keys.
	for i := 1; i < len(res); i++ {
		if k.kf.Comparer.Compare(res[i].key, res[i-1].key) == 0 {
			panic(fmt.Sprintf("duplicate external ingest key %q", res[i].key))
		}
	}
	return res
}

func (k *keyManager) ExternalObjectHasOverlappingRangeKeySets(externalObjID objID) bool {
	meta := k.objKeyMeta(externalObjID)
	if len(meta.rangeKeySets) == 0 {
		return false
	}
	ranges := meta.rangeKeySets
	// Sort by start key.
	slices.SortFunc(ranges, func(a, b pebble.KeyRange) int {
		return k.kf.Comparer.Compare(a.Start, b.Start)
	})
	// Check overlap between adjacent ranges.
	for i := 0; i < len(ranges)-1; i++ {
		if ranges[i].OverlapsKeyRange(k.kf.Comparer.Compare, ranges[i+1]) {
			return true
		}
	}
	return false
}

// getSetOfVisibleKeys returns a sorted slice of keys that are visible in the
// provided reader object under the object's current history.
func (k *keyManager) getSetOfVisibleKeys(readerID objID) [][]byte {
	okm := k.objKeyMeta(readerID)
	keys := make([][]byte, 0, len(okm.keys))
	for k, km := range okm.keys {
		if km.history.hasVisibleValue() {
			keys = append(keys, unsafe.Slice(unsafe.StringData(k), len(k)))
		}
	}
	slices.SortFunc(keys, k.kf.Comparer.Compare)
	return keys
}

// newKeyManager returns a pointer to a new keyManager. Callers should
// interact with this using addNewKey, knownKeys, update methods only.
func newKeyManager(numInstances int, kf KeyFormat) *keyManager {
	m := &keyManager{
		kf:                   kf,
		byObj:                make(map[objID]*objKeyMeta),
		globalKeysMap:        make(map[string]bool),
		globalKeyPrefixesMap: make(map[string]struct{}),
	}
	for i := 1; i <= max(numInstances, 1); i++ {
		m.objKeyMeta(makeObjID(dbTag, uint32(i)))
	}
	return m
}

// addNewKey adds the given key to the key manager for global key tracking.
// Returns false iff this is not a new key.
func (k *keyManager) addNewKey(key []byte) bool {
	k.kf.Comparer.ValidateKey.MustValidate(key)
	if k.globalKeysMap[string(key)] {
		return false
	}
	insertSorted(k.kf.Comparer.Compare, &k.globalKeys, key)
	k.globalKeysMap[string(key)] = true

	prefixLen := k.kf.Comparer.Split(key)
	if prefixLen == 0 {
		panic(fmt.Sprintf("key %q has zero length prefix", key))
	}
	if _, ok := k.globalKeyPrefixesMap[string(key[:prefixLen])]; !ok {
		insertSorted(k.kf.Comparer.Compare, &k.globalKeyPrefixes, key[:prefixLen])
		k.globalKeyPrefixesMap[string(key[:prefixLen])] = struct{}{}
	}
	return true
}

// getOrInit returns the keyMeta for the (objID, key) pair, if it exists, else
// allocates, initializes and returns a new value.
func (k *keyManager) getOrInit(id objID, key []byte) *keyMeta {
	k.kf.Comparer.ValidateKey.MustValidate(key)
	objKeys := k.objKeyMeta(id)
	m, ok := objKeys.keys[string(key)]
	if ok {
		return m
	}
	m = &keyMeta{
		objID: id,
		key:   key,
	}
	// Initialize the key-to-meta index.
	objKeys.keys[string(key)] = m
	// Expand the object's bounds to contain this key if they don't already.
	objKeys.bounds.Expand(k.kf.Comparer.Compare, k.makeSingleKeyBounds(key))
	return m
}

// mergeObjectInto merges obj key metadata from an object into another and
// deletes the metadata for the source object (which must not be used again).
func (k *keyManager) mergeObjectInto(from, to objID) {
	toMeta := k.objKeyMeta(to)
	toMeta.MergeFrom(k.objKeyMeta(from), k.kf.Comparer.Compare)

	delete(k.byObj, from)
}

// expandBounds expands the incrementally maintained bounds of o to be at least
// as wide as `b`.
func (k *keyManager) expandBounds(o objID, b bounds) {
	k.objKeyMeta(o).bounds.Expand(k.kf.Comparer.Compare, b)
}

// doObjectBoundsOverlap returns true iff any of the named objects have key
// bounds that overlap any other named object.
func (k *keyManager) doObjectBoundsOverlap(objIDs []objID) bool {
	for i := range objIDs {
		ib, iok := k.byObj[objIDs[i]]
		if !iok {
			continue
		}
		for j := i + 1; j < len(objIDs); j++ {
			jb, jok := k.byObj[objIDs[j]]
			if !jok {
				continue
			}
			if ib.bounds.Overlaps(k.kf.Comparer.Compare, jb.bounds) {
				return true
			}
		}
	}
	return false
}

// checkForSingleDelConflicts examines all the keys written to srcObj, and
// determines whether any of the contained single deletes would be
// nondeterministic if applied to dstObj in dstObj's current state. It returns a
// slice of all the keys that are found to conflict. In order to preserve
// determinism, the caller must delete the key from the destination before
// writing src's mutations to dst in order to ensure determinism.
//
// It takes a `srcCollapsed` parameter that determines whether the source
// history should be "collapsed" (see keyHistory.collapsed) before determining
// whether the applied state will conflict. This is required to facilitate
// ingestOps which are NOT equivalent to committing the batch, because they can
// only commit 1 internal point key at each unique user key.
func (k *keyManager) checkForSingleDelConflicts(srcObj, dstObj objID, srcCollapsed bool) [][]byte {
	dstKeys := k.objKeyMeta(dstObj)
	var conflicts [][]byte
	for _, src := range k.SortedKeysForObj(srcObj) {
		if srcCollapsed {
			src.history = src.history.collapsed()
		}
		if k.checkForSingleDelConflict(src, dstKeys) {
			conflicts = append(conflicts, src.key)
		}
	}
	return conflicts
}

// checkForSingleDelConflict returns true if applying the history of the source
// key on top of the given object results in a possible SingleDel
// nondeterminism. See checkForSingleDelConflicts.
func (k *keyManager) checkForSingleDelConflict(src keyMeta, dstObjKeyMeta *objKeyMeta) bool {
	// Single delete generation logic already ensures that both the source
	// object and the destination object's single deletes are deterministic
	// within the context of their existing writes. However, applying the source
	// keys on top of the destination object may violate the invariants.
	// Consider:
	//
	//    src: a.SET; a.SINGLEDEL;
	//    dst: a.SET;
	//
	// The merged view is:
	//
	//    a.SET; a.SET; a.SINGLEDEL;
	//
	// This is invalid, because there is more than 1 value mutation of the
	// key before the single delete.
	//
	// We walk the source object's history in chronological order, looking
	// for a single delete that was written before a DEL/RANGEDEL. (NB: We
	// don't need to look beyond a DEL/RANGEDEL, because these deletes bound
	// any subsequently-written single deletes to applying to the keys
	// within src's history between the two tombstones. We already know from
	// per-object history invariants that any such single delete must be
	// deterministic with respect to src's keys.)
	var srcHasUnboundedSingleDelete bool
	var srcValuesBeforeSingleDelete int

	// When the srcObj is being ingested (srcCollapsed=t), the semantics
	// change. We must first "collapse" the key's history to represent the
	// ingestion semantics.
	srcHistory := src.history

srcloop:
	for _, item := range srcHistory {
		switch item.opType {
		case OpWriterDelete, OpWriterDeleteRange:
			// We found a DEL or RANGEDEL before any single delete. If src
			// contains additional single deletes, their effects are limited
			// to applying to later keys. Combining the two object histories
			// doesn't pose any determinism risk.
			return false

		case OpWriterSingleDelete:
			// We found a single delete. Since we found this single delete
			// before a DEL or RANGEDEL, this delete has the potential to
			// affect the visibility of keys in `dstObj`. We'll need to look
			// for potential conflicts down below.
			srcHasUnboundedSingleDelete = true
			if srcValuesBeforeSingleDelete > 1 {
				panic(errors.AssertionFailedf("unexpectedly found %d sets/merges before single del",
					srcValuesBeforeSingleDelete))
			}
			break srcloop

		case OpWriterSet, OpWriterMerge:
			// We found a SET or MERGE operation for this key. If there's a
			// subsequent single delete, we'll need to make sure there's not
			// a SET or MERGE in the dst too.
			srcValuesBeforeSingleDelete++

		default:
			panic(errors.AssertionFailedf("unexpected optype %d", item.opType))
		}
	}
	if !srcHasUnboundedSingleDelete {
		return false
	}

	dst, ok := dstObjKeyMeta.keys[string(src.key)]
	// If the destination writer has no record of the key, the combined key
	// history is simply the src object's key history which is valid due to
	// per-object single deletion invariants.
	if !ok {
		return false
	}

	// We need to examine the trailing key history on dst.
	consecutiveValues := srcValuesBeforeSingleDelete
	for i := len(dst.history) - 1; i >= 0; i-- {
		switch dst.history[i].opType {
		case OpWriterSet, OpWriterMerge:
			// A SET/MERGE may conflict if there's more than 1 consecutive
			// SET/MERGEs.
			consecutiveValues++
			if consecutiveValues > 1 {
				return true
			}
		case OpWriterDelete, OpWriterSingleDelete, OpWriterDeleteRange:
			// Dels clear the history, enabling use of single delete.
			return false

		default:
			panic(errors.AssertionFailedf("unexpected optype %d", dst.history[i].opType))
		}
	}
	return false
}

// update updates the internal state of the keyManager according to the given
// op.
func (k *keyManager) update(o op) {
	switch s := o.(type) {
	case *setOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.history = append(meta.history, keyHistoryItem{
			opType: OpWriterSet,
		})
	case *mergeOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.history = append(meta.history, keyHistoryItem{
			opType: OpWriterMerge,
		})
	case *deleteOp:
		meta := k.getOrInit(s.writerID, s.key)
		if meta.objID.tag() == dbTag {
			meta.clear()
		} else {
			meta.history = append(meta.history, keyHistoryItem{
				opType: OpWriterDelete,
			})
		}
	case *deleteRangeOp:
		// We track the history of discrete point keys, but a range deletion
		// applies over a continuous key span of infinite keys. However, the key
		// manager knows all keys that have been used in all operations, so we
		// can discretize the range tombstone by adding it to every known key
		// within the range.
		//
		// TODO(jackson): If s.writerID is a batch, we may not know the set of
		// all keys that WILL exist by the time the batch is committed. This
		// means the delete range history is incomplete. Fix this.
		keyRange := pebble.KeyRange{Start: s.start, End: s.end}
		for _, key := range k.knownKeysInRange(keyRange) {
			meta := k.getOrInit(s.writerID, key)
			if meta.objID.tag() == dbTag {
				meta.clear()
			} else {
				meta.history = append(meta.history, keyHistoryItem{
					opType: OpWriterDeleteRange,
				})
			}
		}
		k.expandBounds(s.writerID, k.makeEndExclusiveBounds(s.start, s.end))
		k.objKeyMeta(s.writerID).hasRangeDels = true

	case *singleDeleteOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.history = append(meta.history, keyHistoryItem{
			opType: OpWriterSingleDelete,
		})
	case *rangeKeyDeleteOp:
		// Range key operations on their own don't determine singledel eligibility,
		// however range key operations could be part of a batch which contains
		// other operations that do affect it. If those batches were to get
		// ingested, we'd need to know what the bounds of sstables generated out
		// of those batches are, as that determines whether that ingestion
		// will succeed or not.
		k.expandBounds(s.writerID, k.makeEndExclusiveBounds(s.start, s.end))
		k.objKeyMeta(s.writerID).hasRangeKeys = true
	case *rangeKeySetOp:
		k.expandBounds(s.writerID, k.makeEndExclusiveBounds(s.start, s.end))
		meta := k.objKeyMeta(s.writerID)
		meta.hasRangeKeys = true
		meta.rangeKeySets = append(meta.rangeKeySets, pebble.KeyRange{
			Start: s.start,
			End:   s.end,
		})
	case *rangeKeyUnsetOp:
		k.expandBounds(s.writerID, k.makeEndExclusiveBounds(s.start, s.end))
		meta := k.objKeyMeta(s.writerID)
		meta.hasRangeKeys = true
		meta.hasRangeKeyUnset = true
	case *ingestOp:
		// Some ingestion operations may attempt to ingest overlapping sstables
		// which is prohibited. We know at generation time whether these
		// ingestions will be successful. If they won't be successful, we should
		// not update the key state because both the batch(es) and target DB
		// will be left unmodified.
		if k.doObjectBoundsOverlap(s.batchIDs) {
			// This ingestion will fail.
			return
		}

		// For each batch, merge the keys into the DB. We can't call
		// keyMeta.mergeInto directly to merge, because ingest operations first
		// "flatten" the batch (because you can't set the same key twice at a
		// single sequence number). Instead we compute the collapsed history and
		// merge that.
		for _, batchID := range s.batchIDs {
			k.objKeyMeta(batchID).CollapseKeys()
			k.mergeObjectInto(batchID, s.dbID)
		}
	case *ingestAndExciseOp:
		// IngestAndExcise does not ingest multiple batches, so we will not see
		// a failure due to overlapping sstables. However we do need to merge
		// the singular batch into the key manager.
		//
		// Remove all keys from the key manager within the excise span before
		// merging the batch into the db.
		for _, key := range k.InRangeKeysForObj(s.dbID, s.exciseStart, s.exciseEnd) {
			m := k.getOrInit(s.dbID, key.key)
			m.clear()
		}
		k.objKeyMeta(s.batchID).CollapseKeys()
		k.mergeObjectInto(s.batchID, s.dbID)
		// TODO(bilal): Handle replicateOp here. We currently disable SingleDelete
		// when these operations are enabled (see multiInstanceConfig).
	case *newExternalObjOp:
		// Collapse and transfer the keys from the batch to the external object.
		k.objKeyMeta(s.batchID).CollapseKeys()
		k.mergeObjectInto(s.batchID, s.externalObjID)
	case *ingestExternalFilesOp:
		// Merge the keys from the external objects (within the restricted bounds)
		// into the database.
		dbMeta := k.objKeyMeta(s.dbID)
		for _, obj := range s.objs {
			for _, keyMeta := range k.KeysForExternalIngest(obj) {
				dbMeta.MergeKey(&keyMeta)
			}
			dbMeta.bounds.Expand(k.kf.Comparer.Compare, k.makeEndExclusiveBounds(obj.bounds.Start, obj.bounds.End))
		}
	case *applyOp:
		// Merge the keys from this batch into the parent writer.
		k.mergeObjectInto(s.batchID, s.writerID)
	case *batchCommitOp:
		// Merge the keys from the batch with the keys from the DB.
		k.mergeObjectInto(s.batchID, s.dbID)
	}
}

func (k *keyManager) knownKeys() (keys [][]byte) {
	return k.globalKeys
}

// knownKeysInRange returns all eligible read keys within the range
// [start,end). The returned slice is owned by the keyManager and must not be
// retained.
func (k *keyManager) knownKeysInRange(kr pebble.KeyRange) (keys [][]byte) {
	s, _ := slices.BinarySearchFunc(k.globalKeys, kr.Start, k.kf.Comparer.Compare)
	e, _ := slices.BinarySearchFunc(k.globalKeys, kr.End, k.kf.Comparer.Compare)
	if s >= e {
		return nil
	}
	return k.globalKeys[s:e]
}

func (k *keyManager) prefixes() (prefixes [][]byte) {
	return k.globalKeyPrefixes
}

// prefixExists returns true if a key has been generated with the provided
// prefix before.
func (k *keyManager) prefixExists(prefix []byte) bool {
	_, exists := k.globalKeyPrefixesMap[string(prefix)]
	return exists
}

// eligibleSingleDeleteKeys returns a slice of keys that can be safely single
// deleted, given the writer id. Restricting single delete keys through this
// method is used to ensure the OLW1 guarantee (see the keyManager comment) for
// the provided object ID.
func (k *keyManager) eligibleSingleDeleteKeys(o objID) (keys [][]byte) {
	// Creating a slice of keys is wasteful given that the caller will pick one,
	// but makes it simpler for unit testing.
	objKeys := k.objKeyMeta(o)
	for _, key := range k.globalKeys {
		meta, ok := objKeys.keys[string(key)]
		if !ok {
			keys = append(keys, key)
			continue
		}
		// Examine the history within this object.
		if meta.history.canSingleDelete() {
			keys = append(keys, key)
		}
	}
	return keys
}

// makeSingleKeyBounds creates a [key, key] bound.
func (k *keyManager) makeSingleKeyBounds(key []byte) bounds {
	return bounds{
		smallest:    key,
		largest:     key,
		largestExcl: false,
	}
}

// makeEndExclusiveBounds creates a [smallest, largest) bound.
func (k *keyManager) makeEndExclusiveBounds(smallest, largest []byte) bounds {
	b := bounds{
		smallest:    smallest,
		largest:     largest,
		largestExcl: true,
	}
	b.checkValid(k.kf.Comparer.Compare)
	return b
}

// a keyHistoryItem describes an individual operation performed on a key.
type keyHistoryItem struct {
	// opType may be writerSet, writerDelete, writerSingleDelete,
	// writerDeleteRange or writerMerge only. No other opTypes may appear here.
	opType OpType
}

// keyHistory captures the history of mutations to a key in chronological order.
type keyHistory []keyHistoryItem

// canSingleDelete examines the tail of the history and returns true if a single
// delete appended to this history would satisfy the single delete invariants.
func (h keyHistory) canSingleDelete() bool {
	if len(h) == 0 {
		return true
	}
	switch o := h[len(h)-1].opType; o {
	case OpWriterDelete, OpWriterDeleteRange, OpWriterSingleDelete:
		return true
	case OpWriterSet, OpWriterMerge:
		if len(h) == 1 {
			return true
		}
		return h[len(h)-2].opType.isDelete()
	default:
		panic(errors.AssertionFailedf("unexpected writer op %v", o))
	}
}

func (h keyHistory) String() string {
	var sb strings.Builder
	for i, it := range h {
		if i > 0 {
			fmt.Fprint(&sb, ", ")
		}
		switch it.opType {
		case OpWriterDelete:
			fmt.Fprint(&sb, "del")
		case OpWriterDeleteRange:
			fmt.Fprint(&sb, "delrange")
		case OpWriterSingleDelete:
			fmt.Fprint(&sb, "singledel")
		case OpWriterSet:
			fmt.Fprint(&sb, "set")
		case OpWriterMerge:
			fmt.Fprint(&sb, "merge")
		default:
			fmt.Fprintf(&sb, "optype[v=%d]", it.opType)
		}
	}
	return sb.String()
}

// hasVisibleKey examines the tail of the history and returns true if the
// history should end in a visible value for this key.
func (h keyHistory) hasVisibleValue() bool {
	if len(h) == 0 {
		return false
	}
	return !h[len(h)-1].opType.isDelete()
}

// collapsed returns a new key history that's equivalent to the history created
// by an ingestOp that "collapses" a batch's keys. See ingestOp.build.
func (h keyHistory) collapsed() keyHistory {
	var ret keyHistory
	// When collapsing a batch, any range deletes are semantically applied
	// first. Look for any range deletes and apply them.
	for _, op := range h {
		if op.opType == OpWriterDeleteRange {
			ret = append(ret, op)
			break
		}
	}
	// Among point keys, the most recently written key wins.
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].opType != OpWriterDeleteRange {
			ret = append(ret, h[i])
			break
		}
	}
	return ret
}

func opWrittenKeys(untypedOp op) [][]byte {
	switch t := untypedOp.(type) {
	case *applyOp:
	case *batchCommitOp:
	case *checkpointOp:
	case *closeOp:
	case *compactOp:
	case *dbRestartOp:
	case *deleteOp:
		return [][]byte{t.key}
	case *deleteRangeOp:
		return [][]byte{t.start, t.end}
	case *flushOp:
	case *getOp:
	case *ingestOp:
	case *initOp:
	case *iterFirstOp:
	case *iterLastOp:
	case *iterNextOp:
	case *iterNextPrefixOp:
	case *iterCanSingleDelOp:
	case *iterPrevOp:
	case *iterSeekGEOp:
	case *iterSeekLTOp:
	case *iterSeekPrefixGEOp:
	case *iterSetBoundsOp:
	case *iterSetOptionsOp:
	case *mergeOp:
		return [][]byte{t.key}
	case *newBatchOp:
	case *newIndexedBatchOp:
	case *newIterOp:
	case *newIterUsingCloneOp:
	case *newSnapshotOp:
	case *rangeKeyDeleteOp:
	case *rangeKeySetOp:
	case *rangeKeyUnsetOp:
	case *setOp:
		return [][]byte{t.key}
	case *singleDeleteOp:
		return [][]byte{t.key}
	case *replicateOp:
		return [][]byte{t.start, t.end}
	}
	return nil
}

func loadPrecedingKeys(ops []op, kg KeyGenerator, m *keyManager) {
	for _, op := range ops {
		// Pretend we're generating all the operation's keys as potential new
		// key, so that we update the key manager's keys and prefix sets.
		for _, k := range opWrittenKeys(op) {
			m.addNewKey(k)
			// Inform the key generator of the key, so it can update the
			// distribution of key suffixes it generates.
			kg.RecordPrecedingKey(k)
		}
		// Update key tracking state.
		m.update(op)
	}
	// We want to retain the keys that made it to the db and clear out everything
	// else. All other objects would conflict with objects from the test itself.
	for objID := range m.byObj {
		if objID.tag() != dbTag {
			delete(m.byObj, objID)
		}
	}
}

func insertSorted(cmp base.Compare, dst *[][]byte, k []byte) {
	s := *dst
	i, _ := slices.BinarySearchFunc(s, k, cmp)
	*dst = slices.Insert(s, i, k)
}

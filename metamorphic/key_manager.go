package metamorphic

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

// objKey is a tuple of (objID, key). This struct is used primarily as a map
// key for keyManager. Only writer objTags can occur here, i.e., dbTag and
// batchTag, since this is used for tracking the keys in a writer.
type objKey struct {
	id  objID
	key []byte
}

// makeObjKey returns a new objKey given and id and key.
func makeObjKey(id objID, key []byte) objKey {
	if id.tag() != dbTag && id.tag() != batchTag {
		panic(fmt.Sprintf("unexpected non-writer tag %v", id.tag()))
	}
	return objKey{id, key}
}

// String implements fmt.Stringer, returning a stable string representation of
// the objKey. This string is used as map key.
func (o objKey) String() string {
	return fmt.Sprintf("%s:%s", o.id, o.key)
}

// keyMeta is metadata associated with an (objID, key) pair, where objID is
// a writer containing the key.
type keyMeta struct {
	objKey
	// history provides the history of writer operations applied against this
	// key on this object. history is always ordered by non-decreasing
	// metaTimestamp.
	history keyHistory
}

func (m *keyMeta) clear() {
	m.history = m.history[:0]
}

// mergeInto merges this metadata into the metadata for other, appending all of
// its individual operations to dst at the provided timestamp.
func (m *keyMeta) mergeInto(dst *keyMeta, ts int) {
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
		if dst.objKey.id.tag() == dbTag && op.opType.isDelete() {
			dst.clear()
			continue
		}
		dst.history = append(dst.history, keyHistoryItem{
			opType:        op.opType,
			metaTimestamp: ts,
		})
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
// sstables. Today, this complication is sidestepped by avoiding ingestion of
// multiple batches containing deletes or single deletes since loss of those
// specific operations on a key are what we cannot tolerate (doing SingleDelete
// on a key that has not been written to because the Set was lost is harmless).
//
// TODO(jackson): Instead, compute smallest and largest bounds of batches so
// that we know at generation-time whether or not an ingestion operation will
// fail and can avoid updating key state.
type keyManager struct {
	comparer *base.Comparer

	// metaTimestamp is used to provide a ordering over certain operations like
	// iter creation, updates to keys. Keeping track of the timestamp allows us
	// to make determinations such as whether a key will be visible to an
	// iterator.
	metaTimestamp int

	// byObjKey tracks the state for each (writer, key) pair. It refers to the
	// same *keyMeta as in the byObj slices. Using a map allows for fast state
	// lookups when changing the state based on a writer operation on the key.
	byObjKey map[string]*keyMeta
	// List of keys per writer, and what has happened to it in that writer.
	// Will be transferred when needed.
	byObj map[objID][]*keyMeta

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

func (k *keyManager) nextMetaTimestamp() int {
	ret := k.metaTimestamp
	k.metaTimestamp++
	return ret
}

// newKeyManager returns a pointer to a new keyManager. Callers should
// interact with this using addNewKey, knownKeys, update,
// canTolerateApplyFailure methods only.
func newKeyManager(numInstances int) *keyManager {
	m := &keyManager{
		comparer:             testkeys.Comparer,
		byObjKey:             make(map[string]*keyMeta),
		byObj:                make(map[objID][]*keyMeta),
		globalKeysMap:        make(map[string]bool),
		globalKeyPrefixesMap: make(map[string]struct{}),
	}
	for i := 1; i <= max(numInstances, 1); i++ {
		m.byObj[makeObjID(dbTag, uint32(i))] = []*keyMeta{}
	}
	return m
}

// addNewKey adds the given key to the key manager for global key tracking.
// Returns false iff this is not a new key.
func (k *keyManager) addNewKey(key []byte) bool {
	if k.globalKeysMap[string(key)] {
		return false
	}
	insertSorted(k.comparer.Compare, &k.globalKeys, key)
	k.globalKeysMap[string(key)] = true

	prefixLen := k.comparer.Split(key)
	if _, ok := k.globalKeyPrefixesMap[string(key[:prefixLen])]; !ok {
		insertSorted(k.comparer.Compare, &k.globalKeyPrefixes, key[:prefixLen])
		k.globalKeyPrefixesMap[string(key[:prefixLen])] = struct{}{}
	}
	return true
}

// getOrInit returns the keyMeta for the (objID, key) pair, if it exists, else
// allocates, initializes and returns a new value.
func (k *keyManager) getOrInit(id objID, key []byte) *keyMeta {
	o := makeObjKey(id, key)
	m, ok := k.byObjKey[o.String()]
	if ok {
		return m
	}
	m = &keyMeta{objKey: makeObjKey(id, key)}
	// Initialize the key-to-meta index.
	k.byObjKey[o.String()] = m
	// Add to the id-to-metas slide.
	k.byObj[o.id] = append(k.byObj[o.id], m)
	return m
}

// mergeKeysInto merges all metadata for all keys associated with the "from" ID
// with the metadata for keys associated with the "to" ID.
func (k *keyManager) mergeKeysInto(from, to objID, mergeFunc func(src, dst *keyMeta, ts int)) {
	msFrom, ok := k.byObj[from]
	if !ok {
		msFrom = []*keyMeta{}
		k.byObj[from] = msFrom
	}
	msTo, ok := k.byObj[to]
	if !ok {
		msTo = []*keyMeta{}
		k.byObj[to] = msTo
	}

	// Sort to facilitate a merge.
	slices.SortFunc(msFrom, func(a, b *keyMeta) int {
		return bytes.Compare(a.key, b.key)
	})
	slices.SortFunc(msTo, func(a, b *keyMeta) int {
		return bytes.Compare(a.key, b.key)
	})

	ts := k.nextMetaTimestamp()
	var msNew []*keyMeta
	var iTo int
	for _, m := range msFrom {
		// Move cursor on mTo forward.
		for iTo < len(msTo) && bytes.Compare(msTo[iTo].key, m.key) < 0 {
			msNew = append(msNew, msTo[iTo])
			iTo++
		}

		var mTo *keyMeta
		if iTo < len(msTo) && bytes.Equal(msTo[iTo].key, m.key) {
			mTo = msTo[iTo]
			iTo++
		} else {
			mTo = &keyMeta{objKey: makeObjKey(to, m.key)}
			k.byObjKey[mTo.String()] = mTo
		}

		mergeFunc(m, mTo, ts)
		msNew = append(msNew, mTo)

		delete(k.byObjKey, m.String()) // Unlink "from".
	}

	// Add any remaining items from the "to" set.
	for iTo < len(msTo) {
		msNew = append(msNew, msTo[iTo])
		iTo++
	}

	k.byObj[to] = msNew   // Update "to".
	delete(k.byObj, from) // Unlink "from".
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
	var conflicts [][]byte
	for _, src := range k.byObj[srcObj] {
		// Single delete generation logic already ensures that both srcObj and
		// dstObj's single deletes are deterministic within the context of their
		// existing writes. However, applying srcObj on top of dstObj may
		// violate the invariants. Consider:
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
		if srcCollapsed {
			srcHistory = src.history.collapsed()
		}

	srcloop:
		for _, item := range srcHistory {
			switch item.opType {
			case writerDelete, writerDeleteRange:
				// We found a DEL or RANGEDEL before any single delete. If src
				// contains additional single deletes, their effects are limited
				// to applying to later keys. Combining the two object histories
				// doesn't pose any determinism risk.
				break srcloop
			case writerSingleDelete:
				// We found a single delete. Since we found this single delete
				// before a DEL or RANGEDEL, this delete has the potential to
				// affect the visibility of keys in `dstObj`. We'll need to look
				// for potential conflicts down below.
				srcHasUnboundedSingleDelete = true
				if srcValuesBeforeSingleDelete > 1 {
					panic(errors.AssertionFailedf("unexpectedly found %d sets/merges within %s before single del",
						srcValuesBeforeSingleDelete, srcObj))
				}
				break srcloop
			case writerSet, writerMerge:
				// We found a SET or MERGE operation for this key. If there's a
				// subsequent single delete, we'll need to make sure there's not
				// a SET or MERGE in the dst too.
				srcValuesBeforeSingleDelete++
			default:
				panic(errors.AssertionFailedf("unexpected optype %d", item.opType))
			}
		}
		if !srcHasUnboundedSingleDelete {
			continue
		}

		dst, ok := k.byObjKey[makeObjKey(dstObj, src.key).String()]
		// If the destination writer has no record of the key, the combined key
		// history is simply the src object's key history which is valid due to
		// per-object single deletion invariants.
		if !ok {
			continue
		}

		// We need to examine the trailing key history on dst.
		consecutiveValues := srcValuesBeforeSingleDelete
	dstloop:
		for i := len(dst.history) - 1; i >= 0; i-- {
			switch dst.history[i].opType {
			case writerSet, writerMerge:
				// A SET/MERGE may conflict if there's more than 1 consecutive
				// SET/MERGEs.
				consecutiveValues++
				if consecutiveValues > 1 {
					conflicts = append(conflicts, src.key)
					break dstloop
				}
			case writerDelete, writerSingleDelete, writerDeleteRange:
				// Dels clear the history, enabling use of single delete.
				break dstloop
			default:
				panic(errors.AssertionFailedf("unexpected optype %d", dst.history[i].opType))
			}
		}
	}
	return conflicts
}

// update updates the internal state of the keyManager according to the given
// op.
func (k *keyManager) update(o op) {
	switch s := o.(type) {
	case *setOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.history = append(meta.history, keyHistoryItem{
			opType:        writerSet,
			metaTimestamp: k.nextMetaTimestamp(),
		})
	case *mergeOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.history = append(meta.history, keyHistoryItem{
			opType:        writerMerge,
			metaTimestamp: k.nextMetaTimestamp(),
		})
	case *deleteOp:
		meta := k.getOrInit(s.writerID, s.key)
		if meta.objKey.id.tag() == dbTag {
			meta.clear()
		} else {
			meta.history = append(meta.history, keyHistoryItem{
				opType:        writerDelete,
				metaTimestamp: k.nextMetaTimestamp(),
			})
		}
	case *deleteRangeOp:
		// We track the history of discrete point keys, but a range deletion
		// applies over a continuous key span of infinite keys. However, the key
		// manager knows all keys that have been used in all operations, so we
		// can discretize the range tombstone by adding it to every known key
		// within the range.
		ts := k.nextMetaTimestamp()
		keyRange := pebble.KeyRange{Start: s.start, End: s.end}
		for _, key := range k.knownKeysInRange(keyRange) {
			meta := k.getOrInit(s.writerID, key)
			if meta.objKey.id.tag() == dbTag {
				meta.clear()
			} else {
				meta.history = append(meta.history, keyHistoryItem{
					opType:        writerDeleteRange,
					metaTimestamp: ts,
				})
			}
		}
	case *singleDeleteOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.history = append(meta.history, keyHistoryItem{
			opType:        writerSingleDelete,
			metaTimestamp: k.nextMetaTimestamp(),
		})
	case *ingestOp:
		// For each batch, merge the keys into the DB. We can't call
		// keyMeta.mergeInto directly to merge, because ingest operations first
		// "flatten" the batch (because you can't set the same key twice at a
		// single sequence number). Instead we compute the collapsed history and
		// merge that.
		for _, batchID := range s.batchIDs {
			k.mergeKeysInto(batchID, s.dbID, func(src, dst *keyMeta, ts int) {
				collapsedSrc := keyMeta{
					objKey:  src.objKey,
					history: src.history.collapsed(),
				}
				collapsedSrc.mergeInto(dst, ts)
			})
		}
	case *applyOp:
		// Merge the keys from this writer into the parent writer.
		k.mergeKeysInto(s.batchID, s.writerID, (*keyMeta).mergeInto)
	case *batchCommitOp:
		// Merge the keys from the batch with the keys from the DB.
		k.mergeKeysInto(s.batchID, s.dbID, (*keyMeta).mergeInto)
	}
}

func (k *keyManager) knownKeys() (keys [][]byte) {
	return k.globalKeys
}

// knownKeysInRange returns all eligible read keys within the range
// [start,end). The returned slice is owned by the keyManager and must not be
// retained.
func (k *keyManager) knownKeysInRange(kr pebble.KeyRange) (keys [][]byte) {
	s, _ := slices.BinarySearchFunc(k.globalKeys, kr.Start, k.comparer.Compare)
	e, _ := slices.BinarySearchFunc(k.globalKeys, kr.End, k.comparer.Compare)
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
	for _, key := range k.globalKeys {
		objKey := makeObjKey(o, key)
		meta, ok := k.byObjKey[objKey.String()]
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

// canTolerateApplyFailure is called with a batch ID and returns true iff a
// failure to apply this batch to the DB can be tolerated.
func (k *keyManager) canTolerateApplyFailure(id objID) bool {
	if id.tag() != batchTag {
		panic("called with an objID that is not a batch")
	}
	ms, ok := k.byObj[id]
	if !ok {
		return true
	}
	for _, m := range ms {
		for i := len(m.history) - 1; i >= 0; i-- {
			if m.history[i].opType.isDelete() {
				return false
			}
		}
	}
	return true
}

// a keyHistoryItem describes an individual operation performed on a key.
type keyHistoryItem struct {
	// opType may be writerSet, writerDelete, writerSingleDelete,
	// writerDeleteRange or writerMerge only. No other opTypes may appear here.
	opType        opType
	metaTimestamp int
}

// keyHistory captures the history of mutations to a key in chronological order.
type keyHistory []keyHistoryItem

// before returns the subslice of the key history that happened strictly before
// the provided meta timestamp.
func (h keyHistory) before(metaTimestamp int) keyHistory {
	i, _ := slices.BinarySearchFunc(h, metaTimestamp, func(a keyHistoryItem, ts int) int {
		return cmp.Compare(a.metaTimestamp, ts)
	})
	return h[:i]
}

// canSingleDelete examines the tail of the history and returns true if a single
// delete appended to this history would satisfy the single delete invariants.
func (h keyHistory) canSingleDelete() bool {
	if len(h) == 0 {
		return true
	}
	switch o := h[len(h)-1].opType; o {
	case writerDelete, writerDeleteRange, writerSingleDelete:
		return true
	case writerSet, writerMerge:
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
		case writerDelete:
			fmt.Fprint(&sb, "del")
		case writerDeleteRange:
			fmt.Fprint(&sb, "delrange")
		case writerSingleDelete:
			fmt.Fprint(&sb, "singledel")
		case writerSet:
			fmt.Fprint(&sb, "set")
		case writerMerge:
			fmt.Fprint(&sb, "merge")
		default:
			fmt.Fprintf(&sb, "optype[v=%d]", it.opType)
		}
		fmt.Fprintf(&sb, "(%d)", it.metaTimestamp)
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
		if op.opType == writerDeleteRange {
			ret = append(ret, op)
			break
		}
	}
	// Among point keys, the most recently written key wins.
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].opType != writerDeleteRange {
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

func loadPrecedingKeys(t TestingT, ops []op, cfg *config, m *keyManager) {
	for _, op := range ops {
		// Pretend we're generating all the operation's keys as potential new
		// key, so that we update the key manager's keys and prefix sets.
		for _, k := range opWrittenKeys(op) {
			m.addNewKey(k)

			// If the key has a suffix, ratchet up the suffix distribution if
			// necessary.
			if s := m.comparer.Split(k); s < len(k) {
				suffix, err := testkeys.ParseSuffix(k[s:])
				require.NoError(t, err)
				if uint64(suffix) > cfg.writeSuffixDist.Max() {
					diff := int(uint64(suffix) - cfg.writeSuffixDist.Max())
					cfg.writeSuffixDist.IncMax(diff)
				}
			}
		}

		// Update key tracking state.
		m.update(op)
	}
}

func insertSorted(cmp base.Compare, dst *[][]byte, k []byte) {
	s := *dst
	i, _ := slices.BinarySearchFunc(s, k, cmp)
	*dst = slices.Insert(s, i, k)
}

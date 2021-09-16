package metamorphic

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
)

// objKey is a tuple of (objID, key). This struct is used primarily as a map
// key for keyManager.
type objKey struct {
	id  objID
	key []byte
}

// makeObjKey returns a new objKey given and id and key.
func makeObjKey(id objID, key []byte) objKey {
	return objKey{id, key}
}

// String implements fmt.Stringer, returning a stable string representation of
// the objKey. This string is used as map key.
func (o objKey) String() string {
	return fmt.Sprintf("%s:%s", o.id, o.key)
}

// keyMeta is metadata associated with an (objID, key) pair participating in a
// metamorphic test.
type keyMeta struct {
	objKey

	sets      int
	merges    int
	singleDel bool
}

// mergeInto merges this metadata this into the metadata for other.
func (m *keyMeta) mergeInto(other *keyMeta) {
	// Sets and merges are additive.
	other.sets += m.sets
	other.merges += m.merges

	// Single deletes are preserved.
	other.singleDel = other.singleDel || m.singleDel
}

// canSingleDelete returns true if this key is eligible for single deletion.
func (m *keyMeta) canSingleDelete() bool {
	return m.sets <= 1 && m.merges == 0
}

// keyManager tracks the operations performed on keys across all readers /
// writers participating in the metamorphic test.
type keyManager struct {
	// TODO(travers): These two maps could likely be combined into something
	// like a multi-map / multi-set.
	byObjKey map[string]*keyMeta
	byObj    map[objID][]*keyMeta

	globalKeys     [][]byte
	globalSetCount map[string]int
}

// newKeyManager returns a pointer to a new keyManager.
func newKeyManager() *keyManager {
	m := &keyManager{
		byObjKey:       make(map[string]*keyMeta),
		byObj:          make(map[objID][]*keyMeta),
		globalSetCount: make(map[string]int),
	}
	m.byObj[makeObjID(dbTag, 0)] = []*keyMeta{}
	return m
}

// addKey adds the given key to the key manager for global key tracking.
func (k *keyManager) addKey(key []byte) {
	k.globalKeys = append(k.globalKeys, key)
}

// getOrInit returns the keyMeta for the (objID, key) pair, if it exists, else
// allocates, initializes and returns a new value.
func (k *keyManager) getOrInit(id objID, key []byte) (m *keyMeta) {
	o := makeObjKey(id, key)
	m, ok := k.byObjKey[o.String()]
	if ok {
		return
	}
	m = &keyMeta{objKey: makeObjKey(id, key)}

	// Initialize the key-to-meta index.
	k.byObjKey[o.String()] = m

	// Initialize the id-to-metas index.
	k.byObj[o.id] = append(k.byObj[o.id], m)

	return
}

// contains returns true if the (objID, key) pair is tracked by the keyManager.
func (k *keyManager) contains(id objID, key []byte) bool {
	_, ok := k.byObjKey[makeObjKey(id, key).String()]
	return ok
}

// mergeKeysInto merges all metadata for all keys associated with the "from" ID
// with the metadata for keys associated with the "to" ID.
func (k *keyManager) mergeKeysInto(from, to objID) {
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
	sort.Slice(msFrom, func(i, j int) bool {
		return msFrom[i].String() < msFrom[j].String()
	})
	sort.Slice(msTo, func(i, j int) bool {
		return msTo[i].String() < msTo[j].String()
	})

	var msNew []*keyMeta
	var iTo int
	for _, m := range msFrom {
		// Move cursor on mTo forward.
		for iTo < len(msTo) && string(msTo[iTo].key) < string(m.key) {
			msNew = append(msNew, msTo[iTo])
			iTo++
		}

		var mNew *keyMeta
		if iTo < len(msTo) && string(msTo[iTo].key) == string(m.key) {
			mNew = msTo[iTo]
			iTo++
		} else {
			mNew = &keyMeta{objKey: makeObjKey(to, m.key)}
			k.byObjKey[mNew.String()] = mNew
		}

		m.mergeInto(mNew)
		msNew = append(msNew, mNew)

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

// update updates the internal state of the keyManager according to the given
// op.
func (k *keyManager) update(o op) {
	switch s := o.(type) {
	case *setOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.sets++           // Update the set count on this specific (id, key) pair.
		k.incGlobalSet(s.key) // Update the set count globally for the key.
	case *mergeOp:
		meta := k.getOrInit(s.writerID, s.key)
		meta.merges++
	case *singleDeleteOp:
		meta := k.getOrInit(s.writerID, s.key)
		// Sanity check. Key should be able to be SingleDeleted.
		if !meta.canSingleDelete() {
			panic(errors.Newf("cannot single delete key: %+v", meta))
		}
		meta.singleDel = true
	case *ingestOp:
		// For each batch, merge all keys with the keys in the DB.
		dbID := makeObjID(dbTag, 0)
		for _, batchID := range s.batchIDs {
			k.mergeKeysInto(batchID, dbID)
		}
	case *applyOp:
		// Merge the keys from this writer into the parent writer.
		k.mergeKeysInto(s.batchID, s.writerID)
	case *batchCommitOp:
		// Merge the keys from the batch with the keys from the DB.
		k.mergeKeysInto(s.batchID, makeObjID(dbTag, 0))
	}
}

// incGlobalSet increments the global set count for the key.
func (k *keyManager) incGlobalSet(key []byte) {
	count, ok := k.globalSetCount[string(key)]
	if !ok {
		count = 0
	}
	count++
	k.globalSetCount[string(key)] = count
}

// getGlobalSet returns the number of set operations that have been performed
// on a key, globally (i.e. across all objIDs).
func (k *keyManager) getGlobalSet(key []byte) int {
	count, ok := k.globalSetCount[string(key)]
	if !ok {
		return 0
	}
	return count
}

// eligibleSingleDeleteKeys returns a slice of keys associated with an ID that
// can be safely single deleted.
func (k *keyManager) eligibleSingleDeleteKeys(id objID) (keys [][]byte) {
	for _, m := range k.byObj[id] {
		if k.getGlobalSet(m.key) == 1 && m.canSingleDelete() {
			keys = append(keys, m.key)
		}
	}
	return
}

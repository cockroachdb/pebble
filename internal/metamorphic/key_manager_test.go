package metamorphic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjKey(t *testing.T) {
	testCases := []struct {
		key  objKey
		want string
	}{
		{
			key:  makeObjKey(makeObjID(dbTag, 0), []byte("foo")),
			want: "db:foo",
		},
		{
			key:  makeObjKey(makeObjID(batchTag, 1), []byte("bar")),
			want: "batch1:bar",
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.want, tc.key.String())
		})
	}
}

func TestKeyMeta_CanSingleDelete(t *testing.T) {
	key := makeObjKey(makeObjID(dbTag, 0), []byte("foo"))
	testCases := []struct {
		meta keyMeta
		want bool
	}{
		{
			meta: keyMeta{
				objKey: key,
			},
			want: true,
		},
		{
			meta: keyMeta{
				objKey: key,
				sets:   1,
			},
			want: true,
		},
		{
			meta: keyMeta{
				objKey: key,
				sets:   2,
			},
			want: false,
		},
		{
			meta: keyMeta{
				objKey: key,
				sets:   1,
				merges: 1,
			},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.want, tc.meta.canSingleDelete())
		})
	}
}

func TestKeyMeta_MergeInto(t *testing.T) {
	key1 := &keyMeta{
		sets:      1,
		merges:    2,
		singleDel: false,
	}
	key2 := &keyMeta{
		sets:      3,
		merges:    1,
		singleDel: true,
	}

	key2.mergeInto(key1)

	require.Equal(t, 4, key1.sets)
	require.Equal(t, 3, key1.merges)
	require.True(t, key1.singleDel)
}

func TestKeyManager_AddKey(t *testing.T) {
	m := newKeyManager()
	require.Empty(t, m.globalKeys)

	k := []byte("foo")
	m.addKey(k)
	require.Len(t, m.globalKeys, 1)
	require.Contains(t, m.globalKeys, k)
}

func TestKeyManager_GetOrInit(t *testing.T) {
	id := makeObjID(batchTag, 1)
	key := []byte("foo")
	o := makeObjKey(id, key)

	m := newKeyManager()
	require.NotContains(t, m.byObjKey, o.String())
	require.NotContains(t, m.byObj, id)
	require.Contains(t, m.byObj, makeObjID(dbTag, 0)) // Always contains the DB key.

	meta1 := m.getOrInit(id, key)
	require.Contains(t, m.byObjKey, o.String())
	require.Contains(t, m.byObj, id)

	// Idempotent.
	meta2 := m.getOrInit(id, key)
	require.Equal(t, meta1, meta2)
}

func TestKeyManager_Contains(t *testing.T) {
	id := makeObjID(dbTag, 0)
	key := []byte("foo")

	m := newKeyManager()
	require.False(t, m.contains(id, key))

	m.getOrInit(id, key)
	require.True(t, m.contains(id, key))
}

func TestKeyManager_MergeInto(t *testing.T) {
	fromID := makeObjID(batchTag, 1)
	toID := makeObjID(dbTag, 0)

	m := newKeyManager()

	// Two keys in "from".
	a := m.getOrInit(fromID, []byte("foo"))
	a.sets = 1
	b := m.getOrInit(fromID, []byte("bar"))
	b.merges = 2

	// One key in "to", with same value as a key in "from", that will be merged.
	m.getOrInit(toID, []byte("foo"))

	// Before, there are two sets.
	require.Len(t, m.byObj[fromID], 2)
	require.Len(t, m.byObj[toID], 1)

	m.mergeKeysInto(fromID, toID)

	// Keys in "from" sets are moved to "to" set.
	require.Len(t, m.byObj[toID], 2)

	// Key "foo" was merged into "to".
	foo := m.getOrInit(toID, []byte("foo"))
	require.Equal(t, 1, foo.sets) // value was merged.

	// Key "bar" was merged into "to".
	bar := m.getOrInit(toID, []byte("bar"))
	require.Equal(t, 2, bar.merges) // value was unchanged.

	// Keys in "from" sets are removed from maps.
	require.NotContains(t, m.byObjKey, makeObjKey(fromID, a.key))
	require.NotContains(t, m.byObjKey, makeObjKey(fromID, b.key))
	require.NotContains(t, m.byObj, fromID)
}

func TestKeyManager_GlobalSetCount(t *testing.T) {
	key := []byte("foo")
	id1 := makeObjID(dbTag, 0)
	id2 := makeObjID(batchTag, 1)

	km := newKeyManager()

	// Perform a set on on the key in the DB.
	km.update(&setOp{writerID: id1, key: key})
	m1 := km.getOrInit(id1, key)
	require.Equal(t, 1, m1.sets)
	require.Equal(t, 1, km.getGlobalSet(key))

	// Perform a set on the key in the batch.
	km.update(&setOp{writerID: id2, key: key})
	m2 := km.getOrInit(id2, key)
	require.Equal(t, 1, m2.sets)              // (id, key) set count is one
	require.Equal(t, 2, km.getGlobalSet(key)) // global set count is two
}

func TestKeyManager_Update(t *testing.T) {
	var (
		idDB = makeObjID(dbTag, 0)
		id1  = makeObjID(batchTag, 0)
		id2  = makeObjID(batchTag, 1)
		key1 = []byte("foo")
		key2 = []byte("bar")
	)

	testCases := []struct {
		description string
		ops         []op
		checkFn     func(t *testing.T, m *keyManager)
		wantPanic   bool
	}{
		{
			description: "single set",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 1)
			},
		},
		{
			description: "multi set; same key; same writer",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
				&setOp{writerID: idDB, key: key1},
				&setOp{writerID: idDB, key: key1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 3, m.getOrInit(idDB, key1).sets)
				require.Empty(t, m.eligibleSingleDeleteKeys(idDB))
			},
		},
		{
			description: "multi set; same key; different writer",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
				&setOp{writerID: id1, key: key1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(id1, key1).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(id1), 0) // global set count violated.
			},
		},
		{
			description: "multi set; different key; same writer",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
				&setOp{writerID: idDB, key: key2},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(idDB, key2).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 2)
			},
		},
		{
			description: "multi set; different key; different writer",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
				&setOp{writerID: id1, key: key2},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(id1, key2).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 1)
				require.Len(t, m.eligibleSingleDeleteKeys(id1), 1)
			},
		},
		{
			description: "set then merge",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
				&mergeOp{writerID: idDB, key: key1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(idDB, key1).merges)
				require.Empty(t, m.eligibleSingleDeleteKeys(idDB))
			},
		},
		{
			description: "single delete",
			ops: []op{
				&setOp{writerID: idDB, key: key1},
				&singleDeleteOp{writerID: idDB, key: key1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.True(t, m.getOrInit(idDB, key1).singleDel)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 1)
			},
		},
		{
			description: "single delete with key not present",
			ops: []op{
				&singleDeleteOp{writerID: idDB, key: key1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.True(t, m.getOrInit(idDB, key1).singleDel)
			},
		},
		{
			description: "single delete with merge present",
			ops: []op{
				&mergeOp{writerID: idDB, key: key1},
				&setOp{writerID: idDB, key: key1},
				&singleDeleteOp{writerID: idDB, key: key1},
			},
			wantPanic: true,
		},
		{
			description: "ingest; single batch; single set",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&ingestOp{batchIDs: []objID{id1}},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 1)
				require.True(t, m.contains(idDB, key1))
				require.False(t, m.contains(id1, key1))
			},
		},
		{
			description: "ingest; multi batch; single key",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&setOp{writerID: id2, key: key1},
				&ingestOp{batchIDs: []objID{id1, id2}},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 2, m.getOrInit(idDB, key1).sets)
				require.Empty(t, m.eligibleSingleDeleteKeys(idDB))
				require.True(t, m.contains(idDB, key1))
				require.False(t, m.contains(id1, key1))
				require.False(t, m.contains(id2, key1))
			},
		},
		{
			description: "ingest; multi batch; different keys",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&setOp{writerID: id2, key: key2},
				&ingestOp{batchIDs: []objID{id1, id2}},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(idDB, key2).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 2)
				require.True(t, m.contains(idDB, key1))
				require.True(t, m.contains(idDB, key2))
				require.False(t, m.contains(id1, key1))
				require.False(t, m.contains(id2, key2))
			},
		},
		{
			description: "apply; single set",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&applyOp{batchID: id1, writerID: idDB},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 1)
				require.True(t, m.contains(idDB, key1))
				require.False(t, m.contains(id1, key1))
			},
		},
		{
			description: "apply; multi set; same key",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&setOp{writerID: id1, key: key1},
				&applyOp{batchID: id1, writerID: idDB},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 2, m.getOrInit(idDB, key1).sets)
				require.Empty(t, m.eligibleSingleDeleteKeys(idDB))
				require.True(t, m.contains(idDB, key1))
				require.False(t, m.contains(id1, key1))
			},
		},
		{
			description: "apply; multi set; different key",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&setOp{writerID: id1, key: key2},
				&applyOp{batchID: id1, writerID: idDB},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(idDB, key2).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 2)
				require.True(t, m.contains(idDB, key1))
				require.True(t, m.contains(idDB, key2))
				require.False(t, m.contains(id1, key1))
				require.False(t, m.contains(id1, key2))
			},
		},
		{
			description: "batch commit; single set",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&batchCommitOp{batchID: id1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 1)
				require.True(t, m.contains(idDB, key1))
				require.False(t, m.contains(id1, key1))
			},
		},
		{
			description: "batch commit; multi set; same key",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&setOp{writerID: id1, key: key1},
				&batchCommitOp{batchID: id1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 2, m.getOrInit(idDB, key1).sets)
				require.Empty(t, m.eligibleSingleDeleteKeys(idDB))
				require.True(t, m.contains(idDB, key1))
				require.False(t, m.contains(id1, key1))
			},
		},
		{
			description: "batch commit; multi set; different key",
			ops: []op{
				&setOp{writerID: id1, key: key1},
				&setOp{writerID: id1, key: key2},
				&batchCommitOp{batchID: id1},
			},
			checkFn: func(t *testing.T, m *keyManager) {
				require.Equal(t, 1, m.getOrInit(idDB, key1).sets)
				require.Equal(t, 1, m.getOrInit(idDB, key2).sets)
				require.Len(t, m.eligibleSingleDeleteKeys(idDB), 2)
				require.True(t, m.contains(idDB, key1))
				require.True(t, m.contains(idDB, key2))
				require.False(t, m.contains(id1, key1))
				require.False(t, m.contains(id1, key2))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			m := newKeyManager()
			tFunc := func() {
				for _, op := range tc.ops {
					m.update(op)
				}
			}
			if tc.wantPanic {
				require.Panics(t, tFunc)
			} else {
				tFunc()
				tc.checkFn(t, m)
			}
		})
	}
}

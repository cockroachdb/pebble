package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func generateRandomKeys(klen uint64, num uint64) [][]byte {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	rand.Seed(time.Now().UnixNano())

	keys := make([][]byte, num)
	for i := uint64(0); i < num; i++ {
		keys[i] = make([]byte, klen)
		for j := range keys[i] {
			keys[i][j] = letters[rand.Intn(len(letters))]
		}
	}

	return keys
}

func printLevels(t *testing.T, d *DB) map[string]bool {
	visible := make(map[string]bool)
	// check the number of table files in each level
	t.Logf("Level metadata")
	var nTables [manifest.NumLevels]int
	readState := d.loadReadState()
	for i := 0; i < manifest.NumLevels; i++ {
		nTables[i] = readState.current.Levels[i].Len()
		t.Logf("  -- level %d has %d tables", i, nTables[i])
		iter := readState.current.Levels[i].Iter()
		fm := iter.First()
		for fm != nil {
			if fm.IsShared {
				t.Logf("    -- sst %d is shared with virtual bound (%s %s) file bound (%s %s)\n",
					fm.FileNum, fm.Smallest.UserKey, fm.Largest.UserKey, fm.FileSmallest.UserKey, fm.FileLargest.UserKey)
				visible[string(fm.Smallest.UserKey)] = true
				visible[string(fm.FileLargest.UserKey)] = false
			} else {
				t.Logf("    -- sst %d is local with bound (%s %s)\n", fm.FileNum, fm.Smallest.UserKey, fm.Largest.UserKey)
				//visible[string(fm.Smallest.UserKey)] = true
				//visible[string(fm.FileLargest.UserKey)] = true
			}
			fm = iter.Next()
		}
	}
	readState.unref()
	return visible
}

func validateBoundaries(t *testing.T, d *DB, visible *map[string]bool) {
	// no compaction should have happened, take a snapshot
	snapshot := d.NewSnapshot()
	t.Logf("Validating ...")
	for key, iv := range *visible {
		_, closer, err := snapshot.Get([]byte(key))
		if iv {
			if err != nil {
				t.Fatalf("  -- get: visible key %s get error (%v)", key, err)
			}
			t.Logf("  -- get: visible key %s found ", key)
			require.NoError(t, closer.Close())
		} else {
			if err != ErrNotFound {
				t.Fatalf("  -- get: invisible key %s get error (%v)", key, err)
			}
			t.Logf("  -- get: invisible key %s not found ", key)
		}
	}
	require.NoError(t, snapshot.Close())
}

func TestMain(m *testing.M) {
	code := m.Run()
	// reset to default function

	os.Exit(code)
}

func TestDBWithSharedSST(t *testing.T) {
	fs := vfs.NewMem()
	sharedfs := vfs.NewMem()

	uid := uint32(rand.Uint32())
	t.Log("Opening ...")
	d, err := Open("", &Options{
		FS:        fs,
		SharedFS:  sharedfs,
		SharedDir: "",
		UniqueID:  uid,
	})
	require.NoError(t, err)

	// this is tricky, we need to make the directory for testing
	const buckets = 10
	for i := 0; i < buckets; i++ {
		// create local and fake foreign buckets
		require.NoError(t, sharedfs.MkdirAll(fmt.Sprintf("%d/%d", uid, i), 0755))
		require.NoError(t, sharedfs.MkdirAll(fmt.Sprintf("%d/%d", uid+1, i), 0755))
	}

	const N = 1000000
	keys := generateRandomKeys(8, N)
	value := bytes.Repeat([]byte("x"), 4096)

	// inject key boundaries function to filter out the upper
	setSharedSSTMetadata = func(meta *manifest.FileMetadata, creatorUniqueID uint32) {
		// The output sst is shared so update its boundaries
		meta.FileSmallest, meta.FileLargest = meta.Smallest, meta.Largest

		// Only one key for each table for testing
		lb, ub := meta.Smallest, meta.Smallest
		meta.Smallest, meta.Largest = lb, ub
		meta.SmallestPointKey, meta.LargestPointKey = lb, ub

		meta.CreatorUniqueID = creatorUniqueID + 1 // fake foreign sst
		meta.PhysicalFileNum = meta.FileNum

		t.Logf("  -- new shared sst with virtual bound (%s %s) with file bound (%s %s)",
			lb.UserKey, ub.UserKey, meta.FileSmallest.UserKey, meta.FileLargest.UserKey)
	}

	// repeatly inserting/updating a random key
	t.Log("Inserting ...")
	for i := 0; i < N; i++ {
		if i%100000 == 0 && i != 0 {
			t.Logf("set %d keys", i)
		}
		key := &keys[i]
		if err := d.Set(*key, value, nil); err != nil {
			t.Fatalf("set key %s error %v", key, err)
		}
	}

	visible := printLevels(t, d)

	validateBoundaries(t, d, &visible)
	require.NoError(t, d.Close())

	t.Log("Reopening ...")
	d, err = Open("", &Options{
		FS:        fs,
		SharedFS:  sharedfs,
		SharedDir: "",
	})
	require.NoError(t, err)

	visible = printLevels(t, d)
	validateBoundaries(t, d, &visible)

	require.NoError(t, d.Close())

	// revert
	setSharedSSTMetadata = func(meta *manifest.FileMetadata, creatorUniqueID uint32) {
		meta.FileSmallest, meta.FileLargest = meta.Smallest, meta.Largest
		lb, ub := meta.Smallest, meta.Largest
		meta.Smallest, meta.Largest = lb, ub
		meta.SmallestPointKey, meta.LargestPointKey = lb, ub
		meta.CreatorUniqueID = creatorUniqueID
		meta.PhysicalFileNum = meta.FileNum
	}
}

func TestIngestSharedSST(t *testing.T) {

	fs1, fs2 := vfs.NewMem(), vfs.NewMem()
	uid1, uid2 := uint32(rand.Uint32()), uint32(rand.Uint32())
	sharedfs := vfs.NewMem()

	t.Log("Opening ...")
	d1, err := Open("", &Options{
		FS:        fs1,
		SharedFS:  sharedfs,
		SharedDir: "",
		UniqueID:  uid1,
	})
	require.NoError(t, err)
	d2, err := Open("", &Options{
		FS:        fs2,
		SharedFS:  sharedfs,
		SharedDir: "",
		UniqueID:  uid2,
	})
	require.NoError(t, err)

	const buckets = 10
	for i := 0; i < buckets; i++ {
		require.NoError(t, sharedfs.MkdirAll(fmt.Sprintf("%d/%d", uid1, i), 0755))
		require.NoError(t, sharedfs.MkdirAll(fmt.Sprintf("%d/%d", uid2, i), 0755))
	}

	const N = 100000
	keys := generateRandomKeys(8, N)
	value := bytes.Repeat([]byte("x"), 4096)

	// repeatly inserting/updating a random key
	t.Log("Inserting ...")
	for i := 0; i < N; i++ {
		if i%10000 == 0 && i != 0 {
			t.Logf("set %d keys", i)
		}
		key := &keys[i]
		if err := d1.Set(*key, value, nil); err != nil {
			t.Fatalf("set key %s error %v", key, err)
		}
	}

	t.Logf("Printing d1 LSM")
	printLevels(t, d1)

	// test skipshared callback
	t.Logf("Iterating and creating sharedmeta...")
	var smeta []SharedSSTMeta

	rand.Seed(time.Now().Unix())
	cb := func(meta *manifest.FileMetadata) {
		// Randomly pick a table here..
		if rand.Uint32()%2 == 1 {
			return
		}
		m := SharedSSTMeta{
			CreatorUniqueID: meta.CreatorUniqueID,
			PhysicalFileNum: meta.PhysicalFileNum,
			Smallest:        meta.Smallest,
			Largest:         meta.Smallest, // Only copy one key
			FileSmallest:    meta.FileSmallest,
			FileLargest:     meta.FileLargest,
		}
		smeta = append(smeta, m)
	}
	iterOpts := &IterOptions{SkipSharedFile: true, SharedFileCallback: cb}
	iter := d1.NewIter(iterOpts)
	require.NotEqual(t, nil, iter)
	require.Equal(t, true, iter.First())
	for iter.Next() {
		// do nothing
	}
	require.NoError(t, iter.Close())

	require.NoError(t, d2.Ingest(nil, smeta))

	t.Logf("Printing d2 LSM")
	printLevels(t, d2)

	t.Logf("Iterating over d2's key space")
	iter = d2.NewIter(&IterOptions{})
	require.NotEqual(t, nil, iter)
	for i := iter.First(); i; i = iter.Next() {
		t.Logf("  - key: %s", iter.Key())
	}
	require.NoError(t, iter.Close())

	require.NoError(t, d1.Close())
	require.NoError(t, d2.Close())
}

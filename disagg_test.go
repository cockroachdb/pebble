package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

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

	rand.Seed(time.Now().UnixNano())
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const N = 1000000
	const K = 8
	var keys [N][]byte
	for i := 0; i < N; i++ {
		keys[i] = make([]byte, K)
		for j := range keys[i] {
			keys[i][j] = letters[rand.Intn(len(letters))]
		}
	}
	value := bytes.Repeat([]byte("x"), 4096)

	// record all visible keys (1 per table in the test)
	visible := make(map[string]bool)

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

	printlevels := func() {
		visible = make(map[string]bool)
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
	}

	printlevels()
	// read some keys to trigger new compactions
	rand.Shuffle(N, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	t.Logf("Touching ...")
	for i := 0; i < N; i++ {
		_, closer, err := d.Get(keys[i])
		if err == nil {
			require.NoError(t, closer.Close())
		}
	}
	printlevels()

	// check the number of files in the shared fs bucket
	// t.Logf("Shared fs metadata")
	// for i := 0; i < buckets; i++ {
	// 	lst, err := sharedfs.List(fmt.Sprintf("%d/%d", uid, i))
	// 	require.NoError(t, err)
	// 	t.Logf("  -- shared fs bucket %d has %d files: %s", i, len(lst), strings.Join(lst, " "))
	// }

	// validating visible and invisible keys
	validateboundaries := func() {
		// no compaction should have happened, take a snapshot
		snapshot := d.NewSnapshot()
		t.Logf("Validating ...")
		for key, iv := range visible {
			v, closer, err := snapshot.Get([]byte(key))
			if iv {
				if err != nil {
					t.Fatalf("  -- get: visible key %s get error (%v)", key, err)
				}
				if !bytes.Equal(v, value) {
					t.Fatalf("  -- get: visible key %s get successfully but returned wrong value", key)
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

	validateboundaries()
	require.NoError(t, d.Close())

	t.Log("Reopening ...")
	d, err = Open("", &Options{
		FS:        fs,
		SharedFS:  sharedfs,
		SharedDir: "",
	})
	require.NoError(t, err)

	printlevels()
	validateboundaries()

	require.NoError(t, d.Close())
}

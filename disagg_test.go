package pebble

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func decimalToString(num uint64, digits uint64) []byte {
	k := []byte(strconv.FormatUint(num, 10))
	zeros := digits - uint64(len(k))
	ret := append(make([]byte, zeros), k...)
	for i := uint64(0); i < zeros; i++ {
		ret[i] = byte('0')
	}
	return ret
}

// generateRandomKeys generates STRING decimal keys from 0 to num-1
func generateRandomKeys(t *testing.T, num uint64) ([][]byte, uint64) {
	t.Helper()
	var digits uint64 = 0
	tnum := num - 1
	for tnum != 0 {
		digits++
		tnum /= 10
	}

	keys := make([][]byte, num)
	for i := uint64(0); i < num; i++ {
		keys[i] = decimalToString(i, digits)
	}

	rand.Shuffle(int(num), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	return keys, digits
}

func printLevels(t *testing.T, d *DB) map[string]bool {
	t.Helper()
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
				t.Logf("    -- sst %d (physical filenum %d) is shared with virtual bound (%s %s) file bound (%s %s)\n",
					fm.FileNum, fm.PhysicalFileNum, fm.Smallest.UserKey, fm.Largest.UserKey, fm.FileSmallest.UserKey, fm.FileLargest.UserKey)
				visible[string(fm.Smallest.UserKey)] = true
				visible[string(fm.Largest.UserKey)] = true
				if d.cmp(fm.Smallest.UserKey, fm.FileSmallest.UserKey) > 0 {
					visible[string(fm.FileSmallest.UserKey)] = false
				}
				if d.cmp(fm.FileLargest.UserKey, fm.Largest.UserKey) > 0 {
					visible[string(fm.FileLargest.UserKey)] = false
				}
			} else {
				t.Logf("    -- sst %d is local with bound (%s %s)\n", fm.FileNum, fm.Smallest.UserKey, fm.Largest.UserKey)
			}
			fm = iter.Next()
		}
	}
	readState.unref()
	return visible
}

func validateBoundaries(t *testing.T, d *DB, visible *map[string]bool) {
	t.Helper()
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

func TestDBWithSharedSST(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

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
	keys, _ := generateRandomKeys(t, N)
	value := bytes.Repeat([]byte("x"), 256)

	setSharedSSTMetadata = func(meta *manifest.FileMetadata, creatorUniqueID uint32) {
		// The output sst is shared so update its boundaries
		meta.FileSmallest, meta.FileLargest = meta.Smallest.Clone(), meta.Largest.Clone()

		// assign virtual boundaries for all boundary properties
		lb, ub := meta.Smallest.Clone(), meta.Smallest.Clone()
		meta.Smallest, meta.Largest = lb, ub
		meta.SmallestPointKey, meta.LargestPointKey = lb, ub

		meta.CreatorUniqueID = creatorUniqueID + 1
		meta.PhysicalFileNum = meta.FileNum
	}

	// repeatly inserting/updating a random key
	t.Log("Inserting ...")
	for i := 0; i < N; i++ {
		if i%(N/10) == 0 && i != 0 {
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
		// The output sst is shared so update its boundaries
		meta.FileSmallest, meta.FileLargest = meta.Smallest.Clone(), meta.Largest.Clone()

		// assign virtual boundaries for all boundary properties
		lb, ub := meta.Smallest.Clone(), meta.Largest.Clone()
		meta.Smallest, meta.Largest = lb, ub
		meta.SmallestPointKey, meta.LargestPointKey = lb, ub

		meta.CreatorUniqueID = creatorUniqueID
		meta.PhysicalFileNum = meta.FileNum
	}
}

func TestIngestSharedSST(t *testing.T) {
	rand.Seed(time.Now().Unix())

	uid1 := uint32(rand.Uint32())
	uid2 := uid1 + 10

	fs1 := vfs.NewMem()
	fs2 := vfs.NewMem()
	sharedfs := vfs.NewMem()

	t.Log("Creating two Pebble instances d1 and d2")
	t.Log("\033[1;32mDone\033[0m")
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

	const N = 1000000
	keys, digits := generateRandomKeys(t, N)
	value := bytes.Repeat([]byte("x"), 256)

	// repeatly inserting/updating a random key
	t.Logf("Inserting %d keys to d1", N)
	for i := 0; i < N; i++ {
		if i%(N/10) == 0 && i != 0 {
			t.Logf("  -- set %d keys", i)
		}
		key := &keys[i]
		if err := d1.Set(*key, value, nil); err != nil {
			t.Fatalf("set key %s error %v", key, err)
		}
	}
	t.Log("\033[1;32mDone\033[0m")

	t.Logf("Printing d1 LSM")
	printLevels(t, d1)
	t.Log("\033[1;32mDone\033[0m")

	// exporting a range
	start := rand.Uint64() % (N / 2)
	end := start + (N / 2) - 1
	startkey := decimalToString(start, digits)
	endkey := decimalToString(end, digits)
	t.Logf("Exporting keys in range %s to %s (inclusive) from d1 and ingest to d2", startkey, endkey)

	var smeta []SharedSSTMeta
	cb := func(meta *manifest.FileMetadata) {
		// If no overlap, just return
		if d1.cmp(meta.Smallest.UserKey, endkey) > 0 || d1.cmp(meta.Largest.UserKey, startkey) < 0 {
			return
		}

		lk := meta.Smallest.UserKey
		if d1.cmp(meta.Smallest.UserKey, startkey) < 0 {
			lk = startkey
		}
		hk := meta.Largest.UserKey
		if d1.cmp(meta.Largest.UserKey, endkey) > 0 {
			hk = endkey
		}

		m := SharedSSTMeta{
			CreatorUniqueID: meta.CreatorUniqueID,
			PhysicalFileNum: meta.PhysicalFileNum,
			Smallest:        base.MakeInternalKey(lk, 0, InternalKeyKindMax),
			Largest:         base.MakeInternalKey(hk, 0, InternalKeyKindMax),
			FileSmallest:    meta.FileSmallest,
			FileLargest:     meta.FileLargest,
		}
		smeta = append(smeta, m)
	}

	f, err := fs1.Create("export")
	require.NoError(t, err)
	w := sstable.NewWriter(f, sstable.WriterOptions{})

	iterOpts := &IterOptions{SkipSharedFile: true, SharedFileCallback: cb}
	iter := d1.NewIter(iterOpts)
	require.NotEqual(t, nil, iter)
	for i := iter.First(); i; i = iter.Next() {
		if d1.cmp(iter.Key(), startkey) >= 0 && d1.cmp(iter.Key(), endkey) <= 0 {
			w.Set(iter.Key(), iter.Value())
		}
	}
	require.NoError(t, iter.Close())
	require.NoError(t, w.Close())

	// copy "export" from d1's fs to d2's fs
	vfs.CopyAcrossFS(fs1, "export", fs2, "export")
	fs1.Remove("export")

	require.NoError(t, d2.Ingest([]string{"export"}, nil))
	require.NoError(t, d2.Ingest(nil, smeta))
	t.Log("\033[1;32mDone\033[0m")

	t.Logf("Printing d2 LSM")
	printLevels(t, d2)
	t.Log("\033[1;32mDone\033[0m")

	t.Logf("Verifying key visibility by iterating over d2's key space")
	v := make(map[string]bool)
	for i := start; i <= end; i++ {
		k := string(decimalToString(i, digits))
		v[k] = false
	}

	iter = d2.NewIter(&IterOptions{SkipSharedFile: false})
	require.NotEqual(t, nil, iter)
	for i := iter.First(); i; i = iter.Next() {
		// first check not ingested keys (should not be included)
		k := iter.Key()
		if _, ok := v[string(k)]; ok {
			v[string(k)] = true
		} else {
			t.Fatalf("out of range key %s found in d2", k)
		}
	}
	t.Logf("  -- No out-of-range-key found in d2")
	for i := start; i <= end; i++ {
		k := string(decimalToString(i, digits))
		if !v[k] {
			t.Fatalf("in range key %s not found in d2", k)
		}
	}
	t.Logf("  -- All in-range key found in d2")
	require.NoError(t, iter.Close())
	t.Log("\033[1;32mDone\033[0m")

	// Print out the contents in the directories
	t.Logf("Printing d1 local files (.sst only):")
	list, err := fs1.List("")
	require.NoError(t, err)
	for i := range list {
		if strings.HasSuffix(list[i], ".sst") {
			t.Logf("  - %s", list[i])
		}
	}
	t.Log("\033[1;32mDone\033[0m")

	t.Logf("Printing d2 local files (.sst only):")
	list, err = fs2.List("")
	require.NoError(t, err)
	for i := range list {
		if strings.HasSuffix(list[i], ".sst") {
			t.Logf("  - %s", list[i])
		}
	}
	t.Log("\033[1;32mDone\033[0m")

	require.NoError(t, d1.Close())
	require.NoError(t, d2.Close())
}

// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func writeAndIngest(t *testing.T, mem vfs.FS, d *DB, k InternalKey, v []byte, filename string) {
	path := mem.PathJoin("ext", filename)
	f, err := mem.Create(path)
	require.NoError(t, err)
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Add(k, v))
	require.NoError(t, w.Close())
	require.NoError(t, d.Ingest([]string{path}))
}

// d.mu should be help. logLock should not be held.
func checkBackingSize(t *testing.T, d *DB) {
	d.mu.versions.logLock()
	var backingSizeSum uint64
	for _, backing := range d.mu.versions.backingState.fileBackingMap {
		backingSizeSum += backing.Size
	}
	require.Equal(t, backingSizeSum, d.mu.versions.backingState.fileBackingSize)
	d.mu.versions.logUnlock()
}

// TestLatestRefCounting sanity checks the ref counting implementation for
// FileMetadata.latestRefs, and makes sure that the zombie table implementation
// works when the version edit contains virtual sstables. It also checks that
// we're adding the physical sstable to the obsolete tables list iff the file is
// truly obsolete.
func TestLatestRefCounting(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                          mem,
		MaxManifestFileSize:         1,
		DisableAutomaticCompactions: true,
		FormatMajorVersion:          FormatVirtualSSTables,
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	err = d.Set([]byte{'a'}, []byte{'a'}, nil)
	require.NoError(t, err)
	err = d.Set([]byte{'b'}, []byte{'b'}, nil)
	require.NoError(t, err)

	err = d.Flush()
	require.NoError(t, err)

	iter := d.mu.versions.currentVersion().Levels[0].Iter()
	var f *fileMetadata = iter.First()
	require.NotNil(t, f)
	require.Equal(t, 1, int(f.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))

	// Grab some new file nums.
	d.mu.Lock()
	f1 := d.mu.versions.nextFileNum
	f2 := f1 + 1
	d.mu.versions.nextFileNum += 2
	d.mu.Unlock()

	m1 := &manifest.FileMetadata{
		FileBacking:    f.FileBacking,
		FileNum:        f1,
		CreationTime:   time.Now().Unix(),
		Size:           f.Size / 2,
		SmallestSeqNum: f.SmallestSeqNum,
		LargestSeqNum:  f.LargestSeqNum,
		Smallest:       base.MakeInternalKey([]byte{'a'}, f.Smallest.SeqNum(), InternalKeyKindSet),
		Largest:        base.MakeInternalKey([]byte{'a'}, f.Smallest.SeqNum(), InternalKeyKindSet),
		HasPointKeys:   true,
		Virtual:        true,
	}

	m2 := &manifest.FileMetadata{
		FileBacking:    f.FileBacking,
		FileNum:        f2,
		CreationTime:   time.Now().Unix(),
		Size:           f.Size - m1.Size,
		SmallestSeqNum: f.SmallestSeqNum,
		LargestSeqNum:  f.LargestSeqNum,
		Smallest:       base.MakeInternalKey([]byte{'b'}, f.Largest.SeqNum(), InternalKeyKindSet),
		Largest:        base.MakeInternalKey([]byte{'b'}, f.Largest.SeqNum(), InternalKeyKindSet),
		HasPointKeys:   true,
		Virtual:        true,
	}

	m1.LargestPointKey = m1.Largest
	m1.SmallestPointKey = m1.Smallest

	m2.LargestPointKey = m2.Largest
	m2.SmallestPointKey = m2.Smallest

	m1.ValidateVirtual(f)
	d.checkVirtualBounds(m1)
	m2.ValidateVirtual(f)
	d.checkVirtualBounds(m2)

	fileMetrics := func(ve *versionEdit) map[int]*LevelMetrics {
		metrics := newFileMetrics(ve.NewFiles)
		for de, f := range ve.DeletedFiles {
			lm := metrics[de.Level]
			if lm == nil {
				lm = &LevelMetrics{}
				metrics[de.Level] = lm
			}
			metrics[de.Level].NumFiles--
			metrics[de.Level].Size -= int64(f.Size)
		}
		return metrics
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	applyVE := func(ve *versionEdit) error {
		d.mu.versions.logLock()
		jobID := d.mu.nextJobID
		d.mu.nextJobID++

		err := d.mu.versions.logAndApply(jobID, ve, fileMetrics(ve), false, func() []compactionInfo {
			return d.getInProgressCompactionInfoLocked(nil)
		})
		d.updateReadStateLocked(nil)
		return err
	}

	// Virtualize f.
	ve := manifest.VersionEdit{}
	d1 := manifest.DeletedFileEntry{Level: 0, FileNum: f.FileNum}
	n1 := manifest.NewFileEntry{Level: 0, Meta: m1}
	n2 := manifest.NewFileEntry{Level: 0, Meta: m2}

	ve.DeletedFiles = make(map[manifest.DeletedFileEntry]*manifest.FileMetadata)
	ve.DeletedFiles[d1] = f
	ve.NewFiles = append(ve.NewFiles, n1)
	ve.NewFiles = append(ve.NewFiles, n2)
	ve.CreatedBackingTables = append(ve.CreatedBackingTables, f.FileBacking)

	require.NoError(t, applyVE(&ve))
	// 2 latestRefs from 2 virtual sstables in the latest version which refer
	// to the physical sstable.
	require.Equal(t, 2, int(m1.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 1, len(d.mu.versions.backingState.fileBackingMap))
	_, ok := d.mu.versions.backingState.fileBackingMap[f.FileBacking.DiskFileNum]
	require.True(t, ok)
	require.Equal(t, f.Size, m2.FileBacking.VirtualizedSize.Load())
	checkBackingSize(t, d)

	// Make sure that f is not present in zombie list, because it is not yet a
	// zombie.
	require.Equal(t, 0, len(d.mu.versions.zombieTables))

	// Delete the virtual sstable m1.
	ve = manifest.VersionEdit{}
	d1 = manifest.DeletedFileEntry{Level: 0, FileNum: m1.FileNum}
	ve.DeletedFiles = make(map[manifest.DeletedFileEntry]*manifest.FileMetadata)
	ve.DeletedFiles[d1] = m1
	require.NoError(t, applyVE(&ve))

	// Only one virtual sstable in the latest version, confirm that the latest
	// version ref counting is correct.
	require.Equal(t, 1, int(m2.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.zombieTables))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 1, len(d.mu.versions.backingState.fileBackingMap))
	_, ok = d.mu.versions.backingState.fileBackingMap[f.FileBacking.DiskFileNum]
	require.True(t, ok)
	require.Equal(t, m2.Size, m2.FileBacking.VirtualizedSize.Load())
	checkBackingSize(t, d)

	// Move m2 from L0 to L6 to test the move compaction case.
	ve = manifest.VersionEdit{}
	d1 = manifest.DeletedFileEntry{Level: 0, FileNum: m2.FileNum}
	n1 = manifest.NewFileEntry{Level: 6, Meta: m2}
	ve.DeletedFiles = make(map[manifest.DeletedFileEntry]*manifest.FileMetadata)
	ve.DeletedFiles[d1] = m2
	ve.NewFiles = append(ve.NewFiles, n1)
	require.NoError(t, applyVE(&ve))
	checkBackingSize(t, d)

	require.Equal(t, 1, int(m2.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.zombieTables))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 1, len(d.mu.versions.backingState.fileBackingMap))
	_, ok = d.mu.versions.backingState.fileBackingMap[f.FileBacking.DiskFileNum]
	require.True(t, ok)
	require.Equal(t, m2.Size, m2.FileBacking.VirtualizedSize.Load())

	// Delete m2 from L6.
	ve = manifest.VersionEdit{}
	d1 = manifest.DeletedFileEntry{Level: 6, FileNum: m2.FileNum}
	ve.DeletedFiles = make(map[manifest.DeletedFileEntry]*manifest.FileMetadata)
	ve.DeletedFiles[d1] = m2
	require.NoError(t, applyVE(&ve))
	checkBackingSize(t, d)

	// All virtual sstables are gone.
	require.Equal(t, 0, int(m2.LatestRefs()))
	require.Equal(t, 1, len(d.mu.versions.zombieTables))
	require.Equal(t, f.Size, d.mu.versions.zombieTables[f.FileBacking.DiskFileNum])
	require.Equal(t, 0, len(d.mu.versions.backingState.fileBackingMap))
	_, ok = d.mu.versions.backingState.fileBackingMap[f.FileBacking.DiskFileNum]
	require.False(t, ok)
	require.Equal(t, 0, int(m2.FileBacking.VirtualizedSize.Load()))
	checkBackingSize(t, d)

	// Make sure that the backing file is added to the obsolete tables list.
	require.Equal(t, 1, len(d.mu.versions.obsoleteTables))

}

// TODO(bananabrick): Convert TestLatestRefCounting and this test into a single
// datadriven test.
func TestVirtualSSTableManifestReplay(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FormatMajorVersion:          FormatVirtualSSTables,
		FS:                          mem,
		MaxManifestFileSize:         1,
		DisableAutomaticCompactions: true,
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	err = d.Set([]byte{'a'}, []byte{'a'}, nil)
	require.NoError(t, err)
	err = d.Set([]byte{'b'}, []byte{'b'}, nil)
	require.NoError(t, err)

	err = d.Flush()
	require.NoError(t, err)

	iter := d.mu.versions.currentVersion().Levels[0].Iter()
	var f *fileMetadata = iter.First()
	require.NotNil(t, f)
	require.Equal(t, 1, int(f.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))

	// Grab some new file nums.
	d.mu.Lock()
	f1 := d.mu.versions.nextFileNum
	f2 := f1 + 1
	d.mu.versions.nextFileNum += 2
	d.mu.Unlock()

	m1 := &manifest.FileMetadata{
		FileBacking:    f.FileBacking,
		FileNum:        f1,
		CreationTime:   time.Now().Unix(),
		Size:           f.Size / 2,
		SmallestSeqNum: f.SmallestSeqNum,
		LargestSeqNum:  f.LargestSeqNum,
		Smallest:       base.MakeInternalKey([]byte{'a'}, f.Smallest.SeqNum(), InternalKeyKindSet),
		Largest:        base.MakeInternalKey([]byte{'a'}, f.Smallest.SeqNum(), InternalKeyKindSet),
		HasPointKeys:   true,
		Virtual:        true,
	}

	m2 := &manifest.FileMetadata{
		FileBacking:    f.FileBacking,
		FileNum:        f2,
		CreationTime:   time.Now().Unix(),
		Size:           f.Size - m1.Size,
		SmallestSeqNum: f.SmallestSeqNum,
		LargestSeqNum:  f.LargestSeqNum,
		Smallest:       base.MakeInternalKey([]byte{'b'}, f.Largest.SeqNum(), InternalKeyKindSet),
		Largest:        base.MakeInternalKey([]byte{'b'}, f.Largest.SeqNum(), InternalKeyKindSet),
		HasPointKeys:   true,
		Virtual:        true,
	}

	m1.LargestPointKey = m1.Largest
	m1.SmallestPointKey = m1.Smallest
	m1.Stats.NumEntries = 1

	m2.LargestPointKey = m2.Largest
	m2.SmallestPointKey = m2.Smallest
	m2.Stats.NumEntries = 1

	m1.ValidateVirtual(f)
	d.checkVirtualBounds(m1)
	m2.ValidateVirtual(f)
	d.checkVirtualBounds(m2)

	fileMetrics := func(ve *versionEdit) map[int]*LevelMetrics {
		metrics := newFileMetrics(ve.NewFiles)
		for de, f := range ve.DeletedFiles {
			lm := metrics[de.Level]
			if lm == nil {
				lm = &LevelMetrics{}
				metrics[de.Level] = lm
			}
			metrics[de.Level].NumFiles--
			metrics[de.Level].Size -= int64(f.Size)
		}
		return metrics
	}

	d.mu.Lock()
	applyVE := func(ve *versionEdit) error {
		d.mu.versions.logLock()
		jobID := d.mu.nextJobID
		d.mu.nextJobID++

		err := d.mu.versions.logAndApply(jobID, ve, fileMetrics(ve), false, func() []compactionInfo {
			return d.getInProgressCompactionInfoLocked(nil)
		})
		d.updateReadStateLocked(nil)
		return err
	}

	// Virtualize f.
	ve := manifest.VersionEdit{}
	d1 := manifest.DeletedFileEntry{Level: 0, FileNum: f.FileNum}
	n1 := manifest.NewFileEntry{Level: 0, Meta: m1}
	n2 := manifest.NewFileEntry{Level: 0, Meta: m2}

	ve.DeletedFiles = make(map[manifest.DeletedFileEntry]*manifest.FileMetadata)
	ve.DeletedFiles[d1] = f
	ve.NewFiles = append(ve.NewFiles, n1)
	ve.NewFiles = append(ve.NewFiles, n2)
	ve.CreatedBackingTables = append(ve.CreatedBackingTables, f.FileBacking)

	require.NoError(t, applyVE(&ve))
	checkBackingSize(t, d)
	d.mu.Unlock()

	require.Equal(t, 2, int(m1.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 1, len(d.mu.versions.backingState.fileBackingMap))
	_, ok := d.mu.versions.backingState.fileBackingMap[f.FileBacking.DiskFileNum]
	require.True(t, ok)
	require.Equal(t, f.Size, m2.FileBacking.VirtualizedSize.Load())

	// Snapshot version edit will be written to a new manifest due to the flush.
	d.Set([]byte{'c'}, []byte{'c'}, nil)
	d.Flush()

	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)

	d.mu.Lock()
	it := d.mu.versions.currentVersion().Levels[0].Iter()
	var virtualFile *fileMetadata
	for f := it.First(); f != nil; f = it.Next() {
		if f.Virtual {
			virtualFile = f
			break
		}
	}

	require.Equal(t, 2, int(virtualFile.LatestRefs()))
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 1, len(d.mu.versions.backingState.fileBackingMap))
	_, ok = d.mu.versions.backingState.fileBackingMap[f.FileBacking.DiskFileNum]
	require.True(t, ok)
	require.Equal(t, f.Size, virtualFile.FileBacking.VirtualizedSize.Load())
	checkBackingSize(t, d)
	d.mu.Unlock()

	// Will cause the virtual sstables to be deleted, and the file backing should
	// also be removed.
	d.Compact([]byte{'a'}, []byte{'z'}, false)

	d.mu.Lock()
	virtualFile = nil
	it = d.mu.versions.currentVersion().Levels[0].Iter()
	for f := it.First(); f != nil; f = it.Next() {
		if f.Virtual {
			virtualFile = f
			break
		}
	}
	require.Nil(t, virtualFile)
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 0, len(d.mu.versions.backingState.fileBackingMap))
	checkBackingSize(t, d)
	d.mu.Unlock()

	// Close and restart to make sure that the new snapshot written during
	// compaction doesn't have the file backing.
	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)

	d.mu.Lock()
	virtualFile = nil
	it = d.mu.versions.currentVersion().Levels[0].Iter()
	for f := it.First(); f != nil; f = it.Next() {
		if f.Virtual {
			virtualFile = f
			break
		}
	}
	require.Nil(t, virtualFile)
	require.Equal(t, 0, len(d.mu.versions.obsoleteTables))
	require.Equal(t, 0, len(d.mu.versions.backingState.fileBackingMap))
	checkBackingSize(t, d)
	d.mu.Unlock()
	require.NoError(t, d.Close())
}

func TestVersionSetCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	// Multiple manifest files are created such that the latest one must have a correct snapshot
	// of the preceding state for the DB to be opened correctly and see the written data.
	// Snapshot has no files, so first edit will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	// Snapshot has no files, and manifest has an edit from the previous ingest,
	// so this second ingest will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("d"), "c")
	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)
	checkValue := func(k string, expected string) {
		v, closer, err := d.Get([]byte(k))
		require.NoError(t, err)
		require.Equal(t, expected, string(v))
		closer.Close()
	}
	checkValue("a", "b")
	checkValue("c", "d")
	require.NoError(t, d.Close())
}

func TestVersionSetSeqNums(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
	}
	d, err := Open("", opts)
	require.NoError(t, err)

	// Snapshot has no files, so first edit will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	// Snapshot has no files, and manifest has an edit from the previous ingest,
	// so this second ingest will cause manifest rotation.
	writeAndIngest(t, mem, d, base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("d"), "c")
	require.NoError(t, d.Close())
	d, err = Open("", opts)
	require.NoError(t, err)
	defer d.Close()
	d.TestOnlyWaitForCleaning()

	// Check that the manifest has the correct LastSeqNum, equalling the highest
	// observed SeqNum.
	filenames, err := mem.List("")
	require.NoError(t, err)
	var manifest vfs.File
	for _, filename := range filenames {
		fileType, _, ok := base.ParseFilename(mem, filename)
		if ok && fileType == fileTypeManifest {
			manifest, err = mem.Open(filename)
			require.NoError(t, err)
		}
	}
	require.NotNil(t, manifest)
	defer manifest.Close()
	rr := record.NewReader(manifest, 0 /* logNum */)
	lastSeqNum := uint64(0)
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var ve versionEdit
		err = ve.Decode(r)
		require.NoError(t, err)
		if ve.LastSeqNum != 0 {
			lastSeqNum = ve.LastSeqNum
		}
	}
	// 2 ingestions happened, so LastSeqNum should equal base.SeqNumStart + 1.
	require.Equal(t, uint64(11), lastSeqNum)
	// logSeqNum is always one greater than the last assigned sequence number.
	require.Equal(t, d.mu.versions.logSeqNum.Load(), lastSeqNum+1)
}

// TestCrashDuringManifestWrite_LargeKeys tests a crash mid-manifest write. It
// uses randomly-sized keys with a very high max in order to test version edits
// of variable sizes. Large version edits may be broken into multiple 'chunks'
// across multiple 32KiB blocks within the record package's encoding. There have
// previously been issues specifically decoding these multi-block version edits.
func TestCrashDuringManifestWrite_LargeKeys(t *testing.T) {
	seed := rand.Uint64()
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewSource(int64(seed)))

	// crashClone is nil until a clone of the memFS is constructed, where the
	// clone will lose 50% of the unsynced data. Each iteration constructs one
	// clone at a random time, and the DB keeps setting values until the clone
	// is created. Then a new DB is opened with the cloned memFS.
	var crashed atomic.Bool
	var memFS *vfs.MemFS
	makeFS := func(iter uint64) vfs.FS {
		memFS = vfs.NewStrictMem()
		return errorfs.Wrap(memFS, errorfs.InjectorFunc(func(op errorfs.Op, path string) error {
			if crashed.Load() || op != errorfs.OpFileWrite {
				return nil
			}
			typ, _, ok := base.ParseFilename(memFS, memFS.PathBase(path))
			if !ok || typ != base.FileTypeManifest {
				return nil
			}
			if rng.Intn(5) == 0 {
				memFS.SetIgnoreSyncs(true)
				crashed.Store(true)
			}
			return nil
		}))
	}

	opts := &Options{Logger: testLogger{t: t}}
	lel := MakeLoggingEventListener(opts.Logger)
	opts.EventListener = &lel

	k := append(append([]byte("averyl"), bytes.Repeat([]byte{'o'}, rng.Intn(100000))...), []byte("ngkey")...)
	baseLen := len(k)
	newKey := func(i int) []byte {
		return append(k[:baseLen], fmt.Sprintf("%10d", i)...)
	}

	const numIterations = 10
	var keyIndex int
	for i := 0; i < numIterations; i++ {
		func() {
			crashed.Store(false)

			opts.FS = makeFS(uint64(i))
			d, err := Open("foo", opts)
			require.NoError(t, err)
			func() {
				defer func() { require.NoError(t, d.Close()) }()
				for j := 0; !crashed.Load(); j++ {
					keyIndex++
					require.NoError(t, d.Set(newKey(keyIndex), []byte("value"), Sync))
					if j%10 == 0 {
						_, err := d.AsyncFlush()
						require.NoError(t, err)
					}
				}
			}()
			memFS.SetIgnoreSyncs(false)
			memFS.ResetToSyncedState()

			opts.FS = memFS
			d, err = Open("foo", opts)
			require.NoError(t, err)
			require.NoError(t, d.Close())
		}()
	}
}

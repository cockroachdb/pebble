// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/strparse"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func writeAndIngest(t *testing.T, mem vfs.FS, d *DB, k InternalKey, v []byte, filename string) {
	path := mem.PathJoin("ext", filename)
	f, err := mem.Create(path, vfs.WriteCategoryUnspecified)
	require.NoError(t, err)
	w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), sstable.WriterOptions{})
	require.NoError(t, w.Add(k, v, false /* forceObsolete */))
	require.NoError(t, w.Close())
	require.NoError(t, d.Ingest(context.Background(), []string{path}))
}

func TestVersionSet(t *testing.T) {
	opts := &Options{
		FS:       vfs.NewMem(),
		Comparer: base.DefaultComparer,
		Logger:   testLogger{t},
	}
	opts.EnsureDefaults()
	mu := &sync.Mutex{}
	marker, _, err := atomicfs.LocateMarker(opts.FS, "", manifestMarkerName)
	require.NoError(t, err)
	provider, err := objstorageprovider.Open(objstorageprovider.DefaultSettings(opts.FS, "" /* dirName */))
	require.NoError(t, err)
	var vs versionSet
	require.NoError(t, vs.create(
		0 /* jobID */, "" /* dirname */, provider, opts, marker,
		func() FormatMajorVersion { return FormatVirtualSSTables },
		mu,
	))
	vs.logSeqNum.Store(100)

	tableMetas := make(map[base.TableNum]*manifest.TableMetadata)
	backings := make(map[base.DiskFileNum]*manifest.TableBacking)
	blobFileIDMappings := make(map[base.BlobFileID]base.DiskFileNum)
	physicalBlobs := make(map[base.DiskFileNum]*manifest.PhysicalBlobFile)
	// When we parse VersionEdits, we get a new TableBacking each time. We need to
	// deduplicate them, since they hold a ref count.
	dedupBacking := func(b *manifest.TableBacking) *manifest.TableBacking {
		if existing, ok := backings[b.DiskFileNum]; ok {
			return existing
		}
		// The first time we see a backing, we also set a size.
		b.Size = uint64(b.DiskFileNum) * 1000
		backings[b.DiskFileNum] = b
		return b
	}

	refs := make(map[string]*manifest.Version)
	datadriven.RunTest(t, "testdata/version_set", func(t *testing.T, td *datadriven.TestData) (retVal string) {
		// createFile only exists to prevent versionSet from complaining that a
		// file that is becoming a zombie does not exist.
		createFile := func(fileNum base.DiskFileNum) {
			w, _, err := provider.Create(context.Background(), base.FileTypeTable, fileNum, objstorage.CreateOptions{})
			require.NoError(t, err)
			require.NoError(t, w.Finish())
		}
		var buf strings.Builder

		switch td.Cmd {
		case "apply":
			ve, err := manifest.ParseVersionEditDebug(td.Input)
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			for _, bm := range ve.NewBlobFiles {
				blobFileIDMappings[bm.FileID] = bm.Physical.FileNum
				physicalBlobs[bm.Physical.FileNum] = bm.Physical
			}
			for _, nf := range ve.NewTables {
				// Set a size that depends on FileNum.
				nf.Meta.Size = uint64(nf.Meta.TableNum) * 100
				nf.Meta.TableBacking = dedupBacking(nf.Meta.TableBacking)
				tableMetas[nf.Meta.TableNum] = nf.Meta
				if !nf.Meta.Virtual {
					createFile(nf.Meta.TableBacking.DiskFileNum)
				}
			}

			for de := range ve.DeletedTables {
				m := tableMetas[de.FileNum]
				if m == nil {
					td.Fatalf(t, "unknown FileNum %s", de.FileNum)
				}
				ve.DeletedTables[de] = m
			}
			for entry := range ve.DeletedBlobFiles {
				ve.DeletedBlobFiles[entry] = physicalBlobs[entry.FileNum]
			}
			for i := range ve.CreatedBackingTables {
				ve.CreatedBackingTables[i] = dedupBacking(ve.CreatedBackingTables[i])
				createFile(ve.CreatedBackingTables[i].DiskFileNum)
			}

			fileMetrics := newFileMetrics(ve.NewTables)
			for de, f := range ve.DeletedTables {
				lm := fileMetrics[de.Level]
				if lm == nil {
					lm = &LevelMetrics{}
					fileMetrics[de.Level] = lm
				}
				lm.TablesCount--
				lm.TablesSize -= int64(f.Size)
				lm.EstimatedReferencesSize -= f.EstimatedReferenceSize()
			}

			mu.Lock()
			err = vs.UpdateVersionLocked(func() (versionUpdate, error) {
				return versionUpdate{
					VE:                      ve,
					Metrics:                 fileMetrics,
					ForceManifestRotation:   rand.IntN(3) == 0,
					InProgressCompactionsFn: func() []compactionInfo { return nil },
				}, nil
			})
			mu.Unlock()
			if err != nil {
				td.Fatalf(t, "%v", err)
			}
			// Show the edit, so that we can see the fields populated by Apply. We
			// zero out the next file number because it is not deterministic (because
			// of the randomized forceRotation).
			ve.NextFileNum = 0
			fmt.Fprintf(&buf, "applied:\n%s", ve.String())

		case "protect-backing":
			n, _ := strconv.Atoi(td.CmdArgs[0].String())
			vs.latest.virtualBackings.Protect(base.DiskFileNum(n))

		case "unprotect-backing":
			n, _ := strconv.Atoi(td.CmdArgs[0].String())
			vs.latest.virtualBackings.Unprotect(base.DiskFileNum(n))

		case "ref-version":
			name := td.CmdArgs[0].String()
			refs[name] = vs.currentVersion()
			refs[name].Ref()

		case "unref-version":
			name := td.CmdArgs[0].String()
			refs[name].Unref()

		case "reopen":
			var err error
			var filename string
			marker, filename, err = atomicfs.LocateMarker(opts.FS, "", manifestMarkerName)
			if err != nil {
				td.Fatalf(t, "error locating marker: %v", err)
			}
			_, manifestNum, ok := base.ParseFilename(opts.FS, filename)
			if !ok {
				td.Fatalf(t, "invalid manifest file name %q", filename)
			}
			vs = versionSet{}
			err = vs.load(
				"", provider, opts, manifestNum, marker,
				func() FormatMajorVersion { return FormatVirtualSSTables }, mu,
			)
			if err != nil {
				td.Fatalf(t, "error loading manifest: %v", err)
			}

			// Repopulate the maps.
			tableMetas = make(map[base.TableNum]*manifest.TableMetadata)
			backings = make(map[base.DiskFileNum]*manifest.TableBacking)
			physicalBlobs = make(map[base.DiskFileNum]*manifest.PhysicalBlobFile)
			blobFileIDMappings = make(map[base.BlobFileID]base.DiskFileNum)
			v := vs.currentVersion()
			for _, l := range v.Levels {
				for f := range l.All() {
					tableMetas[f.TableNum] = f
					dedupBacking(f.TableBacking)
				}
			}

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
		}

		fmt.Fprintf(&buf, "current version:\n")
		for _, l := range strings.Split(vs.currentVersion().DebugString(), "\n") {
			if l != "" {
				fmt.Fprintf(&buf, "  %s\n", l)
			}
		}
		buf.WriteString(vs.latest.virtualBackings.String())
		printObjectBreakdown := func(kind string, zombies zombieObjects, obsolete []obsoleteFile) {
			if zombies.Count() == 0 {
				buf.WriteString(fmt.Sprintf("no zombie %s\n", kind))
			} else {
				nums := slices.Collect(maps.Keys(zombies.objs))
				slices.Sort(nums)
				buf.WriteString(fmt.Sprintf("zombie %s:", kind))
				for _, n := range nums {
					fmt.Fprintf(&buf, " %s", n)
				}
				buf.WriteString("\n")
			}
			if len(obsolete) == 0 {
				buf.WriteString(fmt.Sprintf("no obsolete %s\n", kind))
			} else {
				buf.WriteString(fmt.Sprintf("obsolete %s:", kind))
				for _, fi := range obsolete {
					fmt.Fprintf(&buf, " %s", fi.fileNum)
				}
				buf.WriteString("\n")
			}
		}
		printObjectBreakdown("tables", vs.zombieTables, vs.obsoleteTables)
		printObjectBreakdown("blob files", vs.zombieBlobs, vs.obsoleteBlobs)
		return buf.String()
	})
}

func TestVersionSetCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll("ext", 0755))

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
		Logger:              testLogger{t: t},
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
		Logger:              testLogger{t: t},
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
	var manifestFile vfs.File
	for _, filename := range filenames {
		fileType, _, ok := base.ParseFilename(mem, filename)
		if ok && fileType == base.FileTypeManifest {
			manifestFile, err = mem.Open(filename)
			require.NoError(t, err)
		}
	}
	require.NotNil(t, manifestFile)
	defer manifestFile.Close()
	rr := record.NewReader(manifestFile, 0 /* logNum */)
	var lastSeqNum base.SeqNum
	for {
		r, err := rr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		var ve manifest.VersionEdit
		err = ve.Decode(r)
		require.NoError(t, err)
		if ve.LastSeqNum != 0 {
			lastSeqNum = ve.LastSeqNum
		}
	}
	// 2 ingestions happened, so LastSeqNum should equal base.SeqNumStart + 1.
	require.Equal(t, base.SeqNum(11), lastSeqNum)
	// logSeqNum is always one greater than the last assigned sequence number.
	require.Equal(t, d.mu.versions.logSeqNum.Load(), lastSeqNum+1)
}

// TestLargeKeys is a datadriven test that exercises large keys with shared
// prefixes. These keys can be problematic, in part because they cannot be
// shortened by an index separator.
//
// See #4518.
func TestLargeKeys(t *testing.T) {
	var largeKeyComparer = func() base.Comparer {
		c := *testkeys.Comparer
		c.FormatKey = func(key []byte) fmt.Formatter {
			return formatAbbreviatedKey(key)
		}
		return c
	}()

	opts := &Options{
		Comparer:                    &largeKeyComparer,
		FormatMajorVersion:          internalFormatNewest,
		FS:                          vfs.NewMem(),
		Logger:                      testLogger{t: t},
		MemTableStopWritesThreshold: 4,
		DisableTableStats:           true,
	}
	var d *DB
	defer func() { require.NoError(t, d.Close()) }()
	datadriven.RunTest(t, "testdata/large_keys", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			d, err = runDBDefineCmd(td, opts)
			require.NoError(t, err)
			return runLSMCmd(td, d)
		case "batch-commit":
			b := d.NewBatch()
			for _, line := range crstrings.Lines(td.Input) {
				op, rest := splitAt(line, " ")
				switch op {
				case "set":
					k := parseLargeKey(rest)
					require.NoError(t, b.Set(k, []byte("value"), nil))
				case "delete":
					k := parseLargeKey(rest)
					require.NoError(t, b.Delete(k, nil))
				case "del-range":
					var firstKey string
					firstKey, rest = splitAt(rest, " ")
					start := parseLargeKey(firstKey)
					end := parseLargeKey(rest)
					require.NoError(t, b.DeleteRange(start, end, nil))
				default:
					panic(fmt.Sprintf("unknown op: %s", op))
				}
			}
			require.NoError(t, b.Commit(Sync))
			return ""
		case "flush":
			require.NoError(t, d.Flush())
			return runLSMCmd(td, d)
		case "layout":
			return runLayoutCmd(t, td, d)
		case "properties":
			return runSSTablePropertiesCmd(t, td, d)
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// formatAbbreviatedKey formats a key using the test key formatter, and then
// abbreviates sequences of repeated bytes.
type formatAbbreviatedKey []byte

// Format implements the fmt.Formatter interface.
func (p formatAbbreviatedKey) Format(s fmt.State, c rune) {
	formattedStr := fmt.Sprint(testkeys.Comparer.FormatKey(p))
	s.Write(abbreviateLargeKey(unsafe.Slice(unsafe.StringData(formattedStr), len(formattedStr))))
}

// parseLargeKey parses a key from a string, allowing for repetition of
// a byte. Examples:
//
// (a,10) -> aaaaaaaaaa
// f(o,5)bar -> fooooobar
func parseLargeKey(s string) []byte {
	var buf bytes.Buffer
	p := strparse.MakeParser("(,)", s)
	for !p.Done() {
		t := p.Next()
		if t != "(" {
			buf.WriteString(t)
			continue
		}
		repeat := p.Next()
		p.Expect(",")
		count := p.Int()
		p.Expect(")")
		for i := 0; i < count; i++ {
			buf.WriteString(repeat)
		}
	}
	return buf.Bytes()
}

// abbreviateLargeKey abbreviates a large key by replacing repeated bytes with
// a tuple of the byte and the count. Examples:
//
// aaaaaaaaaa -> (a,10)
// fooooobar -> f(o,5)bar
func abbreviateLargeKey(k []byte) []byte {
	var buf bytes.Buffer
	var duplicateCount int
	for i, b := range k {
		if i+1 < len(k) && k[i+1] == b {
			duplicateCount++
		} else {
			if duplicateCount > 0 {
				buf.WriteString(fmt.Sprintf("(%s,%d)", string(b), duplicateCount+1))
			} else {
				buf.WriteByte(b)
			}
			duplicateCount = 0
		}
	}
	return buf.Bytes()
}

func splitAt(s string, chars string) (string, string) {
	i := strings.IndexAny(s, chars)
	if i == -1 {
		return s, ""
	}
	return s[:i], s[i+1:]
}

// TestCrashDuringManifestWrite_LargeKeys tests a crash mid-manifest write. It
// uses randomly-sized keys with a very high max in order to test version edits
// of variable sizes. Large version edits may be broken into multiple 'chunks'
// across multiple 32KiB blocks within the record package's encoding. There have
// previously been issues specifically decoding these multi-block version edits.
func TestCrashDuringManifestWrite_LargeKeys(t *testing.T) {
	seed := rand.Uint64()
	t.Logf("seed: %d", seed)
	rng := rand.New(rand.NewPCG(seed, 0))

	// crashClone is nil until a clone of the memFS is constructed, where the
	// clone will lose 50% of the unsynced data. Each iteration constructs one
	// clone at a random time, and the DB keeps setting values until the clone
	// is created. Then a new DB is opened with the cloned memFS.
	var crashClone atomic.Pointer[vfs.MemFS]
	makeFS := func(iter uint64) vfs.FS {
		memFS := vfs.NewCrashableMem()
		return errorfs.Wrap(memFS, errorfs.InjectorFunc(func(op errorfs.Op) error {
			if crashClone.Load() != nil || op.Kind != errorfs.OpFileWrite {
				return nil
			}
			typ, _, ok := base.ParseFilename(memFS, memFS.PathBase(op.Path))
			if !ok || typ != base.FileTypeManifest {
				return nil
			}
			if rng.IntN(5) == 0 {
				crashClone.Store(memFS.CrashClone(vfs.CrashCloneCfg{
					UnsyncedDataPercent: 50,
					RNG:                 rand.New(rand.NewPCG(seed, iter)),
				}))
			}
			return nil
		}))
	}

	opts := &Options{Logger: testLogger{t: t}}
	lel := MakeLoggingEventListener(opts.Logger)
	opts.EventListener = &lel

	k := slices.Concat([]byte("averyl"), bytes.Repeat([]byte{'o'}, rng.IntN(100000)), []byte("ngkey"))
	baseLen := len(k)
	newKey := func(i int) []byte {
		return append(k[:baseLen], fmt.Sprintf("%10d", i)...)
	}

	const numIterations = 10
	var keyIndex int
	for i := 0; i < numIterations; i++ {
		func() {
			crashClone.Store(nil)

			opts.FS = makeFS(uint64(i))
			d, err := Open("foo", opts)
			require.NoError(t, err)
			func() {
				defer func() { require.NoError(t, d.Close()) }()
				for j := 0; crashClone.Load() == nil; j++ {
					keyIndex++
					require.NoError(t, d.Set(newKey(keyIndex), []byte("value"), Sync))
					if j%10 == 0 {
						_, err := d.AsyncFlush()
						require.NoError(t, err)
					}
				}
				require.NotNil(t, crashClone.Load())
			}()

			opts.FS = crashClone.Load()
			d, err = Open("foo", opts)
			require.NoError(t, err)
			require.NoError(t, d.Close())
		}()
	}
}

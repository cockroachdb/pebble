// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/block"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestExcise(t *testing.T) {
	var mem vfs.FS
	var d *DB
	var flushed bool
	var efos map[string]*EventuallyFileOnlySnapshot
	defer func() {
		for _, e := range efos {
			require.NoError(t, e.Close())
		}
		require.NoError(t, d.Close())
	}()
	clearFlushed := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		// Wait for any ongoing flushes to stop first, otherwise the
		// EventListener may set flushed=true due to a flush that was already in
		// progress.
		for d.mu.compact.flushing {
			d.mu.compact.cond.Wait()
		}
		flushed = false
	}

	var remoteStorage remote.Storage
	var opts *Options
	reset := func(blockSize int) {
		for _, e := range efos {
			require.NoError(t, e.Close())
		}
		if d != nil {
			require.NoError(t, d.Close())
		}
		efos = make(map[string]*EventuallyFileOnlySnapshot)

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		remoteStorage = remote.NewInMem()
		opts = &Options{
			BlockPropertyCollectors: []func() BlockPropertyCollector{
				sstable.NewTestKeysBlockPropertyCollector,
			},
			Comparer:              testkeys.Comparer,
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			EventListener: &EventListener{FlushEnd: func(info FlushInfo) {
				flushed = true
			}},
			FormatMajorVersion: FormatFlushableIngestExcises,
			Logger:             testLogger{t},
		}
		if blockSize != 0 {
			opts.Levels = append(opts.Levels, LevelOptions{BlockSize: blockSize, IndexBlockSize: 32 << 10})
		}
		opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"external-locator": remoteStorage,
		})
		opts.Experimental.CreateOnShared = remote.CreateOnSharedNone
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true
		// Set this to true to add some testing for the virtual sstable validation
		// code paths.
		opts.Experimental.ValidateOnIngest = true

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
	}
	reset(0 /* blockSize */)

	datadriven.RunTest(t, "testdata/excise", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			var blockSize int
			for i := range td.CmdArgs {
				switch td.CmdArgs[i].Key {
				case "tiny-blocks":
					blockSize = 1
				default:
					return fmt.Sprintf("unexpected arg: %s", td.CmdArgs[i].Key)
				}
			}
			reset(blockSize)
			return ""
		case "reopen":
			require.NoError(t, d.Close())
			var err error
			d, err = Open("", opts)
			require.NoError(t, err)

			return ""
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "build-remote":
			if err := runBuildRemoteCmd(td, d, remoteStorage); err != nil {
				return err.Error()
			}
			return ""
		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""
		case "block-flush":
			d.mu.Lock()
			d.mu.compact.flushing = true
			d.mu.Unlock()
			return ""
		case "allow-flush":
			d.mu.Lock()
			d.mu.compact.flushing = false
			d.mu.Unlock()
			return ""
		case "ingest":
			noWait := td.HasArg("no-wait")
			if !noWait {
				clearFlushed()
			}
			if err := runIngestCmd(td, d, mem); err != nil {
				return err.Error()
			}
			if noWait {
				return ""
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			if flushed {
				return "memtable flushed"
			}
			return ""
		case "ingest-and-excise":
			var prevFlushableIngests uint64
			noWait := td.HasArg("no-wait")
			if !noWait {
				clearFlushed()
				d.mu.Lock()
				prevFlushableIngests = d.mu.versions.metrics.Flush.AsIngestCount
				d.mu.Unlock()
			}

			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			if noWait {
				return ""
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			flushableIngests := d.mu.versions.metrics.Flush.AsIngestCount
			d.mu.Unlock()
			if flushed {
				if prevFlushableIngests < flushableIngests {
					return "flushable ingest"
				}
				return "memtable flushed"
			}

		case "ingest-external":
			if err := runIngestExternalCmd(t, td, d, remoteStorage, "external-locator"); err != nil {
				return err.Error()
			}

		case "file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			var keyRanges []KeyRange
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					return "expected two fields for file-only snapshot KeyRanges"
				}
				kr := KeyRange{Start: []byte(fields[0]), End: []byte(fields[1])}
				keyRanges = append(keyRanges, kr)
			}

			s := d.NewEventuallyFileOnlySnapshot(keyRanges)
			efos[name] = s
			return "ok"
		case "get":
			return runGetCmd(t, td, d)
		case "iter":
			opts := &IterOptions{
				KeyTypes: IterKeyTypePointsAndRanges,
			}
			var reader Reader = d
			for i := range td.CmdArgs {
				switch td.CmdArgs[i].Key {
				case "range-key-masking":
					opts.RangeKeyMasking = RangeKeyMasking{
						Suffix: []byte(td.CmdArgs[i].Vals[0]),
						Filter: func() BlockPropertyFilterMask {
							return sstable.NewTestKeysMaskingFilter()
						},
					}
				case "lower":
					if len(td.CmdArgs[i].Vals) != 1 {
						return fmt.Sprintf(
							"%s expects at most 1 value for lower", td.Cmd)
					}
					opts.LowerBound = []byte(td.CmdArgs[i].Vals[0])
				case "upper":
					if len(td.CmdArgs[i].Vals) != 1 {
						return fmt.Sprintf(
							"%s expects at most 1 value for upper", td.Cmd)
					}
					opts.UpperBound = []byte(td.CmdArgs[i].Vals[0])
				case "snapshot":
					reader = efos[td.CmdArgs[i].Vals[0]]
				default:
					return fmt.Sprintf("unexpected argument: %s", td.CmdArgs[i].Key)
				}
			}
			iter, _ := reader.NewIter(opts)
			return runIterCmd(td, iter, true)
		case "lsm":
			return runLSMCmd(td, d)
		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()

			return d.Metrics().StringForTests()
		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)
		case "excise":
			if err := runExciseCmd(td, d); err != nil {
				return err.Error()
			}
			return ""
		case "excise-dryrun":
			ve := &versionEdit{
				DeletedTables: map[deletedFileEntry]*tableMetadata{},
			}
			var exciseSpan KeyRange
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for compact command")
			}
			exciseSpan.Start = []byte(td.CmdArgs[0].Key)
			exciseSpan.End = []byte(td.CmdArgs[1].Key)

			d.mu.Lock()
			d.mu.versions.logLock()
			d.mu.Unlock()
			current := d.mu.versions.currentVersion()

			exciseBounds := exciseSpan.UserKeyBounds()
			for l, ls := range current.AllLevelsAndSublevels() {
				iter := ls.Iter()
				for m := iter.SeekGE(d.cmp, exciseSpan.Start); m != nil && d.cmp(m.Smallest().UserKey, exciseSpan.End) < 0; m = iter.Next() {
					leftTable, rightTable, err := d.exciseTable(context.Background(), exciseBounds, m, l.Level(), tightExciseBounds)
					if err != nil {
						td.Fatalf(t, "error when excising %s: %s", m.FileNum, err.Error())
					}
					applyExciseToVersionEdit(ve, m, leftTable, rightTable, l.Level())
				}
			}

			d.mu.Lock()
			d.mu.versions.logUnlock()
			d.mu.Unlock()
			return fmt.Sprintf("would excise %d files, use ingest-and-excise to excise.\n%s", len(ve.DeletedTables), ve.DebugString(base.DefaultFormatter))
		case "confirm-backing":
			// Confirms that the files have the same FileBacking.
			fileNums := make(map[base.FileNum]struct{})
			for i := range td.CmdArgs {
				fNum, err := strconv.Atoi(td.CmdArgs[i].Key)
				if err != nil {
					panic("invalid file number")
				}
				fileNums[base.FileNum(fNum)] = struct{}{}
			}
			d.mu.Lock()
			defer d.mu.Unlock()
			currVersion := d.mu.versions.currentVersion()
			var ptr *manifest.FileBacking
			for _, level := range currVersion.Levels {
				for f := range level.All() {
					if _, ok := fileNums[f.FileNum]; ok {
						if ptr == nil {
							ptr = f.FileBacking
							continue
						}
						if f.FileBacking != ptr {
							d.mu.Unlock()
							return "file backings are not the same"
						}
					}
				}
			}
			return "file backings are the same"
		case "compact":
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for compact command")
			}
			l := td.CmdArgs[0].Key
			r := td.CmdArgs[1].Key
			err := d.Compact(context.Background(), []byte(l), []byte(r), false)
			if err != nil {
				return err.Error()
			}

		default:
			td.Fatalf(t, "unknown command: %s", td.Cmd)
		}
		return ""
	})
}

func TestConcurrentExcise(t *testing.T) {
	var d, d1, d2 *DB
	var efos map[string]*EventuallyFileOnlySnapshot
	compactionErrs := make(chan error, 5)
	var compactions map[string]*blockedCompaction
	defer func() {
		for _, e := range efos {
			require.NoError(t, e.Close())
		}
		if d1 != nil {
			require.NoError(t, d1.Close())
		}
		if d2 != nil {
			require.NoError(t, d2.Close())
		}
	}()
	creatorIDCounter := uint64(1)
	replicateCounter := 1

	var wg sync.WaitGroup
	defer wg.Wait()
	var blockNextCompaction bool
	var blockedJobID int
	var blockedCompactionName string
	var blockedCompactionsMu sync.Mutex // protects the above three variables.

	reset := func() {
		wg.Wait()
		for _, e := range efos {
			require.NoError(t, e.Close())
		}
		if d1 != nil {
			require.NoError(t, d1.Close())
		}
		if d2 != nil {
			require.NoError(t, d2.Close())
		}
		efos = make(map[string]*EventuallyFileOnlySnapshot)
		compactions = make(map[string]*blockedCompaction)
		compactionErrs = make(chan error, 5)

		var el EventListener
		el.EnsureDefaults(testLogger{t: t})
		el.FlushBegin = func(info FlushInfo) {
			// Don't block flushes
		}
		el.CompactionBegin = func(info CompactionInfo) {
			if info.Reason == "move" {
				return
			}
			blockedCompactionsMu.Lock()
			defer blockedCompactionsMu.Unlock()
			if blockNextCompaction {
				blockNextCompaction = false
				blockedJobID = info.JobID
			}
		}
		el.CompactionEnd = func(info CompactionInfo) {
			if info.Err != nil {
				compactionErrs <- info.Err
			}
		}
		el.TableCreated = func(info TableCreateInfo) {
			blockedCompactionsMu.Lock()
			if info.JobID != blockedJobID {
				blockedCompactionsMu.Unlock()
				return
			}
			blockedJobID = 0
			c := compactions[blockedCompactionName]
			blockedCompactionName = ""
			blockedCompactionsMu.Unlock()
			c.startBlock <- struct{}{}
			<-c.unblock
		}

		sstorage := remote.NewInMem()
		mem1 := vfs.NewMem()
		mem2 := vfs.NewMem()
		require.NoError(t, mem1.MkdirAll("ext", 0755))
		require.NoError(t, mem2.MkdirAll("ext", 0755))
		opts1 := &Options{
			Comparer:              testkeys.Comparer,
			FS:                    mem1,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			FormatMajorVersion:    FormatVirtualSSTables,
			Logger:                testLogger{t},
		}
		lel := MakeLoggingEventListener(testLogger{t})
		tel := TeeEventListener(lel, el)
		opts1.EventListener = &tel
		opts1.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": sstorage,
		})
		opts1.Experimental.CreateOnShared = remote.CreateOnSharedAll
		opts1.Experimental.CreateOnSharedLocator = ""
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts1.DisableAutomaticCompactions = true
		opts1.Experimental.MultiLevelCompactionHeuristic = NoMultiLevel{}

		opts2 := &Options{}
		*opts2 = *opts1
		opts2.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			"": sstorage,
		})
		opts2.Experimental.CreateOnShared = remote.CreateOnSharedAll
		opts2.Experimental.CreateOnSharedLocator = ""
		opts2.FS = mem2

		var err error
		d1, err = Open("", opts1)
		require.NoError(t, err)
		require.NoError(t, d1.SetCreatorID(creatorIDCounter))
		creatorIDCounter++
		d2, err = Open("", opts2)
		require.NoError(t, err)
		require.NoError(t, d2.SetCreatorID(creatorIDCounter))
		creatorIDCounter++
		d = d1
	}
	reset()

	datadriven.RunTest(t, "testdata/concurrent_excise", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset()
			return ""
		case "switch":
			if len(td.CmdArgs) != 1 {
				return "usage: switch <1 or 2>"
			}
			switch td.CmdArgs[0].Key {
			case "1":
				d = d1
			case "2":
				d = d2
			default:
				return "usage: switch <1 or 2>"
			}
			return "ok"
		case "batch":
			b := d.NewIndexedBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""
		case "build":
			if err := runBuildCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d, d.opts.FS); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			d.mu.Unlock()
			return ""

		case "ingest-and-excise":
			d.mu.Lock()
			prevFlushableIngests := d.mu.versions.metrics.Flush.AsIngestCount
			d.mu.Unlock()
			if err := runIngestAndExciseCmd(td, d); err != nil {
				return err.Error()
			}
			// Wait for a possible flush.
			d.mu.Lock()
			for d.mu.compact.flushing {
				d.mu.compact.cond.Wait()
			}
			flushableIngests := d.mu.versions.metrics.Flush.AsIngestCount
			d.mu.Unlock()
			if prevFlushableIngests < flushableIngests {
				return "flushable ingest"
			}
			return ""

		case "replicate":
			if len(td.CmdArgs) != 4 {
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			var from, to *DB
			switch td.CmdArgs[0].Key {
			case "1":
				from = d1
			case "2":
				from = d2
			default:
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			switch td.CmdArgs[1].Key {
			case "1":
				to = d1
			case "2":
				to = d2
			default:
				return "usage: replicate <from-db-num> <to-db-num> <start-key> <end-key>"
			}
			startKey := []byte(td.CmdArgs[2].Key)
			endKey := []byte(td.CmdArgs[3].Key)

			writeOpts := d.opts.MakeWriterOptions(0 /* level */, to.TableFormat())
			sstPath := fmt.Sprintf("ext/replicate%d.sst", replicateCounter)
			f, err := to.opts.FS.Create(sstPath, vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			replicateCounter++
			w := sstable.NewRawWriter(objstorageprovider.NewFileWritable(f), writeOpts)

			var sharedSSTs []SharedSSTMeta
			err = from.ScanInternal(context.TODO(), block.CategoryUnknown, startKey, endKey,
				func(key *InternalKey, value LazyValue, _ IteratorLevel) error {
					val, _, err := value.Value(nil)
					require.NoError(t, err)
					require.NoError(t, w.Add(base.MakeInternalKey(key.UserKey, 0, key.Kind()), val, false /* forceObsolete */))
					return nil
				},
				func(start, end []byte, seqNum base.SeqNum) error {
					require.NoError(t, w.EncodeSpan(keyspan.Span{
						Start: start,
						End:   end,
						Keys:  []keyspan.Key{{Trailer: base.MakeTrailer(0, base.InternalKeyKindRangeDelete)}},
					}))
					return nil
				},
				func(start, end []byte, keys []keyspan.Key) error {
					require.NoError(t, w.EncodeSpan(keyspan.Span{
						Start:     start,
						End:       end,
						Keys:      keys,
						KeysOrder: 0,
					}))
					return nil
				},
				func(sst *SharedSSTMeta) error {
					sharedSSTs = append(sharedSSTs, *sst)
					return nil
				},
				nil,
			)
			require.NoError(t, err)
			require.NoError(t, w.Close())

			_, err = to.IngestAndExcise(context.Background(), []string{sstPath}, sharedSSTs, nil, KeyRange{Start: startKey, End: endKey})
			require.NoError(t, err)
			return fmt.Sprintf("replicated %d shared SSTs", len(sharedSSTs))

		case "get":
			return runGetCmd(t, td, d)

		case "iter":
			o := &IterOptions{KeyTypes: IterKeyTypePointsAndRanges}
			var reader Reader
			reader = d
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "mask-suffix":
					o.RangeKeyMasking.Suffix = []byte(arg.Vals[0])
				case "mask-filter":
					o.RangeKeyMasking.Filter = func() BlockPropertyFilterMask {
						return sstable.NewTestKeysMaskingFilter()
					}
				case "snapshot":
					reader = efos[arg.Vals[0]]
				}
			}
			iter, err := reader.NewIter(o)
			if err != nil {
				return err.Error()
			}
			return runIterCmd(td, iter, true)

		case "lsm":
			return runLSMCmd(td, d)

		case "metrics":
			// The asynchronous loading of table stats can change metrics, so
			// wait for all the tables' stats to be loaded.
			d.mu.Lock()
			d.waitTableStats()
			d.mu.Unlock()

			return d.Metrics().StringForTests()

		case "wait-pending-table-stats":
			return runTableStatsCmd(td, d)

		case "excise":
			ve := &versionEdit{
				DeletedTables: map[deletedFileEntry]*tableMetadata{},
			}
			var exciseSpan KeyRange
			if len(td.CmdArgs) != 2 {
				panic("insufficient args for excise command")
			}
			exciseSpan.Start = []byte(td.CmdArgs[0].Key)
			exciseSpan.End = []byte(td.CmdArgs[1].Key)

			d.mu.Lock()
			d.mu.versions.logLock()
			d.mu.Unlock()
			current := d.mu.versions.currentVersion()
			exciseBounds := exciseSpan.UserKeyBounds()
			for level := range current.Levels {
				iter := current.Levels[level].Iter()
				for m := iter.SeekGE(d.cmp, exciseSpan.Start); m != nil && d.cmp(m.Smallest().UserKey, exciseSpan.End) < 0; m = iter.Next() {
					leftTable, rightTable, err := d.exciseTable(context.Background(), exciseBounds, m, level, tightExciseBounds)
					if err != nil {
						d.mu.Lock()
						d.mu.versions.logUnlock()
						d.mu.Unlock()
						return fmt.Sprintf("error when excising %s: %s", m.FileNum, err.Error())
					}
					applyExciseToVersionEdit(ve, m, leftTable, rightTable, level)
				}
			}
			d.mu.Lock()
			d.mu.versions.logUnlock()
			d.mu.Unlock()
			return fmt.Sprintf("would excise %d files, use ingest-and-excise to excise.\n%s", len(ve.DeletedTables), ve.String())

		case "file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			var keyRanges []KeyRange
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					return "expected two fields for file-only snapshot KeyRanges"
				}
				kr := KeyRange{Start: []byte(fields[0]), End: []byte(fields[1])}
				keyRanges = append(keyRanges, kr)
			}

			s := d.NewEventuallyFileOnlySnapshot(keyRanges)
			efos[name] = s
			return "ok"

		case "wait-for-file-only-snapshot":
			if len(td.CmdArgs) != 1 {
				panic("insufficient args for file-only-snapshot command")
			}
			name := td.CmdArgs[0].Key
			err := efos[name].WaitForFileOnlySnapshot(context.TODO(), 1*time.Millisecond)
			if err != nil {
				return err.Error()
			}
			return "ok"

		case "unblock":
			name := td.CmdArgs[0].Key
			blockedCompactionsMu.Lock()
			c := compactions[name]
			delete(compactions, name)
			blockedCompactionsMu.Unlock()
			c.unblock <- struct{}{}
			return "ok"

		case "compact":
			async := false
			var otherArgs []datadriven.CmdArg
			var bc *blockedCompaction
			for i := range td.CmdArgs {
				switch td.CmdArgs[i].Key {
				case "block":
					name := td.CmdArgs[i].Vals[0]
					bc = &blockedCompaction{startBlock: make(chan struct{}), unblock: make(chan struct{})}
					blockedCompactionsMu.Lock()
					compactions[name] = bc
					blockNextCompaction = true
					blockedCompactionName = name
					blockedCompactionsMu.Unlock()
					async = true
				default:
					otherArgs = append(otherArgs, td.CmdArgs[i])
				}
			}
			var tdClone datadriven.TestData
			tdClone = *td
			tdClone.CmdArgs = otherArgs
			if !async {
				err := runCompactCmd(td, d)
				if err != nil {
					return err.Error()
				}
			} else {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = runCompactCmd(&tdClone, d)
				}()
				<-bc.startBlock
				return "spun off in separate goroutine"
			}
			return "ok"
		case "wait-for-compaction-error":
			err := <-compactionErrs
			return err.Error()
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestExciseBounds(t *testing.T) {
	const sstPath = "foo.sst"
	var fs vfs.FS
	var m *manifest.TableMetadata
	cmp := base.DefaultComparer.Compare

	datadriven.RunTest(t, "testdata/excise_bounds", func(t *testing.T, td *datadriven.TestData) string {
		checkErr := func(err error) {
			if err != nil {
				t.Helper()
				td.Fatalf(t, "%v", err)
			}
		}
		var buf strings.Builder
		printBounds := func(title string, m *tableMetadata) {
			fmt.Fprintf(&buf, "%s:\n", title)
			fmt.Fprintf(&buf, "  overall: %v - %v\n", m.Smallest(), m.Largest())
			if m.HasPointKeys {
				fmt.Fprintf(&buf, "  point:   %v - %v\n", m.SmallestPointKey, m.LargestPointKey)
			}
			if m.HasRangeKeys {
				fmt.Fprintf(&buf, "  range:   %v - %v\n", m.SmallestRangeKey, m.LargestRangeKey)
			}
		}
		switch td.Cmd {
		case "build-sst":
			fs = vfs.NewMem()
			var writerOpts sstable.WriterOptions
			writerOpts.TableFormat = sstable.TableFormat(rand.IntN(int(sstable.TableFormatMax)) + 1)
			// We need at least v2 for tests with range keys.
			writerOpts.TableFormat = max(writerOpts.TableFormat, sstable.TableFormatPebblev2)
			sstMeta, err := runBuildSSTCmd(td.Input, td.CmdArgs, sstPath, fs, withDefaultWriterOpts(writerOpts))
			checkErr(err)

			m = &manifest.TableMetadata{}
			if sstMeta.HasPointKeys {
				m.ExtendPointKeyBounds(cmp, sstMeta.SmallestPoint, sstMeta.LargestPoint)
			}
			if sstMeta.HasRangeDelKeys {
				m.ExtendPointKeyBounds(cmp, sstMeta.SmallestRangeDel, sstMeta.LargestRangeDel)
			}
			if sstMeta.HasRangeKeys {
				m.ExtendRangeKeyBounds(cmp, sstMeta.SmallestRangeKey, sstMeta.LargestRangeKey)
			}
			printBounds("Bounds", m)

		case "excise":
			f, err := fs.Open(sstPath)
			checkErr(err)
			readable, err := sstable.NewSimpleReadable(f)
			checkErr(err)
			ctx := context.Background()
			r, err := sstable.NewReader(ctx, readable, sstable.ReaderOptions{})
			checkErr(err)
			pointIter, err := r.NewPointIter(ctx, sstable.IterOptions{})
			checkErr(err)
			rangeDelIter, err := r.NewRawRangeDelIter(ctx, sstable.NoFragmentTransforms, sstable.ReadEnv{})
			checkErr(err)
			rangeKeyIter, err := r.NewRawRangeKeyIter(ctx, sstable.NoFragmentTransforms, sstable.ReadEnv{})
			checkErr(err)
			iters := iterSet{
				point:         pointIter,
				rangeDeletion: rangeDelIter,
				rangeKey:      rangeKeyIter,
			}

			exciseSpan := base.ParseUserKeyBounds(td.Input)
			if cmp(m.Smallest().UserKey, exciseSpan.Start) < 0 {
				var t manifest.TableMetadata
				checkErr(determineLeftTableBounds(cmp, m, &t, exciseSpan.Start, iters))
				printBounds("Left table bounds", &t)
				t = manifest.TableMetadata{}
				looseLeftTableBounds(cmp, m, &t, exciseSpan.Start)
				printBounds("Left table bounds (loose)", &t)
			}

			if !exciseSpan.End.IsUpperBoundForInternalKey(cmp, m.Largest()) {
				var t manifest.TableMetadata
				err := func() (err error) {
					// determineRightTableBounds can return assertion errors which we want
					// to see in the tests but which panic in invariant builds.
					defer func() {
						if p := recover(); p != nil {
							if e, ok := p.(error); ok {
								err = e
							} else {
								panic(p)
							}
						}
					}()
					return determineRightTableBounds(cmp, m, &t, exciseSpan.End, iters)
				}()
				if err != nil {
					fmt.Fprintf(&buf, "determineRightTableBounds error: %v", err)
				} else {
					printBounds("Right table bounds", &t)
				}
				if exciseSpan.End.Kind == base.Exclusive {
					t = manifest.TableMetadata{}
					looseRightTableBounds(cmp, m, &t, exciseSpan.End.Key)
					printBounds("Right table bounds (loose)", &t)
				}
			}
			checkErr(iters.CloseAll())

		default:
			td.Fatalf(t, "unknown command %q", td.Cmd)
		}

		return buf.String()
	})
}

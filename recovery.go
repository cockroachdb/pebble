// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"slices"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/internal/arenaskl"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
	"github.com/cockroachdb/pebble/wal"
)

// recoverState reads the named database directory and recovers the set of files
// encoding the database state at the moment the previous process exited.
// recoverState is read only and does not mutate the on-disk state.
func recoverState(opts *Options, dirname string) (s *recoveredState, err error) {
	rs := &recoveredState{
		dirname: dirname,
		fs:      opts.FS,
	}
	if err := rs.init(opts, dirname); err != nil {
		return nil, errors.CombineErrors(err, rs.Close())
	}
	return rs, nil
}

func (rs *recoveredState) init(opts *Options, dirname string) error {
	dirs, err := prepareOpenAndLockDirs(dirname, opts)
	if err != nil {
		err = errors.Wrapf(err, "error opening database at %q", dirname)
		err = errors.CombineErrors(err, dirs.Close())
		return err
	}
	rs.dirs = dirs

	// List the directory contents. This also happens to include WAL log files,
	// if they are in the same dir.
	if rs.ls, err = opts.FS.List(dirname); err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}
	// Find the currently format major version and active manifest.
	rs.fmv, rs.fmvMarker, err = lookupFormatMajorVersion(opts.FS, dirname, rs.ls)
	if err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}

	// Open the object storage provider.
	providerSettings := opts.MakeObjStorageProviderSettings(dirname)
	providerSettings.Local.FSDirInitialListing = rs.ls
	rs.objProvider, err = objstorageprovider.Open(providerSettings)
	if err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}

	// Determine which manifest is current, and if one exists, replay it to
	// recover the current Version of the LSM.
	var manifestExists bool
	rs.manifestMarker, rs.manifestFileNum, manifestExists, err = findCurrentManifest(opts.FS, dirname, rs.ls)
	if err != nil {
		return errors.Wrapf(err, "pebble: database %q", dirname)
	}
	if manifestExists {
		recoveredVersion, err := recoverVersion(opts, dirname, rs.objProvider, rs.manifestFileNum)
		if err != nil {
			return err
		}
		if !opts.DisableConsistencyCheck {
			if err := checkConsistency(recoveredVersion.version, rs.objProvider); err != nil {
				return err
			}
		}
		rs.recoveredVersion = recoveredVersion
	}

	// Identify the maximal file number in the directory. We do not want to
	// reuse any existing file numbers even if they are obsolete file numbers to
	// avoid modifying an ingested sstable's original external file.
	//
	// We also identify the most recent OPTIONS file, so we can validate our
	// configured Options against the previous options, and we collect any
	// orphaned temporary files that should be removed.
	var previousOptionsFileNum base.DiskFileNum
	for _, filename := range rs.ls {
		ft, fn, ok := base.ParseFilename(opts.FS, filename)
		if !ok {
			continue
		}
		rs.maxFilenumUsed = max(rs.maxFilenumUsed, fn)
		switch ft {
		case base.FileTypeLog:
			// Ignore.
		case base.FileTypeOptions:
			if previousOptionsFileNum < fn {
				previousOptionsFileNum = fn
				rs.previousOptionsFilename = filename
			}
		case base.FileTypeTemp, base.FileTypeOldTemp:
			rs.obsoleteTempFilenames = append(rs.obsoleteTempFilenames, filename)
		}
	}

	// Validate the most-recent OPTIONS file, if there is one.
	if rs.previousOptionsFilename != "" {
		path := opts.FS.PathJoin(dirname, rs.previousOptionsFilename)
		previousOptions, err := readOptionsFile(opts, path)
		if err != nil {
			return err
		}
		if err := opts.CheckCompatibility(dirname, previousOptions); err != nil {
			return err
		}
	}

	// Ratchet rs.maxFilenumUsed ahead of all known objects in the objProvider.
	// This avoids FileNum collisions with obsolete sstables.
	objects := rs.objProvider.List()
	for _, obj := range objects {
		rs.maxFilenumUsed = max(rs.maxFilenumUsed, obj.DiskFileNum)
	}

	// Find all the WAL files across the various WAL directories.
	wals, err := wal.Scan(rs.dirs.WALDirs()...)
	if err != nil {
		return err
	}
	for _, w := range wals {
		// Don't reuse any obsolete file numbers to avoid modifying an ingested
		// sstable's original external file.
		rs.maxFilenumUsed = max(rs.maxFilenumUsed, base.DiskFileNum(w.Num))
		if rs.recoveredVersion == nil || base.DiskFileNum(w.Num) >= rs.recoveredVersion.minUnflushedLogNum {
			rs.walsReplay = append(rs.walsReplay, w)
		} else {
			rs.walsObsolete = append(rs.walsObsolete, w)
		}
	}
	return nil
}

// recoveredState encapsulates state recovered from reading the database
// directory.
type recoveredState struct {
	dirname                 string
	dirs                    *resolvedDirs
	fmv                     FormatMajorVersion
	fmvMarker               *atomicfs.Marker
	fs                      vfs.FS
	ls                      []string
	manifestMarker          *atomicfs.Marker
	manifestFileNum         base.DiskFileNum
	maxFilenumUsed          base.DiskFileNum
	obsoleteTempFilenames   []string
	objProvider             objstorage.Provider
	previousOptionsFilename string
	recoveredVersion        *recoveredVersion
	walsObsolete            wal.Logs
	walsReplay              wal.Logs
}

// RemoveObsolete removes obsolete files uncovered during recovery.
func (rs *recoveredState) RemoveObsolete(opts *Options) error {
	var err error
	// Atomic markers may leave behind obsolete files if there's a crash
	// mid-update.
	if rs.fmvMarker != nil {
		err = errors.CombineErrors(err, rs.fmvMarker.RemoveObsolete())
	}
	if rs.manifestMarker != nil {
		err = errors.CombineErrors(err, rs.manifestMarker.RemoveObsolete())
	}
	// Some codepaths write to a temporary file and then rename it to its final
	// location when complete.  A temp file is leftover if a process exits
	// before the rename. Remove any that were found.
	for _, filename := range rs.obsoleteTempFilenames {
		err = errors.CombineErrors(err, rs.fs.Remove(rs.fs.PathJoin(rs.dirname, filename)))
	}
	// Remove any WAL files that are already obsolete. Pebble keeps some old WAL
	// files around for recycling.
	for _, w := range rs.walsObsolete {
		for i := range w.NumSegments() {
			fs, path := w.SegmentLocation(i)
			rmErr := fs.Remove(path)
			opts.EventListener.WALDeleted(WALDeleteInfo{
				JobID:   0,
				Path:    path,
				FileNum: base.DiskFileNum(w.Num),
				Err:     rmErr,
			})
		}
	}
	return err
}

// Close closes resources held by the RecoveredState, including open file
// descriptors.
func (rs *recoveredState) Close() error {
	var err error
	if rs.fmvMarker != nil {
		err = errors.CombineErrors(err, rs.fmvMarker.Close())
	}
	if rs.manifestMarker != nil {
		err = errors.CombineErrors(err, rs.manifestMarker.Close())
	}
	if rs.objProvider != nil {
		err = errors.CombineErrors(err, rs.objProvider.Close())
	}
	if rs.dirs != nil {
		err = errors.CombineErrors(err, rs.dirs.Close())
	}
	return err
}

// recoveredVersion describes the latest Version of the LSM recovered by
// replaying a manifest file.
type recoveredVersion struct {
	manifestFileNum    base.DiskFileNum
	minUnflushedLogNum base.DiskFileNum
	nextFileNum        base.DiskFileNum
	logSeqNum          base.SeqNum
	latest             *latestVersionState
	version            *manifest.Version
}

// recoverVersion replays the named manifest file to recover the latest version
// of the LSM from persisted state.
func recoverVersion(
	opts *Options, dirname string, provider objstorage.Provider, manifestFileNum base.DiskFileNum,
) (*recoveredVersion, error) {
	rv := &recoveredVersion{
		manifestFileNum: manifestFileNum,
		nextFileNum:     1,
		logSeqNum:       base.SeqNumStart,
		latest: &latestVersionState{
			l0Organizer:     manifest.NewL0Organizer(opts.Comparer, opts.FlushSplitBytes),
			virtualBackings: manifest.MakeVirtualBackings(),
		},
	}
	manifestPath := base.MakeFilepath(opts.FS, dirname, base.FileTypeManifest, rv.manifestFileNum)
	manifestFilename := opts.FS.PathBase(manifestPath)

	// Read the versionEdits in the manifest file.
	var bve manifest.BulkVersionEdit
	bve.AllAddedTables = make(map[base.TableNum]*manifest.TableMetadata)
	manifestFile, err := opts.FS.Open(manifestPath)
	if err != nil {
		return nil, errors.Wrapf(err, "pebble: could not open manifest file %q for DB %q",
			errors.Safe(manifestFilename), dirname)
	}
	defer manifestFile.Close()
	rr := record.NewReader(manifestFile, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return nil, errors.Wrapf(err, "pebble: error when loading manifest file %q",
				errors.Safe(manifestFilename))
		}
		var ve manifest.VersionEdit
		err = ve.Decode(r)
		if err != nil {
			// Break instead of returning an error if the record is corrupted
			// or invalid.
			if err == io.EOF || record.IsInvalidRecord(err) {
				break
			}
			return nil, err
		}
		if ve.ComparerName != "" {
			if ve.ComparerName != opts.Comparer.Name {
				return nil, errors.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					errors.Safe(manifestFilename), dirname, errors.Safe(ve.ComparerName), errors.Safe(opts.Comparer.Name))
			}
		}
		if err := bve.Accumulate(&ve); err != nil {
			return nil, err
		}
		if ve.MinUnflushedLogNum != 0 {
			rv.minUnflushedLogNum = ve.MinUnflushedLogNum
		}
		if ve.NextFileNum != 0 {
			rv.nextFileNum = base.DiskFileNum(ve.NextFileNum)
		}
		if ve.LastSeqNum != 0 {
			// logSeqNum is the _next_ sequence number that will be assigned,
			// while LastSeqNum is the last assigned sequence number. Note that
			// this behaviour mimics that in RocksDB; the first sequence number
			// assigned is one greater than the one present in the manifest
			// (assuming no WALs contain higher sequence numbers than the
			// manifest's LastSeqNum). Increment LastSeqNum by 1 to get the
			// next sequence number that will be assigned.
			//
			// If LastSeqNum is less than SeqNumStart, increase it to at least
			// SeqNumStart to leave ample room for reserved sequence numbers.
			rv.logSeqNum = max(ve.LastSeqNum+1, base.SeqNumStart)
		}
	}

	// We have already set vs.nextFileNum=1 at the beginning of the function and
	// could have only updated it to some other non-zero value, so it cannot be
	// 0 here.
	if rv.minUnflushedLogNum == 0 {
		if rv.nextFileNum >= 2 {
			// We either have a freshly created DB, or a DB created by RocksDB
			// that has not had a single flushed SSTable yet. This is because
			// RocksDB bumps up nextFileNum in this case without bumping up
			// minUnflushedLogNum, even if WALs with non-zero file numbers are
			// present in the directory.
		} else {
			return nil, base.CorruptionErrorf("pebble: malformed manifest file %q for DB %q",
				errors.Safe(manifestFilename), dirname)
		}
	}
	rv.nextFileNum = max(rv.nextFileNum, rv.minUnflushedLogNum+1)

	// Populate the virtual backings for virtual sstables since we have finished
	// version edit accumulation.
	for _, b := range bve.AddedFileBacking {
		placement := objstorage.Placement(provider, base.FileTypeTable, b.DiskFileNum)
		rv.latest.virtualBackings.AddAndRef(b, placement)
	}
	for l, addedLevel := range bve.AddedTables {
		for _, m := range addedLevel {
			if m.Virtual {
				rv.latest.virtualBackings.AddTable(m, l)
			}
		}
	}

	if invariants.Enabled {
		// There should be no deleted tables or backings, since we're starting
		// from an empty state.
		for _, deletedLevel := range bve.DeletedTables {
			if len(deletedLevel) != 0 {
				panic("deleted files after manifest replay")
			}
		}
		if len(bve.RemovedFileBacking) > 0 {
			panic("deleted backings after manifest replay")
		}
	}

	emptyVersion := manifest.NewInitialVersion(opts.Comparer)
	newVersion, err := bve.Apply(emptyVersion, opts.Experimental.ReadCompactionRate)
	if err != nil {
		return nil, err
	}
	rv.latest.l0Organizer.PerformUpdate(rv.latest.l0Organizer.PrepareUpdate(&bve, newVersion), newVersion)
	rv.latest.l0Organizer.InitCompactingFileInfo(nil /* in-progress compactions */)
	rv.latest.blobFiles.Init(&bve, manifest.BlobRewriteHeuristic{
		CurrentTime: opts.private.timeNow,
		MinimumAge:  opts.Experimental.ValueSeparationPolicy().RewriteMinimumAge,
	})
	rv.version = newVersion
	return rv, nil
}

// replayWAL replays the edits in the specified WAL. If the DB is in read
// only mode, then the WALs are replayed into memtables and not flushed. If
// the DB is not in read only mode, then the contents of the WAL are
// guaranteed to be flushed when a flush is scheduled after this method is run.
// Note that this flushing is very important for guaranteeing durability:
// the application may have had a number of pending
// fsyncs to the WAL before the process crashed, and those fsyncs may not have
// happened but the corresponding data may now be readable from the WAL (while
// sitting in write-back caches in the kernel or the storage device). By
// reading the WAL (including the non-fsynced data) and then flushing all
// these changes (flush does fsyncs), we are able to guarantee that the
// initial state of the DB is durable.
//
// This method mutates d.mu.mem.queue and possibly d.mu.mem.mutable and replays
// WALs into the flushable queue. Flushing of the queue is expected to be handled
// by callers. A list of flushable ingests (but not memtables) replayed is returned.
//
// d.mu must be held when calling this, but the mutex may be dropped and
// re-acquired during the course of this method.
func (d *DB) replayWAL(
	jobID JobID, ll wal.LogicalLog, strictWALTail bool,
) (flushableIngests []*ingestedFlushable, maxSeqNum base.SeqNum, err error) {
	rr := ll.OpenForRead()
	defer func() { _ = rr.Close() }()
	var (
		b               Batch
		buf             bytes.Buffer
		mem             *memTable
		entry           *flushableEntry
		offset          wal.Offset
		lastFlushOffset int64
		keysReplayed    int64 // number of keys replayed
		batchesReplayed int64 // number of batches replayed
	)

	// TODO(jackson): This function is interspersed with panics, in addition to
	// corruption error propagation. Audit them to ensure we're truly only
	// panicking where the error points to Pebble bug and not user or
	// hardware-induced corruption.

	// "Flushes" (ie. closes off) the current memtable, if not nil.
	flushMem := func() {
		if mem == nil {
			return
		}
		mem.writerUnref()
		if d.mu.mem.mutable == mem {
			d.mu.mem.mutable = nil
		}
		entry.flushForced = !d.opts.ReadOnly
		var logSize uint64
		mergedOffset := offset.Physical + offset.PreviousFilesBytes
		if mergedOffset >= lastFlushOffset {
			logSize = uint64(mergedOffset - lastFlushOffset)
		}
		// Else, this was the initial memtable in the read-only case which must have
		// been empty, but we need to flush it since we don't want to add to it later.
		lastFlushOffset = mergedOffset
		entry.logSize = logSize
		mem, entry = nil, nil
	}

	mem = d.mu.mem.mutable
	if mem != nil {
		entry = d.mu.mem.queue[len(d.mu.mem.queue)-1]
		if !d.opts.ReadOnly {
			flushMem()
		}
	}

	// Creates a new memtable if there is no current memtable.
	ensureMem := func(seqNum base.SeqNum) {
		if mem != nil {
			return
		}
		mem, entry = d.newMemTable(base.DiskFileNum(ll.Num), seqNum, 0 /* minSize */)
		d.mu.mem.mutable = mem
		d.mu.mem.queue = append(d.mu.mem.queue, entry)
	}

	defer func() {
		if err != nil {
			err = errors.WithDetailf(err, "replaying wal %d, offset %s", ll.Num, offset)
		}
	}()

	for {
		var r io.Reader
		var err error
		r, offset, err = rr.NextRecord()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			// It is common to encounter a zeroed or invalid chunk due to WAL
			// preallocation and WAL recycling. However zeroed or invalid chunks
			// can also be a consequence of corruption / disk rot. When the log
			// reader encounters one of these cases, it attempts to disambiguate
			// by reading ahead looking for a future record. If a future chunk
			// indicates the chunk at the original offset should've been valid, it
			// surfaces record.ErrInvalidChunk or record.ErrZeroedChunk. These
			// errors are always indicative of corruption and data loss.
			//
			// Otherwise, the reader surfaces record.ErrUnexpectedEOF indicating
			// that the WAL terminated uncleanly and ambiguously. If the WAL is
			// the most recent logical WAL, the caller passes in
			// (strictWALTail=false), indicating we should tolerate the unclean
			// ending. If the WAL is an older WAL, the caller passes in
			// (strictWALTail=true), indicating that the WAL should have been
			// closed cleanly, and we should interpret the
			// `record.ErrUnexpectedEOF` as corruption and stop recovery.
			if errors.Is(err, io.EOF) {
				break
			} else if errors.Is(err, record.ErrUnexpectedEOF) && !strictWALTail {
				break
			} else if (errors.Is(err, record.ErrUnexpectedEOF) && strictWALTail) ||
				errors.Is(err, record.ErrInvalidChunk) || errors.Is(err, record.ErrZeroedChunk) {
				// If a read-ahead returns record.ErrInvalidChunk or
				// record.ErrZeroedChunk, then there's definitively corruption.
				//
				// If strictWALTail=true, then record.ErrUnexpectedEOF should
				// also be considered corruption because the strictWALTail
				// indicates we expect a clean end to the WAL.
				//
				// Other I/O related errors should not be marked with corruption
				// and simply returned.
				err = errors.Mark(err, ErrCorruption)
			}

			return nil, 0, errors.Wrap(err, "pebble: error when replaying WAL")
		}

		if buf.Len() < batchrepr.HeaderLen {
			return nil, 0, base.CorruptionErrorf("pebble: corrupt wal %s (offset %s)",
				errors.Safe(base.DiskFileNum(ll.Num)), offset)
		}

		if d.opts.ErrorIfNotPristine {
			return nil, 0, errors.WithDetailf(ErrDBNotPristine, "location: %q", d.dirname)
		}

		// Specify Batch.db so that Batch.SetRepr will compute Batch.memTableSize
		// which is used below.
		b = Batch{}
		b.db = d
		if err := b.SetRepr(buf.Bytes()); err != nil {
			return nil, 0, err
		}
		seqNum := b.SeqNum()
		maxSeqNum = seqNum + base.SeqNum(b.Count())
		keysReplayed += int64(b.Count())
		batchesReplayed++
		{
			br := b.Reader()
			if kind, _, _, ok, err := br.Next(); err != nil {
				return nil, 0, err
			} else if ok && (kind == InternalKeyKindIngestSST || kind == InternalKeyKindIngestSSTWithBlobs || kind == InternalKeyKindExcise) {
				// We're in the flushable ingests (+ possibly excises) case.
				//
				// Ingests require an up-to-date view of the LSM to determine the target
				// level of ingested sstables, and to accurately compute excises. Instead of
				// doing an ingest in this function, we just enqueue a flushable ingest
				// in the flushables queue and run a regular flush.
				flushMem()
				// mem is nil here.
				entry, err = d.replayIngestedFlushable(&b, base.DiskFileNum(ll.Num))
				if err != nil {
					return nil, 0, err
				}
				fi := entry.flushable.(*ingestedFlushable)
				flushableIngests = append(flushableIngests, fi)
				d.mu.mem.queue = append(d.mu.mem.queue, entry)
				// A flushable ingest is always followed by a WAL rotation.
				break
			}
		}

		if b.memTableSize >= uint64(d.largeBatchThreshold) {
			flushMem()
			// Make a copy of the data slice since it is currently owned by buf and will
			// be reused in the next iteration.
			b.data = slices.Clone(b.data)
			b.flushable, err = newFlushableBatch(&b, d.opts.Comparer)
			if err != nil {
				return nil, 0, err
			}
			entry := d.newFlushableEntry(b.flushable, base.DiskFileNum(ll.Num), b.SeqNum())
			// Disable memory accounting by adding a reader ref that will never be
			// removed.
			entry.readerRefs.Add(1)
			d.mu.mem.queue = append(d.mu.mem.queue, entry)
		} else {
			ensureMem(seqNum)
			if err = mem.prepare(&b); err != nil && err != arenaskl.ErrArenaFull {
				return nil, 0, err
			}
			// We loop since DB.newMemTable() slowly grows the size of allocated memtables, so the
			// batch may not initially fit, but will eventually fit (since it is smaller than
			// largeBatchThreshold).
			for err == arenaskl.ErrArenaFull {
				flushMem()
				ensureMem(seqNum)
				err = mem.prepare(&b)
				if err != nil && err != arenaskl.ErrArenaFull {
					return nil, 0, err
				}
			}
			if err = mem.apply(&b, seqNum); err != nil {
				return nil, 0, err
			}
			mem.writerUnref()
		}
		buf.Reset()
	}

	d.opts.Logger.Infof("[JOB %d] WAL %s stopped reading at offset: %s; replayed %d keys in %d batches",
		jobID, ll.String(), offset, keysReplayed, batchesReplayed)
	if !d.opts.ReadOnly {
		flushMem()
	}

	// mem is nil here, if !ReadOnly.
	return flushableIngests, maxSeqNum, err
}

func (d *DB) replayIngestedFlushable(
	b *Batch, logNum base.DiskFileNum,
) (entry *flushableEntry, err error) {
	br := b.Reader()
	seqNum := b.SeqNum()

	fileNums := make([]base.DiskFileNum, 0, b.Count())
	// Map from table file number to blob file IDs for that table.
	tableBlobFileIDs := make(map[base.DiskFileNum][]base.BlobFileID)
	var exciseSpan KeyRange
	addFileNum := func(encodedFileNum []byte) base.DiskFileNum {
		fileNum, n := binary.Uvarint(encodedFileNum)
		if n <= 0 {
			panic("pebble: ingest sstable file num is invalid")
		}
		diskFileNum := base.DiskFileNum(fileNum)
		fileNums = append(fileNums, diskFileNum)
		return diskFileNum
	}

	for i := 0; i < int(b.Count()); i++ {
		kind, key, val, ok, err := br.Next()
		if err != nil {
			return nil, err
		}
		if kind != InternalKeyKindIngestSST && kind != InternalKeyKindIngestSSTWithBlobs && kind != InternalKeyKindExcise {
			panic("pebble: invalid batch key kind")
		}
		if !ok {
			panic("pebble: invalid batch count")
		}
		if kind == base.InternalKeyKindExcise {
			if exciseSpan.Valid() {
				panic("pebble: multiple excise spans in a single batch")
			}
			exciseSpan.Start = slices.Clone(key)
			exciseSpan.End = slices.Clone(val)
			continue
		}
		fileNum := addFileNum(key)
		if kind == InternalKeyKindIngestSSTWithBlobs {
			blobFileIDs, ok := batchrepr.DecodeBlobFileIDs(val)
			if !ok {
				panic("pebble: corrupt blob file IDs in InternalKeyKindIngestSSTWithBlobs")
			}
			tableBlobFileIDs[fileNum] = blobFileIDs
		}
	}

	if _, _, _, ok, err := br.Next(); err != nil {
		return nil, err
	} else if ok {
		panic("pebble: invalid number of entries in batch")
	}

	meta := make([]*manifest.TableMetadata, len(fileNums))
	var lastRangeKey keyspan.Span
	for i, n := range fileNums {
		readable, err := d.objProvider.OpenForReading(context.TODO(), base.FileTypeTable, n,
			objstorage.OpenOptions{MustExist: true})
		if err != nil {
			return nil, errors.Wrap(err, "pebble: error when opening flushable ingest files")
		}
		blobReadables := make([]objstorage.Readable, 0, len(tableBlobFileIDs[n]))
		blobFileNums := make([]base.DiskFileNum, 0, len(tableBlobFileIDs[n]))
		for _, blobFileID := range tableBlobFileIDs[n] {
			blobReadable, err := d.objProvider.OpenForReading(context.TODO(), base.FileTypeBlob, base.DiskFileNum(blobFileID),
				objstorage.OpenOptions{MustExist: true})
			if err != nil {
				return nil, errors.Wrap(err, "pebble: error when opening flushable ingest blob files")
			}
			blobReadables = append(blobReadables, blobReadable)
			blobFileNums = append(blobFileNums, base.DiskFileNum(blobFileID))
		}
		// NB: ingestLoad1 will close readable.
		res, err := ingestLoad1(context.TODO(), d.opts, d.FormatMajorVersion(), readable, d.cacheHandle, &d.compressionCounters, base.PhysicalTableFileNum(fileNums[i]), disableRangeKeyChecks(), blobReadables, blobFileNums)
		if err != nil {
			return nil, errors.Wrap(err, "pebble: error when loading flushable ingest files")
		}
		meta[i] = res.meta
		lastRangeKey = res.lastRangeKey
	}
	if lastRangeKey.Valid() && d.opts.Comparer.Split.HasSuffix(lastRangeKey.End) {
		return nil, errors.AssertionFailedf("pebble: last ingest sstable has suffixed range key end %s",
			d.opts.Comparer.FormatKey(lastRangeKey.End))
	}

	numFiles := len(meta)
	if exciseSpan.Valid() {
		numFiles++
	}
	if uint32(numFiles) != b.Count() {
		panic("pebble: couldn't load all files in WAL entry")
	}

	return d.newIngestedFlushableEntry(meta, seqNum, logNum, exciseSpan)
}

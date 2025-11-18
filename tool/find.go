// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/rangedel"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/wal"
	"github.com/spf13/cobra"
)

type findRef struct {
	key      base.InternalKey
	value    []byte
	fileNum  base.FileNum
	filename string
}

// findT implements the find tool.
//
// TODO(bananabrick): Add support for virtual sstables in this tool. Currently,
// the tool will work because we're parsing files from disk, so virtual sstables
// will never be added to findT.tables. The manifest could contain information
// about virtual sstables. This is fine because the manifest is only used to
// compute the findT.editRefs, and editRefs is only used if a file in
// findT.tables contains a key. Of course, the tool won't be completely
// accurate without dealing with virtual sstable case.
type findT struct {
	Root *cobra.Command

	// Configuration.
	opts      *pebble.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers

	// Flags.
	comparerName string
	fmtKey       keyFormatter
	fmtValue     valueFormatter
	verbose      bool
	blobModeLoad bool

	// Map from file num to version edit index which references the file num.
	editRefs map[base.DiskFileNum][]int
	// List of version edits.
	edits []manifest.VersionEdit
	// List of WAL files sorted by disk file num.
	logs []wal.LogicalLog
	// List of manifest files sorted by disk file num.
	manifests []fileLoc
	// List of table files sorted by disk file num.
	tables []fileLoc
	// Set of tables that contains references to the search key.
	tableRefs map[base.FileNum]bool
	// Map from file num to table metadata.
	tableMeta map[base.FileNum]*manifest.TableMetadata
	// Blob file mappings.
	blobMappings *blobFileMappings
	// List of error messages for SSTables that could not be decoded.
	errors []string
}

type fileLoc struct {
	base.DiskFileNum
	path string
}

func cmpFileLoc(a, b fileLoc) int {
	return cmp.Compare(a.DiskFileNum, b.DiskFileNum)
}

func newFind(
	opts *pebble.Options,
	comparers sstable.Comparers,
	defaultComparer string,
	mergers sstable.Mergers,
) *findT {
	f := &findT{
		opts:      opts,
		comparers: comparers,
		mergers:   mergers,
	}
	f.fmtKey.mustSet("quoted")
	f.fmtValue.mustSet("[%x]")

	f.Root = &cobra.Command{
		Use:   "find <dir> <key>",
		Short: "find references to the specified key",
		Long: `
Find references to the specified key and any range tombstones that contain the
key. This includes references to the key in WAL files and sstables, and the
provenance of the sstables (flushed, ingested, compacted).
`,
		Args: cobra.ExactArgs(2),
		Run:  f.run,
	}

	f.Root.Flags().BoolVarP(
		&f.verbose, "verbose", "v", false, "verbose output")
	f.Root.Flags().StringVar(
		&f.comparerName, "comparer", defaultComparer, "comparer name")
	f.Root.Flags().Var(
		&f.fmtKey, "key", "key formatter")
	f.Root.Flags().Var(
		&f.fmtValue, "value", "value formatter")
	f.Root.Flags().BoolVar(
		&f.blobModeLoad, "load-blobs", false, "load values from blob file when encountered")
	return f
}

func (f *findT) run(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	var key key
	if err := key.Set(args[1]); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
		return
	}

	if err := f.findFiles(stdout, stderr, args[0]); err != nil {
		fmt.Fprintf(stdout, "%s\n", err)
		return
	}
	f.readManifests(stdout)
	var err error
	f.blobMappings, err = newBlobFileMappings(stderr, f.opts.FS, args[0], f.manifests)
	if err != nil {
		fmt.Fprintf(stderr, "%s\n", err)
		return
	}
	defer func() { _ = f.blobMappings.Close() }()

	f.opts.Comparer = f.comparers[f.comparerName]
	if f.opts.Comparer == nil {
		fmt.Fprintf(stderr, "unknown comparer %q", f.comparerName)
		return
	}
	f.fmtKey.setForComparer(f.opts.Comparer.Name, f.comparers)
	f.fmtValue.setForComparer(f.opts.Comparer.Name, f.comparers)

	refs := f.search(stdout, key)
	var lastFilename string
	for i := range refs {
		r := &refs[i]
		if lastFilename != r.filename {
			lastFilename = r.filename
			fmt.Fprintf(stdout, "%s", r.filename)
			if m := f.tableMeta[r.fileNum]; m != nil {
				fmt.Fprintf(stdout, " ")
				smallest := m.Smallest()
				largest := m.Largest()
				formatKeyRange(stdout, f.fmtKey, &smallest, &largest)
			}
			fmt.Fprintf(stdout, "\n")
			if p := f.tableProvenance(r.fileNum); p != "" {
				fmt.Fprintf(stdout, "    (%s)\n", p)
			}
		}
		fmt.Fprintf(stdout, "    ")
		formatKeyValue(stdout, f.fmtKey, f.fmtValue, &r.key, r.value)
	}

	for _, errorMsg := range f.errors {
		fmt.Fprint(stdout, errorMsg)
	}
}

// Find all of the manifests, logs, and tables in the specified directory.
func (f *findT) findFiles(stdout, stderr io.Writer, dir string) error {
	f.editRefs = make(map[base.DiskFileNum][]int)
	f.logs = nil
	f.manifests = nil
	f.tables = nil
	f.tableMeta = make(map[base.FileNum]*manifest.TableMetadata)

	if _, err := f.opts.FS.Stat(dir); err != nil {
		return err
	}

	var walAccumulator wal.FileAccumulator
	walk(stderr, f.opts.FS, dir, func(path string) {
		if isLogFile, err := walAccumulator.MaybeAccumulate(f.opts.FS, path); err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", path, err)
			return
		} else if isLogFile {
			return
		}

		ft, fileNum, ok := base.ParseFilename(f.opts.FS, path)
		if !ok {
			return
		}
		fl := fileLoc{DiskFileNum: fileNum, path: path}
		switch ft {
		case base.FileTypeManifest:
			f.manifests = append(f.manifests, fl)
		case base.FileTypeTable:
			f.tables = append(f.tables, fl)
		default:
			return
		}
	})

	// TODO(jackson): Provide a means of scanning the secondary WAL directory
	// too.

	f.logs = walAccumulator.Finish()
	slices.SortFunc(f.manifests, cmpFileLoc)
	slices.SortFunc(f.tables, cmpFileLoc)

	if f.verbose {
		fmt.Fprintf(stdout, "%s\n", dir)
		fmt.Fprintf(stdout, "%5d %s\n", len(f.manifests), makePlural("manifest", int64(len(f.manifests))))
		fmt.Fprintf(stdout, "%5d %s\n", len(f.logs), makePlural("log", int64(len(f.logs))))
		fmt.Fprintf(stdout, "%5d %s\n", len(f.tables), makePlural("sstable", int64(len(f.tables))))
	}
	return nil
}

// Read the manifests and populate the editRefs map which is used to determine
// the provenance and metadata of tables.
func (f *findT) readManifests(stdout io.Writer) {
	for _, fl := range f.manifests {
		func() {
			mf, err := f.opts.FS.Open(fl.path)
			if err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
				return
			}
			defer mf.Close()

			if f.verbose {
				fmt.Fprintf(stdout, "%s\n", fl.path)
			}

			rr := record.NewReader(mf, 0 /* logNum */)
			for {
				r, err := rr.Next()
				if err != nil {
					if err != io.EOF {
						fmt.Fprintf(stdout, "%s: %s\n", fl.path, err)
					}
					break
				}

				var ve manifest.VersionEdit
				if err := ve.Decode(r); err != nil {
					fmt.Fprintf(stdout, "%s: %s\n", fl.path, err)
					break
				}
				i := len(f.edits)
				f.edits = append(f.edits, ve)

				if ve.ComparerName != "" {
					f.comparerName = ve.ComparerName
				}
				if num := ve.MinUnflushedLogNum; num != 0 {
					f.editRefs[num] = append(f.editRefs[num], i)
				}
				for df := range ve.DeletedTables {
					diskFileNum := base.PhysicalTableDiskFileNum(df.FileNum)
					f.editRefs[diskFileNum] = append(f.editRefs[diskFileNum], i)
				}
				for _, nf := range ve.NewTables {
					// The same file can be deleted and added in a single version edit
					// which indicates a "move" compaction. Only add the edit to the list
					// once.
					diskFileNum := base.PhysicalTableDiskFileNum(nf.Meta.TableNum)
					refs := f.editRefs[diskFileNum]
					if n := len(refs); n == 0 || refs[n-1] != i {
						f.editRefs[diskFileNum] = append(refs, i)
					}
					if _, ok := f.tableMeta[nf.Meta.TableNum]; !ok {
						f.tableMeta[nf.Meta.TableNum] = nf.Meta
					}
				}
			}
		}()
	}

	if f.verbose {
		fmt.Fprintf(stdout, "%5d %s\n", len(f.edits), makePlural("edit", int64(len(f.edits))))
	}
}

// Search the logs and sstables for references to the specified key.
func (f *findT) search(stdout io.Writer, key []byte) []findRef {
	refs := f.searchLogs(stdout, key, nil)
	refs = f.searchTables(stdout, key, refs)

	// For a given file (log or table) the references are already in the correct
	// order. We simply want to order the references by fileNum using a stable
	// sort.
	//
	// TODO(peter): I'm not sure if this is perfectly correct with regards to log
	// files and ingested sstables, but it is close enough and doing something
	// better is onerous. Revisit if this ever becomes problematic (e.g. if we
	// allow finding more than one key at a time).
	//
	// An example of the problem with logs and ingestion (which can only occur
	// with distinct keys). If I write key "a" to a log, I can then ingested key
	// "b" without causing "a" to be flushed. Then I can write key "c" to the
	// log. Ideally, we'd show the key "a" from the log, then the key "b" from
	// the ingested sstable, then key "c" from the log.
	slices.SortStableFunc(refs, func(a, b findRef) int {
		if v := cmp.Compare(a.fileNum, b.fileNum); v != 0 {
			return v
		}
		return cmp.Compare(a.filename, b.filename)
	})
	return refs
}

// Search the logs for references to the specified key.
func (f *findT) searchLogs(stdout io.Writer, searchKey []byte, refs []findRef) []findRef {
	cmp := f.opts.Comparer.Compare
	for _, ll := range f.logs {
		_ = func() (err error) {
			rr := ll.OpenForRead()
			if f.verbose {
				fmt.Fprintf(stdout, "%s", ll)
				defer fmt.Fprintf(stdout, "\n")
			}
			defer func() {
				switch err {
				case record.ErrZeroedChunk:
					if f.verbose {
						fmt.Fprintf(stdout, ": EOF [%s] (may be due to WAL preallocation)", err)
					}
				case record.ErrInvalidChunk:
					if f.verbose {
						fmt.Fprintf(stdout, ": EOF [%s] (may be due to WAL recycling)", err)
					}
				default:
					if err != io.EOF {
						if f.verbose {
							fmt.Fprintf(stdout, ": %s", err)
						} else {
							fmt.Fprintf(stdout, "%s: %s\n", ll, err)
						}
					}
				}
			}()

			var b pebble.Batch
			var buf bytes.Buffer
			for {
				r, off, err := rr.NextRecord()
				if err == nil {
					buf.Reset()
					_, err = io.Copy(&buf, r)
				}
				if err != nil {
					return err
				}

				b = pebble.Batch{}
				if err := b.SetRepr(buf.Bytes()); err != nil {
					fmt.Fprintf(stdout, "%s: corrupt log file: %v", ll, err)
					continue
				}
				seqNum := b.SeqNum()
				for r := b.Reader(); ; seqNum++ {
					kind, ukey, value, ok, err := r.Next()
					if !ok {
						if err != nil {
							fmt.Fprintf(stdout, "%s: corrupt log file: %v", ll, err)
							break
						}
						break
					}
					ikey := base.MakeInternalKey(ukey, seqNum, kind)
					switch kind {
					case base.InternalKeyKindDelete,
						base.InternalKeyKindDeleteSized,
						base.InternalKeyKindSet,
						base.InternalKeyKindMerge,
						base.InternalKeyKindSingleDelete,
						base.InternalKeyKindSetWithDelete:
						if cmp(searchKey, ikey.UserKey) != 0 {
							continue
						}
					case base.InternalKeyKindRangeDelete:
						// Output tombstones that contain or end with the search key.
						t := rangedel.Decode(ikey, value, nil)
						if !t.Contains(cmp, searchKey) && cmp(t.End, searchKey) != 0 {
							continue
						}
					default:
						continue
					}

					refs = append(refs, findRef{
						key:      ikey.Clone(),
						value:    append([]byte(nil), value...),
						fileNum:  base.FileNum(ll.Num),
						filename: filepath.Base(off.PhysicalFile),
					})
				}
			}
		}()
	}
	return refs
}

// Search the tables for references to the specified key.
func (f *findT) searchTables(stdout io.Writer, searchKey []byte, refs []findRef) []findRef {
	cache := pebble.NewCache(128 << 20 /* 128 MB */)
	defer cache.Unref()
	ch := cache.NewHandle()
	defer ch.Close()

	f.tableRefs = make(map[base.FileNum]bool)
	for _, fl := range f.tables {
		_ = func() (err error) {
			tf, err := f.opts.FS.Open(fl.path)
			if err != nil {
				fmt.Fprintf(stdout, "%s\n", err)
				return
			}

			m := f.tableMeta[base.PhysicalTableFileNum(fl.DiskFileNum)]
			if f.verbose {
				fmt.Fprintf(stdout, "%s", fl.path)
				if m != nil && m.SmallestSeqNum == m.LargestSeqNum {
					fmt.Fprintf(stdout, ": global seqnum: %d", m.LargestSeqNum)
				}
				defer fmt.Fprintf(stdout, "\n")
			}
			defer func() {
				switch {
				case err != nil:
					if f.verbose {
						fmt.Fprintf(stdout, ": %v", err)
					} else {
						fmt.Fprintf(stdout, "%s: %v\n", fl.path, err)
					}
				}
			}()

			opts := f.opts.MakeReaderOptions()
			opts.Comparers = f.comparers
			opts.Mergers = f.mergers
			opts.CacheOpts = sstableinternal.CacheOptions{
				CacheHandle: ch,
				FileNum:     fl.DiskFileNum,
			}
			readable, err := objstorage.NewSimpleReadable(tf)
			if err != nil {
				return err
			}
			r, err := sstable.NewReader(context.Background(), readable, opts)
			if err != nil {
				err = errors.CombineErrors(err, readable.Close())
				f.errors = append(f.errors, fmt.Sprintf("Unable to decode sstable %s, %s", fl.path, err.Error()))
				// Ensure the error only gets printed once.
				err = nil
				return
			}
			defer func() { _ = r.Close() }()

			var transforms sstable.IterTransforms
			var fragTransforms sstable.FragmentIterTransforms
			if m != nil {
				transforms = m.IterTransforms()
				fragTransforms = m.FragmentIterTransforms()
			}

			blobContext := sstable.DebugHandlesBlobContext
			if f.blobModeLoad {
				blobContext = f.blobMappings.LoadValueBlobContext(base.PhysicalTableFileNum(fl.DiskFileNum))
			}
			iter, err := r.NewIter(transforms, nil, nil, blobContext)
			if err != nil {
				return err
			}
			defer func() { _ = iter.Close() }()
			kv := iter.SeekGE(searchKey, base.SeekGEFlagsNone)

			// We configured sstable.Reader to return raw tombstones which requires a
			// bit more work here to put them in a form that can be iterated in
			// parallel with the point records.
			rangeDelIter, err := func() (keyspan.FragmentIterator, error) {
				iter, err := r.NewRawRangeDelIter(context.Background(), fragTransforms, sstable.NoReadEnv)
				if err != nil {
					return nil, err
				}
				if iter == nil {
					return keyspan.NewIter(r.Comparer.Compare, nil), nil
				}
				defer iter.Close()

				var tombstones []keyspan.Span
				t, err := iter.First()
				for ; t != nil; t, err = iter.Next() {
					if !t.Contains(r.Comparer.Compare, searchKey) {
						continue
					}
					tombstones = append(tombstones, t.Clone())
				}
				if err != nil {
					return nil, err
				}

				slices.SortFunc(tombstones, func(a, b keyspan.Span) int {
					return r.Comparer.Compare(a.Start, b.Start)
				})
				return keyspan.NewIter(r.Comparer.Compare, tombstones), nil
			}()
			if err != nil {
				return err
			}

			defer rangeDelIter.Close()
			rangeDel, err := rangeDelIter.First()
			if err != nil {
				return err
			}

			foundRef := false
			for kv != nil || rangeDel != nil {
				if kv != nil &&
					(rangeDel == nil || r.Comparer.Compare(kv.K.UserKey, rangeDel.Start) < 0) {
					if r.Comparer.Compare(searchKey, kv.K.UserKey) != 0 {
						kv = nil
						continue
					}
					v, _, err := kv.Value(nil)
					if err != nil {
						return err
					}
					refs = append(refs, findRef{
						key:      kv.K.Clone(),
						value:    slices.Clone(v),
						fileNum:  base.PhysicalTableFileNum(fl.DiskFileNum),
						filename: filepath.Base(fl.path),
					})
					kv = iter.Next()
				} else {
					// Use rangedel.Encode to add a reference for each key
					// within the span.
					err := rangedel.Encode(*rangeDel, func(k base.InternalKey, v []byte) error {
						refs = append(refs, findRef{
							key:      k.Clone(),
							value:    slices.Clone(v),
							fileNum:  base.PhysicalTableFileNum(fl.DiskFileNum),
							filename: filepath.Base(fl.path),
						})
						return nil
					})
					if err != nil {
						return err
					}
					rangeDel, err = rangeDelIter.Next()
					if err != nil {
						return err
					}
				}
				foundRef = true
			}

			if foundRef {
				f.tableRefs[base.PhysicalTableFileNum(fl.DiskFileNum)] = true
			}
			return nil
		}()
	}
	return refs
}

// Determine the provenance of the specified table. We search the version edits
// for the first edit which created the table, and then analyze the edit to
// determine if it was a compaction, flush, or ingestion. Returns an empty
// string if the provenance of a table cannot be determined.
func (f *findT) tableProvenance(fileNum base.FileNum) string {
	editRefs := f.editRefs[base.PhysicalTableDiskFileNum(fileNum)]
	for len(editRefs) > 0 {
		ve := f.edits[editRefs[0]]
		editRefs = editRefs[1:]
		for _, nf := range ve.NewTables {
			if fileNum != nf.Meta.TableNum {
				continue
			}

			var buf bytes.Buffer
			switch {
			case len(ve.DeletedTables) > 0:
				// A version edit with deleted files is a compaction. The deleted
				// files are the inputs to the compaction. We're going to
				// reconstruct the input files and display those inputs that
				// contain the search key (i.e. are list in refs) and use an
				// ellipsis to indicate when there were other inputs that have
				// been elided.
				var sourceLevels []int
				levels := make(map[int][]base.FileNum)
				for df := range ve.DeletedTables {
					files := levels[df.Level]
					if len(files) == 0 {
						sourceLevels = append(sourceLevels, df.Level)
					}
					levels[df.Level] = append(files, df.FileNum)
				}

				sort.Ints(sourceLevels)
				if sourceLevels[len(sourceLevels)-1] != nf.Level {
					sourceLevels = append(sourceLevels, nf.Level)
				}

				sep := " "
				fmt.Fprintf(&buf, "compacted")
				for _, level := range sourceLevels {
					files := levels[level]
					slices.Sort(files)

					fmt.Fprintf(&buf, "%sL%d [", sep, level)
					sep = ""
					elided := false
					for _, fileNum := range files {
						if f.tableRefs[fileNum] {
							fmt.Fprintf(&buf, "%s%s", sep, fileNum)
							sep = " "
						} else {
							elided = true
						}
					}
					if elided {
						fmt.Fprintf(&buf, "%s...", sep)
					}
					fmt.Fprintf(&buf, "]")
					sep = " + "
				}

			case ve.MinUnflushedLogNum != 0:
				// A version edit with a min-unflushed log indicates a flush
				// operation.
				fmt.Fprintf(&buf, "flushed to L%d", nf.Level)

			case nf.Meta.SmallestSeqNum == nf.Meta.LargestSeqNum:
				// If the smallest and largest seqnum are the same, the file was
				// ingested. Note that this can also occur for a flushed sstable
				// that contains only a single key, though that would have
				// already been captured above.
				fmt.Fprintf(&buf, "ingested to L%d", nf.Level)

			default:
				// The provenance of the table is unclear. This is usually due to
				// the MANIFEST rolling over and taking a snapshot of the LSM
				// state.
				fmt.Fprintf(&buf, "added to L%d", nf.Level)
			}

			// After a table is created, it can be moved to a different level via a
			// move compaction. This is indicated by a version edit that deletes the
			// table from one level and adds the same table to a different
			// level. Loop over the remaining version edits for the table looking for
			// such moves.
			for len(editRefs) > 0 {
				ve := f.edits[editRefs[0]]
				editRefs = editRefs[1:]
				for _, nf := range ve.NewTables {
					if fileNum == nf.Meta.TableNum {
						for df := range ve.DeletedTables {
							if fileNum == df.FileNum {
								fmt.Fprintf(&buf, ", moved to L%d", nf.Level)
								break
							}
						}
						break
					}
				}
			}

			return buf.String()
		}
	}
	return ""
}

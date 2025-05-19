package pebble

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// Simple sanity tests for the flushable interface implementation for ingested
// sstables.
func TestIngestedSSTFlushableAPI(t *testing.T) {
	var mem vfs.FS
	var d *DB
	defer func() {
		require.NoError(t, d.Close())
	}()
	var flushable flushable

	reset := func() {
		if d != nil {
			require.NoError(t, d.Close())
		}

		mem = vfs.NewMem()
		require.NoError(t, mem.MkdirAll("ext", 0755))
		opts := &Options{
			FS:                    mem,
			L0CompactionThreshold: 100,
			L0StopWritesThreshold: 100,
			DebugCheck:            DebugCheckLevels,
			FormatMajorVersion:    internalFormatNewest,
		}
		// Disable automatic compactions because otherwise we'll race with
		// delete-only compactions triggered by ingesting range tombstones.
		opts.DisableAutomaticCompactions = true

		var err error
		d, err = Open("", opts)
		require.NoError(t, err)
		flushable = nil
	}
	reset()

	loadFileMeta := func(paths []string, exciseSpan KeyRange, seqNum base.SeqNum) []*tableMetadata {
		pendingOutputs := make([]base.FileNum, len(paths))
		for i := range paths {
			pendingOutputs[i] = d.mu.versions.getNextFileNum()
		}
		jobID := d.newJobID()

		// We can reuse the ingestLoad function for this test even if we're
		// not actually ingesting a file.
		lr, err := ingestLoad(context.Background(), d.opts, d.FormatMajorVersion(), paths, nil, nil, d.cacheHandle, pendingOutputs)
		if err != nil {
			t.Fatal(err)
		}
		meta := make([]*tableMetadata, len(lr.local))
		if exciseSpan.Valid() {
			seqNum++
		}
		for i := range meta {
			meta[i] = lr.local[i].tableMetadata
			if err := setSeqNumInMetadata(meta[i], seqNum+base.SeqNum(i), d.cmp, d.opts.Comparer.FormatKey); err != nil {
				t.Fatal(err)
			}
		}
		if len(meta) == 0 {
			// All of the sstables to be ingested were empty. Nothing to do.
			panic("empty sstable")
		}
		// The file cache requires the *fileMetadata to have a positive
		// reference count. Fake a reference before we try to load the file.
		for _, f := range meta {
			f.TableBacking.Ref()
		}

		// Verify the sstables do not overlap.
		if err := ingestSortAndVerify(d.cmp, lr, exciseSpan); err != nil {
			t.Fatal(err)
		}

		// Hard link the sstables into the DB directory. Since the sstables aren't
		// referenced by a version, they won't be used. If the hard linking fails
		// (e.g. because the files reside on a different filesystem), ingestLink will
		// fall back to copying, and if that fails we undo our work and return an
		// error.
		if err := ingestLinkLocal(context.Background(), jobID, d.opts, d.objProvider, lr.local); err != nil {
			t.Fatal(err)
		}

		// Fsync the directory we added the tables to. We need to do this at some
		// point before we update the MANIFEST (via UpdateVersionLocked), otherwise
		// a crash can have the tables referenced in the MANIFEST, but not present
		// in the directory.
		if err := d.dataDir.Sync(); err != nil {
			t.Fatal(err)
		}

		return meta
	}

	var seqNum uint64
	datadriven.RunTest(t, "testdata/ingested_flushable_api", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			reset()
			return ""
		case "build":
			if err := runBuildCmd(td, d, mem); err != nil {
				return err.Error()
			}
			return ""
		case "flushable":
			// Creates an ingestedFlushable over the input files.
			paths := make([]string, 0, len(td.CmdArgs))
			var exciseSpan KeyRange
			startSeqNum := base.SeqNum(seqNum)
			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "excise":
					parts := strings.Split(arg.Vals[0], "-")
					if len(parts) != 2 {
						return fmt.Sprintf("invalid excise range: %s", arg.Vals[0])
					}
					exciseSpan.Start = []byte(parts[0])
					exciseSpan.End = []byte(parts[1])
					seqNum++
				default:
					paths = append(paths, arg.String())
					seqNum++
				}
			}

			meta := loadFileMeta(paths, exciseSpan, startSeqNum)
			flushable = newIngestedFlushable(meta, d.opts.Comparer, d.newIters, d.tableNewRangeKeyIter, exciseSpan, base.SeqNum(startSeqNum))
			return ""
		case "iter":
			iter := flushable.newIter(nil)
			var buf bytes.Buffer
			for x := iter.First(); x != nil; x = iter.Next() {
				buf.WriteString(x.K.String())
				buf.WriteString("\n")
			}
			iter.Close()
			return buf.String()
		case "rangekeyIter":
			iter := flushable.newRangeKeyIter(nil)
			var buf bytes.Buffer
			if iter != nil {
				span, err := iter.First()
				for ; span != nil; span, err = iter.Next() {
					buf.WriteString(span.String())
					buf.WriteString("\n")
				}
				iter.Close()
				if err != nil {
					fmt.Fprintf(&buf, "err=%q", err.Error())
				}
			}
			return buf.String()
		case "rangedelIter":
			iter := flushable.newRangeDelIter(nil)
			var buf bytes.Buffer
			if iter != nil {
				span, err := iter.First()
				for ; span != nil; span, err = iter.Next() {
					buf.WriteString(span.String())
					buf.WriteString("\n")
				}
				iter.Close()
				if err != nil {
					fmt.Fprintf(&buf, "err=%q", err.Error())
				}
			}
			return buf.String()
		case "readyForFlush":
			if flushable.readyForFlush() {
				return "true"
			}
			return "false"
		case "containsRangeKey":
			if flushable.containsRangeKeys() {
				return "true"
			}
			return "false"
		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

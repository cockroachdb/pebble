// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCompactionDeleteOnlyHints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	parseUint64 := func(s string) uint64 {
		v, err := strconv.ParseUint(s, 10, 64)
		require.NoError(t, err)
		return v
	}
	var d *DB
	defer func() {
		if d != nil {
			require.NoError(t, closeAllSnapshots(d))
			require.NoError(t, d.Close())
		}
	}()

	var compactInfo []CompactionInfo
	reset := func() (*Options, error) {
		if d != nil {
			if err := closeAllSnapshots(d); err != nil {
				return nil, err
			}
			if err := d.Close(); err != nil {
				return nil, err
			}
		}
		compactInfo = nil
		el := TeeEventListener(
			EventListener{
				CompactionEnd: func(info CompactionInfo) {
					compactInfo = append(compactInfo, info)
				},
			},
			MakeLoggingEventListener(testutils.Logger{T: t}),
		)
		opts := &Options{
			FS:         vfs.NewMem(),
			DebugCheck: DebugCheckLevels,
			// Collection of table stats can trigger compactions. As we want full
			// control over when compactions are run, disable stats by default.
			DisableTableStats: true,
			// EventListener: &EventListener{
			//	CompactionEnd: func(info CompactionInfo) {
			//		if compactInfo != nil {
			//			return
			//		}
			//		compactInfo = &info
			//	},
			// },
			EventListener:      &el,
			FormatMajorVersion: internalFormatNewest,
			Logger:             testutils.Logger{T: t},
		}
		opts.WithFSDefaults()
		opts.Experimental.EnableDeleteOnlyCompactionExcises = func() bool { return true }
		return opts, nil
	}

	compactInfo = nil
	compactionString := func() string {
		for d.mu.compact.compactProcesses > 0 {
			d.mu.compact.cond.Wait()
		}
		slices.SortFunc(compactInfo, func(a, b CompactionInfo) int {
			return cmp.Compare(a.String(), b.String())
		})

		var b strings.Builder
		if len(compactInfo) == 0 {
			return "(none)\n"
		}

		for _, c := range compactInfo {
			// Fix the job ID and durations for determinism.
			c.JobID = 100
			c.Duration = time.Second
			c.TotalDuration = 2 * time.Second
			b.WriteString(fmt.Sprintf("%s\n", c.String()))
		}
		compactInfo = nil
		return b.String()
	}

	var err error
	var opts *Options
	datadriven.RunTest(t, "testdata/compaction_delete_only_hints",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "batch":
				b := d.NewBatch()
				err := runBatchDefineCmd(td, b)
				if err != nil {
					return err.Error()
				}
				if err = b.Commit(Sync); err != nil {
					return err.Error()
				}
				return ""

			case "define":
				opts, err = reset()
				if err != nil {
					return err.Error()
				}
				d, err = runDBDefineCmd(td, opts)
				if err != nil {
					return err.Error()
				}
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "flush":
				if err := d.Flush(); err != nil {
					return err.Error()
				}
				return runLSMCmd(td, d)

			case "force-set-hints":
				d.mu.Lock()
				defer d.mu.Unlock()
				d.mu.compact.deletionHints = d.mu.compact.deletionHints[:0]
				var buf bytes.Buffer
				for data := range crstrings.LinesSeq(td.Input) {
					parts := strings.FieldsFunc(strings.TrimSpace(data),
						func(r rune) bool { return r == '-' || r == ' ' || r == '.' })

					start, end := []byte(parts[2]), []byte(parts[3])

					var tombstoneFile *manifest.TableMetadata
					tombstoneLevel := int(parseUint64(parts[0][1:]))

					// Set file number to the value provided in the input.
					tombstoneFile = &manifest.TableMetadata{
						TableNum: base.TableNum(parseUint64(parts[1])),
					}

					var keyType manifest.KeyType
					switch typ := parts[6]; typ {
					case "point_key_only":
						keyType = manifest.KeyTypePoint
					case "range_key_only":
						keyType = manifest.KeyTypeRange
					case "point_and_range_key":
						keyType = manifest.KeyTypePointAndRange
					default:
						return fmt.Sprintf("unknown hint type: %s", typ)
					}

					h := deleteCompactionHint{
						keyType:        keyType,
						bounds:         base.UserKeyBoundsEndExclusive(start, end),
						tombstoneLevel: tombstoneLevel,
						tombstoneFile:  tombstoneFile,
						tombstoneSeqNums: base.SeqNumRange{
							Low:  base.SeqNum(parseUint64(parts[4])),
							High: base.SeqNum(parseUint64(parts[5])),
						},
					}
					d.mu.compact.deletionHints = append(d.mu.compact.deletionHints, h)
					fmt.Fprintln(&buf, h.String())
				}
				return buf.String()

			case "get-hints":
				d.mu.Lock()
				defer d.mu.Unlock()

				// Force collection of table stats. This requires re-enabling the
				// collection flag. We also do not want compactions to run as part of
				// the stats collection job, so we disable it temporarily.
				d.opts.DisableTableStats = false
				d.opts.DisableAutomaticCompactions = true
				defer func() {
					d.opts.DisableTableStats = true
					d.opts.DisableAutomaticCompactions = false
				}()

				// NB: collectTableStats attempts to acquire the lock. Temporarily
				// unlock here to avoid a deadlock.
				func() {
					d.mu.Unlock()
					defer d.mu.Lock()
					if didRun := d.collectTableStats(); !didRun {
						// If a job was already running, wait for the results.
						d.waitTableStats()
					}
				}()

				hints := d.mu.compact.deletionHints
				if len(hints) == 0 {
					return "(none)"
				}
				var buf bytes.Buffer
				for _, h := range hints {
					buf.WriteString(h.String() + "\n")
				}
				return buf.String()

			case "maybe-compact":
				d.mu.Lock()
				d.maybeScheduleCompaction()

				var buf bytes.Buffer
				fmt.Fprintf(&buf, "Deletion hints:\n")
				for _, h := range d.mu.compact.deletionHints {
					fmt.Fprintf(&buf, "  %s\n", h.String())
				}
				if len(d.mu.compact.deletionHints) == 0 {
					fmt.Fprintf(&buf, "  (none)\n")
				}
				fmt.Fprintf(&buf, "Compactions:\n")
				fmt.Fprintf(&buf, "  %s", compactionString())
				d.mu.Unlock()
				return buf.String()

			case "compact":
				if err := runCompactCmd(t, td, d); err != nil {
					return err.Error()
				}
				d.mu.Lock()
				compactInfo = nil
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			case "close-snapshot":
				seqNum := base.ParseSeqNum(strings.TrimSpace(td.Input))
				d.mu.Lock()
				var s *Snapshot
				l := &d.mu.snapshots
				for i := l.root.next; i != &l.root; i = i.next {
					if i.seqNum == seqNum {
						s = i
					}
				}
				d.mu.Unlock()
				if s == nil {
					return "(not found)"
				} else if err := s.Close(); err != nil {
					return err.Error()
				}

				d.mu.Lock()
				// Closing the snapshot may have triggered a compaction.
				str := compactionString()
				d.mu.Unlock()
				return str

			case "iter":
				snap := Snapshot{
					db:     d,
					seqNum: base.SeqNumMax,
				}
				iter, _ := snap.NewIter(nil)
				return runIterCmd(td, iter, true)

			case "reset":
				opts, err = reset()
				if err != nil {
					return err.Error()
				}
				d, err = Open("", opts)
				if err != nil {
					return err.Error()
				}
				return ""

			case "snapshot":
				// NB: It's okay that we're letting the snapshot out of scope
				// without closing it; the close snapshot command will pull the
				// relevant seqnum off the DB to close it.
				s := d.NewSnapshot()
				return fmt.Sprintf("snapshot seqnum: %d", s.seqNum)

			case "ingest":
				if err = runBuildCmd(td, d, d.opts.FS); err != nil {
					return err.Error()
				}
				if err = runIngestCmd(td, d); err != nil {
					return err.Error()
				}
				return "OK"

			case "describe-lsm":
				d.mu.Lock()
				s := d.mu.versions.currentVersion().String()
				d.mu.Unlock()
				return s

			default:
				return fmt.Sprintf("unknown command: %s", td.Cmd)
			}
		})
}

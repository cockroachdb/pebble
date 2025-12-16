// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestCompactionDeleteOnlyHints(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
			EventListener:              &el,
			FormatMajorVersion:         internalFormatNewest,
			Logger:                     testutils.Logger{T: t},
			CompactionConcurrencyRange: func() (lower, upper int) { return 1, 1 },
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
		if len(compactInfo) == 0 {
			return "(none)\n"
		}
		var lines []string
		for _, c := range compactInfo {
			// Fix the job ID, durations and file numbers for determinism.
			c.JobID = 100
			c.Duration = time.Second
			c.TotalDuration = 2 * time.Second
			for i := range c.Input {
				for j := range c.Input[i].Tables {
					c.Input[i].Tables[j].FileNum = 0
				}
			}
			for i := range c.Output.Tables {
				c.Output.Tables[i].FileNum = 0
			}
			lines = append(lines, c.String())
		}
		compactInfo = nil
		slices.Sort(lines)
		return strings.Join(lines, "\n")
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
				return d.mu.compact.wideTombstones.String()

			case "maybe-compact":
				d.mu.Lock()
				d.maybeScheduleCompaction()

				var buf bytes.Buffer
				fmt.Fprint(&buf, strings.TrimSpace(d.mu.compact.wideTombstones.String()))
				fmt.Fprintln(&buf)
				fmt.Fprintln(&buf, "Compactions:")
				fmt.Fprint(&buf, compactionString())
				d.mu.Unlock()
				return buf.String()

			case "compact":
				if err := runCompactCmd(t, td, d); err != nil {
					return err.Error()
				}
				d.mu.Lock()
				defer d.mu.Unlock()
				compactInfo = nil
				return d.mu.versions.currentVersion().String()

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
				defer d.mu.Unlock()
				// Closing the snapshot may have triggered a compaction.
				return compactionString()

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

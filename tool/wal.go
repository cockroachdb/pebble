// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/batchrepr"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/rangekey"
	"github.com/cockroachdb/pebble/v2/record"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/wal"
	"github.com/spf13/cobra"
)

// walT implements WAL-level tools, including both configuration state and the
// commands themselves.
type walT struct {
	Root       *cobra.Command
	Dump       *cobra.Command
	DumpMerged *cobra.Command

	opts     *pebble.Options
	fmtKey   keyFormatter
	fmtValue valueFormatter

	defaultComparer string
	comparers       sstable.Comparers
	verbose         bool
}

func newWAL(opts *pebble.Options, comparers sstable.Comparers, defaultComparer string) *walT {
	w := &walT{
		opts: opts,
	}
	w.fmtKey.mustSet("quoted")
	w.fmtValue.mustSet("size")
	w.comparers = comparers
	w.defaultComparer = defaultComparer

	w.Root = &cobra.Command{
		Use:   "wal",
		Short: "WAL introspection tools",
	}
	w.Dump = &cobra.Command{
		Use:   "dump <wal-files>",
		Short: "print WAL contents",
		Long: `
Print the contents of the WAL files.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  w.runDump,
	}
	w.DumpMerged = &cobra.Command{
		Use:   "dump-merged <wal-files>",
		Short: "print WAL contents",
		Long: `
Print the merged contents of multiple WAL segment files that
together form a single logical WAL.
`,
		Args: cobra.MinimumNArgs(1),
		Run:  w.runDumpMerged,
	}

	w.Root.AddCommand(w.Dump)
	w.Root.AddCommand(w.DumpMerged)
	w.Root.PersistentFlags().BoolVarP(&w.verbose, "verbose", "v", false, "verbose output")

	w.Dump.Flags().Var(
		&w.fmtKey, "key", "key formatter")
	w.Dump.Flags().Var(
		&w.fmtValue, "value", "value formatter")
	return w
}

type errAndArg struct {
	err error
	arg string
}

func (w *walT) runDump(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	w.fmtKey.setForComparer(w.defaultComparer, w.comparers)
	w.fmtValue.setForComparer(w.defaultComparer, w.comparers)
	var errs []errAndArg
	logErr := func(arg string, offset int64, err error) {
		err = errors.Wrapf(err, "%s: offset %d: ", arg, offset)
		fmt.Fprintf(stderr, "%s\n", err)
		errs = append(errs, errAndArg{
			arg: arg,
			err: err,
		})
	}

	for _, arg := range args {
		func() {
			// Parse the filename in order to extract the file number. This is
			// necessary in case WAL recycling was used (which it is usually is). If
			// we can't parse the filename or it isn't a log file, we'll plow ahead
			// anyways (which will likely fail when we try to read the file).
			fileName := path.Base(arg)
			fileNum, _, ok := wal.ParseLogFilename(fileName)
			if !ok {
				fileNum = 0
			}

			f, err := w.opts.FS.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}
			defer f.Close()

			fmt.Fprintf(stdout, "%s\n", arg)

			var b pebble.Batch
			var buf bytes.Buffer
			rr := record.NewReader(f, base.DiskFileNum(fileNum))
			for {
				offset := rr.Offset()
				r, err := rr.Next()
				if err == nil {
					buf.Reset()
					_, err = io.Copy(&buf, r)
				}
				if err != nil {
					// It is common to encounter a zeroed or invalid chunk due to WAL
					// preallocation and WAL recycling. We need to distinguish these
					// errors from EOF in order to recognize that the record was
					// truncated, but want to otherwise treat them like EOF.
					switch {
					case errors.Is(err, record.ErrZeroedChunk):
						fmt.Fprintf(stdout, "EOF [%s] (may be due to WAL preallocation)\n", err)
					case errors.Is(err, record.ErrInvalidChunk):
						fmt.Fprintf(stdout, "EOF [%s] (may be due to WAL recycling)\n", err)
					default:
						fmt.Fprintf(stdout, "%s\n", err)
					}
					return
				}

				b = pebble.Batch{}
				if err := b.SetRepr(buf.Bytes()); err != nil {
					fmt.Fprintf(stdout, "corrupt batch within log file %q: %v", arg, err)
					return
				}
				fmt.Fprintf(stdout, "%d(%d) seq=%d count=%d, len=%d\n",
					offset, len(b.Repr()), b.SeqNum(), b.Count(), buf.Len())
				w.dumpBatch(stdout, &b, b.Reader(), func(err error) {
					logErr(arg, offset, err)
				})
			}
		}()
	}
	if len(errs) > 0 {
		fmt.Fprintln(stderr, "Errors: ")
		for _, ea := range errs {
			fmt.Fprintf(stderr, "%s: %s\n", ea.arg, ea.err)
		}
	}
}

func (w *walT) runDumpMerged(cmd *cobra.Command, args []string) {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	w.fmtKey.setForComparer(w.defaultComparer, w.comparers)
	w.fmtValue.setForComparer(w.defaultComparer, w.comparers)
	var a wal.FileAccumulator
	for _, arg := range args {
		isLog, err := a.MaybeAccumulate(w.opts.FS, arg)
		if !isLog {
			fmt.Fprintf(stderr, "%q does not parse as a log file\n", arg)
			os.Exit(1)
		} else if err != nil {
			fmt.Fprintf(stderr, "%s: %s\n", arg, err)
			os.Exit(1)
		}
	}
	logs := a.Finish()
	var errs []error
	for _, log := range logs {
		fmt.Fprintf(stdout, "log file %s contains %d segment files:\n", log.Num, log.NumSegments())
		errs = append(errs, w.runDumpMergedOne(cmd, log)...)
	}
	if len(errs) > 0 {
		fmt.Fprintln(stderr, "Errors: ")
		for _, err := range errs {
			fmt.Fprintf(stderr, "%s\n", err)
		}
	}
}

func (w *walT) runDumpMergedOne(cmd *cobra.Command, ll wal.LogicalLog) []error {
	stdout, stderr := cmd.OutOrStdout(), cmd.OutOrStderr()
	var buf bytes.Buffer
	var errs []error
	var b pebble.Batch

	logErr := func(offset wal.Offset, err error) {
		err = errors.Wrapf(err, "offset %s: ", offset)
		fmt.Fprintln(stderr, err)
		errs = append(errs, err)
	}

	rr := ll.OpenForRead()
	for {
		buf.Reset()
		r, offset, err := rr.NextRecord()
		if err == nil {
			_, err = io.Copy(&buf, r)
		}
		if err != nil {
			// It is common to encounter a zeroed or invalid chunk due to WAL
			// preallocation and WAL recycling. We need to distinguish these
			// errors from EOF in order to recognize that the record was
			// truncated and to avoid replaying subsequent WALs, but want
			// to otherwise treat them like EOF.
			if err == io.EOF {
				break
			} else if record.IsInvalidRecord(err) {
				break
			}
			return append(errs, err)
		}
		if buf.Len() < batchrepr.HeaderLen {
			logErr(offset, errors.Newf("%d-byte batch too short", buf.Len()))
			continue
		}
		b = pebble.Batch{}
		if err := b.SetRepr(buf.Bytes()); err != nil {
			logErr(offset, errors.Newf("unable to parse batch: %x", buf.Bytes()))
			continue
		}
		fmt.Fprintf(stdout, "%s(%d) seq=%d count=%d, len=%d\n",
			offset, len(b.Repr()), b.SeqNum(), b.Count(), buf.Len())
		w.dumpBatch(stdout, &b, b.Reader(), func(err error) {
			logErr(offset, err)
		})
	}
	return nil
}

func (w *walT) dumpBatch(
	stdout io.Writer, b *pebble.Batch, r batchrepr.Reader, logErr func(error),
) {
	for idx := 0; ; idx++ {
		kind, ukey, value, ok, err := r.Next()
		if !ok {
			if err != nil {
				logErr(errors.Newf("unable to decode %d'th key in batch; %s", idx, err))
			}
			break
		}
		fmt.Fprintf(stdout, "    %s(", kind)
		switch kind {
		case base.InternalKeyKindDelete:
			fmt.Fprintf(stdout, "%s", w.fmtKey.fn(ukey))
		case base.InternalKeyKindSet:
			fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtValue.fn(ukey, value))
		case base.InternalKeyKindMerge:
			fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtValue.fn(ukey, value))
		case base.InternalKeyKindLogData:
			fmt.Fprintf(stdout, "<%d>", len(value))
		case base.InternalKeyKindIngestSST:
			fileNum, _ := binary.Uvarint(ukey)
			fmt.Fprintf(stdout, "%s", base.FileNum(fileNum))
		case base.InternalKeyKindExcise:
			fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtKey.fn(value))
		case base.InternalKeyKindSingleDelete:
			fmt.Fprintf(stdout, "%s", w.fmtKey.fn(ukey))
		case base.InternalKeyKindSetWithDelete:
			fmt.Fprintf(stdout, "%s", w.fmtKey.fn(ukey))
		case base.InternalKeyKindRangeDelete:
			fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtKey.fn(value))
		case base.InternalKeyKindRangeKeySet, base.InternalKeyKindRangeKeyUnset, base.InternalKeyKindRangeKeyDelete:
			ik := base.MakeInternalKey(ukey, b.SeqNum()+base.SeqNum(idx), kind)
			s, err := rangekey.Decode(ik, value, nil)
			if err != nil {
				logErr(errors.Newf("%s: error decoding %s", w.fmtKey.fn(ukey), err))
			} else {
				fmt.Fprintf(stdout, "%s", s.Pretty(w.fmtKey.fn))
			}
		case base.InternalKeyKindDeleteSized:
			v, _ := binary.Uvarint(value)
			fmt.Fprintf(stdout, "%s,%d", w.fmtKey.fn(ukey), v)
		default:
			err := errors.Newf("invalid key kind %d in key at index %d/%d of batch with seqnum %d at offset %d",
				kind, idx, b.Count(), b.SeqNum())
			fmt.Fprintf(stdout, "<error: %s>", err)
			logErr(err)
		}
		fmt.Fprintf(stdout, ")\n")
	}
}

// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/record"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

// walT implements WAL-level tools, including both configuration state and the
// commands themselves.
type walT struct {
	Root *cobra.Command
	Dump *cobra.Command

	fmtKey   formatter
	fmtValue formatter

	comparers sstable.Comparers
}

func newWAL(opts *base.Options, comparers sstable.Comparers) *walT {
	w := &walT{}
	w.fmtKey.mustSet("quoted")
	w.fmtValue.mustSet("size")
	w.comparers = comparers

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

	w.Root.AddCommand(w.Dump)

	w.Dump.Flags().Var(
		&w.fmtKey, "key", "key formatter")
	w.Dump.Flags().Var(
		&w.fmtValue, "value", "value formatter")
	return w
}

func (w *walT) runDump(cmd *cobra.Command, args []string) {
	w.fmtKey.setForComparer("", w.comparers)
	for _, arg := range args {
		func() {
			// Parse the filename in order to extract the file number. This is
			// necessary in case WAL recycling was used (which it is usually is). If
			// we can't parse the filename or it isn't a log file, we'll plow ahead
			// anyways (which will likely fail when we try to read the file).
			_, fileNum, ok := base.ParseFilename(arg)
			if !ok {
				fileNum = 0
			}

			f, err := vfs.Default.Open(arg)
			if err != nil {
				fmt.Fprintf(stderr, "%s\n", err)
				return
			}
			defer f.Close()

			fmt.Fprintf(stdout, "%s\n", arg)

			var b pebble.Batch
			var buf bytes.Buffer
			rr := record.NewReader(f, fileNum)
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
					switch err {
					case record.ErrZeroedChunk:
						fmt.Fprintf(stdout, "EOF [%s] (may be due to WAL preallocation)\n", err)
					case record.ErrInvalidChunk:
						fmt.Fprintf(stdout, "EOF [%s] (may be due to WAL recycling)\n", err)
					default:
						fmt.Fprintf(stdout, "%s\n", err)
					}
					return
				}

				b = pebble.Batch{}
				if err := b.SetRepr(buf.Bytes()); err != nil {
					fmt.Fprintf(stdout, "corrupt log file %q: %v", arg, err)
					return
				}
				fmt.Fprintf(stdout, "%d(%d) seq=%d count=%d\n",
					offset, len(b.Repr()), b.SeqNum(), b.Count())
				for r := b.Reader(); ; {
					kind, ukey, value, ok := r.Next()
					if !ok {
						break
					}
					fmt.Fprintf(stdout, "    %s(", kind)
					switch kind {
					case base.InternalKeyKindDelete:
						fmt.Fprintf(stdout, "%s", w.fmtKey.fn(ukey))
					case base.InternalKeyKindSet:
						fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtValue.fn(value))
					case base.InternalKeyKindMerge:
						fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtValue.fn(value))
					case base.InternalKeyKindLogData:
						fmt.Fprintf(stdout, "%s", w.fmtValue.fn(value))
					case base.InternalKeyKindSingleDelete:
						fmt.Fprintf(stdout, "%s", w.fmtKey.fn(ukey))
					case base.InternalKeyKindRangeDelete:
						fmt.Fprintf(stdout, "%s,%s", w.fmtKey.fn(ukey), w.fmtKey.fn(value))
					}
					fmt.Fprintf(stdout, ")\n")
				}
			}
		}()
	}
}

// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package itertest provides facilities for testing internal iterators.
package itertest

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/v2/internal/base"
	"github.com/cockroachdb/pebble/v2/internal/keyspan"
	"github.com/cockroachdb/pebble/v2/internal/testkeys"
	"github.com/stretchr/testify/require"
)

type iterCmdOpts struct {
	fmtKV           formatKV
	showCommands    bool
	withoutNewlines bool
	stats           *base.InternalIteratorStats
}

// An IterOpt configures the behavior of RunInternalIterCmd.
type IterOpt func(*iterCmdOpts)

// A formatKV configures the formatting to use when presenting key-value pairs.
type formatKV func(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator)

// Condensed configures RunInternalIterCmd to output condensed results without
// values, collapsed onto a single line.
func Condensed(opts *iterCmdOpts) {
	opts.fmtKV = condensedFormatKV
	opts.withoutNewlines = true
}

// ShowCommands configures RunInternalIterCmd to show the command in each output
// line (so you don't have to visually match the line to the command).
func ShowCommands(opts *iterCmdOpts) {
	opts.showCommands = true
}

// Verbose configures RunInternalIterCmd to output verbose results.
func Verbose(opts *iterCmdOpts) { opts.fmtKV = verboseFormatKV }

// WithSpan configures RunInternalIterCmd to print the span returned by spanFunc
// after each iteration operation.
func WithSpan(spanFunc func() *keyspan.Span) IterOpt {
	return func(opts *iterCmdOpts) {
		prevFmtKV := opts.fmtKV
		opts.fmtKV = func(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
			prevFmtKV(w, key, v, iter)
			if s := spanFunc(); s != nil {
				fmt.Fprintf(w, " / %s", s)
			}
		}
	}
}

// WithStats configures RunInternalIterCmd to collect iterator stats in the
// struct pointed to by stats.
func WithStats(stats *base.InternalIteratorStats) IterOpt {
	return func(opts *iterCmdOpts) { opts.stats = stats }
}

func defaultFormatKV(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
	if key != nil {
		fmt.Fprintf(w, "%s:%s", key.UserKey, v)
	} else if err := iter.Error(); err != nil {
		fmt.Fprintf(w, "err=%v", err)
	} else {
		fmt.Fprintf(w, ".")
	}
}

// condensedFormatKV is a FormatKV that outputs condensed results.
func condensedFormatKV(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
	if key != nil {
		fmt.Fprintf(w, "<%s:%d>", key.UserKey, key.SeqNum())
	} else if err := iter.Error(); err != nil {
		fmt.Fprintf(w, "err=%v", err)
	} else {
		fmt.Fprint(w, ".")
	}
}

// verboseFormatKV is a FormatKV that outputs verbose results.
func verboseFormatKV(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
	if key != nil {
		fmt.Fprintf(w, "%s:%s", key, v)
		return
	}
	defaultFormatKV(w, key, v, iter)
}

// RunInternalIterCmd evaluates a datadriven command controlling an internal
// iterator, returning a string with the results of the iterator operations.
func RunInternalIterCmd(
	t *testing.T, d *datadriven.TestData, iter base.InternalIterator, opts ...IterOpt,
) string {
	var buf bytes.Buffer
	RunInternalIterCmdWriter(t, &buf, d, iter, opts...)
	return buf.String()
}

// RunInternalIterCmdWriter evaluates a datadriven command controlling an
// internal iterator, writing the results of the iterator operations to the
// provided Writer.
func RunInternalIterCmdWriter(
	t *testing.T, w io.Writer, d *datadriven.TestData, iter base.InternalIterator, opts ...IterOpt,
) {
	o := iterCmdOpts{fmtKV: defaultFormatKV}
	for _, opt := range opts {
		opt(&o)
	}

	var prefix []byte
	var prevKey []byte
	getKV := func(kv *base.InternalKV) (*base.InternalKey, []byte) {
		if kv == nil {
			prevKey = nil
			return nil, nil
		}
		prevKey = kv.K.UserKey
		v, _, err := kv.Value(nil)
		require.NoError(t, err)
		return &kv.K, v
	}
	lines := crstrings.Lines(d.Input)
	maxCmdLen := 1
	for _, line := range lines {
		maxCmdLen = max(maxCmdLen, len(line))
	}
	for _, line := range lines {
		parts := strings.Fields(line)
		var key *base.InternalKey
		var value []byte
		if o.showCommands {
			fmt.Fprintf(w, "%*s: ", min(maxCmdLen, 40), line)
		}
		switch parts[0] {
		case "seek-ge":
			if len(parts) < 2 || len(parts) > 3 {
				fmt.Fprint(w, "seek-ge <key> [<try-seek-using-next>]\n")
				return
			}
			prefix = nil
			var flags base.SeekGEFlags
			if len(parts) == 3 {
				if trySeekUsingNext, err := strconv.ParseBool(parts[2]); err != nil {
					fmt.Fprintf(w, "%s", err.Error())
					return
				} else if trySeekUsingNext {
					flags = flags.EnableTrySeekUsingNext()
				}
			}
			key, value = getKV(iter.SeekGE([]byte(strings.TrimSpace(parts[1])), flags))
		case "seek-prefix-ge":
			if len(parts) != 2 && len(parts) != 3 {
				fmt.Fprint(w, "seek-prefix-ge <key> [<try-seek-using-next>]\n")
				return
			}
			prefix = []byte(strings.TrimSpace(parts[1]))
			var flags base.SeekGEFlags
			if len(parts) == 3 {
				if trySeekUsingNext, err := strconv.ParseBool(parts[2]); err != nil {
					fmt.Fprintf(w, "%s", err.Error())
					return
				} else if trySeekUsingNext {
					flags = flags.EnableTrySeekUsingNext()
				}
			}
			key, value = getKV(iter.SeekPrefixGE(prefix, prefix /* key */, flags))
		case "seek-lt":
			if len(parts) != 2 {
				fmt.Fprint(w, "seek-lt <key>\n")
				return
			}
			prefix = nil
			key, value = getKV(iter.SeekLT([]byte(strings.TrimSpace(parts[1])), base.SeekLTFlagsNone))
		case "first":
			prefix = nil
			key, value = getKV(iter.First())
		case "last":
			prefix = nil
			key, value = getKV(iter.Last())
		case "next":
			key, value = getKV(iter.Next())
		case "next-prefix":
			succKey := testkeys.Comparer.ImmediateSuccessor(prevKey[:testkeys.Comparer.Split(prevKey)], nil)
			key, value = getKV(iter.NextPrefix(succKey))
		case "prev":
			key, value = getKV(iter.Prev())
		case "set-bounds":
			if len(parts) <= 1 || len(parts) > 3 {
				fmt.Fprint(w, "set-bounds lower=<lower> upper=<upper>\n")
				return
			}
			var lower []byte
			var upper []byte
			for _, part := range parts[1:] {
				arg := strings.Split(strings.TrimSpace(part), "=")
				switch arg[0] {
				case "lower":
					lower = []byte(arg[1])
				case "upper":
					upper = []byte(arg[1])
				default:
					fmt.Fprintf(w, "set-bounds: unknown arg: %s", arg)
					return
				}
			}
			iter.SetBounds(lower, upper)
			continue
		case "stats":
			if o.stats != nil {
				// The timing is non-deterministic, so set to 0.
				o.stats.BlockReadDuration = 0
				fmt.Fprintf(w, "%+v\n", *o.stats)
			}
			continue
		case "reset-stats":
			if o.stats != nil {
				*o.stats = base.InternalIteratorStats{}
			}
			continue
		case "is-lower-bound":
			// This command is specific to colblk.DataBlockIter.
			if len(parts) != 2 {
				fmt.Fprint(w, "is-lower-bound <key>\n")
				return
			}
			i := iter.(interface{ IsLowerBound(key []byte) bool })
			fmt.Fprintf(w, "%v\n", i.IsLowerBound([]byte(parts[1])))
			continue
		default:
			fmt.Fprintf(w, "unknown op: %s", parts[0])
			return
		}
		o.fmtKV(w, key, value, iter)
		if !o.withoutNewlines {
			fmt.Fprintln(w)
		}
	}
}

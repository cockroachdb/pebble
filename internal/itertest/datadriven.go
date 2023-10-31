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

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

type iterCmdOpts struct {
	fmtKV func(io.Writer, *base.InternalKey, []byte, base.InternalIterator)
	stats *base.InternalIteratorStats
}

// An IterOpt configures the behavior of RunInternalIterCmd.
type IterOpt func(*iterCmdOpts)

// Verbose configures RunInternalIterCmd to output verbose results.
func Verbose(opts *iterCmdOpts) { opts.fmtKV = verboseFmt }

// Condensed configures RunInternalIterCmd to output condensed results without
// values.
func Condensed(opts *iterCmdOpts) { opts.fmtKV = condensedFmt }

// WithStats configures RunInternalIterCmd to collect iterator stats in the
// struct pointed to by stats.
func WithStats(stats *base.InternalIteratorStats) IterOpt {
	return func(opts *iterCmdOpts) {
		opts.stats = stats
	}
}

func defaultFmt(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
	if key != nil {
		fmt.Fprintf(w, "%s:%s\n", key.UserKey, v)
	} else if err := iter.Error(); err != nil {
		fmt.Fprintf(w, "err=%v\n", err)
	} else {
		fmt.Fprintf(w, ".\n")
	}
}

func condensedFmt(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
	if key != nil {
		fmt.Fprintf(w, "<%s:%d>", key.UserKey, key.SeqNum())
	} else if err := iter.Error(); err != nil {
		fmt.Fprintf(w, "err=%v", err)
	} else {
		fmt.Fprint(w, ".")
	}
}

func verboseFmt(w io.Writer, key *base.InternalKey, v []byte, iter base.InternalIterator) {
	if key != nil {
		fmt.Fprintf(w, "%s:%s\n", key, v)
		return
	}
	defaultFmt(w, key, v, iter)
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
	o := iterCmdOpts{fmtKV: defaultFmt}
	for _, opt := range opts {
		opt(&o)
	}

	getKV := func(key *base.InternalKey, val base.LazyValue) (*base.InternalKey, []byte) {
		v, _, err := val.Value(nil)
		require.NoError(t, err)
		return key, v
	}
	var prefix []byte
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var key *base.InternalKey
		var value []byte
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
		default:
			fmt.Fprintf(w, "unknown op: %s", parts[0])
			return
		}
		o.fmtKV(w, key, value, iter)

	}
}

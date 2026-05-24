// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestManualFlush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	getOptions := func() *Options {
		opts := &Options{
			FS:                    vfs.NewMem(),
			L0CompactionThreshold: 10,
		}
		opts.DisableAutomaticCompactions = true
		return opts
	}
	d, err := Open("", getOptions())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	datadriven.RunTest(t, "testdata/manual_flush", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			b.Commit(nil)
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "async-flush":
			d.mu.Lock()
			cur := d.mu.versions.currentVersion()
			d.mu.Unlock()

			if _, err := d.AsyncFlush(); err != nil {
				return err.Error()
			}

			err := try(100*time.Microsecond, 20*time.Second, func() error {
				d.mu.Lock()
				defer d.mu.Unlock()
				if cur == d.mu.versions.currentVersion() {
					return errors.New("flush has not occurred")
				}
				return nil
			})
			if err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().String()
			d.mu.Unlock()
			return s

		case "reset":
			if err := d.Close(); err != nil {
				return err.Error()
			}
			d, err = Open("", getOptions())
			if err != nil {
				return err.Error()
			}
			return ""

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

// TestFlushDelRangeEmptyKey tests flushing a range tombstone that begins with
// an empty key. The empty key is a valid key but can be confused with nil.
func TestFlushDelRangeEmptyKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	d, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	require.NoError(t, d.DeleteRange([]byte{}, []byte("z"), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Close())
}

// TestFlushEmptyKey tests that flushing an empty key does not trigger that key
// order invariant assertions.
func TestFlushEmptyKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	d, err := Open("", &Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	require.NoError(t, d.Set(nil, []byte("hello"), nil))
	require.NoError(t, d.Flush())
	val, closer, err := d.Get(nil)
	require.NoError(t, err)
	require.Equal(t, val, []byte("hello"))
	require.NoError(t, closer.Close())
	require.NoError(t, d.Close())
}

// TestFlushIfOverlapping verifies that `flushIfOverlapping` flushes iff some
// memtable entry intersects the requested span (or the caller-supplied extra
// bounds), and that point keys, range deletions, and range keys all
// participate in the overlap check.
func TestFlushIfOverlapping(t *testing.T) {
	defer leaktest.AfterTest(t)()

	open := func(t *testing.T, opts *Options) *DB {
		if opts == nil {
			opts = &Options{}
		}
		if opts.FS == nil {
			opts.FS = vfs.NewMem()
		}
		if opts.FormatMajorVersion == 0 {
			opts.FormatMajorVersion = FormatNewest
		}
		d, err := Open("", opts)
		require.NoError(t, err)
		return d
	}
	span := func(s, e string) KeyRange { return KeyRange{Start: []byte(s), End: []byte(e)} }
	abz := span("a", "z")

	t.Run("overlap", func(t *testing.T) {
		type populate func(d *DB) error
		point := func(k string) populate {
			return func(d *DB) error { return d.Set([]byte(k), nil, nil) }
		}
		rangeDel := func(s, e string) populate {
			return func(d *DB) error { return d.DeleteRange([]byte(s), []byte(e), nil) }
		}
		rangeKey := func(s, e, suffix string) populate {
			return func(d *DB) error {
				return d.RangeKeySet([]byte(s), []byte(e), []byte(suffix), nil, nil)
			}
		}

		for _, tc := range []struct {
			name      string
			populate  populate
			span      KeyRange
			wantFlush bool
		}{
			{"empty memtable", nil, abz, false},
			{"point not in span", point("a"), span("x", "z"), false},
			{"point in span", point("m"), abz, true},
			{"point at span start (inclusive)", point("m"), span("m", "n"), true},
			{"point at span end (exclusive)", point("m"), span("a", "m"), false},
			{"range del intersects", rangeDel("m", "n"), abz, true},
			{"range del disjoint", rangeDel("m", "n"), span("x", "z"), false},
			{"range key intersects", rangeKey("m", "n", "@1"), abz, true},
			{"range key disjoint", rangeKey("m", "n", "@1"), span("x", "z"), false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				d := open(t, nil)
				defer func() { require.NoError(t, d.Close()) }()
				if tc.populate != nil {
					require.NoError(t, tc.populate(d))
				}
				before := d.Metrics().Flush.Count
				require.NoError(t, d.flushIfOverlapping(tc.span, nil))
				after := d.Metrics().Flush.Count
				if tc.wantFlush {
					require.Greater(t, after, before, "expected flush")
				} else {
					require.Equal(t, before, after, "expected no flush")
				}
			})
		}
	})

	t.Run("extra bounds callback", func(t *testing.T) {
		// The DSR-style use case: the caller's span is disjoint from the
		// memtable's content, but the callback supplies an extra range that
		// does overlap, and we expect a flush.
		d := open(t, nil)
		defer func() { require.NoError(t, d.Close()) }()
		require.NoError(t, d.Set([]byte("m"), nil, nil))
		extra := KeyRange{Start: []byte("l"), End: []byte("n")}
		before := d.Metrics().Flush.Count
		require.NoError(t, d.flushIfOverlapping(
			span("x", "z"),
			func() []bounded { return []bounded{extra} },
		))
		require.Greater(t, d.Metrics().Flush.Count, before,
			"expected flush triggered by extra bounds")

		// Callback returning no extra bounds is equivalent to nil callback.
		d2 := open(t, nil)
		defer func() { require.NoError(t, d2.Close()) }()
		require.NoError(t, d2.Set([]byte("m"), nil, nil))
		before2 := d2.Metrics().Flush.Count
		require.NoError(t, d2.flushIfOverlapping(
			span("x", "z"),
			func() []bounded { return nil },
		))
		require.Equal(t, before2, d2.Metrics().Flush.Count,
			"expected no flush when neither span nor extra bounds overlap")
	})

	t.Run("closed DB panics", func(t *testing.T) {
		d := open(t, nil)
		require.NoError(t, d.Close())
		require.Panics(t, func() { _ = d.flushIfOverlapping(abz, nil) })
	})

	t.Run("read-only returns ErrReadOnly", func(t *testing.T) {
		fs := vfs.NewMem()
		require.NoError(t, open(t, &Options{FS: fs}).Close())
		d := open(t, &Options{FS: fs, ReadOnly: true})
		defer func() { require.NoError(t, d.Close()) }()
		require.ErrorIs(t, d.flushIfOverlapping(abz, nil), ErrReadOnly)
	})

	t.Run("invalid KeyRange", func(t *testing.T) {
		d := open(t, nil)
		defer func() { require.NoError(t, d.Close()) }()
		require.Error(t, d.flushIfOverlapping(KeyRange{}, nil))
		require.Error(t, d.flushIfOverlapping(KeyRange{Start: []byte("a")}, nil))
		require.Error(t, d.flushIfOverlapping(KeyRange{End: []byte("z")}, nil))
	})
}

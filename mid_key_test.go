// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestApproximateMidKey(t *testing.T) {
	fs := vfs.NewMem()
	var d *DB
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()

	datadriven.RunTest(t, "testdata/mid_key", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "open":
			if d != nil {
				require.NoError(t, d.Close())
				d = nil
			}
			opts := &Options{
				FS:                          fs,
				FormatMajorVersion:          FormatExciseBoundsRecord,
				DisableAutomaticCompactions: true,
			}
			require.NoError(t, parseDBOptionsArgs(opts, td.CmdArgs))
			var err error
			d, err = Open("", opts)
			require.NoError(t, err)
			return ""

		case "close":
			if d != nil {
				require.NoError(t, d.Close())
				d = nil
			}
			return ""

		case "batch":
			b := d.NewBatch()
			if err := runBatchDefineCmd(td, b); err != nil {
				return err.Error()
			}
			if err := b.Commit(nil); err != nil {
				return err.Error()
			}
			return ""

		case "flush":
			if err := d.Flush(); err != nil {
				return err.Error()
			}
			return ""

		case "build":
			if err := runBuildCmd(td, d, fs); err != nil {
				return err.Error()
			}
			return ""

		case "ingest":
			if err := runIngestCmd(td, d); err != nil {
				return err.Error()
			}
			return ""

		case "compact":
			if err := runCompactCmd(t, td, d); err != nil {
				return err.Error()
			}
			return runLSMCmd(td, d)

		case "lsm":
			return runLSMCmd(td, d)

		case "approximate-mid-key":
			start := []byte("a")
			end := []byte("z")
			if len(td.CmdArgs) >= 2 {
				start = []byte(td.CmdArgs[0].Key)
				end = []byte(td.CmdArgs[1].Key)
			}
			midKey, _, err := d.ApproximateMidKey(
				context.Background(), start, end, 64<<10, /* epsilon */
			)
			if err != nil {
				return err.Error()
			}
			if midKey == nil {
				return "no mid key"
			}
			var buf strings.Builder
			fmt.Fprintf(&buf, "mid key: %s\n", midKey)

			if d.cmp(midKey, start) <= 0 || d.cmp(midKey, end) >= 0 {
				fmt.Fprintf(&buf, "ERROR: mid key outside bounds\n")
			}

			leftSize, err := d.EstimateDiskUsage(start, midKey)
			require.NoError(t, err)
			rightSize, err := d.EstimateDiskUsage(midKey, end)
			require.NoError(t, err)
			totalSize, err := d.EstimateDiskUsage(start, end)
			require.NoError(t, err)

			fmt.Fprintf(&buf, "left size: %d, right size: %d, total: %d", leftSize, rightSize, totalSize)

			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestApproximateMidKeyClosedDB(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{FS: mem})
	require.NoError(t, err)
	require.NoError(t, d.Set([]byte("key"), []byte("value"), nil))
	require.NoError(t, d.Close())
	require.Panics(t, func() {
		d.ApproximateMidKey(context.Background(), []byte("a"), []byte("z"), 0)
	})
}

func TestApproximateMidKeyInvalidRange(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{FS: mem})
	require.NoError(t, err)
	defer d.Close()

	_, _, err = d.ApproximateMidKey(context.Background(), []byte("z"), []byte("a"), 0)
	require.Error(t, err)
}

func TestApproximateMidKeyEmpty(t *testing.T) {
	mem := vfs.NewMem()
	d, err := Open("", &Options{FS: mem})
	require.NoError(t, err)
	defer d.Close()

	midKey, lhsSize, err := d.ApproximateMidKey(context.Background(), []byte("a"), []byte("z"), 0)
	require.NoError(t, err)
	require.Nil(t, midKey)
	require.Zero(t, lhsSize)
}

// noCompressionOpts returns Options with compression disabled for all levels.
func noCompressionOpts(fs vfs.FS) *Options {
	opts := &Options{
		FS:                          fs,
		DisableAutomaticCompactions: true,
	}
	opts.ApplyCompressionSettings(func() DBCompressionSettings {
		return DBCompressionNone
	})
	return opts
}

func TestApproximateMidKeyUniform(t *testing.T) {
	mem := vfs.NewMem()
	opts := noCompressionOpts(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	value := make([]byte, 4096)
	rand.Read(value)
	for i := 0; i < 128; i++ {
		key := fmt.Sprintf("key%03d", i)
		require.NoError(t, d.Set([]byte(key), value, nil))
		if (i+1)%32 == 0 {
			require.NoError(t, d.Flush())
		}
	}

	midKey, _, err := d.ApproximateMidKey(
		context.Background(), []byte("key000"), []byte("key999"), 64<<10,
	)
	require.NoError(t, err)
	require.NotNil(t, midKey)
	require.True(t, d.cmp(midKey, []byte("key000")) > 0)
	require.True(t, d.cmp(midKey, []byte("key999")) < 0)

	leftSize, err := d.EstimateDiskUsage([]byte("key000"), midKey)
	require.NoError(t, err)
	rightSize, err := d.EstimateDiskUsage(midKey, []byte("key999"))
	require.NoError(t, err)
	t.Logf("mid=%s left=%d right=%d", midKey, leftSize, rightSize)

	total := leftSize + rightSize
	require.Greater(t, float64(leftSize)/float64(total), 0.15,
		"left side too small: left=%d right=%d total=%d", leftSize, rightSize, total)
	require.Greater(t, float64(rightSize)/float64(total), 0.15,
		"right side too small: left=%d right=%d total=%d", leftSize, rightSize, total)
}

func TestApproximateMidKeyMultipleLevels(t *testing.T) {
	mem := vfs.NewMem()
	opts := noCompressionOpts(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	value := make([]byte, 4096)
	rand.Read(value)

	for i := 0; i < 64; i++ {
		key := fmt.Sprintf("key%03d", i)
		require.NoError(t, d.Set([]byte(key), value, nil))
	}
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact(context.Background(), []byte("key000"), []byte("key999"), false))

	for i := 64; i < 128; i++ {
		key := fmt.Sprintf("key%03d", i)
		require.NoError(t, d.Set([]byte(key), value, nil))
	}
	require.NoError(t, d.Flush())

	midKey, _, err := d.ApproximateMidKey(
		context.Background(), []byte("key000"), []byte("key999"), 64<<10,
	)
	require.NoError(t, err)
	require.NotNil(t, midKey)
	require.True(t, d.cmp(midKey, []byte("key000")) > 0)
	require.True(t, d.cmp(midKey, []byte("key999")) < 0)
}

func TestApproximateMidKeyAllInOneSST(t *testing.T) {
	mem := vfs.NewMem()
	opts := noCompressionOpts(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	value := make([]byte, 4096)
	rand.Read(value)
	for i := 0; i < 128; i++ {
		key := fmt.Sprintf("key%03d", i)
		require.NoError(t, d.Set([]byte(key), value, nil))
	}
	require.NoError(t, d.Flush())

	midKey, _, err := d.ApproximateMidKey(
		context.Background(), []byte("key000"), []byte("key999"), 64<<10,
	)
	require.NoError(t, err)
	require.NotNil(t, midKey)
	require.True(t, d.cmp(midKey, []byte("key000")) > 0)
	require.True(t, d.cmp(midKey, []byte("key999")) < 0)
}

func TestApproximateMidKeyTightEpsilon(t *testing.T) {
	mem := vfs.NewMem()
	opts := noCompressionOpts(mem)
	d, err := Open("", opts)
	require.NoError(t, err)
	defer d.Close()

	// Write enough data that a tight epsilon forces the merge-walk path.
	value := make([]byte, 4096)
	rand.Read(value)
	for i := 0; i < 256; i++ {
		key := fmt.Sprintf("key%03d", i)
		require.NoError(t, d.Set([]byte(key), value, nil))
		if (i+1)%64 == 0 {
			require.NoError(t, d.Flush())
		}
	}

	midKey, _, err := d.ApproximateMidKey(
		context.Background(), []byte("key000"), []byte("key999"),
		1<<10, /* epsilon: 1KB — very tight, forces refinement */
	)
	require.NoError(t, err)
	require.NotNil(t, midKey)
	require.True(t, d.cmp(midKey, []byte("key000")) > 0)
	require.True(t, d.cmp(midKey, []byte("key999")) < 0)

	leftSize, err := d.EstimateDiskUsage([]byte("key000"), midKey)
	require.NoError(t, err)
	rightSize, err := d.EstimateDiskUsage(midKey, []byte("key999"))
	require.NoError(t, err)
	t.Logf("tight epsilon: mid=%s left=%d right=%d", midKey, leftSize, rightSize)
}

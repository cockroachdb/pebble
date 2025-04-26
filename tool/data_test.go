// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func runTests(t *testing.T, path string) {
	paths, err := filepath.Glob(path)
	require.NoError(t, err)

	root := filepath.Dir(path)
	for {
		next := filepath.Dir(root)
		if next == "." {
			break
		}
		root = next
	}

	for _, path := range paths {
		name, err := filepath.Rel(root, path)
		require.NoError(t, err)

		fs := vfs.NewMem()
		clonedPaths := make(map[string]string)
		t.Run(name, func(t *testing.T) {
			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
				// Register a test comparer and merger so that we can check the
				// behavior of tools when the comparer and merger do not match.
				comparer := func() *Comparer {
					c := *base.DefaultComparer
					c.Name = "test-comparer"
					c.FormatKey = func(key []byte) fmt.Formatter {
						return fmtFormatter{
							fmt: "test formatter: %s",
							v:   key,
						}
					}
					c.FormatValue = func(_, value []byte) fmt.Formatter {
						return fmtFormatter{
							fmt: "test value formatter: %s",
							v:   value,
						}
					}
					return &c
				}()
				altComparer := func() *Comparer {
					c := *base.DefaultComparer
					c.Name = "alt-comparer"
					return &c
				}()
				merger := func() *Merger {
					m := *base.DefaultMerger
					m.Name = "test-merger"
					return &m
				}()

				if d.Cmd == "create" {
					dbDir := d.CmdArgs[0].String()
					opts := &pebble.Options{
						Comparer:           comparer,
						Merger:             merger,
						FS:                 fs,
						FormatMajorVersion: pebble.FormatVirtualSSTables,
					}
					db, err := pebble.Open(dbDir, opts)
					if err != nil {
						d.Fatalf(t, "%v", err)
					}
					db.Close()
					return ""
				}

				args := []string{d.Cmd}
				for _, arg := range d.CmdArgs {
					args = append(args, arg.String())
				}
				args = append(args, strings.Fields(d.Input)...)

				// If any args are paths, clone them into the in-memory filesystem. We
				// only do this the first time we see each particular path (so that the
				// test can observe any modifications).
				for i := range args {
					if dest, ok := clonedPaths[args[i]]; ok {
						args[i] = dest
						continue
					}
					// Replace path separator in case we are running on Windows.
					src := strings.Replace(args[i], "/", string(os.PathSeparator), -1)
					dest := vfs.Default.PathBase(src)
					if ok, err := vfs.Clone(vfs.Default, fs, src, dest); err != nil {
						return err.Error()
					} else if ok {
						clonedPaths[args[i]] = dest
						args[i] = dest
					}
				}

				var buf bytes.Buffer
				var secs int64
				timeNow = func() time.Time { secs++; return time.Unix(secs, 0) }

				defer func() {
					timeNow = time.Now
				}()

				openErrEnhancer := func(err error) error {
					if errors.Is(err, base.ErrCorruption) {
						return base.CorruptionErrorf("%v\nCustom message in case of corruption error.", err)
					}
					return err
				}

				tool := New(
					DefaultComparer(comparer),
					Comparers(altComparer, testkeys.Comparer),
					Mergers(merger),
					FS(fs),
					OpenErrEnhancer(openErrEnhancer),
				)

				c := &cobra.Command{}
				c.AddCommand(tool.Commands...)
				c.SetArgs(args)
				c.SetOut(&buf)
				c.SetErr(&buf)
				if err := c.Execute(); err != nil {
					return err.Error()
				}
				output := buf.String()
				if output == "" {
					return ""
				}
				output = strings.TrimSuffix(output, "\n")
				buf.Reset()
				for _, l := range strings.Split(output, "\n") {
					for _, flakyPrefix := range []string{"Table stats:", "Cgo memory usage:"} {
						if strings.HasPrefix(l, flakyPrefix) {
							l = flakyPrefix + " <redacted>"
							break
						}
					}
					buf.WriteString(l)
					buf.WriteString("\n")
				}
				return buf.String()
			})
		})
	}
}

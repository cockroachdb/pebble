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

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/spf13/cobra"
)

func runTests(t *testing.T, path string) {
	paths, err := filepath.Glob(path)
	if err != nil {
		t.Fatal(err)
	}
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
		if err != nil {
			t.Fatal(err)
		}
		t.Run(name, func(t *testing.T) {
			datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
				args := []string{d.Cmd}
				for _, arg := range d.CmdArgs {
					args = append(args, arg.String())
				}
				args = append(args, strings.Fields(d.Input)...)

				var buf bytes.Buffer
				stdout = &buf
				stderr = &buf
				osExit = func(int) {}

				var secs int64
				timeNow = func() time.Time { secs++; return time.Unix(secs, 0) }

				defer func() {
					stdout = os.Stdout
					stderr = os.Stderr
					osExit = os.Exit
					timeNow = time.Now
				}()

				tool := New()
				// Register a test comparer and merger so that we can check the
				// behavior of tools when the comparer and merger do not match.
				tool.RegisterComparer(func() *Comparer {
					var c Comparer
					c = *base.DefaultComparer
					c.Name = "test-comparer"
					c.Format = func(key []byte) fmt.Formatter {
						return fmtFormatter{
							fmt: "test formatter: %s",
							v:   key,
						}
					}
					return &c
				}())
				tool.RegisterMerger(func() *Merger {
					var m Merger
					m = *base.DefaultMerger
					m.Name = "test-merger"
					return &m
				}())

				c := &cobra.Command{}
				c.AddCommand(tool.Commands...)
				c.SetArgs(args)
				c.SetOutput(&buf)
				if err := c.Execute(); err != nil {
					return err.Error()
				}
				return buf.String()
			})
		})
	}
}

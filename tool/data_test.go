// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/spf13/cobra"
)

func TestCommands(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
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

			defer func() {
				stdout = os.Stdout
				stderr = os.Stderr
				osExit = os.Exit
				sstableConfig.start = ""
				sstableConfig.end = ""
				sstableConfig.verbose = false
			}()

			c := &cobra.Command{}
			c.AddCommand(AllCmds...)
			c.SetArgs(args)
			c.SetOutput(&buf)
			if err := c.Execute(); err != nil {
				return err.Error()
			}
			return buf.String()
		})
	})
}

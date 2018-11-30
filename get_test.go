// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
)

func TestGet(t *testing.T) {
	var d *DB

	datadriven.RunTest(t, "testdata/get", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			var err error
			if d, err = runDBDefineCmd(td); err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := fmt.Sprintf("mem: %d\n%s", len(d.mu.mem.queue), d.mu.versions.currentVersion())
			d.mu.Unlock()
			return s

		case "get":
			snap := Snapshot{
				db:     d,
				seqNum: db.InternalKeySeqNumMax,
			}

			for _, arg := range td.CmdArgs {
				if len(arg.Vals) != 1 {
					return fmt.Sprintf("%s: %s=<value>", td.Cmd, arg.Key)
				}
				switch arg.Key {
				case "seq":
					var err error
					snap.seqNum, err = strconv.ParseUint(arg.Vals[0], 10, 64)
					if err != nil {
						return err.Error()
					}
				default:
					return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
				}
			}

			var buf bytes.Buffer
			for _, data := range strings.Split(td.Input, "\n") {
				v, err := snap.Get([]byte(data))
				if err != nil {
					fmt.Fprintf(&buf, "%s\n", err)
				} else {
					fmt.Fprintf(&buf, "%s\n", v)
				}
			}
			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
		return ""
	})
}

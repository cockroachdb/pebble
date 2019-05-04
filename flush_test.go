// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"testing"
	"time"

	"github.com/petermattis/pebble/db"
	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/vfs"
)

func TestManualFlush(t *testing.T) {
	d, err := Open("", &db.Options{
		VFS: vfs.NewMem(),
	})
	if err != nil {
		t.Fatal(err)
	}

	datadriven.RunTest(t, "testdata/manual_flush", func(td *datadriven.TestData) string {
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
			s := d.mu.versions.currentVersion().DebugString()
			d.mu.Unlock()
			return s

		case "async-flush":
			d.mu.Lock()
			cur := d.mu.versions.currentVersion()
			d.mu.Unlock()

			if err := d.AsyncFlush(); err != nil {
				return err.Error()
			}

			err := try(100*time.Microsecond, 20*time.Second, func() error {
				d.mu.Lock()
				defer d.mu.Unlock()
				if cur == d.mu.versions.currentVersion() {
					return fmt.Errorf("flush has not occurred")
				}
				return nil
			})
			if err != nil {
				return err.Error()
			}

			d.mu.Lock()
			s := d.mu.versions.currentVersion().DebugString()
			d.mu.Unlock()
			return s

		case "reset":
			d, err = Open("", &db.Options{
				VFS: vfs.NewMem(),
			})
			if err != nil {
				return err.Error()
			}
			return ""

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

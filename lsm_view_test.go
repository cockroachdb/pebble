// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestLSMViewURL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.RunTest(t, "testdata/lsm_view",
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "define":
				d, err := runDBDefineCmd(td, &Options{})
				if err != nil {
					td.Fatalf(t, "error: %s", err)
				}
				defer d.Close()
				return d.LSMViewURL()

			default:
				td.Fatalf(t, "unknown command %q", td.Cmd)
				return ""
			}
		})
}

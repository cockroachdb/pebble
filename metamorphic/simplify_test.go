// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestSimplifyKeys(t *testing.T) {
	datadriven.RunTest(t, "testdata/simplify", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "simplify-keys":
			retainSuffixes := d.HasArg("retain-suffixes")
			res := TryToSimplifyKeys([]byte(d.Input), retainSuffixes)
			return string(res)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

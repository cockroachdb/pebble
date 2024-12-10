// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestDiagram(t *testing.T) {
	datadriven.RunTest(t, "testdata/diagram", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "diagram":
			res, err := TryToGenerateDiagram(TestkeysKeyFormat, []byte(d.Input))
			if err != nil {
				return err.Error()
			}
			return res

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	datadriven.RunTest(t, "testdata/parser", func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "parse":
			ops, err := parse([]byte(d.Input))
			if err != nil {
				return err.Error()
			}
			return formatOps(ops)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestParserRandom(t *testing.T) {
	ops := generate(randvar.NewRand(), 10000, defaultConfig)
	src := formatOps(ops)

	parsedOps, err := parse([]byte(src))
	if err != nil {
		t.Fatalf("%s\n%s", err, src)
	}

	if diff := pretty.Diff(ops, parsedOps); diff != nil {
		t.Fatalf("%s\n%s", strings.Join(diff, "\n"), src)
	}
}

func TestParserNilBounds(t *testing.T) {
	formatted := formatOps([]op{
		&newIterOp{
			readerID: makeObjID(dbTag, 0),
			iterID:   makeObjID(iterTag, 1),
			lower:    nil,
			upper:    nil,
		},
	})
	parsedOps, err := parse([]byte(formatted))
	require.NoError(t, err)
	require.Equal(t, 1, len(parsedOps))
	v := parsedOps[0].(*newIterOp)
	require.Nil(t, v.lower)
	require.Nil(t, v.upper)
}

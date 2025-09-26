// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	kf := TestkeysKeyFormat
	datadriven.RunTest(t, "testdata/parser", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "parse":
			ops, err := parse([]byte(d.Input), parserOpts{
				parseFormattedUserKey:       kf.ParseFormattedKey,
				parseFormattedUserKeySuffix: kf.ParseFormattedKeySuffix,
				parseMaximumSuffixProperty:  kf.ParseMaximumSuffixProperty,
			})
			if err != nil {
				return err.Error()
			}
			return formatOps(kf, ops)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestParserRandom(t *testing.T) {
	cfgs := []string{"default", "multiInstance"}
	for i := range cfgs {
		t.Run(fmt.Sprintf("config=%s", cfgs[i]), func(t *testing.T) {
			cfg := DefaultOpConfig()
			if cfgs[i] == "multiInstance" {
				cfg = multiInstanceConfig()
				cfg.numInstances = 2
			}
			km := newKeyManager(cfg.numInstances, TestkeysKeyFormat)
			g := newGenerator(randvar.NewRand(), cfg, km)
			ops := g.generate(10000)
			src := formatOps(km.kf, ops)

			parsedOps, err := parse([]byte(src), parserOpts{
				parseMaximumSuffixProperty: km.kf.ParseMaximumSuffixProperty,
			})
			if err != nil {
				t.Fatalf("%s\n%s", err, src)
			}
			require.Equal(t, ops, parsedOps)
		})
	}
}

func TestParserNilBounds(t *testing.T) {
	kf := TestkeysKeyFormat
	formatted := formatOps(kf, []op{
		&newIterOp{
			readerID: makeObjID(dbTag, 1),
			iterID:   makeObjID(iterTag, 1),
			iterOpts: iterOpts{},
		},
	})
	parsedOps, err := parse([]byte(formatted), parserOpts{
		parseFormattedUserKey:       kf.ParseFormattedKey,
		parseFormattedUserKeySuffix: kf.ParseFormattedKeySuffix,
		parseMaximumSuffixProperty:  kf.ParseMaximumSuffixProperty,
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(parsedOps))
	v := parsedOps[0].(*newIterOp)
	require.Nil(t, v.lower)
	require.Nil(t, v.upper)
}

// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package errorfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidDSL(t *testing.T) {
	testCases := map[string]bool{
		`always`:                                              true,
		`alwoes`:                                              false,
		`always()`:                                            false,
		`pathMatch("foo/*.sst", always)`:                      true,
		`pathMatch(foo/*.sst, always)`:                        false,
		`pathMatch("foo/*.sst", alwoes)`:                      false,
		`pathMatch("foo/*.sst", "", always)`:                  false,
		`pathMatch(always, "foo/*.sst")`:                      false,
		`onIndex(1, always)`:                                  true,
		`pathMatch("foo/bar/*.sst", onIndex(1, always))`:      true,
		`onIndex(always, "foo/*.sst")`:                        false,
		`any(always, always, always)`:                         true,
		`any(onIndex(2, always), pathMatch("*.sst", always))`: true,
		`any(1, 4, 5)`:                                        false,
		`reads(pathMatch("*.sst", always))`:                   true,
		`writes(pathMatch("*.sst", always))`:                  true,
		`any(always, always, always`:                          false,
		`onIndex(foo, always)`:                                false,
	}
	for dsl, ok := range testCases {
		t.Run(dsl, func(t *testing.T) {
			_, err := ParseInjectorFromDSL(dsl)
			if ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

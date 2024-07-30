// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
)

func TestComparer(t *testing.T) {
	t.Skipf("TODO(radu): the crdb comparer is currently broken")
	prefixes := [][]byte{
		{},
		EncodeMVCCKey(nil, []byte("abc"), 0, 0),
		EncodeMVCCKey(nil, []byte("d"), 0, 0),
		EncodeMVCCKey(nil, []byte("ef"), 0, 0),
	}

	suffixes := [][]byte{{}}
	for walltime := 3; walltime > 0; walltime-- {
		for logical := 2; logical >= 0; logical-- {
			key := EncodeMVCCKey(nil, []byte("foo"), uint64(walltime), uint32(logical))
			suffix := key[Comparer.Split(key):]
			suffixes = append(suffixes, suffix)
		}
	}
	if err := base.CheckComparer(&Comparer, prefixes, suffixes); err != nil {
		t.Error(err)
	}
}

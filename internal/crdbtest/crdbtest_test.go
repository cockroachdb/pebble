// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"bytes"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
)

func TestComparer(t *testing.T) {
	prefixes := [][]byte{
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

			if len(suffix) == withWall {
				// Append a suffix that encodes a zero logical value that should be
				// ignored in key comparisons, but not suffix comparisons.
				newSuffix := slices.Concat(suffix[:withWall-1], zeroLogical[:], []byte{withLogical})
				if Comparer.CompareSuffixes(suffix, newSuffix) != 1 {
					t.Fatalf("expected suffixes %x < %x", suffix, newSuffix)
				}
				if Comparer.Compare(slices.Concat(prefixes[0], suffix), slices.Concat(prefixes[0], newSuffix)) != 0 {
					t.Fatalf("expected keys with suffixes %x and %x to be equal", suffix, newSuffix)
				}
				suffixes = append(suffixes, newSuffix)
				suffix = newSuffix
			}
			if len(suffix) != withLogical {
				t.Fatalf("unexpected suffix %x", suffix)
			}
			// Append a synthetic bit that should be ignored in key comparisons, but
			// not suffix comparisons.
			newSuffix := slices.Concat(suffix[:withLogical-1], []byte{1}, []byte{withSynthetic})
			if Comparer.CompareSuffixes(suffix, newSuffix) != 1 {
				t.Fatalf("expected suffixes %x < %x", suffix, newSuffix)
			}
			if Comparer.Compare(slices.Concat(prefixes[0], suffix), slices.Concat(prefixes[0], newSuffix)) != 0 {
				t.Fatalf("expected keys with suffixes %x and %x to be equal", suffix, newSuffix)
			}
			suffixes = append(suffixes, newSuffix)
		}
	}
	// Add some lock table suffixes.
	suffixes = append(suffixes, append(bytes.Repeat([]byte{1}, withLockTableLen-1), withLockTableLen))
	suffixes = append(suffixes, append(bytes.Repeat([]byte{2}, withLockTableLen-1), withLockTableLen))
	if err := base.CheckComparer(&Comparer, prefixes, suffixes); err != nil {
		t.Error(err)
	}
}

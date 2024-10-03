// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package crdbtest

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/testutils"
	"golang.org/x/exp/rand"
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

func TestDataDriven(t *testing.T) {
	var keys [][]byte
	datadriven.RunTest(t, "testdata", func(t *testing.T, d *datadriven.TestData) string {
		seed := uint64(1234)
		count := 10
		valueLen := 4
		cfg := KeyConfig{
			PrefixAlphabetLen: 8,
			PrefixLenShared:   4,
			PrefixLen:         8,
			AvgKeysPerPrefix:  2,
			PercentLogical:    10,
		}
		const layout = "2006-01-02T15:04:05"
		baseWallTime := "2020-01-01T00:00:00"
		d.MaybeScanArgs(t, "seed", &seed)
		d.MaybeScanArgs(t, "count", &count)
		d.MaybeScanArgs(t, "alpha-len", &cfg.PrefixAlphabetLen)
		d.MaybeScanArgs(t, "prefix-len-shared", &cfg.PrefixLenShared)
		d.MaybeScanArgs(t, "prefix-len", &cfg.PrefixLen)
		d.MaybeScanArgs(t, "avg-keys-pre-prefix", &cfg.AvgKeysPerPrefix)
		d.MaybeScanArgs(t, "percent-logical", &cfg.PercentLogical)
		d.MaybeScanArgs(t, "base-wall-time", &baseWallTime)
		d.MaybeScanArgs(t, "value-len", &valueLen)
		cfg.BaseWallTime = uint64(testutils.CheckErr(time.Parse(layout, baseWallTime)).UnixNano())

		rng := rand.New(rand.NewSource(seed))
		var buf strings.Builder
		switch d.Cmd {
		case "rand-kvs":
			var vals [][]byte
			keys, vals = RandomKVs(rng, count, cfg, valueLen)
			for i, key := range keys {
				n := Split(key)
				prefix := key[:n-1]
				suffix := key[n : len(key)-1]
				fmt.Fprintf(&buf, "%s @ %X = %X\n", prefix, suffix, vals[i])
			}

		case "rand-query-keys":
			queryKeys := RandomQueryKeys(rng, count, keys, cfg.BaseWallTime)
			for _, key := range queryKeys {
				n := Split(key)
				prefix := key[:n-1]
				suffix := key[n : len(key)-1]
				fmt.Fprintf(&buf, "%s @ %X\n", prefix, suffix)
			}

		default:
			d.Fatalf(t, "unknown command %q", d.Cmd)
		}
		return buf.String()
	})
}

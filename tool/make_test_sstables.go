// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build make_test_sstables

// Run using: go run -tags make_test_sstables ./tool/make_test_sstables.go
package main

import (
	"log"
	"math/rand/v2"

	"github.com/cockroachdb/pebble/cockroachkvs"
	"github.com/cockroachdb/pebble/internal/sstableinternal"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

func makeOutOfOrderSST() {
	fs := vfs.Default
	f, err := fs.Create("tool/testdata/out-of-order.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		log.Fatal(err)
	}
	opts := sstable.WriterOptions{
		TableFormat: sstable.TableFormatPebblev1,
	}
	opts.SetInternal(sstableinternal.WriterOptions{
		DisableKeyOrderChecks: true,
	})
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), opts)

	set := func(key string) {
		if err := w.Set([]byte(key), nil); err != nil {
			log.Fatal(err)
		}
	}

	set("a")
	set("c")
	set("b")

	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
}

func makeCockroachSchemaSST() {
	fs := vfs.Default
	f, err := fs.Create("tool/testdata/cr-schema.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		log.Fatal(err)
	}
	opts := sstable.WriterOptions{
		TableFormat: sstable.TableFormatMax,
		Comparer:    &cockroachkvs.Comparer,
		KeySchema:   &cockroachkvs.KeySchema,
		BlockSize:   32,
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), opts)

	//	2025/04/03 15:38:23 background error: pebble: keys must be added in strictly increasing order:
	//   tcestilr\x00\x00\x00\x00\x1a5\xaddx\x09#20,SET
	//   tcestilr\x00\x00\x00\x01\xc8\xc0@\xe4%\x09#19,SET

	cfg := cockroachkvs.KeyGenConfig{
		PrefixAlphabetLen:  20,
		PrefixLenShared:    2,
		RoachKeyLen:        8,
		AvgKeysPerPrefix:   2,
		BaseWallTime:       1,
		PercentLogical:     10,
		PercentEmptySuffix: 5,
		PercentLockSuffix:  5,
	}
	rng := rand.New(rand.NewPCG(1, 1))
	keys, vals := cockroachkvs.RandomKVs(rng, 20, cfg, 16)

	for i := range keys {
		if err := w.Set(keys[i], vals[i]); err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	makeOutOfOrderSST()
	makeCockroachSchemaSST()
}

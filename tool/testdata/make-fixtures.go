// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/objstorage/remote"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func makeBrokenExternalDB() {
	const version = pebble.FormatSyntheticPrefixSuffix
	const dbName = "broken-external-db"
	opts := &pebble.Options{
		FormatMajorVersion:          version,
		DisableAutomaticCompactions: true,
		ErrorIfExists:               true,
	}
	store := remote.NewInMem()
	opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
		"external": store,
	})
	opts.EnsureDefaults()

	f, err := store.CreateObject("foo.sst")
	if err != nil {
		log.Fatal(err)
	}
	w := sstable.NewWriter(objstorageprovider.NewRemoteWritable(f), opts.MakeWriterOptions(0, version.MaxTableFormat()))
	for _, k := range strings.Fields("a25 b1 b2 b3 c15") {
		err := w.Set([]byte(k), []byte(k))
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	path, err := filepath.Abs(dbName)
	if err != nil {
		log.Fatal(err)
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	for _, k := range strings.Fields("a10 a20 a30") {
		if err := db.Set([]byte(k), []byte(k), pebble.Sync); err != nil {
			log.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := db.Compact(
		context.Background(), []byte("a"), []byte("b"), false /* parallelize */); err != nil {
		log.Fatal(err)
	}
	for _, k := range strings.Fields("c10 c20 c30") {
		if err := db.Set([]byte(k), []byte(k), pebble.Sync); err != nil {
			log.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := db.Compact(
		context.Background(), []byte("c"), []byte("d"), false /* parallelize */); err != nil {
		log.Fatal(err)
	}

	if _, err := db.IngestExternalFiles(context.Background(), []pebble.ExternalFile{{
		Locator:     "external",
		ObjName:     "foo.sst",
		Size:        123,
		StartKey:    []byte("a25"),
		EndKey:      []byte("c19"),
		HasPointKey: true,
	}}); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Generated db with following LSM:\n%s\n", db.DebugString())
	err = db.Close()
	db = nil
	if err != nil {
		log.Fatal(err)
	}
}

func makeCRSchemaDB() {
	const version = pebble.FormatWALSyncChunks
	const dbName = "cr-schema-db"
	opts := &pebble.Options{
		FormatMajorVersion:          version,
		DisableAutomaticCompactions: true,
		ErrorIfExists:               true,
		Comparer:                    &cockroachkvs.Comparer,
		KeySchema:                   cockroachkvs.KeySchema.Name,
		KeySchemas:                  sstable.MakeKeySchemas(&cockroachkvs.KeySchema),
	}
	opts.EnsureDefaults()

	path, err := filepath.Abs(dbName)
	if err != nil {
		log.Fatal(err)
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	rng := rand.New(rand.NewPCG(1, 1))
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
	keys, vals := cockroachkvs.RandomKVs(rng, 50, cfg, 16)
	for i := range keys {
		if err := db.Set(keys[i], vals[i], pebble.Sync); err != nil {
			log.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := db.Compact(
		context.Background(), keys[0], keys[len(keys)-1], false /* parallelize */); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Generated db with following LSM:\n%s\n", db.DebugString())
	err = db.Close()
	db = nil
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	makeBrokenExternalDB()
	makeCRSchemaDB()
}

// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cockroachdb/pebble/v2"
)

const version = pebble.FormatFlushableIngest

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [1,2,3,4]\n", os.Args[0])
	os.Exit(1)
}

func main() {
	if len(os.Args) != 2 {
		usage()
	}
	// The program consists of up to 4 stages. If stage is in the range [1, 4],
	// the program will exit after the stage'th stage.
	// 1. create an empty DB.
	// 2. add some key/value pairs.
	// 3. close and re-open the DB, which forces a compaction.
	// 4. add some more key/value pairs.
	stage, err := strconv.Atoi(os.Args[1])
	if err != nil || stage < 1 || stage > 4 {
		usage()
	}
	dbName := fmt.Sprintf("db-stage-%d", stage)
	opts := &pebble.Options{
		FormatMajorVersion: version,
	}

	fmt.Printf("Stage 1\n")
	db, err := pebble.Open(dbName, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	if stage < 2 {
		return
	}
	fmt.Printf("Stage 2\n")

	if err := db.Set([]byte("foo"), []byte("one"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	if err := db.Set([]byte("bar"), []byte("two"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	if err := db.Set([]byte("baz"), []byte("three"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	if err := db.Set([]byte("foo"), []byte("four"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	if err := db.Delete([]byte("bar"), pebble.Sync); err != nil {
		log.Fatal(err)
	}

	if stage < 3 {
		return
	}
	fmt.Printf("Stage 3\n")

	db.Close()
	db = nil
	db, err = pebble.Open(dbName, opts)
	if err != nil {
		log.Fatal(err)
	}

	if stage < 4 {
		return
	}
	fmt.Printf("Stage 4\n")

	if err := db.Set([]byte("foo"), []byte("five"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	if err := db.Set([]byte("quux"), []byte("six"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	if err := db.Delete([]byte("baz"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
}

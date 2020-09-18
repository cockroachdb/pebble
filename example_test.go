// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble_test

import (
	"fmt"
	"log"
	"os"

	"github.com/cockroachdb/pebble"
)

func Example() {
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// Don't do this in real life, this is just example code.
		_ = os.RemoveAll("demo")
	}()
	key := []byte("hello")
	if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
		log.Fatal(err)
	}
	value, closer, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s\n", key, value)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// hello world
}

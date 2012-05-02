// Copyright 2012 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This program locks a file for 3 seconds, unlocks the file, sleeps for a
// further 3 seconds, and exits.
//
// Running the program by itself is mostly uninteresting. Run this program
// multiple times simultaneously to observe process-level mutual exclusion.
package main

import (
	"fmt"
	"os"
	"time"

	"code.google.com/p/leveldb-go/leveldb/db"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s filename", os.Args[0])
		os.Exit(1)
	}
	filename := os.Args[1]

	// Avoid truncating an existing, non-empty file.
	fi, err := os.Stat(filename)
	if err == nil && fi.Size() != 0 {
		fmt.Printf("The file %s is not empty\n", filename)
		os.Exit(1)
	}

	fmt.Printf("Locking %s\n", filename)
	lock, err := db.DefaultFileSystem.Lock(filename)
	if err != nil {
		fmt.Printf("Could not lock %s: %v\n", filename, err)
		os.Exit(1)
	}

	time.Sleep(3 * time.Second)

	fmt.Printf("Unlocking %s\n", filename)
	if err := lock.Close(); err != nil {
		fmt.Printf("Could not unlock %s: %v\n", filename, err)
		os.Exit(1)
	}

	time.Sleep(3 * time.Second)

	fmt.Printf("OK\n")
}

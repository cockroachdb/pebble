// Copyright 2014 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package db_test

import (
	"bytes"
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"

	"code.google.com/p/leveldb-go/leveldb/db"
)

var lockFilename = flag.String("lockfile", "", "File to lock.  Non-empty value pimples child process.")

func spawn(prog, filename string) ([]byte, error) {
	return exec.Command(prog, "-lockfile", filename, "-test.v",
		"-test.run=TestLock$").CombinedOutput()
}

// TestLock locks a file, spawns a second process that attempts to grab the
// lock to verify it fails.
// Then it closes the lock, and spawns a third copy to verify it can be
// relocked.
func TestLock(t *testing.T) {
	child := *lockFilename != ""
	var filename string
	if child {
		filename = *lockFilename
	} else {
		f, err := ioutil.TempFile("", "")
		if err != nil {
			t.Fatal(err)
		}
		filename = f.Name()
		defer os.Remove(filename)
	}

	// Avoid truncating an existing, non-empty file.
	fi, err := os.Stat(filename)
	if err == nil && fi.Size() != 0 {
		t.Fatal("The file %s is not empty", filename)
	}

	t.Logf("Locking %s\n", filename)
	lock, err := db.DefaultFileSystem.Lock(filename)
	if err != nil {
		t.Fatalf("Could not lock %s: %v", filename, err)
	}

	if !child {
		t.Logf("Spawning child, should fail to grab lock.")
		out, err := spawn(os.Args[0], filename)
		if err == nil {
			t.Fatalf("Attempt to grab open lock should have failed.\n%s", out)
		}
		if !bytes.Contains(out, []byte("Could not lock")) {
			t.Fatalf("Child failed with unexpected output: %s\n", out)
		}
		t.Logf("Child failed to grab lock as expected.")
	}

	t.Logf("Unlocking %s", filename)
	if err := lock.Close(); err != nil {
		t.Fatalf("Could not unlock %s: %v", filename, err)
	}

	if !child {
		t.Logf("Spawning child, should successfully grab lock.")
		if out, err := spawn(os.Args[0], filename); err != nil {
			t.Fatalf("Attempt to re-open lock should have succeeded: %v\n%s",
				err, out)
		}
		t.Logf("Child grabbed lock.")
	}
}

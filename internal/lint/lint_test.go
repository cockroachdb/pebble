// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lint

import (
	"bytes"
	"go/build"
	"os/exec"
	"testing"

	"github.com/ghemawat/stream"
)

func dirCmd(
	t *testing.T, dir string, name string, args ...string,
) stream.Filter {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	switch err.(type) {
	case nil:
	case *exec.ExitError:
		// Non-zero exit is expected.
	default:
		t.Fatal(err)
	}
	return stream.ReadLines(bytes.NewReader(out))
}

func TestLint(t *testing.T) {
	const root = "github.com/cockroachdb/pebble"

	pkg, err := build.Import(root, "", 0)
	if err != nil {
		t.Fatal(err)
	}

	f := dirCmd(t, "", "go", "list", root+"/...")
	var pkgs []string
	stream.ForEach(f, func(s string) {
		pkgs = append(pkgs, s)
	})

	t.Run("TestGolint", func(t *testing.T) {
		t.Parallel()

		filter := dirCmd(t, pkg.Dir, "golint", pkgs...)
		// This is overkill right now, but provides a structure for filtering out
		// lint errors we don't care about.
		if err := stream.ForEach(
			stream.Sequence(
				filter,
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestGoVet", func(t *testing.T) {
		t.Parallel()

		filter := dirCmd(t, pkg.Dir, "go", "vet", "-all", "./...")
		if err := stream.ForEach(
			stream.Sequence(
				filter,
				stream.GrepNot(`^#`), // ignore comment lines
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}
	})
}

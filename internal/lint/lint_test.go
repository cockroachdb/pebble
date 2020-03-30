// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lint

import (
	"bytes"
	"go/build"
	"os/exec"
	"runtime"
	"testing"

	"github.com/ghemawat/stream"
	"github.com/stretchr/testify/require"
)

func dirCmd(t *testing.T, dir string, name string, args ...string) stream.Filter {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	switch err.(type) {
	case nil:
	case *exec.ExitError:
		// Non-zero exit is expected.
	default:
		require.NoError(t, err)
	}
	return stream.ReadLines(bytes.NewReader(out))
}

func ignoreGoMod() stream.Filter {
	return stream.GrepNot(`^go: (finding|extracting|downloading)`)
}

func TestLint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("lint checks skipped on Windows")
	}

	const root = "github.com/cockroachdb/pebble"

	pkg, err := build.Import(root, "../..", 0)
	require.NoError(t, err)

	var pkgs []string
	if err := stream.ForEach(
		stream.Sequence(
			dirCmd(t, pkg.Dir, "go", "list", "./..."),
			ignoreGoMod(),
		), func(s string) {
			pkgs = append(pkgs, s)
		}); err != nil {
		require.NoError(t, err)
	}

	t.Run("TestGolint", func(t *testing.T) {
		t.Parallel()

		// This is overkill right now, but provides a structure for filtering out
		// lint errors we don't care about.
		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "golint", pkgs...),
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestGoVet", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "go", "vet", "-all", "./..."),
				stream.GrepNot(`^#`), // ignore comment lines
				ignoreGoMod(),
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}
	})
}

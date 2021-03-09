// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lint

import (
	"bytes"
	"go/build"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
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

	t.Run("TestFmtErrorf", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "git", "grep", "fmt\\.Errorf("),
				stream.GrepNot(`^vendor/`), // ignore vendor
			), func(s string) {
				t.Errorf("\n%s <- please use \"errors.Errorf\" instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestOSIsErr", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "git", "grep", "os\\.Is"),
				stream.GrepNot(`^vendor/`), // ignore vendor
			), func(s string) {
				t.Errorf("\n%s <- please use the \"oserror\" equivalent instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestSetFinalizer", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "git", "grep", "runtime\\.SetFinalizer("),
				stream.GrepNot(`^vendor/`), // ignore vendor
				stream.GrepNot(`^internal/invariants/finalizer_on.go`),
			), func(s string) {
				t.Errorf("\n%s <- please use the \"invariants.SetFinalizer\" equivalent instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestForbiddenImports", func(t *testing.T) {
		t.Parallel()

		// Forbidden-import-pkg -> permitted-replacement-pkg
		forbiddenImports := map[string]string{
			"errors":     "github.com/cockroachdb/errors",
			"pkg/errors": "github.com/cockroachdb/errors",
		}

		// grepBuf creates a grep string that matches any forbidden import pkgs.
		var grepBuf bytes.Buffer
		grepBuf.WriteByte('(')
		for forbiddenPkg := range forbiddenImports {
			grepBuf.WriteByte('|')
			grepBuf.WriteString(regexp.QuoteMeta(forbiddenPkg))
		}
		grepBuf.WriteString(")$")

		filter := stream.FilterFunc(func(arg stream.Arg) error {
			for _, path := range pkgs {
				buildContext := build.Default
				buildContext.UseAllFiles = true
				importPkg, err := buildContext.Import(path, pkg.Dir, 0)
				if _, ok := err.(*build.MultiplePackageError); ok {
					buildContext.UseAllFiles = false
					importPkg, err = buildContext.Import(path, pkg.Dir, 0)
				}

				switch err.(type) {
				case nil:
					for _, s := range importPkg.Imports {
						arg.Out <- importPkg.ImportPath + ": " + s
					}
					for _, s := range importPkg.TestImports {
						arg.Out <- importPkg.ImportPath + ": " + s
					}
					for _, s := range importPkg.XTestImports {
						arg.Out <- importPkg.ImportPath + ": " + s
					}
				case *build.NoGoError:
				default:
					return errors.Wrapf(err, "error loading package %s", path)
				}
			}
			return nil
		})
		if err := stream.ForEach(stream.Sequence(
			filter,
			stream.Sort(),
			stream.Uniq(),
			stream.Grep(grepBuf.String()),
		), func(s string) {
			pkgStr := strings.Split(s, ": ")
			importedPkg := pkgStr[1]

			// Test that a disallowed package is not imported.
			if replPkg, ok := forbiddenImports[importedPkg]; ok {
				t.Errorf("\n%s <- please use %q instead of %q", s, replPkg, importedPkg)
			}
		}); err != nil {
			t.Error(err)
		}
	})
}

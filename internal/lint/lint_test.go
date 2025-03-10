// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package lint

import (
	"bytes"
	"fmt"
	"go/build"
	"os/exec"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/buildtags"
	"github.com/ghemawat/stream"
	"github.com/stretchr/testify/require"
)

const (
	staticcheck = "honnef.co/go/tools/cmd/staticcheck"
	crlfmt      = "github.com/cockroachdb/crlfmt"
	gcassert    = "github.com/jordanlewis/gcassert/cmd/gcassert"
	roachvet    = "github.com/cockroachdb/pebble/internal/devtools/roachvet"
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

func installTool(t *testing.T, path string) {
	cmd := exec.Command("go", "install", "-C", "../devtools", path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cannot install %q: %v\n%s\n", path, err, out)
	}
}

func TestLint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("lint checks skipped on Windows")
	}
	if runtime.GOARCH == "386" {
		// GOARCH=386 messes with the installation of devtools.
		t.Skip("lint checks skipped on GOARCH=386")
	}
	if buildtags.Instrumented {
		// We are not interested in race-testing the linters themselves.
		t.Skip("lint checks skipped on instrumented builds")
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

	// TestGoVet is the fastest check that verifies that all files build, so we
	// want to run it first (and not in parallel).
	t.Run("TestGoVet", func(t *testing.T) {
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

	// In most cases, go vet fails because of a build error; running the rest of
	// the checks would just result in a lot of noise.
	if t.Failed() {
		t.Fatal("go vet failed; skipping other lint checks")
	}

	t.Run("TestGCAssert", func(t *testing.T) {
		installTool(t, gcassert)
		t.Parallel()

		// Build a list of all packages that contain a gcassert directive.
		var packages []string
		if err := stream.ForEach(
			dirCmd(
				t, pkg.Dir, "git", "grep", "-nE", `// ?gcassert`,
			), func(s string) {
				// s here is of the form
				//   some/package/file.go:123:// gcassert:inline
				// and we want to extract the package path.
				filePath := s[:strings.Index(s, ":")]                  // up to the line number
				pkgPath := filePath[:strings.LastIndex(filePath, "/")] // up to the file name
				path := fmt.Sprintf("./%s", pkgPath)
				if !slices.Contains(packages, path) {
					packages = append(packages, path)
				}
			}); err != nil {
			t.Error(err)
		}
		slices.Sort(packages)

		if err := stream.ForEach(
			dirCmd(t, pkg.Dir, "gcassert", packages...),
			func(s string) {
				if strings.HasPrefix(s, "See ") && strings.HasSuffix(s, " for full output.") {
					t.Log(s)
					return
				}
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestStaticcheck", func(t *testing.T) {
		installTool(t, staticcheck)
		t.Parallel()

		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "staticcheck", pkgs...),
				ignoreGoMod(),
			), func(s string) {
				t.Errorf("\n%s", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestRoachVet", func(t *testing.T) {
		installTool(t, roachvet)
		out, err := exec.Command("which", "roachvet").Output()
		if err != nil {
			t.Fatal(err)
		}
		roachVetPath := strings.TrimSpace(string(out))
		t.Parallel()
		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "go", "vet", "-vettool="+roachVetPath, "./..."),
				ignoreGoMod(),
			), func(s string) {
				t.Errorf("%s", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestFmtErrorf", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			dirCmd(t, pkg.Dir, "git", "grep", "fmt\\.Errorf("),
			func(s string) {
				t.Errorf("\n%s <- please use \"errors.Errorf\" instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestOSIsErr", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			dirCmd(t, pkg.Dir, "git", "grep", "os\\.Is"),
			func(s string) {
				t.Errorf("\n%s <- please use the \"oserror\" equivalent instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestSetFinalizer", func(t *testing.T) {
		t.Parallel()

		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "git", "grep", "-B1", "runtime\\.SetFinalizer("),
				lintIgnore("lint:ignore SetFinalizer"),
				stream.GrepNot(`^internal/invariants/invariants.go`),
			), func(s string) {
				t.Errorf("\n%s <- please use the \"invariants.SetFinalizer\" equivalent instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	// Disallow "raw" atomics; wrappers like atomic.Int32 provide much better
	// safety and alignment guarantees.
	t.Run("TestRawAtomics", func(t *testing.T) {
		t.Parallel()
		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "git", "grep", `atomic\.\(Load\|Store\|Add\|Swap\|Compare\)`),
				lintIgnore("lint:ignore RawAtomics"),
			), func(s string) {
				t.Errorf("\n%s <- please use atomic wrappers (like atomic.Int32) instead", s)
			}); err != nil {
			t.Error(err)
		}
	})

	t.Run("TestForbiddenImports", func(t *testing.T) {
		t.Parallel()

		// Forbidden-import-pkg -> permitted-replacement-pkg
		forbiddenImports := map[string]string{
			"errors":                  "github.com/cockroachdb/errors",
			"pkg/errors":              "github.com/cockroachdb/errors",
			"golang.org/x/exp/slices": "slices",
			"golang.org/x/exp/rand":   "math/rand/v2",
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

	t.Run("TestCrlfmt", func(t *testing.T) {
		installTool(t, crlfmt)
		t.Parallel()

		args := []string{"-fast", "-tab", "2", "."}
		var buf bytes.Buffer
		if err := stream.ForEach(
			stream.Sequence(
				dirCmd(t, pkg.Dir, "crlfmt", args...),
				stream.GrepNot("go: downloading"),
			),
			func(s string) {
				fmt.Fprintln(&buf, s)
			}); err != nil {
			t.Error(err)
		}
		errs := buf.String()
		if len(errs) > 0 {
			t.Errorf("\n%s", errs)
		}

		if t.Failed() {
			reWriteCmd := []string{"crlfmt", "-w"}
			reWriteCmd = append(reWriteCmd, args...)
			t.Logf("run the following to fix your formatting:\n"+
				"\n%s\n\n"+
				"Don't forget to add amend the result to the correct commits.",
				strings.Join(reWriteCmd, " "),
			)
		}
	})
}

// lintIgnore is a stream.FilterFunc that filters out lines that are preceded by
// the given ignore directive. The function assumes the input stream receives a
// sequence of strings that are to be considered as pairs. If the first string
// in the sequence matches the ignore directive, the following string is
// dropped, else it is emitted.
//
// For example, given the sequence "foo", "bar", "baz", "bam", and an ignore
// directive "foo", the sequence "baz", "bam" would be emitted. If the directive
// was "baz", the sequence "foo", "bar" would be emitted.
func lintIgnore(ignore string) stream.FilterFunc {
	return func(arg stream.Arg) error {
		var prev string
		var i int
		for s := range arg.In {
			if i%2 == 0 {
				// Fist string in the pair is used as the filter. Store it.
				prev = s
			} else {
				// Second string is emitted only if it _does not_ match the directive.
				if !strings.Contains(prev, ignore) {
					arg.Out <- s
				}
			}
			i++
		}
		return nil
	}
}

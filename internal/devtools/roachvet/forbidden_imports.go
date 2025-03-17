// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/packages"
)

// Define the list of top-level packages to check.
var packagesToCheck = map[string]bool{
	"github.com/cockroachdb/pebble":            true,
	"github.com/cockroachdb/pebble/sstable":    true,
	"github.com/cockroachdb/pebble/cmd/pebble": true,
}

// Forbidden packages (direct & transitive dependencies).
var forbiddenPackages = map[string]string{
	"testing": "Unit test utilities should not be imported by this package.",
}

// ForbiddenImportsAnalyzer verifies that certain packages don't depend (dirctly
// or indirectly) on a list of forbidden imports.
var ForbiddenImportsAnalyzer = &analysis.Analyzer{
	Name: "forbidimport",
	Doc:  "Checks for forbidden direct and transitive imports in specific top-level packages",
	Run:  run,
}

// Checks direct imports in the file.
func checkDirectImports(pass *analysis.Pass, file *ast.File) {
	fileName := pass.Fset.Position(file.Pos()).Filename

	// Skip test files.
	if strings.HasSuffix(fileName, "_test.go") {
		return
	}

	for _, imp := range file.Imports {
		importPath := imp.Path.Value[1 : len(imp.Path.Value)-1] // Remove quotes
		if reason, forbidden := forbiddenPackages[importPath]; forbidden {
			pass.Reportf(imp.Pos(), "forbidden import %q: %s", importPath, reason)
		}
	}
}

// Checks transitive dependencies using go/packages.
func checkTransitiveImports(pass *analysis.Pass) {
	cfg := &packages.Config{
		Mode: packages.NeedImports | packages.NeedDeps | packages.NeedName,
	}
	pkgs, err := packages.Load(cfg, pass.Pkg.Path())
	if err != nil {
		pass.Reportf(token.NoPos, "error loading package dependencies: %v", err)
		return
	}

	visited := make(map[string]bool)

	var checkDeps func(pkg *packages.Package, pkgPaths ...string)
	checkDeps = func(pkg *packages.Package, pkgPaths ...string) {
		if visited[pkg.PkgPath] {
			return
		}
		visited[pkg.PkgPath] = true

		// Skip test packages
		if strings.HasSuffix(pkg.PkgPath, "_test") {
			return
		}
		pkgPaths = append(pkgPaths, pkg.PkgPath)

		if reason, forbidden := forbiddenPackages[pkg.PkgPath]; forbidden {
			var buf strings.Builder
			fmt.Fprintf(&buf, "%s\n", reason)
			for i := range pkgPaths {
				fmt.Fprintf(&buf, "  %s%s\n", strings.Repeat(" ", i), pkgPaths[i])
			}
			pass.Reportf(token.NoPos, "forbidden transitive dependency: %s", buf.String())
		}

		for _, imp := range pkg.Imports {
			checkDeps(imp, pkgPaths...)
		}
	}

	for _, pkg := range pkgs {
		checkDeps(pkg)
	}
}

func run(pass *analysis.Pass) (interface{}, error) {
	// Only check allowed top-level packages
	if !packagesToCheck[pass.Pkg.Path()] {
		return nil, nil
	}

	// Check direct imports
	for _, file := range pass.Files {
		checkDirectImports(pass, file)
	}

	// Check transitive imports
	checkTransitiveImports(pass)

	return nil, nil
}

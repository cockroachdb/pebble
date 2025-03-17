// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/deferloop"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/nocopy"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/returnerrcheck"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() {
	unitchecker.Main(
		deferloop.Analyzer,
		nocopy.Analyzer,
		returnerrcheck.Analyzer,
		ForbiddenImportsAnalyzer,
	)
}

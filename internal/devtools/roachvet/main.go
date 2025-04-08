// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/deferloop"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/nocopy"
	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/returnerrcheck"
	"github.com/kisielk/errcheck/errcheck"
	"golang.org/x/tools/go/analysis/unitchecker"
)

const errcheckExcludes = `fmt.Fprint
fmt.Fprintf
fmt.Fprintln
(*bufio.Writer).Flush
(*database/sql.DB).Close
(*database/sql.Rows).Close
(*database/sql.Stmt).Close
(*os.File).Close
(io.Closer).Close
(net.Conn).Close
(*compress/gzip.Reader).Close
(*strings.Builder).WriteByte
(*strings.Builder).WriteRune
(*strings.Builder).WriteString
(*strings.Builder).Write
(fmt.State).Write
crypto/rand.Read
`

func initErrCheck() (cleanup func()) {
	// Write the excludes to a temporary files, so we can pass it to errcheck.
	f, err := os.CreateTemp("", "errcheck-excludes")
	if err != nil {
		panic(err)
	}
	path := f.Name()
	if _, err := f.Write([]byte(errcheckExcludes)); err != nil {
		panic(err)
	}
	if err := f.Close(); err != nil {
		panic(err)
	}
	errcheck.Analyzer.Flags.Set("exclude", path)
	return func() {
		os.Remove(path)
	}
}

func main() {
	defer initErrCheck()()

	unitchecker.Main(
		deferloop.Analyzer,
		errcheck.Analyzer,
		nocopy.Analyzer,
		returnerrcheck.Analyzer,
		ForbiddenImportsAnalyzer,
	)
}

// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sstable

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/compression"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/stretchr/testify/require"
)

func runTestFixtureOutput(fixture TestFixtureInfo) error {
	// Check that a freshly made table is byte-for-byte equal to a pre-made
	// table.
	want, err := os.ReadFile(filepath.Join("testdata", fixture.Filename))
	if err != nil {
		return err
	}

	fs := vfs.NewMem()
	if err := fixture.Build(fs, "test.sst"); err != nil {
		return err
	}
	f, err := fs.Open("test.sst")
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	got := make([]byte, stat.Size())
	_, err = f.ReadAt(got, 0)
	if err != nil {
		return err
	}

	var rewrite bool
	for _, arg := range os.Args {
		rewrite = rewrite || arg == "--rewrite"
	}
	if rewrite {
		return os.WriteFile(filepath.Join("testdata", fixture.Filename), got, 0644)
	}

	if !bytes.Equal(got, want) {
		i := 0
		for ; i < len(got) && i < len(want) && got[i] == want[i]; i++ {
		}
		os.WriteFile("fail.txt", got, 0644)
		return errors.Errorf("built table testdata/%s does not match pre-made table. From byte %d onwards,\ngot:\n% x\nwant:\n% x",
			fixture.Filename, i, got[i:], want[i:])
	}
	return nil
}

func TestFixtureOutput(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, fixture := range TestFixtures {
		// Note: we disabled the zstd fixture test when CGO_ENABLED=0, because the
		// implementation between DataDog/zstd and klauspost/compress are
		// different, which leads to different compression output
		// <https://github.com/klauspost/compress/issues/109#issuecomment-498763233>.
		// Since the fixture test requires bit-to-bit reproducibility, we cannot
		// run the zstd test when the implementation is not based on facebook/zstd.
		if !compression.UseStandardZstdLib && fixture.Compression == block.ZstdCompression {
			continue
		}
		t.Run(fixture.Filename, func(t *testing.T) {
			require.NoError(t, runTestFixtureOutput(fixture))
		})
	}
}

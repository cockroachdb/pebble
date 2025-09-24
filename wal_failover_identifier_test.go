// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/wal"
	"github.com/stretchr/testify/require"
)

// TestWALFailoverIdentifier tests the WAL failover identifier mechanism, which
// ensures that the correct secondary WAL directory is being used during database
// recovery. The identifier is a unique string stored in a "failover_identifier"
// file in the secondary WAL directory that helps prevent accidental use of the
// wrong secondary directory (e.g., when the wrong disk is mounted).
//
// The test covers three scenarios:
//  1. first_time_generation: when opening a database with WAL failover for the
//     first time, a new identifier should be generated and written to both the
//     secondary directory and the OPTIONS file
//  2. identifier_mismatch_error: when the secondary directory contains a
//     different identifier than what's expected in the options, the database
//     should fail to open with an error indicating the wrong disk may be mounted
//  3. no_primary_has_secondary: when no identifier is specified in options but
//     the secondary directory already contains an identifier, the database should
//     adopt the existing identifier and write it to the OPTIONS file
func TestWALFailoverIdentifier(t *testing.T) {
	t.Run("first_time_generation", func(t *testing.T) {
		mem := vfs.NewMem()

		// First time opening with WAL failover should generate an identifier.
		opts := &Options{
			FS: mem,
			WALFailover: &WALFailoverOptions{
				Secondary: wal.Dir{Dirname: "secondary", FS: mem},
			},
		}

		db, err := Open("testdb", opts)
		require.NoError(t, err, "failed to open database")
		defer db.Close()

		// Check that identifier file exists in secondary directory.
		identifierFile := mem.PathJoin("secondary", "failover_identifier")
		failoverId, err := mem.Open(identifierFile)
		require.NoError(t, err)

		// Read the identifier from the file.
		defer failoverId.Close()
		data := make([]byte, 100)
		n, err := failoverId.Read(data)
		if err != nil && err != io.EOF {
			t.Fatalf("failed to read identifier file: %v", err)
		}

		identifier := strings.TrimSpace(string(data[:n]))
		require.True(t, identifier != "", "expected identifier to be written to file")
		t.Logf("identifier from failover_identifier file: %q", identifier)

		// Check that the identifier is written to the OPTIONS file.
		entries, err := mem.List("testdb")
		require.NoError(t, err, "failed to list testdb directory")
		var optionsFile string
		for _, entry := range entries {
			if strings.HasPrefix(entry, "OPTIONS-") {
				optionsFile = mem.PathJoin("testdb", entry)
				break
			}
		}
		require.NotEmpty(t, optionsFile, "OPTIONS file should exist")

		optionsF, err := mem.Open(optionsFile)
		require.NoError(t, err, "failed to open OPTIONS file")
		defer optionsF.Close()

		optionsData, err := io.ReadAll(optionsF)
		require.NoError(t, err, "failed to read OPTIONS file")

		optionsContent := string(optionsData)
		// Verify the OPTIONS file contains the [WAL Failover] section with the
		// identifier we found in the failover_identifier above.
		require.Contains(t, optionsContent, "[WAL Failover]",
			"OPTIONS file should contain WAL Failover section")
		require.Contains(t, optionsContent, "secondary_identifier="+identifier,
			"OPTIONS file should contain the generated identifier")
	})

	t.Run("identifier_mismatch_error", func(t *testing.T) {
		mem := vfs.NewMem()
		secondaryDir := "secondary"
		err := mem.MkdirAll(secondaryDir, 0755)
		require.NoError(t, err)

		// Create a secondary directory with a different identifier.
		identifierFile := mem.PathJoin("secondary", "failover_identifier")
		existingIdentifier := "44444444444444444444444444444444"
		err = writeTestIdentifier(mem, identifierFile, existingIdentifier)
		require.NoError(t, err)

		// Try to open with a different identifier in options.
		opts := &Options{
			FS: mem,
			WALFailover: &WALFailoverOptions{
				Secondary: wal.Dir{
					Dirname: "secondary",
					FS:      mem,
					ID:      "7f495fb4914ecfc04deeafa41de42b78",
				},
			},
		}

		_, err = Open("testdb", opts)
		require.Error(t, err, "expected error due to identifier mismatch")
		require.Contains(t, err.Error(), "wrong disk may be mounted")
	})

	t.Run("no_primary_has_secondary", func(t *testing.T) {
		// Test case: no identifier in primary options, secondary has identifier
		// The system should adopt the existing identifier from the secondary directory.
		mem := vfs.NewMem()
		identifierFile := mem.PathJoin("secondary", "failover_identifier")
		err := mem.MkdirAll("secondary", 0755)
		require.NoError(t, err)

		// Write an existing identifier to the secondary directory.
		existingIdentifier := "44444444444444444444444444444444"
		err = writeTestIdentifier(mem, identifierFile, existingIdentifier)
		require.NoError(t, err)

		// Open without specifying SecondaryIdentifier in options.
		opts := &Options{
			FS: mem,
			WALFailover: &WALFailoverOptions{
				Secondary: wal.Dir{Dirname: "secondary", FS: mem},
			},
		}

		db, err := Open("testdb", opts)
		require.NoError(t, err, "should succeed and adopt the existing identifier")
		defer db.Close()

		// Verify that the OPTIONS file now contains the adopted identifier.
		entries, err := mem.List("testdb")
		require.NoError(t, err, "failed to list testdb directory")
		var optionsFile string
		for _, entry := range entries {
			if strings.HasPrefix(entry, "OPTIONS-") {
				optionsFile = mem.PathJoin("testdb", entry)
				break
			}
		}
		require.NotEmpty(t, optionsFile, "OPTIONS file should exist")

		optionsF, err := mem.Open(optionsFile)
		require.NoError(t, err, "failed to open OPTIONS file")
		defer optionsF.Close()

		optionsData, err := io.ReadAll(optionsF)
		require.NoError(t, err, "failed to read OPTIONS file")

		optionsContent := string(optionsData)
		require.Contains(t, optionsContent, "[WAL Failover]",
			"OPTIONS file should contain WAL Failover section")
		require.Contains(t, optionsContent, "secondary_identifier="+existingIdentifier,
			"OPTIONS file should contain the adopted identifier")
	})
}

func writeTestIdentifier(fs vfs.FS, filename, identifier string) error {
	f, err := fs.Create(filename, "pebble-wal")
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.WriteString(f, identifier)
	if err != nil {
		return err
	}

	return f.Sync()
}

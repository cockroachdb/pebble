// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	dataDirPath = "./testdata/data"
	dataJSPath  = "./testdata/data.js"
)

func TestParseYCSB_FromScratch(t *testing.T) {
	// Write out a new data.js file from the input data.
	fPath := filepath.Join(t.TempDir(), "data.js")
	parseYCSB(dataDirPath, fPath, fPath)

	// Confirm the two data.js files are now equal.
	err := filesEqual(dataJSPath, fPath)
	require.NoError(t, err)
}

func TestYCSB_Existing(t *testing.T) {
	// Set up the test directory.
	testDir := t.TempDir()
	newDataDir := filepath.Join(testDir, "data")
	newDataJS := filepath.Join(testDir, "data.js")

	// Copy all files into the test dir excluding one day.
	err := copyDir(dataDirPath, newDataDir)
	require.NoError(t, err)
	err = os.RemoveAll(filepath.Join(newDataDir, "20211027"))
	require.NoError(t, err)

	// Construct the data.js file on the test data with a single day removed.
	parseYCSB(newDataDir, newDataJS, newDataJS)

	// Confirm the two data.js files are not equal.
	err = filesEqual(dataJSPath, newDataJS)
	require.Error(t, err)

	// Re-construct the data.js file with the full set of data.
	parseYCSB(dataDirPath, dataJSPath, newDataJS)

	// Confirm the two data.js files are now equal.
	err = filesEqual(dataJSPath, newDataJS)
	require.NoError(t, err)
}

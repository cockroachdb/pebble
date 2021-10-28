package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors/oserror"
	"github.com/stretchr/testify/require"
)

const (
	dataDirPath = "./testdata/data"
	dataJSPath  = "./testdata/data.js"
)

func Test_FromScratch(t *testing.T) {
	// Write out a new data.js file from the input data.
	fPath := filepath.Join(t.TempDir(), "data.js")
	ParseYCSB(dataDirPath, fPath, fPath)

	// Confirm the two data.js files are now equal.
	equal, err := filesEqual(dataJSPath, fPath)
	require.NoError(t, err)
	require.True(t, equal)
}

func Test_Existing(t *testing.T) {
	const includeDay = "20211027"

	// Set up the test directory.
	testDir := t.TempDir()
	newDataDir := filepath.Join(testDir, "data")
	err := os.Mkdir(newDataDir, 0700)
	require.NoError(t, err)
	newDataJS := filepath.Join(testDir, "data.js")

	// Copy all files into the test dir.
	err = filepath.Walk(dataDirPath, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}

		rel, err := filepath.Rel(dataDirPath, path)
		if err != nil {
			return err
		}

		// Exclude all but a single day's worth of data.
		if !strings.HasPrefix(rel, includeDay) {
			return nil
		}

		// Preserve the directory structure.
		if info.IsDir() {
			err := os.Mkdir(filepath.Join(newDataDir, rel), 0700)
			if err != nil && !oserror.IsNotExist(err) {
				return err
			}
			return nil
		}

		// Copy files.
		fIn, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() { _ = fIn.Close() }()

		fOut, err := os.OpenFile(filepath.Join(newDataDir, rel), os.O_CREATE|os.O_WRONLY, 0700)
		if err != nil {
			return err
		}
		defer func() { _ = fOut.Close() }()

		_, err = io.Copy(fOut, fIn)
		return err
	})
	require.NoError(t, err)

	// Construct the data.js file on the test data with a single day removed.
	ParseYCSB(newDataDir, newDataJS, newDataJS)

	// Confirm the two data.js files are not equal.
	equal, err := filesEqual(dataJSPath, newDataJS)
	require.NoError(t, err)
	require.False(t, equal)

	// Re-construct the data.js file with the full set of data.
	ParseYCSB(dataDirPath, dataJSPath, newDataJS)

	// Confirm the two data.js files are now equal.
	equal, err = filesEqual(dataJSPath, newDataJS)
	require.NoError(t, err)
	require.True(t, equal)
}

// filesEqual returns true if two files are byte-wise equal.
func filesEqual(a, b string) (bool, error) {
	aBytes, err := ioutil.ReadFile(a)
	if err != nil {
		return false, err
	}
	bBytes, err := ioutil.ReadFile(b)
	if err != nil {
		return false, err
	}

	// Normalize newlines.
	aBytes = bytes.Replace(aBytes, []byte{13, 10} /* \r\n */, []byte{10} /* \n */, -1)
	bBytes = bytes.Replace(bBytes, []byte{13, 10}, []byte{10}, -1)

	return bytes.Equal(aBytes, bBytes), nil
}

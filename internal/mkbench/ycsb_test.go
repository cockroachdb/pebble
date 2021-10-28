package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/kr/pretty"
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
	err := filesEqual(dataJSPath, fPath)
	require.NoError(t, err)
}

func Test_Existing(t *testing.T) {
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
	ParseYCSB(newDataDir, newDataJS, newDataJS)

	// Confirm the two data.js files are not equal.
	err = filesEqual(dataJSPath, newDataJS)
	require.Error(t, err)

	// Re-construct the data.js file with the full set of data.
	ParseYCSB(dataDirPath, dataJSPath, newDataJS)

	// Confirm the two data.js files are now equal.
	err = filesEqual(dataJSPath, newDataJS)
	require.NoError(t, err)
}

// diff returns the diff between contents of a and b.
func filesEqual(a, b string) error {
	aBytes, err := ioutil.ReadFile(a)
	if err != nil {
		return err
	}
	bBytes, err := ioutil.ReadFile(b)
	if err != nil {
		return err
	}

	// Normalize newlines.
	aBytes = bytes.Replace(aBytes, []byte{13, 10} /* \r\n */, []byte{10} /* \n */, -1)
	bBytes = bytes.Replace(bBytes, []byte{13, 10}, []byte{10}, -1)

	d := pretty.Diff(string(aBytes), string(bBytes))
	if d != nil {
		return errors.Newf("a != b; diff = %s", strings.Join(d, "\n"))
	}

	return nil
}

// copyDir recursively copies the fromPath to toPath, excluding certain paths.
func copyDir(fromPath, toPath string) error {
	return filepath.Walk(fromPath, func(path string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}

		rel, err := filepath.Rel(fromPath, path)
		if err != nil {
			return err
		}

		// Preserve the directory structure.
		if info.IsDir() {
			err := os.Mkdir(filepath.Join(toPath, rel), 0700)
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

		fOut, err := os.OpenFile(filepath.Join(toPath, rel), os.O_CREATE|os.O_WRONLY, 0700)
		if err != nil {
			return err
		}
		defer func() { _ = fOut.Close() }()

		_, err = io.Copy(fOut, fIn)
		return err
	})
}

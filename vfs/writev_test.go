// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteV(t *testing.T) {
	f, err := ioutil.TempFile("", "pebble-db-writev-")
	require.NoError(t, err)

	filename := f.Name()
	defer os.Remove(filename)

	// Write "testing123" repeatedly with every permutation of chunks.
	const str = "testing123"
	var permute func(val []byte) [][][]byte
	permute = func(val []byte) [][][]byte {
		if len(val) == 2 {
			// One group or two.
			return [][][]byte{
				{{val[0], val[1]}},
				{{val[0]}, {val[1]}},
			}
		}
		pre := val[0:1:1]
		sufOpts := permute(val[1:])
		res := make([][][]byte, 0, 2*len(sufOpts))
		for _, sufOpt := range sufOpts {
			// With pre alone.
			resAlone := [][]byte{pre}
			resAlone = append(resAlone, sufOpt...)
			res = append(res, resAlone)
			// With pre in first group.
			resComb := [][]byte{append(pre, sufOpt[0]...)}
			resComb = append(resComb, sufOpt[1:]...)
			res = append(res, resComb)
		}
		return res
	}
	groups := permute([]byte(str))
	require.Equal(t, 512, len(groups))

	for _, g := range groups {
		n, err := WriteV(f, g)
		require.NoError(t, err)
		require.Equal(t, len(str), n)
	}

	// Read the file back. Should match expectations.
	exp := bytes.Repeat([]byte(str), len(groups))

	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	data, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, exp, data)
}

func TestWriteVManyBuffers(t *testing.T) {
	f, err := ioutil.TempFile("", "pebble-db-writev-")
	require.NoError(t, err)

	filename := f.Name()
	defer os.Remove(filename)

	// Write "testing123" repeatedly across many small buffers. We make sure to
	// use at lease IOV_MAX (1024) buffers so that writevFile needs to iterate.
	const str = "testing123"
	const repeat = 3000
	bufs := make([][]byte, repeat)
	for i := range bufs {
		bufs[i] = []byte(str)
	}

	n, err := WriteV(f, bufs)
	require.NoError(t, err)
	require.Equal(t, repeat*len(str), n)

	// Read the file back. Should match expectations.
	exp := bytes.Repeat([]byte(str), repeat)

	_, err = f.Seek(0, 0)
	require.NoError(t, err)
	data, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, exp, data)
}

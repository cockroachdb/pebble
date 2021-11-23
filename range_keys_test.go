// Copyright 2021 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRangeKeys(t *testing.T) {
	d, err := Open("", &Options{
		FS:       vfs.NewMem(),
		Comparer: testkeys.Comparer,
	})
	require.NoError(t, err)
	defer d.Close()

	b := d.NewBatch()
	b.Experimental().RangeKeySet([]byte("a"), []byte("c"), []byte("@t10"), []byte("hello world"), nil)
	b.Experimental().RangeKeySet([]byte("b"), []byte("f"), []byte("@t20"), []byte("hello monde"), nil)
	require.NoError(t, b.Commit(nil))

	b = d.NewBatch()
	b.Experimental().RangeKeySet([]byte("h"), []byte("q"), []byte("@t30"), []byte("foo"), nil)
	require.NoError(t, b.Commit(nil))

	b = d.NewBatch()
	b.Experimental().RangeKeyUnset([]byte("e"), []byte("j"), []byte("@t20"), nil)
	b.Experimental().RangeKeyUnset([]byte("e"), []byte("j"), []byte("@t10"), nil)
	b.Experimental().RangeKeyUnset([]byte("e"), []byte("j"), []byte("@t30"), nil)
	require.NoError(t, b.Commit(nil))

	// TODO(jackson): Fill out when implemented.
}

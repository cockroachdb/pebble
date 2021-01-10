// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// +build darwin dragonfly freebsd linux netbsd openbsd

package vfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteVConsume(t *testing.T) {
	testCases := []struct {
		in      [][]byte
		consume int
		want    [][]byte
	}{
		{
			in:      [][]byte{[]byte("foo"), []byte("bar")},
			consume: 0,
			want:    [][]byte{[]byte("foo"), []byte("bar")},
		},
		{
			in:      [][]byte{[]byte("foo"), []byte("bar")},
			consume: 2,
			want:    [][]byte{[]byte("o"), []byte("bar")},
		},
		{
			in:      [][]byte{[]byte("foo"), []byte("bar")},
			consume: 3,
			want:    [][]byte{[]byte("bar")},
		},
		{
			in:      [][]byte{[]byte("foo"), []byte("bar")},
			consume: 4,
			want:    [][]byte{[]byte("ar")},
		},
		{
			in:      [][]byte{nil, nil, nil, []byte("bar")},
			consume: 1,
			want:    [][]byte{[]byte("ar")},
		},
		{
			in:      [][]byte{nil, nil, nil, []byte("foo")},
			consume: 0,
			want:    [][]byte{[]byte("foo")},
		},
		{
			in:      [][]byte{nil, nil, nil},
			consume: 0,
			want:    [][]byte{},
		},
	}
	for _, c := range testCases {
		out := consume(c.in, c.consume)
		require.Equal(t, c.want, out)
	}
}

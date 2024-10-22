// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package block

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterTransforms(t *testing.T) {
	require.True(t, NoTransforms.NoTransforms())
	var transforms IterTransforms
	require.True(t, transforms.NoTransforms())
	require.False(t, transforms.SyntheticPrefix.IsSet())
	require.False(t, transforms.SyntheticSuffix.IsSet())
	transforms.SyntheticPrefix = []byte{}
	require.False(t, transforms.SyntheticPrefix.IsSet())
	transforms.SyntheticSuffix = []byte{}
	require.False(t, transforms.SyntheticSuffix.IsSet())
	require.True(t, transforms.NoTransforms())
	transforms.HideObsoletePoints = true
	require.False(t, transforms.NoTransforms())

	transforms = NoTransforms
	transforms.SyntheticSeqNum = 123
	require.False(t, transforms.NoTransforms())

	transforms = NoTransforms
	transforms.SyntheticPrefix = []byte{1}
	require.False(t, transforms.NoTransforms())

	transforms = NoTransforms
	transforms.SyntheticSuffix = []byte{1}
	require.False(t, transforms.NoTransforms())
}

func TestFragmentIterTransforms(t *testing.T) {
	require.True(t, NoFragmentTransforms.NoTransforms())
	var transforms FragmentIterTransforms
	require.True(t, transforms.NoTransforms())
	require.False(t, transforms.SyntheticPrefix.IsSet())
	require.False(t, transforms.SyntheticSuffix.IsSet())
	transforms.SyntheticPrefix = []byte{}
	require.False(t, transforms.SyntheticPrefix.IsSet())
	transforms.SyntheticSuffix = []byte{}
	require.False(t, transforms.SyntheticSuffix.IsSet())
	require.True(t, transforms.NoTransforms())

	transforms.SyntheticSeqNum = 123
	require.False(t, transforms.NoTransforms())

	transforms = NoFragmentTransforms
	transforms.SyntheticPrefix = []byte{1}
	require.False(t, transforms.NoTransforms())

	transforms = NoFragmentTransforms
	transforms.SyntheticSuffix = []byte{1}
	require.False(t, transforms.NoTransforms())
}

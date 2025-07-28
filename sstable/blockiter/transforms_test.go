// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package blockiter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransforms(t *testing.T) {
	require.True(t, NoTransforms.NoTransforms())
	var transforms Transforms
	require.True(t, transforms.NoTransforms())
	require.True(t, transforms.SyntheticPrefixAndSuffix.IsUnset())
	transforms.SyntheticPrefixAndSuffix = MakeSyntheticPrefixAndSuffix([]byte{}, []byte{})
	require.True(t, transforms.SyntheticPrefixAndSuffix.IsUnset())
	require.True(t, transforms.NoTransforms())
	transforms.HideObsoletePoints = true
	require.False(t, transforms.NoTransforms())

	transforms = NoTransforms
	transforms.SyntheticSeqNum = 123
	require.False(t, transforms.NoTransforms())

	transforms = NoTransforms
	transforms.SyntheticPrefixAndSuffix = MakeSyntheticPrefixAndSuffix([]byte{1}, []byte{})
	require.False(t, transforms.NoTransforms())

	transforms = NoTransforms
	transforms.SyntheticPrefixAndSuffix = MakeSyntheticPrefixAndSuffix([]byte{}, []byte{1})
	require.False(t, transforms.NoTransforms())
}

func TestFragmentTransforms(t *testing.T) {
	require.True(t, NoFragmentTransforms.NoTransforms())
	var transforms FragmentTransforms
	require.True(t, transforms.NoTransforms())
	require.True(t, transforms.SyntheticPrefixAndSuffix.IsUnset())
	transforms.SyntheticPrefixAndSuffix = MakeSyntheticPrefixAndSuffix([]byte{}, []byte{})
	require.True(t, transforms.SyntheticPrefixAndSuffix.IsUnset())
	require.True(t, transforms.NoTransforms())

	transforms.SyntheticSeqNum = 123
	require.False(t, transforms.NoTransforms())

	transforms = NoFragmentTransforms
	transforms.SyntheticPrefixAndSuffix = MakeSyntheticPrefixAndSuffix([]byte{1}, []byte{})
	require.False(t, transforms.NoTransforms())

	transforms = NoFragmentTransforms
	transforms.SyntheticPrefixAndSuffix = MakeSyntheticPrefixAndSuffix([]byte{}, []byte{1})
	require.False(t, transforms.NoTransforms())
}

func TestSyntheticPrefixAndSuffix(t *testing.T) {
	var ps SyntheticPrefixAndSuffix

	require.True(t, ps.IsUnset())
	require.False(t, ps.HasPrefix())
	require.Nil(t, ps.Prefix())
	require.Zero(t, ps.PrefixLen())
	require.False(t, ps.HasSuffix())
	require.Zero(t, ps.SuffixLen())
	require.Nil(t, ps.Suffix())

	ps = MakeSyntheticPrefixAndSuffix([]byte("some-prefix"), []byte("suffix"))
	require.False(t, ps.IsUnset())
	require.True(t, ps.HasPrefix())
	require.Equal(t, uint32(11), ps.PrefixLen())
	require.Equal(t, "some-prefix", string(ps.Prefix()))
	require.True(t, ps.HasSuffix())
	require.Equal(t, uint32(6), ps.SuffixLen())
	require.Equal(t, "suffix", string(ps.Suffix()))

	ps = MakeSyntheticPrefixAndSuffix([]byte("some-prefix"), []byte{})
	require.False(t, ps.IsUnset())
	require.True(t, ps.HasPrefix())
	require.Equal(t, uint32(11), ps.PrefixLen())
	require.Equal(t, "some-prefix", string(ps.Prefix()))
	require.False(t, ps.HasSuffix())
	require.Zero(t, ps.SuffixLen())
	require.Nil(t, ps.Suffix())

	ps = MakeSyntheticPrefixAndSuffix([]byte("some-prefix"), []byte("suffix"))
	ps = ps.RemoveSuffix()
	require.False(t, ps.IsUnset())
	require.True(t, ps.HasPrefix())
	require.Equal(t, "some-prefix", string(ps.Prefix()))
	require.False(t, ps.HasSuffix())
	require.Zero(t, ps.SuffixLen())
	require.Nil(t, ps.Suffix())

	ps = MakeSyntheticPrefixAndSuffix([]byte{}, []byte("suffix"))
	require.False(t, ps.IsUnset())
	require.False(t, ps.HasPrefix())
	require.Zero(t, ps.PrefixLen())
	require.Nil(t, ps.Prefix())
	require.True(t, ps.HasSuffix())
	require.Equal(t, uint32(6), ps.SuffixLen())
	require.Equal(t, "suffix", string(ps.Suffix()))

	require.True(t, ps.RemoveSuffix().IsUnset())
}

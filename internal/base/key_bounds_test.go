// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUserKeyBoundary(t *testing.T) {
	cmp := DefaultComparer.Compare
	a := []byte("a")
	b := []byte("b")

	aExclusive := UserKeyExclusive(a)
	require.False(t, aExclusive.IsUpperBoundFor(cmp, a))
	require.False(t, aExclusive.IsUpperBoundFor(cmp, b))

	aInclusive := UserKeyInclusive(a)
	require.True(t, aInclusive.IsUpperBoundFor(cmp, a))
	require.False(t, aInclusive.IsUpperBoundFor(cmp, b))

	bExclusive := UserKeyExclusive(b)
	require.True(t, bExclusive.IsUpperBoundFor(cmp, a))
	require.False(t, bExclusive.IsUpperBoundFor(cmp, b))

	bInclusive := UserKeyInclusive(b)
	require.True(t, bInclusive.IsUpperBoundFor(cmp, a))
	require.True(t, bInclusive.IsUpperBoundFor(cmp, b))

	ordered := []UserKeyBoundary{aExclusive, aInclusive, bExclusive, bInclusive}
	for i := range ordered {
		for j := range ordered {
			expected := 0
			if i < j {
				expected = -1
			} else if i > j {
				expected = 1
			}
			require.Equalf(t, expected, ordered[i].CompareUpperBounds(cmp, ordered[j]),
				"%v, %v", ordered[i], ordered[j])
		}
	}
}

func TestUserKeyBounds(t *testing.T) {
	var ukb UserKeyBounds
	cmp := DefaultComparer.Compare
	require.False(t, ukb.Valid(cmp))

	a := []byte("a")
	b := []byte("b")
	c := []byte("c")
	d := []byte("d")
	e := []byte("e")

	bb := UserKeyBoundsInclusive(b, b)
	bdi := UserKeyBoundsInclusive(b, d)
	bde := UserKeyBoundsEndExclusive(b, d)
	aci := UserKeyBoundsEndExclusiveIf(a, c, false)
	ace := UserKeyBoundsEndExclusiveIf(a, c, true)
	cde := UserKeyBoundsEndExclusive(c, d)
	adi := UserKeyBoundsInclusive(a, d)
	ade := UserKeyBoundsEndExclusive(a, d)
	empty := UserKeyBounds{}

	t.Run("Valid", func(t *testing.T) {
		require.True(t, bb.Valid(cmp))
		require.True(t, bdi.Valid(cmp))
		require.True(t, bde.Valid(cmp))
		var bad UserKeyBounds
		require.False(t, bad.Valid(cmp))
		bad = UserKeyBoundsEndExclusive(a, a)
		require.False(t, bad.Valid(cmp))
		bad = UserKeyBoundsInclusive(b, a)
		require.False(t, bad.Valid(cmp))
	})

	t.Run("String", func(t *testing.T) {
		require.Equal(t, "[b, b]", bb.String())
		require.Equal(t, "[b, d]", bdi.String())
		require.Equal(t, "[b, d)", bde.String())
	})

	t.Run("Overlaps", func(t *testing.T) {
		require.True(t, bdi.Overlaps(cmp, bdi))
		require.True(t, bdi.Overlaps(cmp, bde))
		require.True(t, bdi.Overlaps(cmp, bb))
		require.True(t, bde.Overlaps(cmp, bde))
		require.True(t, bde.Overlaps(cmp, bdi))
		require.True(t, bde.Overlaps(cmp, aci))
		require.True(t, bde.Overlaps(cmp, bb))
		require.True(t, aci.Overlaps(cmp, cde))
		require.False(t, ace.Overlaps(cmp, cde))
		require.False(t, cde.Overlaps(cmp, bb))
	})

	t.Run("ContainsBounds", func(t *testing.T) {
		require.True(t, bdi.ContainsBounds(cmp, bb))
		require.True(t, bdi.ContainsBounds(cmp, bde))
		require.True(t, bde.ContainsBounds(cmp, bde))
		require.False(t, bde.ContainsBounds(cmp, bdi))
	})

	t.Run("ContainsUserKey", func(t *testing.T) {
		require.True(t, bb.ContainsUserKey(cmp, b))
		require.False(t, bb.ContainsUserKey(cmp, a))
		require.False(t, bb.ContainsUserKey(cmp, c))
		require.False(t, bb.ContainsUserKey(cmp, a))

		require.False(t, bdi.ContainsUserKey(cmp, a))
		require.True(t, bdi.ContainsUserKey(cmp, b))
		require.True(t, bdi.ContainsUserKey(cmp, c))
		require.True(t, bdi.ContainsUserKey(cmp, d))
		require.False(t, bdi.ContainsUserKey(cmp, e))

		require.False(t, bde.ContainsUserKey(cmp, a))
		require.True(t, bde.ContainsUserKey(cmp, b))
		require.True(t, bde.ContainsUserKey(cmp, c))
		require.False(t, bde.ContainsUserKey(cmp, d))
		require.False(t, bde.ContainsUserKey(cmp, e))
	})

	t.Run("ContainsInternalKey", func(t *testing.T) {
		require.False(t, bde.ContainsInternalKey(cmp, MakeRangeDeleteSentinelKey(b)))
		require.True(t, bde.ContainsInternalKey(cmp, MakeInternalKey(b, 0, InternalKeyKindSet)))
		require.True(t, bde.ContainsInternalKey(cmp, MakeInternalKey(c, 0, InternalKeyKindSet)))
		require.True(t, bdi.ContainsInternalKey(cmp, MakeInternalKey(d, 0, InternalKeyKindSet)))
		require.False(t, bde.ContainsInternalKey(cmp, MakeInternalKey(d, 0, InternalKeyKindSet)))
		require.True(t, bde.ContainsInternalKey(cmp, MakeRangeDeleteSentinelKey(d)))
	})

	t.Run("Union", func(t *testing.T) {
		// Identity
		require.Equal(t, bb, bb.Union(cmp, bb))
		require.Equal(t, bdi, bdi.Union(cmp, bdi))
		require.Equal(t, bde, bde.Union(cmp, bde))
		require.Equal(t, aci, aci.Union(cmp, aci))

		require.Equal(t, bdi, bb.Union(cmp, bdi))
		require.Equal(t, bdi, bdi.Union(cmp, bde))
		require.Equal(t, bdi, bde.Union(cmp, bdi))
		require.Equal(t, aci, aci.Union(cmp, ace))

		require.Equal(t, ade, aci.Union(cmp, bde))
		require.Equal(t, adi, aci.Union(cmp, bdi))
		require.Equal(t, ade, ace.Union(cmp, bde))
		require.Equal(t, adi, ace.Union(cmp, bdi))

		require.Equal(t, bde, empty.Union(cmp, bde))
	})
}

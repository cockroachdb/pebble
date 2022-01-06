package rangekey

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/keyspan"
	"github.com/cockroachdb/pebble/internal/testkeys"
	"github.com/stretchr/testify/require"
)

func TestSetSuffixValues_RoundTrip(t *testing.T) {
	testCases := [][]SuffixValue{
		{
			{Suffix: []byte{}, Value: []byte("world")},
		},
		{
			{Suffix: []byte("foo"), Value: []byte("bar")},
		},
		{
			{Suffix: []byte(""), Value: []byte("boop")},
			{Suffix: []byte("foo"), Value: []byte("beep")},
			{Suffix: []byte("bar"), Value: []byte("bop")},
			{Suffix: []byte("bax"), Value: []byte("boink")},
			{Suffix: []byte("zoop"), Value: []byte("zoink")},
		},
	}

	var b []byte
	for _, tc := range testCases {
		// Encode.
		l := EncodedSetSuffixValuesLen(tc)
		if l <= cap(b) {
			b = b[:l]
		} else {
			b = make([]byte, l)
		}
		n := EncodeSetSuffixValues(b, tc)
		require.Equal(t, l, n)

		// Decode.
		var suffixValues []SuffixValue
		for len(b) > 0 {
			sv, rest, ok := DecodeSuffixValue(b)
			require.True(t, ok)
			suffixValues = append(suffixValues, sv)
			b = rest
		}
		require.Equal(t, tc, suffixValues)
	}
}

func TestSetValue_Roundtrip(t *testing.T) {
	testCases := []struct {
		endKey       []byte
		suffixValues []SuffixValue
	}{
		{
			endKey: []byte("hello"),
			suffixValues: []SuffixValue{
				{Suffix: []byte{}, Value: []byte("world")},
			},
		},
		{
			endKey: []byte("hello world"),
			suffixValues: []SuffixValue{
				{Suffix: []byte("foo"), Value: []byte("bar")},
			},
		},
		{
			endKey: []byte("hello world"),
			suffixValues: []SuffixValue{
				{Suffix: []byte(""), Value: []byte("boop")},
				{Suffix: []byte("foo"), Value: []byte("beep")},
				{Suffix: []byte("bar"), Value: []byte("bop")},
				{Suffix: []byte("bax"), Value: []byte("boink")},
				{Suffix: []byte("zoop"), Value: []byte("zoink")},
			},
		},
	}

	var b []byte
	for _, tc := range testCases {
		l := EncodedSetValueLen(tc.endKey, tc.suffixValues)

		if l <= cap(b) {
			b = b[:l]
		} else {
			b = make([]byte, l)
		}

		n := EncodeSetValue(b, tc.endKey, tc.suffixValues)
		require.Equal(t, l, n)

		var endKey, rest []byte
		var ok bool
		endKey, rest, ok = DecodeEndKey(base.InternalKeyKindRangeKeySet, b[:n])
		require.True(t, ok)

		var suffixValues []SuffixValue
		for len(rest) > 0 {
			var sv SuffixValue
			var ok bool
			sv, rest, ok = DecodeSuffixValue(rest)
			require.True(t, ok)
			suffixValues = append(suffixValues, sv)
		}
		require.Equal(t, tc.endKey, endKey)
		require.Equal(t, tc.suffixValues, suffixValues)
	}
}

func TestUnsetSuffixes_RoundTrip(t *testing.T) {
	type suffixes [][]byte
	testCases := []suffixes{
		{{}},
		{[]byte("foo")},
		{
			{},
			[]byte("foo"),
			[]byte("bar"),
			[]byte("bax"),
			[]byte("zoop"),
		},
	}

	var b []byte
	for _, tc := range testCases {
		// Encode.
		l := EncodedUnsetSuffixesLen(tc)
		if l <= cap(b) {
			b = b[:l]
		} else {
			b = make([]byte, l)
		}
		n := EncodeUnsetSuffixes(b, tc)
		require.Equal(t, l, n)

		// Decode.
		var ss suffixes
		for len(b) > 0 {
			s, rest, ok := DecodeSuffix(b)
			require.True(t, ok)
			ss = append(ss, s)
			b = rest
		}
		require.Equal(t, tc, ss)
	}
}

func TestUnsetValue_Roundtrip(t *testing.T) {
	testCases := []struct {
		endKey   []byte
		suffixes [][]byte
	}{
		{
			endKey:   []byte("hello"),
			suffixes: [][]byte{{}},
		},
		{
			endKey:   []byte("hello world"),
			suffixes: [][]byte{[]byte("foo")},
		},
		{
			endKey: []byte("hello world"),
			suffixes: [][]byte{
				{},
				[]byte("foo"),
				[]byte("bar"),
				[]byte("bax"),
				[]byte("zoop"),
			},
		},
	}

	var b []byte
	for _, tc := range testCases {
		l := EncodedUnsetValueLen(tc.endKey, tc.suffixes)

		if l <= cap(b) {
			b = b[:l]
		} else {
			b = make([]byte, l)
		}

		n := EncodeUnsetValue(b, tc.endKey, tc.suffixes)
		require.Equal(t, l, n)

		var ok bool
		var endKey, rest []byte
		endKey, rest, ok = DecodeEndKey(base.InternalKeyKindRangeKeyUnset, b[:n])
		require.True(t, ok)
		var suffixes [][]byte
		for len(rest) > 0 {
			var ok bool
			var suffix []byte
			suffix, rest, ok = DecodeSuffix(rest)
			require.True(t, ok)
			suffixes = append(suffixes, suffix)
		}
		require.Equal(t, tc.endKey, endKey)
		require.Equal(t, tc.suffixes, suffixes)
	}
}

func TestParseFormatRoundtrip(t *testing.T) {
	testCases := []string{
		"a.RANGEKEYSET.100: c [(@t22=foo),(@t1=bar)]",
		"apples.RANGEKEYSET.5: bananas [(@t1=bar)]",
		"cat.RANGEKEYUNSET.5: catatonic [@t9,@t8,@t7,@t6,@t5]",
		"a.RANGEKEYDEL.5: catatonic",
	}
	for _, in := range testCases {
		k, v := Parse(in)
		endKey, restValue, ok := DecodeEndKey(k.Kind(), v)
		require.True(t, ok)
		got := fmt.Sprintf("%s", Format(testkeys.Comparer.FormatKey, keyspan.Span{
			Start: k,
			End:   endKey,
			Value: restValue,
		}))
		if got != in {
			t.Errorf("Format(Parse(%q)) = %q, want %q", in, got, in)
		}
	}
}

func TestRecombinedValueLen_RoundTrip(t *testing.T) {
	testCases := []string{
		"a.RANGEKEYSET.1: [(@t22=foo),(@t1=bar)]",
		"a.RANGEKEYSET.1: [(@t1=bar)]",
		"a.RANGEKEYUNSET.1: [@t9,@t8,@t7,@t6,@t5]",
		"a.RANGEKEYDEL.5: foo",
	}
	for i, in := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			k, v := Parse(in)

			// Split the value into an end key and a user-value.
			endKey, restValue, ok := DecodeEndKey(k.Kind(), v)
			require.True(t, ok)

			// Re-encode the end key and user-value.
			dst := make([]byte, RecombinedValueLen(k.Kind(), endKey, restValue))
			RecombineValue(k.Kind(), dst, endKey, restValue)

			require.Equal(t, v, dst)
		})
	}
}

func TestIsRangeKey(t *testing.T) {
	testCases := []struct {
		kind base.InternalKeyKind
		want bool
	}{
		{
			kind: base.InternalKeyKindRangeKeyDelete,
			want: true,
		},
		{
			kind: base.InternalKeyKindRangeKeyUnset,
			want: true,
		},
		{
			kind: base.InternalKeyKindRangeKeyDelete,
			want: true,
		},
		{
			kind: base.InternalKeyKindDelete,
			want: false,
		},
		{
			kind: base.InternalKeyKindSet,
			want: false,
		},
	}
	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.want, IsRangeKey(tc.kind))
		})
	}
}

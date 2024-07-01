// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import "testing"

func TestColumnDesc(t *testing.T) {
	type testCase struct {
		desc        ColumnDesc
		hasConstant bool
		delta       DeltaEncoding
		str         string
	}

	testCases := []testCase{
		{
			desc:        ColumnDesc{DataType: DataTypeBool, Encoding: EncodingDefault},
			hasConstant: false,
			delta:       DeltaEncodingNone,
			str:         "bool",
		},
		{
			desc:        ColumnDesc{DataType: DataTypeUint32, Encoding: EncodingDefault},
			hasConstant: false,
			delta:       DeltaEncodingNone,
			str:         "uint32",
		},
		{
			desc: ColumnDesc{
				DataType: DataTypeUint32,
				Encoding: EncodingDefault.WithConstant().WithDelta(DeltaEncodingUint16),
			},
			hasConstant: true,
			delta:       DeltaEncodingUint16,
			str:         "uint32+const+delta16",
		},
		{
			desc: ColumnDesc{
				DataType: DataTypeUint64,
				Encoding: EncodingDefault.WithConstant(),
			},
			hasConstant: true,
			delta:       DeltaEncodingNone,
			str:         "uint64+const",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.str, func(t *testing.T) {
			if got := tc.desc.String(); got != tc.str {
				t.Errorf("String() = %q; want %q", got, tc.str)
			}
			if d := tc.desc.Encoding.Delta(); d != tc.delta {
				t.Errorf(".Delta() = %q; want %q", d, tc.delta)
			}
			if c := tc.desc.Encoding.HasConstant(); c != tc.hasConstant {
				t.Errorf(".HasConstant() = %t; want %t", c, tc.hasConstant)
			}
		})
	}
}

// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package colblk

import "testing"

func TestColumnDesc(t *testing.T) {
	type testCase struct {
		desc  ColumnDesc
		delta DeltaEncoding
		str   string
	}

	testCases := []testCase{
		{
			desc:  ColumnDesc{DataType: DataTypeBool, Encoding: EncodingDefault},
			delta: DeltaEncodingNone,
			str:   "bool",
		},
		{
			desc:  ColumnDesc{DataType: DataTypeUint32, Encoding: EncodingDefault},
			delta: DeltaEncodingNone,
			str:   "uint32",
		},
		{
			desc: ColumnDesc{
				DataType: DataTypeUint32,
				Encoding: EncodingDefault.WithDelta(DeltaEncodingUint16),
			},
			delta: DeltaEncodingUint16,
			str:   "uint32+delta16",
		},
		{
			desc: ColumnDesc{
				DataType: DataTypeUint64,
				Encoding: EncodingDefault.WithDelta(DeltaEncodingConstant),
			},
			delta: DeltaEncodingConstant,
			str:   "uint64+const",
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
		})
	}
}

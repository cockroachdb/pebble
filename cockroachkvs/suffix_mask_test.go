// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cockroachkvs

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/crlib/testutils/require"
)

func testEncodeMVCCSuffix(wallTime uint64, logical uint32) []byte {
	if wallTime == 0 && logical == 0 {
		return nil
	}
	if logical == 0 {
		buf := make([]byte, suffixLenWithWall)
		binary.BigEndian.PutUint64(buf, wallTime)
		buf[len(buf)-1] = suffixLenWithWall
		return buf
	}
	buf := make([]byte, suffixLenWithLogical)
	binary.BigEndian.PutUint64(buf, wallTime)
	binary.BigEndian.PutUint32(buf[8:], logical)
	buf[len(buf)-1] = suffixLenWithLogical
	return buf
}

func TestSuffixMaskBlockPropertyFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bound := testEncodeMVCCSuffix(100, 0)
	filter := MakeSuffixMaskBlockPropertyFilter(bound)
	require.True(t, filter != nil)

	// Empty bound should return nil filter.
	require.True(t, MakeSuffixMaskBlockPropertyFilter(nil) == nil)
}

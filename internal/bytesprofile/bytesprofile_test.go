// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bytesprofile

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytesProfile(t *testing.T) {
	p := NewProfile()
	for i := 0; i < 100; i++ {
		p.Record(int64(i))
	}
	for i := 0; i < 10; i++ {
		p.Record(11 * int64(i))
	}
	s := p.String()
	require.Contains(t, s, "0: Count: 100 (100), Bytes: 4950 (4.8KB)")
	require.Contains(t, s, "1: Count: 10 (10), Bytes: 495 (495B)")
	t.Log(s)
}

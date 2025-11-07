// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bytesprofile

import "testing"

func TestBytesProfile(t *testing.T) {
	p := NewProfile()
	for i := 0; i < 100; i++ {
		p.Record(int64(i))
	}
	for i := 0; i < 100; i++ {
		p.Record(int64(i))
	}
	t.Log(p.String())
}

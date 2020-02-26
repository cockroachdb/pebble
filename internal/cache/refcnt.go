// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

const (
	refcntWeakBit  = int32(-1 << 31)
	refcntRefsMask = int32(1<<31 - 1)
)

// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package internal

// T is a Template type. The methods in the interface make up its contract.
//lint:ignore U1001 unused
type T interface {
	ID() uint64
	Key() []byte
	String() string
	// Used for testing only.
	New() T
	SetID(uint64)
	SetKey([]byte)
}

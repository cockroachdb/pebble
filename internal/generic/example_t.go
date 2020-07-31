// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package generic

//go:generate ./gen.sh *example generic

type example struct {
	id  uint64
	key exampleKey
}

// Methods required by generic contract.
func (ex *example) ID() uint64      { return ex.id }
func (ex *example) Key() []byte     { return []byte(ex.key.Key) }
func (ex *example) String() string  { return ex.key.String() }
func (ex *example) New() *example   { return new(example) }
func (ex *example) SetID(v uint64)  { ex.id = v }
func (ex *example) SetKey(v []byte) { ex.key.Key = string(v) }

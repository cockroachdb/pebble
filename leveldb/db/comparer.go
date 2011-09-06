// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

import (
	"bytes"
)

// Comparer defines a total ordering over the space of []byte keys: a 'less
// than' relationship.
type Comparer interface {
	Compare(a, b []byte) int
}

var DefaultComparer Comparer = defCmp{}

type defCmp struct{}

func (defCmp) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

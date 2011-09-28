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
	// Compare returns -1, 0, or +1 depending on whether a is 'less than',
	// 'equal to' or 'greater than' b. The two arguments can only be 'equal'
	// if their contents are exactly equal. Furthermore, the empty slice
	// must be 'less than' any non-empty slice.
	Compare(a, b []byte) int
}

// DefaultComparer is the default implementation of the Comparer interface.
// It uses the natural ordering, consistent with bytes.Compare.
var DefaultComparer Comparer = defCmp{}

type defCmp struct{}

func (defCmp) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Copyright 2011 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"errors"
)

// ErrNotFound means that a get or delete call did not find the requested key.
var ErrNotFound = errors.New("pebble: not found")

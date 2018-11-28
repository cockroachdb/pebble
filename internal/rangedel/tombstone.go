// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package rangedel // import "github.com/petermattis/pebble/internal/rangedel"

import (
	"github.com/petermattis/pebble/db"
)

// Tombstone is a range deletion tombstone. A range deletion tombstone deletes
// all of the keys in the range [start,end). Note that the start key is
// inclusive and the end key is exclusive.
type Tombstone struct {
	Start db.InternalKey
	End   []byte
}

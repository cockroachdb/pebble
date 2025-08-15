// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

// TieringPolicyChange is called by the user of DB to inform the DB that some
// tiering policies may have changed significantly.
func (d *DB) TieringPolicyChange() {
	// TODO: implement.
}

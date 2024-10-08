// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package invariants

import "math/rand/v2"

// Sometimes returns true percent% of the time if we were built with the
// "invariants" of "race" build tags
func Sometimes(percent int) bool {
	return Enabled && rand.Uint32N(100) < uint32(percent)
}

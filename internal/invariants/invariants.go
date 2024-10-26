// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package invariants

import (
	"math/rand/v2"
	"runtime"

	"github.com/cockroachdb/pebble/internal/buildtags"
)

// RaceEnabled is true if we were built with the "race" build tag.
const RaceEnabled = buildtags.Race

// Sometimes returns true percent% of the time if we were built with the
// "invariants" of "race" build tags
func Sometimes(percent int) bool {
	return Enabled && rand.Uint32N(100) < uint32(percent)
}

// UseFinalizers is true if we want to use finalizers for assertions around
// object lifetime and cleanup. This happens when the invariants or tracing tags
// are set, but we exclude race builds because we historically ran into some
// finalizer-related race detector bugs.
const UseFinalizers = !RaceEnabled && (Enabled || buildtags.Tracing)

// SetFinalizer is a wrapper around runtime.SetFinalizer that is a no-op under
// race builds or if neither the invariants nor tracing build tags are
// specified.
//
// We exclude race builds because we historically ran into some race detector
// bugs related to finalizers.
func SetFinalizer(obj, finalizer interface{}) {
	if UseFinalizers {
		runtime.SetFinalizer(obj, finalizer)
	}
}

// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package invariants

import (
	"runtime"

	"github.com/cockroachdb/pebble/v2/internal/buildtags"
)

// Enabled is true if we were built with the "invariants" or "race" build tags.
//
// Enabled should be used to gate invariant checks that may be expensive. It
// should not be used to unconditionally alter a code path significantly (e.g.
// wrapping an iterator - see #3678); Sometimes() should be used instead so that
// the production code path gets test coverage as well.
const Enabled = buildtags.Race || buildtags.Invariants

// RaceEnabled is true if we were built with the "race" build tag.
const RaceEnabled = buildtags.Race

// UseFinalizers is true if we want to use finalizers for assertions around
// object lifetime and cleanup. This happens when the invariants or tracing tags
// are set, but we exclude race builds because we historically ran into some
// finalizer-related race detector bugs.
const UseFinalizers = !buildtags.Race && (buildtags.Invariants || buildtags.Tracing)

// SetFinalizer is a wrapper around runtime.SetFinalizer that is a no-op under
// race builds or if neither the invariants nor tracing build tags are
// specified.
//
// We exclude race builds because we historically ran into some race detector
// bugs related to finalizers.
//
// This function is a no-op if UseFinalizers is false and it should inline to
// nothing. However, note that it might not inline so in very hot paths it's
// best to check UseFinalizers first.
func SetFinalizer(obj, finalizer interface{}) {
	if UseFinalizers {
		runtime.SetFinalizer(obj, finalizer)
	}
}

// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package dsl

import (
	"testing"

	"github.com/cockroachdb/crlib/testutils/require"
)

func TestCallStackIncludes(t *testing.T) {
	require.True(t, CallStackIncludes[string]("TestCallStackIncludes").Evaluate(""))
	require.True(t, CallStackIncludes[string]("internal/dsl").Evaluate(""))
	require.False(t, CallStackIncludes[string]("pebble.NewIter").Evaluate(""))
}

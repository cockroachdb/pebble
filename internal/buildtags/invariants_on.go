// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !invariants

package buildtags

// Invariants indicates if the invariants tag is used.
// See invariants.Enabled.
const Invariants = false

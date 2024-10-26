// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build !cgo

package buildtags

// Cgo is true if we were built with the "cgo" build tag.
const Cgo = false

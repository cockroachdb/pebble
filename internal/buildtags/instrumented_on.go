// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build race || asan || msan

package buildtags

// Instrumented is true if this is an instrumented testing build that is likely
// to be significantly slower (like race or address sanitizer builds).
const Instrumented = true

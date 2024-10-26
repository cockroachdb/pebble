// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build tracing

package buildtags

// Tracing indicates if the tracing tag is used.
//
// This tag enables low-level tracing code in the block cache.
const Tracing = true

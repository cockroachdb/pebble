// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package problemspans provides functionality for tracking and managing key
// spans that have been identified as problematic. It allows users to add spans
// with associated expiration times, check if a given key range overlaps any
// active (non-expired) spans, and remove spans when issues are resolved.
//
// This package is designed for efficiently tracking key ranges that may need
// special handling.
//
// Key Attributes:
//
//   - **Span Registration:**
//     Add spans with specified expiration times so that they automatically
//     become inactive after a set duration.
//
//   - **Overlap Detection:**
//     Quickly check if a key range overlaps with any active problematic spans.
//
//   - **Span Excise:**
//     Remove or adjust spans to reflect changes as issues are resolved.
//
//   - **Level-Based Organization:**
//     The package offers a structure to organize and manage problematic spans
//     per level, with built-in support for concurrent operations.
package problemspans

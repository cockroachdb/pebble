// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build mipsle
// +build mipsle

package manual

const (
	// MaxArrayLen is a safe maximum length for slices on this architecture.
	MaxArrayLen = 1 << 30
)

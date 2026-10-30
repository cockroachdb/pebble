// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

//go:build amd64 && !appengine && !noasm && gc && !purego

package compression

const minlzUsesGoEncoder = false

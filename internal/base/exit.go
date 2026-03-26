// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import "os"

// Exit is called when Pebble needs to terminate the process due to an
// invariant violation. By default it calls os.Exit, but embedders
// (e.g. CockroachDB) can override it to route through their own fatal
// logging infrastructure for better crash visibility.
var Exit = func(code int) {
	os.Exit(code)
}

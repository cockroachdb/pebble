// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/redact"
)

// Placement identifies where a file/object is stored.
//
// The zero value is invalid (this is intentional to determine accidantelly
// uninitialized fields).
type Placement uint8

const (
	Local Placement = 1 + iota
	Shared
	External
)

func (p Placement) String() string {
	switch p {
	case Local:
		return "local"
	case Shared:
		return "shared"
	case External:
		return "external"
	default:
		if invariants.Enabled {
			panic(errors.AssertionFailedf("invalid placement type %d", p))
		}
		return "invalid"
	}
}

// SafeFormat implements redact.SafeFormatter.
func (p Placement) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Print(redact.SafeString(p.String()))
}

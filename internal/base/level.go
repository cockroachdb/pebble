// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/invariants"
)

// Level identifies an LSM level. The zero value indicates that the level is
// uninitialized or unknown.
type Level struct {
	// v contains the level in the lowest 7 bits. The highest bit is set iff the
	// value is valid.
	v uint8
}

const validBit = 1 << 7

func (l Level) Get() (level int, ok bool) {
	return int(l.v &^ validBit), l.Valid()
}

func (l Level) Valid() bool {
	return l.v&validBit != 0
}

func (l Level) String() string {
	if level, ok := l.Get(); ok {
		return fmt.Sprintf("L%d", level)
	}
	return "n/a"
}

func MakeLevel(l int) Level {
	if invariants.Enabled && l < 0 || l >= validBit {
		panic(errors.AssertionFailedf("invalid level: %d", l))
	}
	return Level{uint8(l) | validBit}
}

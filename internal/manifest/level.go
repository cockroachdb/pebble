// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import "fmt"

const (
	// 3 bits are necessary to represent level values from 0-6.
	levelBits = 3
	levelMask = (1 << levelBits) - 1

	// InvalidSublevel denotes an invalid or non-applicable sublevel.
	InvalidSublevel = -1
)

// Level encodes a level and optional sublevel for use in log and error
// messages. The encoding has the property that Level(level) ==
// MakeLevel(level, InvalidSublevel).
type Level int

// MakeLevel returns a Level representing the specified level and sublevel.
func MakeLevel(level, sublevel int) Level {
	if level < 0 || level >= NumLevels {
		panic(fmt.Sprintf("invalid level: %d", level))
	}
	if sublevel == InvalidSublevel {
		return Level(level)
	}
	if level != 0 || sublevel < 0 {
		panic(fmt.Sprintf("invalid L%d sublevel: %d", level, sublevel))
	}
	return Level(((sublevel + 1) << levelBits) | level)
}

func (l Level) String() string {
	sublevel := (int(l) >> levelBits) - 1
	level := int(l) & levelMask
	if sublevel != InvalidSublevel {
		return fmt.Sprintf("L%d.%d", level, sublevel)
	}
	return fmt.Sprintf("L%d", level)
}

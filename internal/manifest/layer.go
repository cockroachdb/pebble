// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"math"

	"github.com/cockroachdb/redact"
)

// Layer represents a section of the logical sstable hierarchy. It can represent:
//   - a level L1 through L6, or
//   - the entire L0 level, or
//   - a specific L0 sublevel, or
//   - the layer of flushable ingests (which is conceptually above the LSM).
type Layer struct {
	kind  layerKind
	value uint16
}

// Level returns a Layer that represents an entire level (L0 through L6).
func Level(level int) Layer {
	if level < 0 || level >= NumLevels {
		panic("invalid level")
	}
	return Layer{
		kind:  levelLayer,
		value: uint16(level),
	}
}

// L0Sublevel returns a Layer that represents a specific L0 sublevel.
func L0Sublevel(sublevel int) Layer {
	// Note: Pebble stops writes once we get to 1000 sublevels.
	if sublevel < 0 || sublevel > math.MaxUint16 {
		panic("invalid sublevel")
	}
	return Layer{
		kind:  l0SublevelLayer,
		value: uint16(sublevel),
	}
}

// FlushableIngestsLayer returns a Layer that represents the flushable ingests
// layer (which is logically above L0).
func FlushableIngestsLayer() Layer {
	return Layer{
		kind: flushableIngestsLayer,
	}
}

// IsSet returns true if l has been initialized.
func (l Layer) IsSet() bool {
	return l.kind != 0
}

// IsFlushableIngests returns true if the layer represents flushable ingests.
func (l Layer) IsFlushableIngests() bool {
	return l.kind == flushableIngestsLayer
}

// IsL0Sublevel returns true if the layer represents an L0 sublevel.
func (l Layer) IsL0Sublevel() bool {
	return l.kind == l0SublevelLayer
}

// Level returns the level for the layer. Must not be called if
// the layer represents flushable ingests.
func (l Layer) Level() int {
	switch l.kind {
	case levelLayer:
		return int(l.value)
	case l0SublevelLayer:
		return 0
	case flushableIngestsLayer:
		panic("flushable ingests layer")
	default:
		panic("invalid layer")
	}
}

// Sublevel returns the L0 sublevel. Can only be called if the layer represents
// an L0 sublevel.
func (l Layer) Sublevel() int {
	if !l.IsL0Sublevel() {
		panic("not an L0 sublevel layer")
	}
	return int(l.value)
}

func (l Layer) String() string {
	switch l.kind {
	case levelLayer:
		return fmt.Sprintf("L%d", l.value)
	case l0SublevelLayer:
		return fmt.Sprintf("L0.%d", l.value)
	case flushableIngestsLayer:
		return "flushable-ingests"
	default:
		return "unknown"
	}
}

// SafeFormat implements redact.SafeFormatter.
func (l Layer) SafeFormat(s redact.SafePrinter, verb rune) {
	s.SafeString(redact.SafeString(l.String()))
}

type layerKind uint8

const (
	// Entire level: value contains the level number (0 through 6).
	levelLayer layerKind = iota + 1
	// L0 sublevel: value contains the sublevel number.
	l0SublevelLayer
	// Flushable ingests layer: value is unused.
	flushableIngestsLayer
)

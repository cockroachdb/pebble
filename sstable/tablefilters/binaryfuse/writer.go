// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package binaryfuse

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/zeebo/xxh3"
)

// tableFilterWriter implements base.TableFilterWriter for binary fuse filters.
type tableFilterWriter struct {
	bitsPerFingerprint int
	hc                 hashCollector
}

func newTableFilterWriter(bitsPerFingerprint int) *tableFilterWriter {
	w := &tableFilterWriter{}
	w.init(bitsPerFingerprint)
	return w
}

func (w *tableFilterWriter) init(bitsPerFingerprint int) {
	w.bitsPerFingerprint = bitsPerFingerprint
	w.hc.Init()
}

// AddKey implements the base.TableFilterWriter interface.
func (w *tableFilterWriter) AddKey(key []byte) {
	w.hc.Add(xxh3.Hash(key))
}

// Finish implements the base.TableFilterWriter interface.
func (w *tableFilterWriter) Finish() (_ []byte, _ base.TableFilterFamily, ok bool) {
	data, ok := buildFilter(&w.hc, w.bitsPerFingerprint)
	if !ok {
		return nil, "", false
	}
	w.hc.Reset()
	return data, Family, true
}

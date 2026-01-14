// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tablefilters

import (
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/tablefilters/binaryfuse"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
)

// Decoders contains the decoders for bloom and binary fuse filters.
var Decoders = []base.TableFilterDecoder{bloom.Decoder, binaryfuse.Decoder}

// PolicyFromName returns the TableFilterPolicy corresponding to the given name,
// or false if the string is not recognized as a binary fuse filter policy.
//
// Supports bloom filters and binary fuse filters. The "none" policy is also
// supported.
func PolicyFromName(name string) (_ base.TableFilterPolicy, ok bool) {
	if name == "none" {
		return base.NoFilterPolicy, true
	}
	if p, ok := bloom.PolicyFromName(name); ok {
		return p, true
	}
	if p, ok := binaryfuse.PolicyFromName(name); ok {
		return p, true
	}
	return nil, false
}

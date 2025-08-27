// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2/internal/base"
)

// obsoleteKeyBlockPropertyCollector is a block property collector used to
// implement obsoleteKeyBlockPropertyFilter - a filter that excludes blocks
// which contain only obsolete keys.
//
// For an explanation of obsolete keys, see the comment for TableFormatPebblev4
// which explains obsolete keys.
type obsoleteKeyBlockPropertyCollector struct {
	blockIsNonObsolete bool
	indexIsNonObsolete bool
	tableIsNonObsolete bool
}

var _ BlockPropertyCollector = (*obsoleteKeyBlockPropertyCollector)(nil)

// Name is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) Name() string {
	return "obsolete-key"
}

// AddPointKey is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) AddPointKey(key InternalKey, value []byte) error {
	// Ignore.
	return nil
}

// AddRangeKeys is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) AddRangeKeys(span Span) error {
	// Ignore.
	return nil
}

// AddPoint is an out-of-band method that is specific to this collector.
func (o *obsoleteKeyBlockPropertyCollector) AddPoint(isObsolete bool) {
	o.blockIsNonObsolete = o.blockIsNonObsolete || !isObsolete
}

// FinishDataBlock is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	o.tableIsNonObsolete = o.tableIsNonObsolete || o.blockIsNonObsolete
	return obsoleteKeyBlockPropertyEncode(!o.blockIsNonObsolete, buf), nil
}

// AddPrevDataBlockToIndexBlock is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) AddPrevDataBlockToIndexBlock() {
	o.indexIsNonObsolete = o.indexIsNonObsolete || o.blockIsNonObsolete
	o.blockIsNonObsolete = false
}

// FinishIndexBlock is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	buf = obsoleteKeyBlockPropertyEncode(!o.indexIsNonObsolete, buf)
	o.indexIsNonObsolete = false
	return buf, nil
}

// FinishTable is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) FinishTable(buf []byte) ([]byte, error) {
	return obsoleteKeyBlockPropertyEncode(!o.tableIsNonObsolete, buf), nil
}

// AddCollectedWithSuffixReplacement is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) AddCollectedWithSuffixReplacement(
	oldProp []byte, oldSuffix, newSuffix []byte,
) error {
	// Verify the property is valid.
	_, err := obsoleteKeyBlockPropertyDecode(oldProp)
	if err != nil {
		return err
	}
	// Suffix rewriting currently loses the obsolete bit.
	o.blockIsNonObsolete = true
	return nil
}

// SupportsSuffixReplacement is part of the BlockPropertyCollector interface.
func (o *obsoleteKeyBlockPropertyCollector) SupportsSuffixReplacement() bool {
	return true
}

// obsoleteKeyBlockPropertyFilter implements the filter that excludes blocks
// that only contain obsolete keys. It pairs with
// obsoleteKeyBlockPropertyCollector.
//
// Note that we filter data blocks as well as first-level index blocks.
//
// For an explanation of obsolete keys, see the comment for TableFormatPebblev4
// which explains obsolete keys.
//
// NB: obsoleteKeyBlockPropertyFilter is stateless. This aspect of the filter
// is used in table_cache.go for in-place modification of a filters slice.
type obsoleteKeyBlockPropertyFilter struct{}

var _ BlockPropertyFilter = obsoleteKeyBlockPropertyFilter{}

// Name is part of the BlockPropertyFilter interface. It must match
// obsoleteKeyBlockPropertyCollector.Name.
func (o obsoleteKeyBlockPropertyFilter) Name() string {
	return "obsolete-key"
}

// Intersects is part of the BlockPropertyFilter interface. It returns true
// if the block may contain non-obsolete keys.
func (o obsoleteKeyBlockPropertyFilter) Intersects(prop []byte) (bool, error) {
	isObsolete, err := obsoleteKeyBlockPropertyDecode(prop)
	if err != nil {
		return false, err
	}
	return !isObsolete, nil
}

// SyntheticSuffixIntersects is part of the BlockPropertyFilter interface. It
// expects that synthetic suffix is never used with tables that contain obsolete
// keys.
func (o obsoleteKeyBlockPropertyFilter) SyntheticSuffixIntersects(
	prop []byte, suffix []byte,
) (bool, error) {
	isObsolete, err := obsoleteKeyBlockPropertyDecode(prop)
	if err != nil {
		return false, err
	}
	// A block with suffix replacement should never be obsolete.
	if isObsolete {
		return false, base.AssertionFailedf("block with synthetic suffix is obsolete")
	}
	return true, nil
}

// Encodes the information of whether the block contains only obsolete keys. We
// use the empty encoding for the common case of a block not being obsolete.
func obsoleteKeyBlockPropertyEncode(isObsolete bool, buf []byte) []byte {
	if isObsolete {
		return append(buf, 't')
	}
	return buf
}

// Decodes the information of whether the block contains only obsolete keys (the
// inverse of obsoleteKeyBlockPropertyEncode).
func obsoleteKeyBlockPropertyDecode(prop []byte) (isObsolete bool, _ error) {
	switch {
	case len(prop) == 0:
		return false, nil
	case len(prop) == 1 && prop[0] == 't':
		return true, nil
	default:
		return false, errors.Errorf("invalid obsolete block property %x", prop)
	}
}

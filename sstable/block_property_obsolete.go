// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import "github.com/cockroachdb/errors"

type obsoleteKeyBlockPropertyCollector struct {
	blockIsNonObsolete bool
	indexIsNonObsolete bool
	tableIsNonObsolete bool
}

func encodeNonObsolete(isNonObsolete bool, buf []byte) []byte {
	if isNonObsolete {
		return buf
	}
	return append(buf, 't')
}

func (o *obsoleteKeyBlockPropertyCollector) Name() string {
	return "obsolete-key"
}

func (o *obsoleteKeyBlockPropertyCollector) Add(key InternalKey, value []byte) error {
	// Ignore.
	return nil
}

func (o *obsoleteKeyBlockPropertyCollector) AddPoint(isObsolete bool) {
	o.blockIsNonObsolete = o.blockIsNonObsolete || !isObsolete
}

func (o *obsoleteKeyBlockPropertyCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	o.tableIsNonObsolete = o.tableIsNonObsolete || o.blockIsNonObsolete
	return encodeNonObsolete(o.blockIsNonObsolete, buf), nil
}

func (o *obsoleteKeyBlockPropertyCollector) AddPrevDataBlockToIndexBlock() {
	o.indexIsNonObsolete = o.indexIsNonObsolete || o.blockIsNonObsolete
	o.blockIsNonObsolete = false
}

func (o *obsoleteKeyBlockPropertyCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	indexIsNonObsolete := o.indexIsNonObsolete
	o.indexIsNonObsolete = false
	return encodeNonObsolete(indexIsNonObsolete, buf), nil
}

func (o *obsoleteKeyBlockPropertyCollector) FinishTable(buf []byte) ([]byte, error) {
	return encodeNonObsolete(o.tableIsNonObsolete, buf), nil
}

func (o *obsoleteKeyBlockPropertyCollector) UpdateKeySuffixes(
	oldProp []byte, oldSuffix, newSuffix []byte,
) error {
	_, err := propToIsObsolete(oldProp)
	if err != nil {
		return err
	}
	// Suffix rewriting currently loses the obsolete bit.
	o.blockIsNonObsolete = true
	return nil
}

// NB: obsoleteKeyBlockPropertyFilter is stateless. This aspect of the filter
// is used in table_cache.go for in-place modification of a filters slice.
type obsoleteKeyBlockPropertyFilter struct{}

func (o obsoleteKeyBlockPropertyFilter) Name() string {
	return "obsolete-key"
}

// Intersects returns true if the set represented by prop intersects with
// the set in the filter.
func (o obsoleteKeyBlockPropertyFilter) Intersects(prop []byte) (bool, error) {
	return propToIsObsolete(prop)
}

func (o obsoleteKeyBlockPropertyFilter) SyntheticSuffixIntersects(
	prop []byte, suffix []byte,
) (bool, error) {
	// A block with suffix replacement should never be
	// obselete, so return an assertion error if it is.
	isNotObsolete, err := o.Intersects(prop)
	if err != nil {
		return false, err
	}
	if !isNotObsolete {
		return false, errors.AssertionFailedf("block with synthetic suffix is obselete")
	}
	return true, nil
}

func propToIsObsolete(prop []byte) (bool, error) {
	if len(prop) == 0 {
		return true, nil
	}
	if len(prop) > 1 || prop[0] != 't' {
		return false, errors.Errorf("unexpected property %x", prop)
	}
	return false, nil
}

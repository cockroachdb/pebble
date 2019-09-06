// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import "github.com/cockroachdb/pebble/internal/base"

// Compression exports the base.Compression type.
type Compression = base.Compression

// Exported Compression constants.
const (
	DefaultCompression = base.DefaultCompression
	NoCompression      = base.NoCompression
	SnappyCompression  = base.SnappyCompression
)

// FilterType exports the base.FilterType type.
type FilterType = base.FilterType

// Exported TableFilter constants.
const (
	TableFilter = base.TableFilter
)

// FilterWriter exports the base.FilterWriter type.
type FilterWriter = base.FilterWriter

// FilterPolicy exports the base.FilterPolicy type.
type FilterPolicy = base.FilterPolicy

// TableFormat exports the base.TableFormat type.
type TableFormat = base.TableFormat

// Exported TableFormat constants.
const (
	TableFormatRocksDBv2 = base.TableFormatRocksDBv2
	TableFormatLevelDB   = base.TableFormatLevelDB
)

// TablePropertyCollector exports the base.TablePropertyCollector type.
type TablePropertyCollector = base.TablePropertyCollector

// TableOptions exports the base.LevelOptions type.
type TableOptions = base.LevelOptions

// Options exports the base.Options type.
type Options = base.Options

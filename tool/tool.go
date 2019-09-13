// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/cache"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/spf13/cobra"
)

// Comparer exports the base.Comparer type.
type Comparer = base.Comparer

// FilterPolicy exports the base.FilterPolicy type.
type FilterPolicy = base.FilterPolicy

// Merger exports the base.Merger type.
type Merger = base.Merger

// T is the container for all of the introspection tools.
type T struct {
	Commands  []*cobra.Command
	db        *dbT
	manifest  *manifestT
	sstable   *sstableT
	wal       *walT
	opts      base.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers
}

// New creates a new introspection tool.
func New() *T {
	t := &T{
		opts: base.Options{
			Cache:    cache.New(128 << 20 /* 128 MB */),
			Filters:  make(map[string]FilterPolicy),
			ReadOnly: true,
		},
		comparers: make(sstable.Comparers),
		mergers:   make(sstable.Mergers),
	}

	t.RegisterComparer(base.DefaultComparer)
	t.RegisterFilter(bloom.FilterPolicy(10))
	t.RegisterMerger(base.DefaultMerger)

	t.db = newDB(&t.opts, t.comparers, t.mergers)
	t.manifest = newManifest(&t.opts, t.comparers)
	t.sstable = newSSTable(&t.opts, t.comparers, t.mergers)
	t.wal = newWAL(&t.opts, t.comparers)
	t.Commands = []*cobra.Command{
		t.db.Root,
		t.manifest.Root,
		t.sstable.Root,
		t.wal.Root,
	}
	return t
}

// RegisterComparer registers a comparer for use by the introspection tools.
func (t *T) RegisterComparer(c *Comparer) {
	t.comparers[c.Name] = c
}

// RegisterFilter registers a filter policy for use by the introspection tools.
func (t *T) RegisterFilter(f FilterPolicy) {
	t.opts.Filters[f.Name()] = f
}

// RegisterMerger registers a merger for use by the introspection tools.
func (t *T) RegisterMerger(m *Merger) {
	t.mergers[m.Name] = m
}

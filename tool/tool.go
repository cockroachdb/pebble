// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/spf13/cobra"
)

//go:generate go run -tags make_incorrect_manifests make_incorrect_manifests.go
//go:generate go run -tags make_test_find_db make_test_find_db.go
//go:generate go run -tags make_test_sstables make_test_sstables.go

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
	find      *findT
	lsm       *lsmT
	manifest  *manifestT
	sstable   *sstableT
	wal       *walT
	opts      pebble.Options
	comparers sstable.Comparers
	mergers   sstable.Mergers
}

// New creates a new introspection tool.
func New() *T {
	cache := pebble.NewCache(128 << 20 /* 128 MB */)

	t := &T{
		opts: pebble.Options{
			Cache:    cache,
			Filters:  make(map[string]FilterPolicy),
			FS:       vfs.Default,
			ReadOnly: true,
		},
		comparers: make(sstable.Comparers),
		mergers:   make(sstable.Mergers),
	}

	t.RegisterComparer(base.DefaultComparer)
	t.RegisterFilter(bloom.FilterPolicy(10))
	t.RegisterMerger(base.DefaultMerger)

	t.db = newDB(&t.opts, t.comparers, t.mergers)
	t.find = newFind(&t.opts, t.comparers)
	t.lsm = newLSM(&t.opts, t.comparers)
	t.manifest = newManifest(&t.opts, t.comparers)
	t.sstable = newSSTable(&t.opts, t.comparers, t.mergers)
	t.wal = newWAL(&t.opts, t.comparers)
	t.Commands = []*cobra.Command{
		t.db.Root,
		t.find.Root,
		t.lsm.Root,
		t.manifest.Root,
		t.sstable.Root,
		t.wal.Root,
	}

	runtime.SetFinalizer(t, func(obj interface{}) {
		cache.Unref()
	})
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

// setFS sets the filesystem implementation to use by the introspection tools.
func (t *T) setFS(fs vfs.FS) {
	t.opts.FS = fs
}

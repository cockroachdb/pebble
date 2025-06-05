// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package tool

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/blob"
	"github.com/cockroachdb/pebble/sstable/colblk"
	"github.com/cockroachdb/pebble/vfs"
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
	Commands        []*cobra.Command
	db              *dbT
	find            *findT
	lsm             *lsmT
	manifest        *manifestT
	remotecat       *remoteCatalogT
	sstable         *sstableT
	wal             *walT
	opts            pebble.Options
	comparers       sstable.Comparers
	mergers         sstable.Mergers
	defaultComparer string
	openErrEnhancer func(error) error
	openOptions     []OpenOption
	exciseSpanFn    DBExciseSpanFn
}

// A Option configures the Pebble introspection tool.
type Option func(*T)

// Comparers may be passed to New to register comparers for use by
// the introspesction tools.
func Comparers(cmps ...*Comparer) Option {
	return func(t *T) {
		for _, c := range cmps {
			t.comparers[c.Name] = c
		}
	}
}

// KeySchema configures the name of the schema to use when writing sstables.
func KeySchema(name string) Option {
	return func(t *T) {
		t.opts.KeySchema = name
	}
}

// KeySchemas may be passed to New to register key schemas for use by the
// introspection tools.
func KeySchemas(schemas ...*colblk.KeySchema) Option {
	return func(t *T) {
		if t.opts.KeySchemas == nil {
			t.opts.KeySchemas = make(map[string]*colblk.KeySchema)
		}
		for _, s := range schemas {
			t.opts.KeySchemas[s.Name] = s
		}
	}
}

// DefaultComparer registers a comparer for use by the introspection tools and
// sets it as the default.
func DefaultComparer(c *Comparer) Option {
	return func(t *T) {
		t.comparers[c.Name] = c
		t.defaultComparer = c.Name
	}
}

// Mergers may be passed to New to register mergers for use by the
// introspection tools.
func Mergers(mergers ...*Merger) Option {
	return func(t *T) {
		for _, m := range mergers {
			t.mergers[m.Name] = m
		}
	}
}

// Filters may be passed to New to register filter policies for use by the
// introspection tools.
func Filters(filters ...FilterPolicy) Option {
	return func(t *T) {
		for _, f := range filters {
			t.opts.Filters[f.Name()] = f
		}
	}
}

// OpenOptions may be passed to New to provide a set of OpenOptions that should
// be invoked to configure the *pebble.Options before opening a database.
func OpenOptions(openOptions ...OpenOption) Option {
	return func(t *T) {
		t.openOptions = append(t.openOptions, openOptions...)
	}
}

// FS sets the filesystem implementation to use by the introspection tools.
func FS(fs vfs.FS) Option {
	return func(t *T) {
		t.opts.FS = fs
	}
}

// OpenErrEnhancer sets a function that enhances an error encountered when the
// tool opens a database; used to provide the user additional context, for
// example that a corruption error might be caused by encryption at rest not
// being configured properly.
func OpenErrEnhancer(fn func(error) error) Option {
	return func(t *T) {
		t.openErrEnhancer = fn
	}
}

// DBExciseSpanFn is a function used to obtain the excise span for the `db
// excise` command. This allows a higher-level wrapper to add its own way of
// specifying the span. Returns an invalid KeyRange if none was specified (in
// which case the default --start/end flags are used).
type DBExciseSpanFn func() (pebble.KeyRange, error)

// WithDBExciseSpanFn specifies a function that returns the excise span for the
// `db excise` command.
func WithDBExciseSpanFn(fn DBExciseSpanFn) Option {
	return func(t *T) {
		t.exciseSpanFn = fn
	}
}

// New creates a new introspection tool.
func New(opts ...Option) *T {
	t := &T{
		opts: pebble.Options{
			Filters:  make(map[string]FilterPolicy),
			FS:       vfs.Default,
			ReadOnly: true,
		},
		comparers:       make(sstable.Comparers),
		mergers:         make(sstable.Mergers),
		defaultComparer: base.DefaultComparer.Name,
	}

	opts = append(opts,
		Comparers(base.DefaultComparer),
		Filters(bloom.FilterPolicy(10)),
		Mergers(base.DefaultMerger))

	for _, opt := range opts {
		opt(t)
	}

	t.db = newDB(&t.opts, t.comparers, t.mergers, t.openErrEnhancer, t.openOptions, t.exciseSpanFn)
	t.find = newFind(&t.opts, t.comparers, t.defaultComparer, t.mergers)
	t.lsm = newLSM(&t.opts, t.comparers)
	t.manifest = newManifest(&t.opts, t.comparers)
	t.remotecat = newRemoteCatalog(&t.opts)
	t.sstable = newSSTable(&t.opts, t.comparers, t.mergers)
	t.wal = newWAL(&t.opts, t.comparers, t.defaultComparer)
	t.Commands = []*cobra.Command{
		t.db.Root,
		t.find.Root,
		t.lsm.Root,
		t.manifest.Root,
		t.remotecat.Root,
		t.sstable.Root,
		t.wal.Root,
	}
	return t
}

// ConfigureSharedStorage updates the shared storage options.
func (t *T) ConfigureSharedStorage(
	s remote.StorageFactory,
	createOnShared remote.CreateOnSharedStrategy,
	createOnSharedLocator remote.Locator,
) {
	t.opts.Experimental.RemoteStorage = s
	t.opts.Experimental.CreateOnShared = createOnShared
	t.opts.Experimental.CreateOnSharedLocator = createOnSharedLocator
}

// BlobRefMode specifies how blob references should be handled.
type BlobRefMode int

const (
	// BlobRefModeNone specifies the AssertNoBlobHandles TableBlobContext.
	BlobRefModeNone BlobRefMode = iota
	// BlobRefModePrint specifies the DebugHandlesBlobContext TableBlobContext.
	BlobRefModePrint
	// BlobRefModeLoad specifies the LoadValBlobContext
	// TableBlobContext.
	BlobRefModeLoad
)

func ConvertToBlobRefMode(s string) BlobRefMode {
	switch s {
	case "print":
		return BlobRefModePrint
	case "load":
		return BlobRefModeLoad
	default:
		return BlobRefModeNone
	}
}

// debugReaderProvider is a cache-less ReaderProvider meant for debugging blob
// files.
type debugReaderProvider struct {
	objProvider objstorage.Provider
}

// Assert that *debugReaderProvider implements blob.ReaderProvider.
var _ blob.ReaderProvider = (*debugReaderProvider)(nil)

// GetValueReader returns a blob.ValueReader for a blob file identified by
// fileNum.
func (p *debugReaderProvider) GetValueReader(
	ctx context.Context, fileNum base.DiskFileNum,
) (blob.ValueReader, func(), error) {
	readable, err := p.objProvider.OpenForReading(ctx, base.FileTypeBlob, fileNum, objstorage.OpenOptions{})
	if err != nil {
		return nil, nil, err
	}
	r, err := blob.NewFileReader(ctx, readable, blob.FileReaderOptions{})
	if err != nil {
		return nil, nil, err
	}
	closeHook := func() {
		_ = r.Close()
	}
	return r, closeHook, nil
}

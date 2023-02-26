// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage/shared"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestProvider(t *testing.T) {
	var log base.InMemLogger
	fs := vfs.WithLogging(vfs.NewMem(), func(fmt string, args ...interface{}) {
		log.Infof("<local fs> "+fmt, args...)
	})
	sharedStore := shared.WithLogging(shared.NewInMem(), func(fmt string, args ...interface{}) {
		log.Infof("<shared> "+fmt, args...)
	})

	providers := make(map[string]*Provider)
	backings := make(map[string]SharedObjectBacking)
	var curProvider *Provider
	datadriven.RunTest(t, "testdata/provider", func(t *testing.T, d *datadriven.TestData) string {
		scanArgs := func(desc string, args ...interface{}) {
			t.Helper()
			if len(d.CmdArgs) != len(args) {
				d.Fatalf(t, "usage: %s %s", d.Cmd, desc)
			}
			for i := range args {
				_, err := fmt.Sscan(d.CmdArgs[i].String(), args[i])
				if err != nil {
					d.Fatalf(t, "%s: error parsing argument '%s'", d.Cmd, d.CmdArgs[i])
				}
			}
		}

		log.Reset()
		switch d.Cmd {
		case "open":
			var fsDir string
			var creatorID CreatorID
			scanArgs("<fs-dir> <shared-creator-id>", &fsDir, &creatorID)

			st := DefaultSettings(fs, fsDir)
			if creatorID != 0 {
				st.Shared.Storage = sharedStore
				st.Shared.CreatorID = creatorID
			}
			require.NoError(t, fs.MkdirAll(fsDir, 0755))
			provider, err := Open(st)
			require.NoError(t, err)
			providers[fsDir] = provider
			curProvider = provider

			return log.String()

		case "close":
			require.NoError(t, curProvider.Sync())
			require.NoError(t, curProvider.Close())
			delete(providers, curProvider.st.FSDirName)
			curProvider = nil

			return log.String()

		case "create":
			var fileNum base.FileNum
			var typ string
			scanArgs("<file-num> <local|shared>", &fileNum, &typ)
			var opts CreateOptions
			switch typ {
			case "local":
			case "shared":
				opts.PreferSharedStorage = true
			default:
				d.Fatalf(t, "'%s' should be 'local' or 'shared'", typ)
			}
			w, _, err := curProvider.Create(base.FileTypeTable, fileNum, opts)
			if err != nil {
				return err.Error()
			}
			_, err = w.Write([]byte(d.Input))
			require.NoError(t, err)
			require.NoError(t, w.Sync())
			require.NoError(t, w.Close())

			return log.String()

		case "read":
			var fileNum base.FileNum
			scanArgs("<file-num>", &fileNum)
			r, err := curProvider.OpenForReading(base.FileTypeTable, fileNum)
			if err != nil {
				return err.Error()
			}
			data := make([]byte, int(r.Size()))
			n, err := r.ReadAt(data, 0)
			require.NoError(t, err)
			require.Equal(t, n, len(data))
			return log.String() + fmt.Sprintf("data: %s\n", string(data))

		case "list":
			for _, meta := range curProvider.List() {
				log.Infof("%s -> %s", meta.FileNum, curProvider.Path(meta))
			}
			return log.String()

		case "save-backing":
			var key string
			var fileNum base.FileNum
			scanArgs("<key> <file-num>", &key, &fileNum)
			meta, err := curProvider.Lookup(base.FileTypeTable, fileNum)
			require.NoError(t, err)
			backing, err := meta.SharedObjectBacking()
			if err != nil {
				return err.Error()
			}
			backings[key] = backing
			return log.String()

		case "attach":
			lines := strings.Split(d.Input, "\n")
			if len(lines) == 0 {
				d.Fatalf(t, "at least one row expected; format: <key> <file-num>")
			}
			var objs []SharedObjectToAttach
			for _, l := range lines {
				var key string
				var fileNum base.FileNum
				_, err := fmt.Sscan(l, &key, &fileNum)
				require.NoError(t, err)
				backing, ok := backings[key]
				if !ok {
					d.Fatalf(t, "unknown backing key %q", key)
				}
				objs = append(objs, SharedObjectToAttach{
					FileType: base.FileTypeTable,
					FileNum:  fileNum,
					Backing:  backing,
				})
			}
			metas, err := curProvider.AttachSharedObjects(objs)
			require.NoError(t, err)
			for _, meta := range metas {
				log.Infof("%s -> %s", meta.FileNum, curProvider.Path(meta))
			}
			return log.String()

		default:
			d.Fatalf(t, "unknown command %s", d.Cmd)
			return ""
		}
	})
}

func TestNotExistError(t *testing.T) {
	// TODO(radu): test with shared objects.
	var log base.InMemLogger
	fs := vfs.WithLogging(vfs.NewMem(), log.Infof)
	provider, err := Open(DefaultSettings(fs, ""))
	require.NoError(t, err)

	require.True(t, IsNotExistError(provider.Remove(base.FileTypeTable, 1)))
	_, err = provider.OpenForReading(base.FileTypeTable, 1)
	require.True(t, IsNotExistError(err))

	w, _, err := provider.Create(base.FileTypeTable, 1, CreateOptions{})
	require.NoError(t, err)
	_, err = w.Write([]byte("foo"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	// Remove the underlying file.
	require.NoError(t, fs.Remove(base.MakeFilename(base.FileTypeTable, 1)))
	require.True(t, IsNotExistError(provider.Remove(base.FileTypeTable, 1)))
}

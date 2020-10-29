// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/datadriven"
	"github.com/stretchr/testify/require"
)

func normalizeError(err error) error {
	// It is OS-specific which errors match IsExist, IsNotExist, and
	// IsPermission, with OS-specific error messages. We normalize to the
	// oserror.Err* errors which have standard error messages across
	// platforms.
	switch {
	case oserror.IsExist(err):
		return oserror.ErrExist
	case oserror.IsNotExist(err):
		return oserror.ErrNotExist
	case oserror.IsPermission(err):
		return oserror.ErrPermission
	}
	return err
}

type loggingFS struct {
	FS
	base    string
	w       io.Writer
	linkErr error
}

func (fs loggingFS) stripBase(path string) string {
	if strings.HasPrefix(path, fs.base+"/") {
		return path[len(fs.base)+1:]
	}
	return path
}

func (fs loggingFS) Create(name string) (File, error) {
	f, err := fs.FS.Create(name)
	fmt.Fprintf(fs.w, "create: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return loggingFile{f, fs.PathBase(name), fs.w}, err
}

func (fs loggingFS) Link(oldname, newname string) error {
	err := fs.linkErr
	if err == nil {
		err = fs.FS.Link(oldname, newname)
	}
	fmt.Fprintf(fs.w, "link: %s -> %s [%v]\n",
		fs.stripBase(oldname), fs.stripBase(newname), normalizeError(err))
	return err
}

func (fs loggingFS) ReuseForWrite(oldname, newname string) (File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err == nil {
		f = loggingFile{f, fs.PathBase(newname), fs.w}
	}
	fmt.Fprintf(fs.w, "reuseForWrite: %s -> %s [%v]\n",
		fs.stripBase(oldname), fs.stripBase(newname), normalizeError(err))
	return f, err
}

func (fs loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	err := fs.FS.MkdirAll(dir, perm)
	fmt.Fprintf(fs.w, "mkdir: %s [%v]\n", fs.stripBase(dir), normalizeError(err))
	return err
}

func (fs loggingFS) Open(name string, opts ...OpenOption) (File, error) {
	f, err := fs.FS.Open(name, opts...)
	fmt.Fprintf(fs.w, "open: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return loggingFile{f, fs.stripBase(name), fs.w}, err
}

func (fs loggingFS) Remove(name string) error {
	err := fs.FS.Remove(name)
	fmt.Fprintf(fs.w, "remove: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return err
}

func (fs loggingFS) RemoveAll(name string) error {
	err := fs.FS.RemoveAll(name)
	fmt.Fprintf(fs.w, "remove-all: %s [%v]\n", fs.stripBase(name), normalizeError(err))
	return err
}

type loggingFile struct {
	File
	name string
	w    io.Writer
}

func (f loggingFile) Close() error {
	err := f.File.Close()
	fmt.Fprintf(f.w, "close: %s [%v]\n", f.name, err)
	return err
}

func (f loggingFile) Sync() error {
	err := f.File.Sync()
	fmt.Fprintf(f.w, "sync: %s [%v]\n", f.name, err)
	return err
}

func runTestVFS(t *testing.T, baseFS FS, dir string) {
	var buf bytes.Buffer
	fs := loggingFS{FS: baseFS, base: dir, w: &buf}

	datadriven.RunTest(t, "testdata/vfs", func(td *datadriven.TestData) string {
		switch td.Cmd {
		case "define":
			buf.Reset()

			for _, arg := range td.CmdArgs {
				switch arg.Key {
				case "linkErr":
					if len(arg.Vals) != 1 {
						return fmt.Sprintf("%s: %s expected 1 value", td.Cmd, arg.Key)
					}
					switch arg.Vals[0] {
					case "ErrExist":
						fs.linkErr = oserror.ErrExist
					case "ErrNotExist":
						fs.linkErr = oserror.ErrNotExist
					case "ErrPermission":
						fs.linkErr = oserror.ErrPermission
					default:
						fs.linkErr = errors.New(arg.Vals[0])
					}
				default:
					return fmt.Sprintf("%s: unknown arg: %s", td.Cmd, arg.Key)
				}
			}

			for _, line := range strings.Split(td.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					return fmt.Sprintf("<op> [<args>]")
				}

				switch parts[0] {
				case "clone":
					if len(parts) != 3 {
						return fmt.Sprintf("clone <src> <dest>")
					}
					_, _ = Clone(fs, fs, fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "create":
					if len(parts) != 2 {
						return fmt.Sprintf("create <name>")
					}
					f, _ := fs.Create(fs.PathJoin(dir, parts[1]))
					f.Close()

				case "link":
					if len(parts) != 3 {
						return fmt.Sprintf("link <oldname> <newname>")
					}
					_ = fs.Link(fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "link-or-copy":
					if len(parts) != 3 {
						return fmt.Sprintf("link-or-copy <oldname> <newname>")
					}
					_ = LinkOrCopy(fs, fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "reuseForWrite":
					if len(parts) != 3 {
						return fmt.Sprintf("reuseForWrite <oldname> <newname>")
					}
					_, _ = fs.ReuseForWrite(fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "list":
					if len(parts) != 2 {
						return fmt.Sprintf("list <dir>")
					}
					paths, _ := fs.List(fs.PathJoin(dir, parts[1]))
					sort.Strings(paths)
					for _, p := range paths {
						fmt.Fprintln(&buf, p)
					}

				case "mkdir":
					if len(parts) != 2 {
						return fmt.Sprintf("mkdir <dir>")
					}
					_ = fs.MkdirAll(fs.PathJoin(dir, parts[1]), 0755)

				case "remove":
					if len(parts) != 2 {
						return fmt.Sprintf("remove <name>")
					}
					_ = fs.Remove(fs.PathJoin(dir, parts[1]))

				case "remove-all":
					if len(parts) != 2 {
						return fmt.Sprintf("remove-all <name>")
					}
					_ = fs.RemoveAll(fs.PathJoin(dir, parts[1]))
				}
			}

			return buf.String()

		default:
			return fmt.Sprintf("unknown command: %s", td.Cmd)
		}
	})
}

func TestVFS(t *testing.T) {
	t.Run("mem", func(t *testing.T) {
		runTestVFS(t, NewMem(), "")
	})
	if runtime.GOOS != "windows" {
		t.Run("disk", func(t *testing.T) {
			dir, err := ioutil.TempDir("", "test-vfs")
			require.NoError(t, err)
			defer func() {
				_ = os.RemoveAll(dir)
			}()
			runTestVFS(t, Default, dir)
		})
	}
}

func TestVFSGetFreeSpace(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-free-space")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	_, err = Default.GetFreeSpace(dir)
	require.Nil(t, err)
}

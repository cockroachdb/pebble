// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
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

	datadriven.RunTest(t, "testdata/vfs", func(t *testing.T, td *datadriven.TestData) string {
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
					return "<op> [<args>]"
				}

				switch parts[0] {
				case "clone":
					if len(parts) < 3 {
						return "clone <src> <dest> [disk|mem] [link] [sync]"
					}
					dstFS := fs
					var opts []CloneOption
					for _, p := range parts[3:] {
						switch p {
						case "disk":
							dstFS = loggingFS{FS: Default, base: dir, w: &buf}
						case "mem":
							dstFS = loggingFS{FS: NewMem(), base: dir, w: &buf}
						case "link":
							opts = append(opts, CloneTryLink)
						case "sync":
							opts = append(opts, CloneSync)
						default:
							return fmt.Sprintf("unrecognized argument %q", p)
						}
					}

					_, _ = Clone(fs, dstFS, fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]), opts...)

				case "create":
					if len(parts) != 2 {
						return "create <name>"
					}
					f, _ := fs.Create(fs.PathJoin(dir, parts[1]))
					f.Close()

				case "link":
					if len(parts) != 3 {
						return "link <oldname> <newname>"
					}
					_ = fs.Link(fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "link-or-copy":
					if len(parts) != 3 {
						return "link-or-copy <oldname> <newname>"
					}
					_ = LinkOrCopy(fs, fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "reuseForWrite":
					if len(parts) != 3 {
						return "reuseForWrite <oldname> <newname>"
					}
					_, _ = fs.ReuseForWrite(fs.PathJoin(dir, parts[1]), fs.PathJoin(dir, parts[2]))

				case "list":
					if len(parts) != 2 {
						return "list <dir>"
					}
					paths, _ := fs.List(fs.PathJoin(dir, parts[1]))
					sort.Strings(paths)
					for _, p := range paths {
						fmt.Fprintln(&buf, p)
					}

				case "mkdir":
					if len(parts) != 2 {
						return "mkdir <dir>"
					}
					_ = fs.MkdirAll(fs.PathJoin(dir, parts[1]), 0755)

				case "remove":
					if len(parts) != 2 {
						return "remove <name>"
					}
					_ = fs.Remove(fs.PathJoin(dir, parts[1]))

				case "remove-all":
					if len(parts) != 2 {
						return "remove-all <name>"
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
			dir, err := os.MkdirTemp("", "test-vfs")
			require.NoError(t, err)
			defer func() {
				_ = os.RemoveAll(dir)
			}()
			runTestVFS(t, Default, dir)
		})
	}
}

func TestVFSGetDiskUsage(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-free-space")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	_, err = Default.GetDiskUsage(dir)
	require.Nil(t, err)
}

func TestVFSCreateLinkSemantics(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-create-link")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	for _, fs := range []FS{Default, NewMem()} {
		t.Run(fmt.Sprintf("%T", fs), func(t *testing.T) {
			writeFile := func(path, contents string) {
				path = fs.PathJoin(dir, path)
				f, err := fs.Create(path)
				require.NoError(t, err)
				_, err = f.Write([]byte(contents))
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			readFile := func(path string) string {
				path = fs.PathJoin(dir, path)
				f, err := fs.Open(path)
				require.NoError(t, err)
				b, err := io.ReadAll(f)
				require.NoError(t, err)
				require.NoError(t, f.Close())
				return string(b)
			}
			require.NoError(t, fs.MkdirAll(dir, 0755))

			// Write a file 'foo' and create a hardlink at 'bar'.
			writeFile("foo", "foo")
			require.NoError(t, fs.Link(fs.PathJoin(dir, "foo"), fs.PathJoin(dir, "bar")))

			// Both files should contain equal contents, because they're backed by
			// the same inode.
			require.Equal(t, "foo", readFile("foo"))
			require.Equal(t, "foo", readFile("bar"))

			// Calling Create on 'bar' must NOT truncate 'foo'. It should create a
			// new file at path 'bar' with a new inode.
			writeFile("bar", "bar")

			require.Equal(t, "foo", readFile("foo"))
			require.Equal(t, "bar", readFile("bar"))
		})
	}
}

// TestVFSRootDirName ensures that opening the root directory on both the
// Default and MemFS works and returns a File which has the name of the
// path separator for the FS (always sep for MemFS).
func TestVFSRootDirName(t *testing.T) {
	for _, fs := range []FS{Default, NewMem()} {
		rootDir, err := fs.Open("/")
		require.NoError(t, err)
		fi, err := rootDir.Stat()
		require.NoError(t, err)

		exp := sep
		if fs == Default {
			exp = string(os.PathSeparator)
		}
		require.Equal(t, exp, fi.Name())
	}
}

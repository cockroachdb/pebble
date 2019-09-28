// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/internal/datadriven"
)

func normalizeError(err error) error {
	// It is OS-specific which errors match IsExist, IsNotExist, and
	// IsPermission, with OS-specific error messages. We normalize to the os.Err*
	// errors which have standard error messages across platforms.
	switch {
	case os.IsExist(err):
		return os.ErrExist
	case os.IsNotExist(err):
		return os.ErrNotExist
	case os.IsPermission(err):
		return os.ErrPermission
	}
	return err
}

type loggingFS struct {
	FS
	w       io.Writer
	linkErr error
}

func (fs loggingFS) Create(name string) (File, error) {
	f, err := fs.FS.Create(name)
	fmt.Fprintf(fs.w, "create: %s [%v]\n", fs.PathBase(name), normalizeError(err))
	return loggingFile{f, fs.PathBase(name), fs.w}, err
}

func (fs loggingFS) Link(oldname, newname string) error {
	err := fs.linkErr
	if err == nil {
		err = fs.FS.Link(oldname, newname)
	}
	fmt.Fprintf(fs.w, "link: %s -> %s [%v]\n",
		fs.PathBase(oldname), fs.PathBase(newname), normalizeError(err))
	return err
}

func (fs loggingFS) Open(name string, opts ...OpenOption) (File, error) {
	f, err := fs.FS.Open(name, opts...)
	fmt.Fprintf(fs.w, "open: %s [%v]\n", fs.PathBase(name), normalizeError(err))
	return loggingFile{f, fs.PathBase(name), fs.w}, err
}

func (fs loggingFS) Remove(name string) error {
	err := fs.FS.Remove(name)
	fmt.Fprintf(fs.w, "remove: %s [%v]\n", fs.PathBase(name), normalizeError(err))
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
	fs := loggingFS{FS: baseFS, w: &buf}

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
						fs.linkErr = os.ErrExist
					case "ErrNotExist":
						fs.linkErr = os.ErrNotExist
					case "ErrPermission":
						fs.linkErr = os.ErrPermission
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
				case "create":
					if len(parts) != 2 {
						return fmt.Sprintf("create <name>")
					}
					_, _ = fs.Create(fs.PathJoin(dir, parts[1]))

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

				case "remove":
					if len(parts) != 2 {
						return fmt.Sprintf("remove <name>")
					}
					_ = fs.Remove(fs.PathJoin(dir, parts[1]))
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
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.RemoveAll(dir)
			}()
			runTestVFS(t, Default, dir)
		})
	}
}

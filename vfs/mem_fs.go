// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs // import "github.com/cockroachdb/pebble/vfs"

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/invariants"
)

const sep = "/"

// NewMem returns a new memory-backed FS implementation.
func NewMem() *MemFS {
	return &MemFS{
		root: newRootMemNode(),
	}
}

// NewStrictMem returns a "strict" memory-backed FS implementation. The behaviour is strict wrt
// needing a Sync() call on files or directories for the state changes to be finalized. Any
// changes that are not finalized are visible to reads until MemFS.ResetToSyncedState() is called,
// at which point they are discarded and no longer visible.
//
// Expected usage:
//
//	strictFS := NewStrictMem()
//	db := Open(..., &Options{FS: strictFS})
//	// Do and commit various operations.
//	...
//	// Prevent any more changes to finalized state.
//	strictFS.SetIgnoreSyncs(true)
//	// This will finish any ongoing background flushes, compactions but none of these writes will
//	// be finalized since syncs are being ignored.
//	db.Close()
//	// Discard unsynced state.
//	strictFS.ResetToSyncedState()
//	// Allow changes to finalized state.
//	strictFS.SetIgnoreSyncs(false)
//	// Open the DB. This DB should have the same state as if the earlier strictFS operations and
//	// db.Close() were not called.
//	db := Open(..., &Options{FS: strictFS})
func NewStrictMem() *MemFS {
	return &MemFS{
		root:   newRootMemNode(),
		strict: true,
	}
}

// NewMemFile returns a memory-backed File implementation. The memory-backed
// file takes ownership of data.
func NewMemFile(data []byte) File {
	n := &memNode{}
	n.refs.Store(1)
	n.mu.data.data = data
	n.mu.modTime = time.Now()
	return &memFile{
		n:    n,
		read: true,
	}
}

// MemFS implements FS.
type MemFS struct {
	mu   sync.Mutex
	root *memNode

	// lockFiles holds a map of open file locks. Presence in this map indicates
	// a file lock is currently held. Keys are strings holding the path of the
	// locked file. The stored value is untyped and  unused; only presence of
	// the key within the map is significant.
	lockedFiles sync.Map
	strict      bool
	ignoreSyncs bool
	// Windows has peculiar semantics with respect to hard links and deleting
	// open files. In tests meant to exercise this behavior, this flag can be
	// set to error if removing an open file.
	windowsSemantics bool
}

var _ FS = &MemFS{}

// UseWindowsSemantics configures whether the MemFS implements Windows-style
// semantics, in particular with respect to whether any of an open file's links
// may be removed. Windows semantics default to off.
func (y *MemFS) UseWindowsSemantics(windowsSemantics bool) {
	y.mu.Lock()
	defer y.mu.Unlock()
	y.windowsSemantics = windowsSemantics
}

// String dumps the contents of the MemFS.
func (y *MemFS) String() string {
	y.mu.Lock()
	defer y.mu.Unlock()

	s := new(bytes.Buffer)
	y.root.dump(s, 0, sep)
	return s.String()
}

// SetIgnoreSyncs sets the MemFS.ignoreSyncs field. See the usage comment with NewStrictMem() for
// details.
func (y *MemFS) SetIgnoreSyncs(ignoreSyncs bool) {
	if !y.strict {
		panic("SetIgnoreSyncs can only be used on a strict MemFS")
	}
	y.mu.Lock()
	y.ignoreSyncs = ignoreSyncs
	y.mu.Unlock()
}

// ResetToSyncedState discards state in the FS that is not synced. See the usage comment with
// NewStrictMem() for details.
func (y *MemFS) ResetToSyncedState() {
	if !y.strict {
		panic("ResetToSyncedState can only be used on a strict MemFS")
	}
	y.mu.Lock()
	y.root.resetToSyncedState()
	y.mu.Unlock()
}

// walk walks the directory tree for the fullname, calling f at each step. If
// f returns an error, the walk will be aborted and return that same error.
//
// Each walk is atomic: y's mutex is held for the entire operation, including
// all calls to f.
//
// dir is the directory at that step, frag is the name fragment, and final is
// whether it is the final step. For example, walking "/foo/bar/x" will result
// in 3 calls to f:
//   - "/", "foo", false
//   - "/foo/", "bar", false
//   - "/foo/bar/", "x", true
//
// Similarly, walking "/y/z/", with a trailing slash, will result in 3 calls to f:
//   - "/", "y", false
//   - "/y/", "z", false
//   - "/y/z/", "", true
func (y *MemFS) walk(fullname string, f func(dir *memNode, frag string, final bool) error) error {
	y.mu.Lock()
	defer y.mu.Unlock()

	// For memfs, the current working directory is the same as the root directory,
	// so we strip off any leading "/"s to make fullname a relative path, and
	// the walk starts at y.root.
	for len(fullname) > 0 && fullname[0] == sep[0] {
		fullname = fullname[1:]
	}
	dir := y.root

	for {
		frag, remaining := fullname, ""
		i := strings.IndexRune(fullname, rune(sep[0]))
		final := i < 0
		if !final {
			frag, remaining = fullname[:i], fullname[i+1:]
			for len(remaining) > 0 && remaining[0] == sep[0] {
				remaining = remaining[1:]
			}
		}
		if err := f(dir, frag, final); err != nil {
			return err
		}
		if final {
			break
		}
		child := dir.find(frag)
		if child == nil {
			return &os.PathError{
				Op:   "open",
				Path: fullname,
				Err:  oserror.ErrNotExist,
			}
		}
		if !child.isDir {
			return &os.PathError{
				Op:   "open",
				Path: fullname,
				Err:  errors.New("not a directory"),
			}
		}
		dir, fullname = child, remaining
	}
	return nil
}

// Create implements FS.Create.
func (y *MemFS) Create(fullname string) (File, error) {
	var ret *memFile
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n := &memNode{}
			dir.editedChildren[frag] = n
			ret = &memFile{
				name:  frag,
				n:     n,
				fs:    y,
				read:  true,
				write: true,
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	ret.n.refs.Add(1)
	return ret, nil
}

// Link implements FS.Link.
func (y *MemFS) Link(oldname, newname string) error {
	var n *memNode
	err := y.walk(oldname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n = dir.find(frag)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if n == nil {
		return &os.LinkError{
			Op:  "link",
			Old: oldname,
			New: newname,
			Err: oserror.ErrNotExist,
		}
	}
	return y.walk(newname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			if dir.find(frag) != nil {
				return &os.LinkError{
					Op:  "link",
					Old: oldname,
					New: newname,
					Err: oserror.ErrExist,
				}
			}
			dir.editedChildren[frag] = n
		}
		return nil
	})
}

func (y *MemFS) open(fullname string, openForWrite bool) (File, error) {
	var ret *memFile
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				ret = &memFile{
					name: sep, // this is the root directory
					n:    dir,
					fs:   y,
				}
				return nil
			}
			if n := dir.find(frag); n != nil {
				ret = &memFile{
					name:  frag,
					n:     n,
					fs:    y,
					read:  true,
					write: openForWrite,
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, &os.PathError{
			Op:   "open",
			Path: fullname,
			Err:  oserror.ErrNotExist,
		}
	}
	ret.n.refs.Add(1)
	return ret, nil
}

// Open implements FS.Open.
func (y *MemFS) Open(fullname string, opts ...OpenOption) (File, error) {
	return y.open(fullname, false /* openForWrite */)
}

// OpenReadWrite implements FS.OpenReadWrite.
func (y *MemFS) OpenReadWrite(fullname string, opts ...OpenOption) (File, error) {
	f, err := y.open(fullname, true /* openForWrite */)
	pathErr, ok := err.(*os.PathError)
	if ok && pathErr.Err == oserror.ErrNotExist {
		return y.Create(fullname)
	}
	return f, err
}

// OpenDir implements FS.OpenDir.
func (y *MemFS) OpenDir(fullname string) (File, error) {
	return y.open(fullname, false /* openForWrite */)
}

// Remove implements FS.Remove.
func (y *MemFS) Remove(fullname string) error {
	return y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			child := dir.find(frag)
			if child == nil {
				return oserror.ErrNotExist
			}
			if y.windowsSemantics {
				// Disallow removal of open files/directories which implements
				// Windows semantics. This ensures that we don't regress in the
				// ordering of operations and try to remove a file while it is
				// still open.
				if n := child.refs.Load(); n > 0 {
					return oserror.ErrInvalid
				}
			}
			if !child.empty() {
				return errNotEmpty
			}
			dir.remove(frag)
		}
		return nil
	})
}

// RemoveAll implements FS.RemoveAll.
func (y *MemFS) RemoveAll(fullname string) error {
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			if dir.find(frag) == nil {
				return nil
			}
			dir.remove(frag)
		}
		return nil
	})
	// Match os.RemoveAll which returns a nil error even if the parent
	// directories don't exist.
	if oserror.IsNotExist(err) {
		err = nil
	}
	return err
}

// Rename implements FS.Rename.
func (y *MemFS) Rename(oldname, newname string) error {
	var n *memNode
	err := y.walk(oldname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			if n = dir.find(frag); n == nil {
				return nil
			}
			dir.remove(frag)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if n == nil {
		return &os.PathError{
			Op:   "open",
			Path: oldname,
			Err:  oserror.ErrNotExist,
		}
	}
	return y.walk(newname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			dir.editedChildren[frag] = n
		}
		return nil
	})
}

// ReuseForWrite implements FS.ReuseForWrite.
func (y *MemFS) ReuseForWrite(oldname, newname string) (File, error) {
	if err := y.Rename(oldname, newname); err != nil {
		return nil, err
	}
	f, err := y.Open(newname)
	if err != nil {
		return nil, err
	}
	y.mu.Lock()
	defer y.mu.Unlock()

	mf := f.(*memFile)
	mf.read = false
	mf.write = true
	return f, nil
}

// MkdirAll implements FS.MkdirAll.
func (y *MemFS) MkdirAll(dirname string, perm os.FileMode) error {
	return y.walk(dirname, func(dir *memNode, frag string, final bool) error {
		if frag == "" {
			if final {
				return nil
			}
			return errors.New("pebble/vfs: empty file name")
		}
		child := dir.find(frag)
		if child == nil {
			dir.editedChildren[frag] = &memNode{
				syncedChildren: make(map[string]*memNode),
				editedChildren: make(map[string]*memNode),
				isDir:          true,
			}
			return nil
		}
		if !child.isDir {
			return &os.PathError{
				Op:   "open",
				Path: dirname,
				Err:  errors.New("not a directory"),
			}
		}
		return nil
	})
}

// Lock implements FS.Lock.
func (y *MemFS) Lock(fullname string) (io.Closer, error) {
	// FS.Lock excludes other processes, but other processes cannot see this
	// process' memory. However some uses (eg, Cockroach tests) may open and
	// close the same MemFS-backed database multiple times. We want mutual
	// exclusion in this case too. See cockroachdb/cockroach#110645.
	_, loaded := y.lockedFiles.Swap(fullname, nil /* the value itself is insignificant */)
	if loaded {
		// This file lock has already been acquired. On unix, this results in
		// either EACCES or EAGAIN so we mimic.
		return nil, syscall.EAGAIN
	}
	// Otherwise, we successfully acquired the lock. Locks are visible in the
	// parent directory listing, and they also must be created under an existent
	// directory. Create the path so that we have the normal detection of
	// non-existent directory paths, and make the lock visible when listing
	// directory entries.
	f, err := y.Create(fullname)
	if err != nil {
		// "Release" the lock since we failed.
		y.lockedFiles.Delete(fullname)
		return nil, err
	}
	return &memFileLock{
		y:        y,
		f:        f,
		fullname: fullname,
	}, nil
}

// List implements FS.List.
func (y *MemFS) List(dirname string) ([]string, error) {
	if !strings.HasSuffix(dirname, sep) {
		dirname += sep
	}
	var ret []string
	err := y.walk(dirname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag != "" {
				panic("unreachable")
			}
			ret = dir.children()
		}
		return nil
	})
	return ret, err
}

// Stat implements FS.Stat.
func (y *MemFS) Stat(name string) (os.FileInfo, error) {
	f, err := y.Open(name)
	if err != nil {
		if pe, ok := err.(*os.PathError); ok {
			pe.Op = "stat"
		}
		return nil, err
	}
	defer f.Close()
	return f.Stat()
}

// PathBase implements FS.PathBase.
func (*MemFS) PathBase(p string) string {
	// Note that MemFS uses forward slashes for its separator, hence the use of
	// path.Base, not filepath.Base.
	return path.Base(p)
}

// PathJoin implements FS.PathJoin.
func (*MemFS) PathJoin(elem ...string) string {
	// Note that MemFS uses forward slashes for its separator, hence the use of
	// path.Join, not filepath.Join.
	return path.Join(elem...)
}

// PathDir implements FS.PathDir.
func (*MemFS) PathDir(p string) string {
	// Note that MemFS uses forward slashes for its separator, hence the use of
	// path.Dir, not filepath.Dir.
	return path.Dir(p)
}

// GetDiskUsage implements FS.GetDiskUsage.
func (*MemFS) GetDiskUsage(string) (DiskUsage, error) {
	return DiskUsage{}, ErrUnsupported
}

// memNode holds a file's data or a directory's children.
type memNode struct {
	isDir bool
	refs  atomic.Int32

	// Mutable state.
	// - For a file: data, syncedDate, modTime: A file is only being mutated by a single goroutine,
	//   but there can be concurrent readers e.g. DB.Checkpoint() which can read WAL or MANIFEST
	//   files that are being written to. Additionally Sync() calls can be concurrent with writing.
	// - For a directory: children and syncedChildren. Concurrent writes are possible, and
	//   these are protected using MemFS.mu.
	mu struct {
		sync.Mutex
		data    sliceWithSync
		modTime time.Time
	}

	// INVARIANT: editedChildren[k] == (nil, ok) only if syncedChildren[k] != nil
	syncedChildren map[string]*memNode
	editedChildren map[string]*memNode
}

func newRootMemNode() *memNode {
	return &memNode{
		syncedChildren: make(map[string]*memNode),
		editedChildren: make(map[string]*memNode),
		isDir:          true,
	}
}

func (f *memNode) find(name string) *memNode {
	child, found := f.editedChildren[name]
	if !found {
		child = f.syncedChildren[name]
	}
	return child
}

func (f *memNode) remove(name string) {
	if _, existed := f.syncedChildren[name]; existed {
		f.editedChildren[name] = nil // tombstone
	} else {
		delete(f.editedChildren, name)
	}
}

func (f *memNode) children() []string {
	names := make([]string, 0, len(f.syncedChildren)+len(f.editedChildren))
	for name := range f.syncedChildren {
		if _, updated := f.editedChildren[name]; !updated {
			names = append(names, name)
		}
	}
	for name, n := range f.editedChildren {
		if n != nil { // skip tombstones
			names = append(names, name)
		}
	}
	return names
}

func (f *memNode) empty() bool {
	for _, n := range f.editedChildren {
		if n != nil {
			return false
		}
	}
	// All editedChildren contain a tombstone. By the invariant, they all existed
	// before in syncedChildren. If there were no more children other than those,
	// then the current list is empty.
	return len(f.editedChildren) == len(f.syncedChildren)
}

func (f *memNode) dump(w *bytes.Buffer, level int, name string) {
	if f.isDir {
		w.WriteString("          ")
	} else {
		f.mu.Lock()
		fmt.Fprintf(w, "%8d  ", len(f.mu.data.data))
		f.mu.Unlock()
	}
	for i := 0; i < level; i++ {
		w.WriteString("  ")
	}
	w.WriteString(name)
	if !f.isDir {
		w.WriteByte('\n')
		return
	}
	if level > 0 { // deal with the fact that the root's name is already "/"
		w.WriteByte(sep[0])
	}
	w.WriteByte('\n')
	names := f.children()
	sort.Strings(names)
	for _, name := range names {
		f.find(name).dump(w, level+1, name)
	}
}

func (f *memNode) resetToSyncedState() {
	if f.isDir {
		clear(f.editedChildren)
		for _, v := range f.syncedChildren {
			v.resetToSyncedState()
		}
	} else {
		f.mu.Lock()
		f.mu.data.reset()
		f.mu.Unlock()
	}
}

// memFile is a reader or writer of a node's data. Implements File.
type memFile struct {
	name        string
	n           *memNode
	fs          *MemFS // nil for a standalone memFile
	rpos        int
	wpos        int
	read, write bool
}

var _ File = (*memFile)(nil)

func (f *memFile) Close() error {
	if n := f.n.refs.Add(-1); n < 0 {
		panic(fmt.Sprintf("pebble: close of unopened file: %d", n))
	}
	// Set node pointer to nil, to cause panic on any subsequent method call. This
	// is a defence-in-depth to catch use-after-close or double-close bugs.
	f.n = nil
	return nil
}

func (f *memFile) Read(p []byte) (int, error) {
	if !f.read {
		return 0, errors.New("pebble/vfs: file was not opened for reading")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot read a directory")
	}
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	if f.rpos >= len(f.n.mu.data.data) {
		return 0, io.EOF
	}
	n := copy(p, f.n.mu.data.data[f.rpos:])
	f.rpos += n
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if !f.read {
		return 0, errors.New("pebble/vfs: file was not opened for reading")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot read a directory")
	}
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	if off >= int64(len(f.n.mu.data.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.n.mu.data.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	if !f.write {
		return 0, errors.New("pebble/vfs: file was not created for writing")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot write a directory")
	}
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	f.n.mu.modTime = time.Now()
	f.n.mu.data.writeAt(p, f.wpos)
	f.wpos += len(p)

	if invariants.Enabled {
		// Mutate the input buffer to flush out bugs in Pebble which expect the
		// input buffer to be unmodified.
		for i := range p {
			p[i] ^= 0xff
		}
	}
	return len(p), nil
}

func (f *memFile) WriteAt(p []byte, ofs int64) (int, error) {
	if !f.write {
		return 0, errors.New("pebble/vfs: file was not created for writing")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot write a directory")
	}
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	f.n.mu.modTime = time.Now()
	f.n.mu.data.grow(int(ofs) + len(p))
	f.n.mu.data.writeAt(p, int(ofs))
	return len(p), nil
}

func (f *memFile) Prefetch(offset int64, length int64) error { return nil }
func (f *memFile) Preallocate(offset, length int64) error    { return nil }

func (f *memFile) Stat() (os.FileInfo, error) {
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	return &memFileInfo{
		name:    f.name,
		size:    int64(len(f.n.mu.data.data)),
		modTime: f.n.mu.modTime,
		isDir:   f.n.isDir,
	}, nil
}

func (f *memFile) Sync() error {
	if f.fs == nil || !f.fs.strict {
		return nil
	}
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.ignoreSyncs {
		return nil
	}
	if f.n.isDir {
		for k, v := range f.n.editedChildren {
			if v == nil {
				delete(f.n.syncedChildren, k)
			} else {
				f.n.syncedChildren[k] = v
			}
		}
		clear(f.n.editedChildren)
	} else {
		f.n.mu.Lock()
		f.n.mu.data.sync()
		f.n.mu.Unlock()
	}
	return nil
}

func (f *memFile) SyncData() error {
	return f.Sync()
}

func (f *memFile) SyncTo(length int64) (fullSync bool, err error) {
	// NB: This SyncTo implementation lies, with its return values claiming it
	// synced the data up to `length`. When fullSync=false, SyncTo provides no
	// durability guarantees, so this can help surface bugs where we improperly
	// rely on SyncTo providing durability.
	return false, nil
}

func (f *memFile) Fd() uintptr {
	return InvalidFd
}

// Flush is a no-op and present only to prevent buffering at higher levels
// (e.g. it prevents sstable.Writer from using a bufio.Writer).
func (f *memFile) Flush() error {
	return nil
}

// memFileInfo implements os.FileInfo for a memFile.
type memFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

var _ os.FileInfo = (*memFileInfo)(nil)

func (f *memFileInfo) Name() string {
	return f.name
}

func (f *memFileInfo) Size() int64 {
	return f.size
}

func (f *memFileInfo) Mode() os.FileMode {
	if f.isDir {
		return os.ModeDir | 0755
	}
	return 0755
}

func (f *memFileInfo) ModTime() time.Time {
	return f.modTime
}

func (f *memFileInfo) IsDir() bool {
	return f.isDir
}

func (f *memFileInfo) Sys() interface{} {
	return nil
}

type memFileLock struct {
	y        *MemFS
	f        File
	fullname string
}

func (l *memFileLock) Close() error {
	if l.y == nil {
		return nil
	}
	l.y.lockedFiles.Delete(l.fullname)
	l.y = nil
	return l.f.Close()
}

// sliceWithSync is a []byte slice that can sync and rollback to the last synced
// version. It is most efficient for append-only workloads (as efficient as a
// regular slice).
type sliceWithSync struct {
	data   []byte
	synced []byte

	// INVARIANT: forked == false iff synced is a prefix of data, and is backed by
	// the same slice.
	//
	// If forked == false, it is only safe to modify the data slice starting from
	// index len(synced). The data slice is immutable up to len(synced).
	//
	// If forked == true, it is safe to modify the data slice freely at any index.
	forked bool
}

func (s *sliceWithSync) writeAt(data []byte, at int) {
	if at > len(s.data) {
		panic(fmt.Sprintf("index %d out of bounds (len = %d)", at, len(data)))
	} else if len(data) == 0 {
		return // nothing to write
	}
	var suffix []byte
	if end := at + len(data); end < len(s.data) {
		suffix = s.data[end:]
	}
	if !s.forked && at < len(s.synced) {
		s.data = append(append(s.data[:at:at], data...), suffix...)
		s.forked = true
		return
	}
	if len(suffix) == 0 {
		s.data = append(s.data[:at], data...)
	} else {
		copy(s.data[at:], data)
	}
}

func (s *sliceWithSync) grow(size int) {
	if size <= len(s.data) {
		return
	}
	for i := len(s.data); i < size; i++ {
		s.data = append(s.data, 0)
	}
}

// sync checkpoints the slice at the current state.
func (s *sliceWithSync) sync() {
	s.synced = s.data
	s.forked = false
}

// reset rollbacks the slice to the last sync-ed state, or empty if there was no
// sync before.
func (s *sliceWithSync) reset() {
	s.data = s.synced
	s.forked = false
}

// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs // import "github.com/cockroachdb/pebble/vfs"

import (
	"bytes"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"os"
	"path"
	"slices"
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

// NewCrashableMem returns a memory-backed FS implementation that supports the
// CrashClone() method. This method can be used to obtain a copy of the FS after
// a simulated crash, where only data that was last synced is guaranteed to be
// there (with no guarantees one way or the other about more recently written
// data).
//
// Note: when CrashClone() is not necessary, NewMem() is faster and should be
// preferred.
//
// Expected usage:
//
//		fs := NewCrashableMem()
//		db := Open(..., &Options{FS: fs})
//		// Do and commit various operations.
//		...
//		// Make a clone of the FS after a simulated crash.
//	 crashedFS := fs.CrashClone(CrashCloneCfg{Probability: 50, RNG: rand.New(rand.NewSource(0))})
//
//		// This will finish any ongoing background flushes, compactions but none of these writes will
//		// affect crashedFS.
//		db.Close()
//
//		// Open the DB against the crash clone.
//		db := Open(..., &Options{FS: crashedFS})
func NewCrashableMem() *MemFS {
	return &MemFS{
		root:      newRootMemNode(),
		crashable: true,
	}
}

// NewMemFile returns a memory-backed File implementation. The memory-backed
// file takes ownership of data.
func NewMemFile(data []byte) File {
	n := &memNode{}
	n.refs.Store(1)
	n.mu.data = data
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

	// cloneMu is used to block all modification operations while we clone the
	// filesystem. Only used when crashable is true.
	cloneMu sync.RWMutex

	// lockFiles holds a map of open file locks. Presence in this map indicates
	// a file lock is currently held. Keys are strings holding the path of the
	// locked file. The stored value is untyped and  unused; only presence of
	// the key within the map is significant.
	lockedFiles sync.Map
	crashable   bool
	// Windows has peculiar semantics with respect to hard links and deleting
	// open files. In tests meant to exercise this behavior, this flag can be
	// set to error if removing an open file.
	windowsSemantics bool
	usage            DiskUsage
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

// CrashCloneCfg configures a CrashClone call. The zero value corresponds to the
// crash clone containing exactly the data that was last synced.
type CrashCloneCfg struct {
	// UnsyncedDataPercent is the probability that a data block or directory entry
	// that was not synced will be part of the clone. If 0, the clone will contain
	// exactly the data that was last synced. If 100, the clone will be identical
	// to the current filesystem.
	UnsyncedDataPercent int
	// RNG must be set if UnsyncedDataPercent > 0.
	RNG *rand.Rand
}

// CrashClone creates a new filesystem that reflects a possible state of this
// filesystem after a crash at this moment. The new filesystem will contain all
// data that was synced, and some fraction of the data that was not synced. The
// latter is controlled by CrashCloneCfg.
func (y *MemFS) CrashClone(cfg CrashCloneCfg) *MemFS {
	if !y.crashable {
		panic("not a crashable MemFS")
	}
	// Block all modification operations while we clone.
	y.cloneMu.Lock()
	defer y.cloneMu.Unlock()
	newFS := &MemFS{crashable: true}
	newFS.windowsSemantics = y.windowsSemantics
	newFS.root = y.root.CrashClone(&cfg)
	return newFS
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
	if fullname == "." {
		fullname = ""
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
		child := dir.children[frag]
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
func (y *MemFS) Create(fullname string, category DiskWriteCategory) (File, error) {
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
	var ret *memFile
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n := &memNode{}
			dir.children[frag] = n
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
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
	var n *memNode
	err := y.walk(oldname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n = dir.children[frag]
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
			y.cloneMu.RLock()
			defer y.cloneMu.RUnlock()
			if _, ok := dir.children[frag]; ok {
				return &os.LinkError{
					Op:  "link",
					Old: oldname,
					New: newname,
					Err: oserror.ErrExist,
				}
			}
			dir.children[frag] = n
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
			if n := dir.children[frag]; n != nil {
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
func (y *MemFS) OpenReadWrite(
	fullname string, category DiskWriteCategory, opts ...OpenOption,
) (File, error) {
	f, err := y.open(fullname, true /* openForWrite */)
	pathErr, ok := err.(*os.PathError)
	if ok && pathErr.Err == oserror.ErrNotExist {
		return y.Create(fullname, category)
	}
	return f, err
}

// OpenDir implements FS.OpenDir.
func (y *MemFS) OpenDir(fullname string) (File, error) {
	return y.open(fullname, false /* openForWrite */)
}

// Remove implements FS.Remove.
func (y *MemFS) Remove(fullname string) error {
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
	return y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			child, ok := dir.children[frag]
			if !ok {
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
			if len(child.children) > 0 {
				return errNotEmpty
			}
			delete(dir.children, frag)
		}
		return nil
	})
}

// RemoveAll implements FS.RemoveAll.
func (y *MemFS) RemoveAll(fullname string) error {
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
	err := y.walk(fullname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			_, ok := dir.children[frag]
			if !ok {
				return nil
			}
			delete(dir.children, frag)
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
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
	var n *memNode
	err := y.walk(oldname, func(dir *memNode, frag string, final bool) error {
		if final {
			if frag == "" {
				return errors.New("pebble/vfs: empty file name")
			}
			n = dir.children[frag]
			delete(dir.children, frag)
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
			dir.children[frag] = n
		}
		return nil
	})
}

// ReuseForWrite implements FS.ReuseForWrite.
func (y *MemFS) ReuseForWrite(oldname, newname string, category DiskWriteCategory) (File, error) {
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
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
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
	return y.walk(dirname, func(dir *memNode, frag string, final bool) error {
		if frag == "" {
			if final {
				return nil
			}
			return errors.New("pebble/vfs: empty file name")
		}
		child := dir.children[frag]
		if child == nil {
			dir.children[frag] = &memNode{
				children: make(map[string]*memNode),
				isDir:    true,
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
	if y.crashable {
		y.cloneMu.RLock()
		defer y.cloneMu.RUnlock()
	}
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
	f, err := y.Create(fullname, WriteCategoryUnspecified)
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
			ret = make([]string, 0, len(dir.children))
			for s := range dir.children {
				ret = append(ret, s)
			}
		}
		return nil
	})
	return ret, err
}

// Stat implements FS.Stat.
func (y *MemFS) Stat(name string) (FileInfo, error) {
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

// TestingSetDiskUsage sets the disk usage that will be reported on subsequent
// GetDiskUsage calls; used for testing. If usage is empty, GetDiskUsage will
// return ErrUnsupported (which is also the default when TestingSetDiskUsage is
// not called).
func (y *MemFS) TestingSetDiskUsage(usage DiskUsage) {
	y.mu.Lock()
	defer y.mu.Unlock()
	y.usage = usage
}

// GetDiskUsage implements FS.GetDiskUsage.
func (y *MemFS) GetDiskUsage(string) (DiskUsage, error) {
	y.mu.Lock()
	defer y.mu.Unlock()
	if y.usage != (DiskUsage{}) {
		return y.usage, nil
	}
	return DiskUsage{}, ErrUnsupported
}

// Unwrap implements FS.Unwrap.
func (*MemFS) Unwrap() FS { return nil }

// UnsafeGetFileDataBuffer returns the buffer holding the data for a file. Must
// not be used while concurrent updates are happening to the file. The buffer
// cannot be modified while concurrent file reads are happening.
func (y *MemFS) UnsafeGetFileDataBuffer(fullname string) ([]byte, error) {
	f, err := y.Open(fullname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.(*memFile).n.mu.data, nil
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
		data       []byte
		syncedData []byte
		modTime    time.Time
	}

	children       map[string]*memNode
	syncedChildren map[string]*memNode // may be nil if never synced
}

func newRootMemNode() *memNode {
	return &memNode{
		children: make(map[string]*memNode),
		isDir:    true,
	}
}

func (f *memNode) dump(w *bytes.Buffer, level int, name string) {
	if f.isDir {
		w.WriteString("          ")
	} else {
		f.mu.Lock()
		fmt.Fprintf(w, "%8d  ", len(f.mu.data))
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
	names := slices.Collect(maps.Keys(f.children))
	sort.Strings(names)
	for _, name := range names {
		f.children[name].dump(w, level+1, name)
	}
}

// CrashClone creates a crash-consistent clone of the subtree rooted at f, and
// returns the new subtree. cloneMu must be held (in write mode).
func (f *memNode) CrashClone(cfg *CrashCloneCfg) *memNode {
	newNode := &memNode{isDir: f.isDir}
	if f.isDir {
		newNode.children = maps.Clone(f.syncedChildren)
		if newNode.children == nil {
			// syncedChildren may be nil, but children cannot.
			newNode.children = make(map[string]*memNode)
		}
		// Randomly include some non-synced children.
		for name, child := range f.children {
			if cfg.UnsyncedDataPercent > 0 && cfg.RNG.IntN(100) < cfg.UnsyncedDataPercent {
				newNode.children[name] = child
			}
		}
		for name, child := range newNode.children {
			newNode.children[name] = child.CrashClone(cfg)
		}
		newNode.syncedChildren = maps.Clone(newNode.children)
	} else {
		newNode.mu.data = slices.Clone(f.mu.syncedData)
		newNode.mu.modTime = f.mu.modTime
		// Randomly include some non-synced blocks.
		const blockSize = 4096
		for i := 0; i < len(f.mu.data); i += blockSize {
			if cfg.UnsyncedDataPercent > 0 && cfg.RNG.IntN(100) < cfg.UnsyncedDataPercent {
				block := f.mu.data[i:min(i+blockSize, len(f.mu.data))]
				if grow := i + len(block) - len(newNode.mu.data); grow > 0 {
					// Grow the file, leaving 0s for any unsynced blocks past the synced
					// length.
					newNode.mu.data = append(newNode.mu.data, make([]byte, grow)...)
				}
				copy(newNode.mu.data[i:], block)
			}
		}
		newNode.mu.syncedData = slices.Clone(newNode.mu.data)
	}
	return newNode
}

// memFile is a reader or writer of a node's data. Implements File.
type memFile struct {
	name        string
	n           *memNode
	fs          *MemFS // nil for a standalone memFile
	pos         int
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
	if f.pos >= len(f.n.mu.data) {
		return 0, io.EOF
	}
	n := copy(p, f.n.mu.data[f.pos:])
	f.pos += n
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
	if off >= int64(len(f.n.mu.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.n.mu.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	if f.fs.crashable {
		f.fs.cloneMu.RLock()
		defer f.fs.cloneMu.RUnlock()
	}
	if !f.write {
		return 0, errors.New("pebble/vfs: file was not created for writing")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot write a directory")
	}
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	f.n.mu.modTime = time.Now()
	if f.pos+len(p) <= len(f.n.mu.data) {
		n := copy(f.n.mu.data[f.pos:f.pos+len(p)], p)
		if n != len(p) {
			panic("stuff")
		}
	} else {
		if grow := f.pos - len(f.n.mu.data); grow > 0 {
			f.n.mu.data = append(f.n.mu.data, make([]byte, grow)...)
		}
		f.n.mu.data = append(f.n.mu.data[:f.pos], p...)
	}
	f.pos += len(p)

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
	if f.fs.crashable {
		f.fs.cloneMu.RLock()
		defer f.fs.cloneMu.RUnlock()
	}
	if !f.write {
		return 0, errors.New("pebble/vfs: file was not created for writing")
	}
	if f.n.isDir {
		return 0, errors.New("pebble/vfs: cannot write a directory")
	}
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	f.n.mu.modTime = time.Now()

	for len(f.n.mu.data) < int(ofs)+len(p) {
		f.n.mu.data = append(f.n.mu.data, 0)
	}

	n := copy(f.n.mu.data[int(ofs):int(ofs)+len(p)], p)
	if n != len(p) {
		panic("stuff")
	}

	return len(p), nil
}

func (f *memFile) Prefetch(offset int64, length int64) error { return nil }
func (f *memFile) Preallocate(offset, length int64) error    { return nil }

func (f *memFile) Stat() (FileInfo, error) {
	f.n.mu.Lock()
	defer f.n.mu.Unlock()
	return &memFileInfo{
		name:    f.name,
		size:    int64(len(f.n.mu.data)),
		modTime: f.n.mu.modTime,
		isDir:   f.n.isDir,
	}, nil
}

func (f *memFile) Sync() error {
	if f.fs == nil || !f.fs.crashable {
		return nil
	}
	f.fs.cloneMu.RLock()
	defer f.fs.cloneMu.RUnlock()
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.n.isDir {
		f.n.syncedChildren = maps.Clone(f.n.children)
	} else {
		f.n.mu.Lock()
		f.n.mu.syncedData = append(f.n.mu.syncedData[:0], f.n.mu.data...)
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

func (f *memFileInfo) DeviceID() DeviceID {
	return DeviceID{}
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

// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

// Compression is the per-block compression algorithm to use.
type Compression int

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	nCompression
)

// Options holds the optional parameters for leveldb's DB implementations.
// These options apply to the DB at large; per-query options are defined by
// the ReadOptions and WriteOptions types.
//
// Options are typically passed to a constructor function as a struct literal.
// The GetXxx methods are used inside the DB implementations; they return the
// default parameter value if the *Options receiver is nil or the field value
// is zero.
//
// Read/Write options:
//   - Comparer
//   - FileSystem
//   - MaxOpenFiles
// Read options:
//   - VerifyChecksums
// Write options:
//   - BlockRestartInterval
//   - BlockSize
//   - Compression
//   - ErrorIfDBExists
//   - WriteBufferSize
type Options struct {
	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	//
	// The default value is 16.
	BlockRestartInterval int

	// BlockSize is the minimum uncompressed size in bytes of each table block.
	//
	// The default value is 4096.
	BlockSize int

	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer Comparer

	// Compression defines the per-block compression to use.
	//
	// The default value (DefaultCompression) uses snappy compression.
	Compression Compression

	// ErrorIfDBExists is whether it is an error if the database already exists.
	//
	// The default value is false.
	ErrorIfDBExists bool

	// FileSystem maps file names to byte storage.
	//
	// The default value uses the underlying operating system's file system.
	FileSystem FileSystem

	// MaxOpenFiles is a soft limit on the number of open files that can be
	// used by the DB.
	//
	// The default value is 1000.
	MaxOpenFiles int

	// WriteBufferSize is the amount of data to build up in memory (backed by
	// an unsorted log on disk) before converting to a sorted on-disk file.
	//
	// Larger values increase performance, especially during bulk loads. Up to
	// two write buffers may be held in memory at the same time, so you may
	// wish to adjust this parameter to control memory usage. Also, a larger
	// write buffer will result in a longer recovery time the next time the
	// database is opened.
	//
	// The default value is 4MiB.
	WriteBufferSize int

	// VerifyChecksums is whether to verify the per-block checksums in a DB.
	//
	// The default value is false.
	VerifyChecksums bool
}

func (o *Options) GetBlockRestartInterval() int {
	if o == nil || o.BlockRestartInterval <= 0 {
		return 16
	}
	return o.BlockRestartInterval
}

func (o *Options) GetBlockSize() int {
	if o == nil || o.BlockSize <= 0 {
		return 4096
	}
	return o.BlockSize
}

func (o *Options) GetComparer() Comparer {
	if o == nil || o.Comparer == nil {
		return DefaultComparer
	}
	return o.Comparer
}

func (o *Options) GetCompression() Compression {
	if o == nil || o.Compression <= DefaultCompression || o.Compression >= nCompression {
		// Default to SnappyCompression.
		return SnappyCompression
	}
	return o.Compression
}

func (o *Options) GetErrorIfDBExists() bool {
	if o == nil {
		return false
	}
	return o.ErrorIfDBExists
}

func (o *Options) GetFileSystem() FileSystem {
	if o == nil || o.FileSystem == nil {
		return DefaultFileSystem
	}
	return o.FileSystem
}

func (o *Options) GetMaxOpenFiles() int {
	if o == nil || o.MaxOpenFiles == 0 {
		return 1000
	}
	return o.MaxOpenFiles
}

func (o *Options) GetWriteBufferSize() int {
	if o == nil || o.WriteBufferSize <= 0 {
		return 4 * 1024 * 1024
	}
	return o.WriteBufferSize
}

func (o *Options) GetVerifyChecksums() bool {
	if o == nil {
		return false
	}
	return o.VerifyChecksums
}

// ReadOptions hold the optional per-query parameters for Get and Find
// operations.
//
// Like Options, a nil *ReadOptions is valid and means to use the default
// values.
type ReadOptions struct {
	// No fields so far.
}

// WriteOptions hold the optional per-query parameters for Set and Delete
// operations.
//
// Like Options, a nil *WriteOptions is valid and means to use the default
// values.
type WriteOptions struct {
	// Sync is whether to sync underlying writes from the OS buffer cache
	// through to actual disk, if applicable. Setting Sync can result in
	// slower writes.
	//
	// If false, and the machine crashes, then some recent writes may be lost.
	// Note that if it is just the process that crashes (and the machine does
	// not) then no writes will be lost.
	//
	// In other words, Sync being false has the same semantics as a write
	// system call. Sync being true means write followed by fsync.
	//
	// The default value is false.
	Sync bool
}

func (o *WriteOptions) GetSync() bool {
	return o != nil && o.Sync
}

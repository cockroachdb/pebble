// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package shared

import "io"

// Storage is an interface for a blob storage driver. This is lower-level
// than an FS-like interface, however FS/File-like abstractions can be built on
// top of these methods.
//
// TODO(bilal): Consider pushing shared file obsoletion as well as path
// generation behind this interface.
type Storage interface {
	io.Closer

	// ReadObjectAt returns a Reader for reading the object at the requested name
	// and offset.
	ReadObjectAt(basename string, offset int64) (io.ReadCloser, int64, error)

	// CreateObject returns a writer for the object at the request name. A new
	// empty object is created if CreateObject is called on an existing object.
	//
	// A Writer *must* be closed via either Close, and if closing returns a
	// non-nil error, that error should be handled or reported to the user -- an
	// implementation may buffer written data until Close and only then return
	// an error, or Write may return an opaque io.EOF with the underlying cause
	// returned by the subsequent Close().
	CreateObject(basename string) (io.WriteCloser, error)

	// List enumerates files within the supplied prefix, returning a list of
	// objects within that prefix. If delimiter is non-empty, names which have the
	// same prefix, prior to the delimiter but after the prefix, are grouped into a
	// single result which is that prefix. The order that results are returned is
	// undefined. If a prefix is specified, the prefix is trimmed from the result
	// list.
	//
	// An example would be, if the storage contains objects a, b/4, b/5 and b/6,
	// these would be the return values:
	//   List("", "") -> ["a", "b/4", "b/5", "b/6"]
	//   List("", "/") -> ["a", "b"]
	//   List("b", "/") -> ["4", "5", "6"]
	//   List("b", "") -> ["/4", "/5", "/6"]
	List(prefix, delimiter string) ([]string, error)

	// Delete removes the named object from the store.
	Delete(basename string) error

	// Size returns the length of the named object in bytes.
	Size(basename string) (int64, error)
}

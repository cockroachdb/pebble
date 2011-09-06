// Copyright 2011 The LevelDB-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package db

// Options holds the optional parameters for leveldb's DB implementations.
// They are typically passed to a constructor function as a struct literal.
// The GetXxx methods are used inside the DB implementations; they return the
// default parameter value if the *Options receiver is nil or the field value
// is zero.
type Options struct {
	// Comparer defines a total ordering over the space of []byte keys: a 'less
	// than' relationship. The same comparison algorithm must be used for reads
	// and writes over the lifetime of the DB.
	//
	// The default value uses the same ordering as bytes.Compare.
	Comparer Comparer
}

func (o *Options) GetComparer() Comparer {
	if o == nil || o.Comparer == nil {
		return DefaultComparer
	}
	return o.Comparer
}

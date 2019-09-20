// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package vfs

import (
	"io/ioutil"
	"os"
)

// Clone recursively copies a directory structure from srcFS to dstFS. srcPath
// specifies the path in srcFS to copy from and must be compatible with the
// srcFS path format. dstDir is the target directory in dstFS and must be
// compatible with the dstFS path format. Returns (true,nil) on a successful
// copy, (false,nil) if srcPath does not exist, and (false,err) if an error
// occurred.
func Clone(srcFS, dstFS FS, srcPath, dstDir string) (bool, error) {
	dstPath := dstFS.PathJoin(dstDir, srcFS.PathBase(srcPath))
	srcFile, err := Default.Open(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Ignore non-existent errors. Those will translate into non-existent
			// files in the destination filesystem.
			return false, nil
		}
		return false, err
	}

	stat, err := srcFile.Stat()
	if err != nil {
		return false, err
	}

	if stat.IsDir() {
		if err := srcFile.Close(); err != nil {
			return false, err
		}
		if err := dstFS.MkdirAll(dstPath, 0755); err != nil {
			return false, err
		}
		list, err := srcFS.List(srcPath)
		if err != nil {
			return false, err
		}
		for _, name := range list {
			_, err := Clone(srcFS, dstFS, srcFS.PathJoin(srcPath, name), dstPath)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	data, err := ioutil.ReadAll(srcFile)
	if err != nil {
		return false, err
	}
	if err := srcFile.Close(); err != nil {
		return false, err
	}
	dstFile, err := dstFS.Create(dstPath)
	if err != nil {
		return false, err
	}
	if _, err = dstFile.Write(data); err != nil {
		return false, err
	}
	if err := dstFile.Close(); err != nil {
		return false, err
	}
	return true, nil
}

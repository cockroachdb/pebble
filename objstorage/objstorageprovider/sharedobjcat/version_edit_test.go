// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sharedobjcat

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/kr/pretty"
)

func TestVersionEditRoundTrip(t *testing.T) {
	for _, ve := range []versionEdit{
		{},
		{
			CreatorID: 12345,
		},
		{
			NewObjects: []SharedObjectMetadata{
				{
					FileNum:        base.DiskFileNum{Val: 1},
					FileType:       base.FileTypeTable,
					CreatorID:      12,
					CreatorFileNum: base.DiskFileNum{Val: 123},
					CleanupMethod:  objstorage.SharedNoCleanup,
				},
			},
		},
		{
			DeletedObjects: []base.DiskFileNum{{Val: 1}},
		},
		{
			CreatorID: 12345,
			NewObjects: []SharedObjectMetadata{
				{
					FileNum:        base.DiskFileNum{Val: 1},
					FileType:       base.FileTypeTable,
					CreatorID:      12,
					CreatorFileNum: base.DiskFileNum{Val: 123},
					CleanupMethod:  objstorage.SharedRefTracking,
				},
				{
					FileNum:        base.DiskFileNum{Val: 2},
					FileType:       base.FileTypeTable,
					CreatorID:      22,
					CreatorFileNum: base.DiskFileNum{Val: 223},
				},
				{
					FileNum:        base.DiskFileNum{Val: 3},
					FileType:       base.FileTypeTable,
					CreatorID:      32,
					CreatorFileNum: base.DiskFileNum{Val: 323},
					CleanupMethod:  objstorage.SharedRefTracking,
				},
			},
			DeletedObjects: []base.DiskFileNum{{Val: 4}, {Val: 5}},
		},
	} {
		if err := checkRoundTrip(ve); err != nil {
			t.Fatalf("%+v did not roundtrip: %v", ve, err)
		}
	}
}

func checkRoundTrip(e0 versionEdit) error {
	var e1 versionEdit
	buf := new(bytes.Buffer)
	if err := e0.Encode(buf); err != nil {
		return errors.Wrap(err, "encode")
	}
	if err := e1.Decode(buf); err != nil {
		return errors.Wrap(err, "decode")
	}
	if diff := pretty.Diff(e0, e1); diff != nil {
		return errors.Errorf("%s", strings.Join(diff, "\n"))
	}
	return nil
}

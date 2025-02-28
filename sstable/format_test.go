// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"testing"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/stretchr/testify/require"
)

func TestTableFormat_RoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcs := []struct {
		name    string
		magic   string
		version uint32
		want    TableFormat
		wantErr string
	}{
		// Valid cases.
		{
			name:    "LevelDB",
			magic:   levelDBMagic,
			version: 0,
			want:    TableFormatLevelDB,
		},
		{
			name:    "RocksDBv2",
			magic:   rocksDBMagic,
			version: 2,
			want:    TableFormatRocksDBv2,
		},
		{
			name:    "PebbleDBv1",
			magic:   pebbleDBMagic,
			version: 1,
			want:    TableFormatPebblev1,
		},
		{
			name:    "PebbleDBv2",
			magic:   pebbleDBMagic,
			version: 2,
			want:    TableFormatPebblev2,
		},
		{
			name:    "PebbleDBv3",
			magic:   pebbleDBMagic,
			version: 3,
			want:    TableFormatPebblev3,
		},
		{
			name:    "PebbleDBv4",
			magic:   pebbleDBMagic,
			version: 4,
			want:    TableFormatPebblev4,
		},
		{
			name:    "PebbleDBv5",
			magic:   pebbleDBMagic,
			version: 5,
			want:    TableFormatPebblev5,
		},
		{
			name:    "PebbleDBv6",
			magic:   pebbleDBMagic,
			version: 6,
			want:    TableFormatPebblev6,
		},
		// Invalid cases.
		{
			name:    "Invalid RocksDB version",
			magic:   rocksDBMagic,
			version: 1,
			wantErr: "pebble/table: invalid table 000001: (unsupported rocksdb format version 1)",
		},
		{
			name:    "Invalid PebbleDB version",
			magic:   pebbleDBMagic,
			version: 7,
			wantErr: "pebble/table: invalid table 000001: (unsupported pebble format version 7)",
		},
		{
			name:    "Unknown magic string",
			magic:   "foo",
			wantErr: "pebble/table: invalid table 000001: (bad magic number: 0x666f6f)",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Tuple -> TableFormat.
			f, err := parseTableFormat([]byte(tc.magic), tc.version)
			if err != nil {
				// Keep the formatting consistent with what the Reader observes
				// through readFooter.
				err = errors.Wrapf(err, "pebble/table: invalid table %s", errors.Safe(base.DiskFileNum(1)))
			}
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, tc.wantErr, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, f)

			// TableFormat -> Tuple.
			s, v := f.AsTuple()
			require.Equal(t, tc.magic, s)
			require.Equal(t, tc.version, v)
		})
	}
}

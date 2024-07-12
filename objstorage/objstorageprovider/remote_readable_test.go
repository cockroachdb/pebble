// Copyright 2023 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package objstorageprovider

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/stretchr/testify/require"
)

type testObjectReader struct {
	buf []byte
	b   strings.Builder
}

func (r *testObjectReader) init(size int) {
	r.buf = make([]byte, size)
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = len(letters)
	for i := 0; i < len(r.buf); i++ {
		r.buf[i] = letters[rand.Intn(lettersLen)]
	}
}

func (r *testObjectReader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	fmt.Fprintf(&r.b, "ReadAt(len=%d, offset=%d)\n", len(p), offset)
	limit := int(offset) + len(p)
	if limit > len(r.buf) {
		return io.EOF
	}
	copy(p, r.buf[offset:limit])
	return nil
}

func (r *testObjectReader) Close() error {
	fmt.Fprintf(&r.b, "Close()\n")
	return nil
}

func TestRemoteReadHandle(t *testing.T) {
	var or testObjectReader
	var rr *remoteReadable
	var rh objstorage.ReadHandle
	defer func() {
		if rh != nil {
			require.NoError(t, rh.Close())
		}
		if rr != nil {
			require.NoError(t, rr.Close())
		}
	}()
	datadriven.RunTest(t, "testdata/remote_read_handle", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init-readable":
			if rr != nil {
				require.NoError(t, rr.Close())
			}
			var size int64
			d.ScanArgs(t, "size", &size)
			or.init(int(size))
			rr = &remoteReadable{
				objReader: &or,
				size:      size,
			}
			return ""

		case "new-read-handle":
			if rh != nil {
				require.NoError(t, rh.Close())
			}
			var readBeforeSize int
			d.ScanArgs(t, "read-before-size", &readBeforeSize)
			rh = rr.NewReadHandle(objstorage.ReadBeforeSize(readBeforeSize))
			if d.HasArg("setup-for-compaction") {
				rh.SetupForCompaction()
			}
			return ""

		case "read":
			var length int
			d.ScanArgs(t, "len", &length)
			b := make([]byte, length)
			var offset int64
			d.ScanArgs(t, "offset", &offset)
			err := rh.ReadAt(context.Background(), b, offset)
			if err != nil {
				fmt.Fprintf(&or.b, "err: %s\n", err.Error())
			} else {
				require.Equal(t, string(or.buf[offset:int(offset)+length]), string(b))
			}
			str := or.b.String()
			or.b.Reset()
			return str

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

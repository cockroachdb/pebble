// Copyright 2018 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/petermattis/pebble/internal/datadriven"
)

type iterCmdOpt int

const (
	iterCmdVerboseKey iterCmdOpt = iota
)

func runInternalIterCmd(d *datadriven.TestData, iter internalIterator, opts ...iterCmdOpt) string {
	var verboseKey bool
	for _, opt := range opts {
		if opt == iterCmdVerboseKey {
			verboseKey = true
		}
	}

	var b bytes.Buffer
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "seek-ge":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-ge <key>\n")
			}
			iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
		case "seek-lt":
			if len(parts) != 2 {
				return fmt.Sprintf("seek-lt <key>\n")
			}
			iter.SeekLT([]byte(strings.TrimSpace(parts[1])))
		case "first":
			iter.First()
		case "last":
			iter.Last()
		case "next":
			iter.Next()
		case "prev":
			iter.Prev()
		default:
			return fmt.Sprintf("unknown op: %s", parts[0])
		}
		if iter.Valid() {
			if verboseKey {
				fmt.Fprintf(&b, "%s:%s\n", iter.Key(), iter.Value())
			} else {
				fmt.Fprintf(&b, "%s:%s\n", iter.Key().UserKey, iter.Value())
			}
		} else if err := iter.Error(); err != nil {
			fmt.Fprintf(&b, "err=%v\n", err)
		} else {
			fmt.Fprintf(&b, ".\n")
		}
	}
	return b.String()
}

func runBatchDefineCmd(d *datadriven.TestData, b *Batch) error {
	for _, line := range strings.Split(d.Input, "\n") {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		var err error
		switch parts[0] {
		case "set":
			if len(parts) != 3 {
				return fmt.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.Set([]byte(parts[1]), []byte(parts[2]), nil)
		case "del":
			if len(parts) != 2 {
				return fmt.Errorf("%s expects 1 argument", parts[0])
			}
			err = b.Delete([]byte(parts[1]), nil)
		case "del-range":
			if len(parts) != 3 {
				return fmt.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.DeleteRange([]byte(parts[1]), []byte(parts[2]), nil)
		case "merge":
			if len(parts) != 3 {
				return fmt.Errorf("%s expects 2 arguments", parts[0])
			}
			err = b.Merge([]byte(parts[1]), []byte(parts[2]), nil)
		default:
			return fmt.Errorf("unknown op: %s", parts[0])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

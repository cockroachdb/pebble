// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/petermattis/pebble/internal/datadriven"
	"github.com/petermattis/pebble/internal/rate"
	"github.com/petermattis/pebble/vfs"
)

func TestCompactionPacerMaybeThrottle(t *testing.T) {
	datadriven.RunTest(t, "testdata/compaction_pacer_maybe_throttle",
		func(d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				db, _ := Open("", &Options{
					FS: vfs.NewMem(),
					MemTableSize: 1000,
				})

				if len(d.CmdArgs) != 1 {
					return fmt.Sprintf("%s expects 1 argument", d.Cmd)
				}

				var amount uint64
				if len(d.Input) > 0 {
					for _, data := range strings.Split(d.Input, "\n") {
						parts := strings.Split(data, ":")
						if len(parts) != 2 {
							return fmt.Sprintf("malformed test:\n%s", d.Input)
						}
						varKey := parts[0]
						varValue, err := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 64)
						if err != nil {
							return err.Error()
						}

						switch varKey {
						case "ratelimit":
							limiter := rate.NewLimiter(rate.Limit(varValue), int(varValue))
							db.flushLimiter = limiter
							db.compactionLimiter = limiter
						case "amount":
							amount = varValue
						default:
							return fmt.Sprintf("unknown command: %s", varKey)
						}

					}
				}

				var buf bytes.Buffer
				var pacer pacer
				switch d.CmdArgs[0].Key {
				case "flush":
					pacer = db.newFlushPacer()
				case "compaction":
					pacer = db.newCompactionPacer()
				default:
					return fmt.Sprintf("unknown command: %s", d.Cmd)
				}

				start := time.Now()
				err := pacer.maybeThrottle(amount)
				end := time.Now()

				duration := end.Sub(start)

				_, err = fmt.Fprintf(&buf, "%d\n", int(duration.Seconds()))
				if err != nil {
					t.Fatal(err)
				}

				return buf.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

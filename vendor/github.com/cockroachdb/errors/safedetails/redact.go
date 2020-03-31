// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package safedetails

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors/errbase"
	"github.com/cockroachdb/errors/markers"
	"github.com/cockroachdb/errors/withstack"
)

// Redact returns a redacted version of the supplied item that is safe to use in
// anonymized reporting.
func Redact(r interface{}) string {
	var buf strings.Builder

	switch t := r.(type) {
	case SafeMessager:
		buf.WriteString(t.SafeMessage())
	case error:
		redactErr(&buf, t)
	default:
		typAnd(&buf, r, "")
	}

	return buf.String()
}

func redactErr(buf *strings.Builder, err error) {
	if c := errbase.UnwrapOnce(err); c == nil {
		// This is a leaf error. Decode the leaf and return.
		if file, line, _, ok := withstack.GetOneLineSource(err); ok {
			fmt.Fprintf(buf, "%s:%d: ", file, line)
		}
		redactLeafErr(buf, err)
	} else /* c != nil */ {
		// Print the inner error before the outer error.
		redactErr(buf, c)
		redactWrapper(buf, err)
	}

	// Add any additional safe strings from the wrapper, if present.
	if payload := errbase.GetSafeDetails(err); len(payload.SafeDetails) > 0 {
		buf.WriteString("\n(more details:)")
		for _, sd := range payload.SafeDetails {
			buf.WriteByte('\n')
			buf.WriteString(strings.TrimSpace(sd))
		}
	}
}

func redactWrapper(buf *strings.Builder, err error) {
	buf.WriteString("\nwrapper: ")
	switch t := err.(type) {
	case *os.SyscallError:
		typAnd(buf, t, t.Syscall)
	case *os.PathError:
		typAnd(buf, t, t.Op)
	case *os.LinkError:
		fmt.Fprintf(buf, "%T: %s %s %s", t, t.Op, t.Old, t.New)
	case *net.OpError:
		typAnd(buf, t, t.Op)
		if t.Net != "" {
			fmt.Fprintf(buf, " %s", t.Net)
		}
		if t.Source != nil {
			buf.WriteString("<redacted>")
		}
		if t.Addr != nil {
			if t.Source != nil {
				buf.WriteString("->")
			}
			buf.WriteString("<redacted>")
		}
	default:
		typAnd(buf, err, "")
	}
}

func redactLeafErr(buf *strings.Builder, err error) {
	// Is it a sentinel error? These are safe.
	if markers.IsAny(err,
		context.DeadlineExceeded,
		context.Canceled,
		os.ErrInvalid,
		os.ErrPermission,
		os.ErrExist,
		os.ErrNotExist,
		os.ErrClosed,
		os.ErrNoDeadline,
	) {
		typAnd(buf, err, err.Error())
		return
	}

	if redactPre113Wrappers(buf, err) {
		return
	}

	// The following two types are safe too.
	switch t := err.(type) {
	case runtime.Error:
		typAnd(buf, t, t.Error())
	case syscall.Errno:
		typAnd(buf, t, t.Error())
	default:
		// No further information about this error, simply report its type.
		typAnd(buf, err, "")
	}
}

func typAnd(buf *strings.Builder, r interface{}, msg string) {
	if msg == "" {
		fmt.Fprintf(buf, "<%T>", r)
	} else {
		fmt.Fprintf(buf, "%T: %s", r, msg)
	}
}

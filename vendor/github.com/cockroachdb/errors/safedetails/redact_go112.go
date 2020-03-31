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

// +build !go1.13

package safedetails

import (
	"net"
	"os"
	"strings"
)

func redactPre113Wrappers(buf *strings.Builder, err error) bool {
	// The following cases are needed for go 1.12 and previous
	// versions. Go 1.13 instances of these errors will be recognized
	// by the unwrapping performed in redactErr().
	switch t := err.(type) {
	case *os.SyscallError:
		redactErr(buf, t.Err)
		redactWrapper(buf, err)
		return true
	case *os.PathError:
		redactErr(buf, t.Err)
		redactWrapper(buf, err)
		return true
	case *net.OpError:
		redactErr(buf, t.Err)
		redactWrapper(buf, err)
		return true
	case *os.LinkError:
		redactErr(buf, t.Err)
		redactWrapper(buf, err)
		return true
	}
	return false
}

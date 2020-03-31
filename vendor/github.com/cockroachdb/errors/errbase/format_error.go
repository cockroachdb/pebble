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

// This file is forked and modified from golang.org/x/xerrors,
// at commit 3ee3066db522c6628d440a3a91c4abdd7f5ef22f (2019-05-10).
// From the original code:
// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Changes specific to this fork marked as inline comments.

package errbase

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
)

// FormatError formats an error according to s and verb.
//
// If the error implements errors.Formatter, FormatError calls its
// FormatError method of f with an errors.Printer configured according
// to s and verb, and writes the result to s.
//
// Otherwise, if it is a wrapper, FormatError prints out its error prefix,
// then recurses on its cause.
//
// Otherwise, its Error() text is printed.
func FormatError(err error, s fmt.State, verb rune) {
	// Assuming this function is only called from the Format method, and given
	// that FormatError takes precedence over Format, it cannot be called from
	// any package that supports errors.Formatter. It is therefore safe to
	// disregard that State may be a specific printer implementation and use one
	// of our choice instead.

	// limitations: does not support printing error as Go struct.

	var (
		sep    = " " // separator before next error
		p      = &state{State: s}
		direct = true
	)

	switch verb {
	// Note that this switch must match the preference order
	// for ordinary string printing (%#v before %+v, and so on).

	case 'v':
		if s.Flag('#') {
			if stringer, ok := err.(fmt.GoStringer); ok {
				io.WriteString(&p.buf, stringer.GoString())
				goto exit
			}
			// proceed as if it were %v
		} else if s.Flag('+') {
			p.printDetail = true
			sep = "\n  - "
		}
	case 's':
	case 'q', 'x', 'X':
		// Use an intermediate buffer in the rare cases that precision,
		// truncation, or one of the alternative verbs (q, x, and X) are
		// specified.
		direct = false

	default:
		p.buf.WriteString("%!")
		p.buf.WriteRune(verb)
		p.buf.WriteByte('(')
		switch {
		case err != nil:
			p.buf.WriteString(reflect.TypeOf(err).String())
		default:
			p.buf.WriteString("<nil>")
		}
		p.buf.WriteByte(')')
		io.Copy(s, &p.buf)
		return
	}

	// CHANGE (cockroachdb/errors): FormatError() accepts `error` and
	// not `Formatter` as first argument, and uses a separate path when
	// the object does not implement `Formatter`. This makes it possible
	// to start implementing a wrapper with just a `Format()` method
	// that forwards to `FormatError()`, without implementing
	// `errors.Formatter` just yet.
	if _, ok := err.(Formatter); !ok {
		// formatSimpleError prints Error() or, if a wrapper, the
		// error prefix then recurses FormatError() on the causes.
		err = formatSimpleError(err, p, sep)
	}
	// if formatSimpleError() has taken over, skip the loop.
	if err == nil {
		goto exit
	}

loop:
	for {
		// CHANGE (cockroachdb/errors): we only emit the inter-error
		// separator if something was printed since the last error.
		// This way, an error of the form err1 -> err2 -> err3 will
		// properly print as "err1: err3" if err2 has no message.
		// In the original code, this would have been (incorrectly)
		// rendered as "err1: : err3".
		prevLen := p.buf.Len()

		switch v := err.(type) {
		case Formatter:
			err = v.FormatError((*printer)(p))
		case fmt.Formatter:
			v.Format(p, 'v')
			break loop
		default:
			// CHANGE (cockroachdb/errors): if the error did not
			// implement errors.Formatter nor fmt.Formatter, but it is a wrapper,
			// still attempt best effort: print what we can at this level, then
			// recurse to give a chance to the cause to self-format
			// properly.
			if cause := UnwrapOnce(err); cause != nil {
				pref := extractPrefix(err, cause)
				p.buf.WriteString(pref)
				err = cause
			} else {
				io.WriteString(&p.buf, v.Error())
				break loop
			}
		}

		if err == nil {
			break
		}

		// CHANGE (cockroachdb/errors): if there was nothing printed
		// for this intermediate error, skip printing the separator.
		skipSep := p.buf.Len() == prevLen
		if !skipSep {
			if p.needColon || !p.printDetail {
				p.buf.WriteByte(':')
			}
			p.buf.WriteString(sep)
		}

		p.inDetail = false
		p.needNewline = false
		p.needColon = false
	}

	///////////////////////////////////////////////////////////////////////////////
	// cockroachdb/errors: the remainder of the file is unchanged from the
	// original code in xerrors.

exit:
	width, okW := s.Width()
	prec, okP := s.Precision()

	if !direct || (okW && width > 0) || okP {
		// Construct format string from State s.
		format := []byte{'%'}
		if s.Flag('-') {
			format = append(format, '-')
		}
		if s.Flag('+') {
			format = append(format, '+')
		}
		if s.Flag(' ') {
			format = append(format, ' ')
		}
		if okW {
			format = strconv.AppendInt(format, int64(width), 10)
		}
		if okP {
			format = append(format, '.')
			format = strconv.AppendInt(format, int64(prec), 10)
		}
		format = append(format, string(verb)...)
		fmt.Fprintf(s, string(format), p.buf.String())
	} else {
		io.Copy(s, &p.buf)
	}
}

var detailSep = []byte("\n    ")

// state tracks error printing state. It implements fmt.State.
type state struct {
	fmt.State
	buf bytes.Buffer

	printDetail bool
	inDetail    bool
	needColon   bool
	needNewline bool
}

func (s *state) Write(b []byte) (n int, err error) {
	if s.printDetail {
		if len(b) == 0 {
			return 0, nil
		}
		if s.inDetail && s.needColon {
			s.needNewline = true
			if b[0] == '\n' {
				b = b[1:]
			}
		}
		k := 0
		for i, c := range b {
			if s.needNewline {
				if s.inDetail && s.needColon {
					s.buf.WriteByte(':')
					s.needColon = false
				}
				s.buf.Write(detailSep)
				s.needNewline = false
			}
			if c == '\n' {
				s.buf.Write(b[k:i])
				k = i + 1
				s.needNewline = true
			}
		}
		s.buf.Write(b[k:])
		if !s.inDetail {
			s.needColon = true
		}
	} else if !s.inDetail {
		s.buf.Write(b)
	}
	return len(b), nil
}

// printer wraps a state to implement an xerrors.Printer.
type printer state

func (s *printer) Print(args ...interface{}) {
	if !s.inDetail || s.printDetail {
		fmt.Fprint((*state)(s), args...)
	}
}

func (s *printer) Printf(format string, args ...interface{}) {
	if !s.inDetail || s.printDetail {
		fmt.Fprintf((*state)(s), format, args...)
	}
}

func (s *printer) Detail() bool {
	s.inDetail = true
	return s.printDetail
}

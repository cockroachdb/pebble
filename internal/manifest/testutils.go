// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package manifest

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// debugParser is a helper used to implement parsing of debug strings, like
// ParseFileMetadataDebug.
//
// It takes a string and splits it into tokens. Tokens are separated by
// whitespace; in addition separators "_-[]()" are always separate tokens. For
// example, the string `000001:[a - b]` results in tokens `000001`,
// `:`, `[`, `a`, `-`, `b`, `]`, .
//
// All debugParser methods throw panics instead of returning errors. The code
// that uses a debugParser can recover them and convert them to errors.
type debugParser struct {
	original  string
	tokens    []string
	lastToken string
}

const debugParserSeparators = ":-[]()"

func makeDebugParser(s string) debugParser {
	p := debugParser{
		original: s,
	}
	for _, f := range strings.Fields(s) {
		for f != "" {
			pos := strings.IndexAny(f, debugParserSeparators)
			if pos == -1 {
				p.tokens = append(p.tokens, f)
				break
			}
			if pos > 0 {
				p.tokens = append(p.tokens, f[:pos])
			}
			p.tokens = append(p.tokens, f[pos:pos+1])
			f = f[pos+1:]
		}
	}
	return p
}

// Done returns true if there are no more tokens.
func (p *debugParser) Done() bool {
	return len(p.tokens) == 0
}

// Peek returns the next token, without consuming the token. Returns "" if there
// are no more tokens.
func (p *debugParser) Peek() string {
	if p.Done() {
		p.lastToken = ""
		return ""
	}
	p.lastToken = p.tokens[0]
	return p.tokens[0]
}

// Next returns the next token, or "" if there are no more tokens.
func (p *debugParser) Next() string {
	res := p.Peek()
	if res != "" {
		p.tokens = p.tokens[1:]
	}
	return res
}

// Remaining returns all the remaining tokens, separated by spaces.
func (p *debugParser) Remaining() string {
	res := strings.Join(p.tokens, " ")
	p.tokens = nil
	return res
}

// Expect consumes the next tokens, verifying that they exactly match the
// arguments.
func (p *debugParser) Expect(tokens ...string) {
	for _, tok := range tokens {
		if res := p.Next(); res != tok {
			p.Errf("expected %q, got %q", tok, res)
		}
	}
}

// TryLevel tries to parse a token as a level (e.g. L1, L0.2). If successful,
// the token is consumed.
func (p *debugParser) TryLevel() (level int, ok bool) {
	t := p.Peek()
	if regexp.MustCompile(`^L[0-9](|\.[0-9]+)$`).MatchString(t) {
		p.Next()
		return int(t[1] - '0'), true
	}
	return 0, false
}

// Level parses the next token as a level.
func (p *debugParser) Level() int {
	level, ok := p.TryLevel()
	if !ok {
		p.Errf("cannot parse level")
	}
	return level
}

// Int parses the next token as an integer.
func (p *debugParser) Int() int {
	x, err := strconv.Atoi(p.Next())
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return x
}

// Uint64 parses the next token as an uint64.
func (p *debugParser) Uint64() uint64 {
	x, err := strconv.ParseUint(p.Next(), 10, 64)
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return x
}

// Uint64 parses the next token as a sequence number.
func (p *debugParser) SeqNum() base.SeqNum {
	return base.ParseSeqNum(p.Next())
}

// FileNum parses the next token as a FileNum.
func (p *debugParser) FileNum() base.FileNum {
	return base.FileNum(p.Int())
}

// DiskFileNum parses the next token as a DiskFileNum.
func (p *debugParser) DiskFileNum() base.DiskFileNum {
	return base.DiskFileNum(p.Int())
}

// InternalKey parses the next token as an internal key.
func (p *debugParser) InternalKey() base.InternalKey {
	return base.ParseInternalKey(p.Next())
}

// Errf panics with an error which includes the original string and the last
// token.
func (p *debugParser) Errf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	panic(errors.Errorf("error parsing %q at token %q: %s", p.original, p.lastToken, msg))
}

// errFromPanic can be used in a recover block to convert panics into errors.
func errFromPanic(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return errors.Errorf("%v", r)
}

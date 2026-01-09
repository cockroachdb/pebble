// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package strparse provides facilities for parsing strings, intended for use in
// tests and debug input.
package strparse

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
)

// Parser is a helper used to implement parsing of strings, like
// manifest.ParseFileMetadataDebug.
//
// It takes a string and splits it into tokens. Tokens are separated by
// whitespace; in addition user-specified separators are also always separate
// tokens. For example, when passed the separators `:-[]();` the string
// `000001:[a - b]` results in tokens `000001`, `:`, `[`, `a`, `-`, `b`, `]`, .
//
// All Parser methods throw panics instead of returning errors. The code
// that uses a Parser can recover them and convert them to errors.
type Parser struct {
	original  string
	tokens    []token
	lastToken token
}

type token struct {
	tok    string
	offset int
}

// MakeParser constructs a new Parser that converts any instance of the runes
// contained in [separators] into separate tokens, and consumes the provided
// input string.
func MakeParser(separators string, input string) Parser {
	p := Parser{original: input}

	s := input
	off := 0
	for len(s) > 0 {
		nonWhiteSpacePos := strings.IndexFunc(s, func(r rune) bool { return !unicode.IsSpace(r) })
		switch nonWhiteSpacePos {
		case -1:
			// Only whitespace.
			off += len(s)
			s = s[len(s):]
		case 0:
			// s is the beginning of a non-whitespace token.
			// It might be a separator, or it might be an arbitrary token
			wsPos := strings.IndexFunc(s, unicode.IsSpace)
			switch pos := strings.IndexAny(s, separators); pos {
			case -1:
				if wsPos == -1 {
					wsPos = len(s)
				}
				p.tokens = append(p.tokens, token{tok: s[:wsPos], offset: off})
				off += wsPos
				s = s[wsPos:]
			case 0:
				p.tokens = append(p.tokens, token{tok: s[:1], offset: off})
				off += 1
				s = s[1:]
			default:
				if wsPos != -1 && wsPos < pos {
					pos = wsPos
				}
				p.tokens = append(p.tokens, token{tok: s[:pos], offset: off})
				off += pos
				s = s[pos:]
			}
		default:
			// Whitespace.
			off += nonWhiteSpacePos
			s = s[nonWhiteSpacePos:]
		}
	}
	return p
}

// Done returns true if there are no more tokens.
func (p *Parser) Done() bool {
	return len(p.tokens) == 0
}

// Offset returns the offset of the next token.
func (p *Parser) Offset() int {
	if p.Done() {
		return len(p.original)
	}
	return p.tokens[0].offset
}

// Peek returns the next token, without consuming the token. Returns "" if there
// are no more tokens.
func (p *Parser) Peek() string {
	if p.Done() {
		p.lastToken = token{}
		return ""
	}
	p.lastToken = p.tokens[0]
	return p.tokens[0].tok
}

// Next returns the next token, or "" if there are no more tokens.
func (p *Parser) Next() string {
	res := p.Peek()
	if res != "" {
		p.tokens = p.tokens[1:]
	}
	return res
}

// ExpectAll consumes the next token, verifying that it contains only characters
// for which fn returns true. It returns the token itself.
func (p *Parser) ExpectAll(fn func(r rune) bool) string {
	next := p.Next()
	for _, r := range next {
		if !fn(r) {
			p.Errf("expected all characters to satisfy fn, %q of %q did not", r, next)
		}
	}
	return next
}

// Remaining returns all the remaining tokens, separated by spaces.
func (p *Parser) Remaining() string {
	var buf strings.Builder
	for _, tok := range p.tokens {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(tok.tok)
	}
	p.tokens = nil
	return buf.String()
}

// Expect consumes the next tokens, verifying that they exactly match the
// arguments.
func (p *Parser) Expect(tokens ...string) {
	for _, tok := range tokens {
		if res := p.Next(); res != tok {
			p.Errf("expected %q, got %q", tok, res)
		}
	}
}

// TryLevel tries to parse a token as a level (e.g. L1, L0.2). If successful,
// the token is consumed.
func (p *Parser) TryLevel() (level int, ok bool) {
	t := p.Peek()
	if regexp.MustCompile(`^L[0-9](|\.[0-9]+)$`).MatchString(t) {
		p.Next()
		return int(t[1] - '0'), true
	}
	return 0, false
}

// BracketedRange parses UserKeyBounds as string form [a, b], [bb, cc),
// handles exclusive and inclusive cases of excise bounds.
func (p *Parser) BracketedRange() string {
	if p.Done() {
		p.Errf("expected bracketed range, but no tokens found")
	}
	open := p.Peek()
	if !(open == "[") {
		p.Errf("expected opening bracket, got %q", open)
	}

	p.Next()
	var builder strings.Builder
	builder.WriteString(open)
	first := true
	for !p.Done() {
		tok := p.Next()
		if tok == "]" || tok == ")" {
			builder.WriteString(tok)
			break
		}
		if !first {
			builder.WriteString(" ")
		}
		builder.WriteString(tok)
		first = false
	}
	return builder.String()
}

// Level parses the next token as a level.
func (p *Parser) Level() int {
	level, ok := p.TryLevel()
	if !ok {
		p.Errf("cannot parse level")
	}
	return level
}

// Int parses the next token as an integer.
func (p *Parser) Int() int {
	x, err := strconv.Atoi(p.Next())
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return x
}

// Uint64 parses the next token as an uint64.
func (p *Parser) Uint64() uint64 {
	x, err := strconv.ParseUint(p.Next(), 10, 64)
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return x
}

// Uint32 parses the next token as an uint32.
func (p *Parser) Uint32() uint32 {
	x, err := strconv.ParseUint(p.Next(), 10, 32)
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return uint32(x)
}

// SeqNum parses the next token as a sequence number.
func (p *Parser) SeqNum() base.SeqNum {
	return base.ParseSeqNum(p.Next())
}

// Uint64 parses the next token as a sequence number with a "#" prefix.
func (p *Parser) HashSeqNum() base.SeqNum {
	tok := p.Next()
	return base.ParseSeqNum(tok[1:])
}

// SeqNumRange parses the next token as a sequence number range.
func (p *Parser) SeqNumRange() base.SeqNumRange {
	p.Expect("[")
	low := p.HashSeqNum()
	p.Expect("-")
	high := p.HashSeqNum()
	p.Expect("]")
	return base.SeqNumRange{Low: low, High: high}
}

// BlobFileID parses the next token as a BlobFileID.
func (p *Parser) BlobFileID() base.BlobFileID {
	s := p.Next()
	if !strings.HasPrefix(s, "B") {
		p.Errf("expected blob file ID, got %q", s)
	}
	v, err := strconv.ParseUint(s[1:], 10, 64)
	if err != nil {
		p.Errf("cannot parse blob file ID: %v", err)
	}
	return base.BlobFileID(v)
}

// FileNum parses the next token as a FileNum.
func (p *Parser) FileNum() base.FileNum {
	return base.FileNum(p.Int())
}

// DiskFileNum parses the next token as a DiskFileNum.
func (p *Parser) DiskFileNum() base.DiskFileNum {
	return base.DiskFileNum(p.Int())
}

// InternalKey parses the next token as an internal key.
func (p *Parser) InternalKey() base.InternalKey {
	return base.ParseInternalKey(p.Next())
}

// UserKeyBounds parses the next token as an user key interval.
func (p *Parser) UserKeyBounds() base.UserKeyBounds {
	return base.ParseUserKeyBounds(p.BracketedRange())
}

// Errf panics with an error which includes the original string and the last
// token.
func (p *Parser) Errf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	panic(errors.Errorf("error parsing %q at token %q: %s", p.original, p.lastToken, msg))
}

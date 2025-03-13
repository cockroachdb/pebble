// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable/blob"
)

// BlobValues is a helper for using blob handles in tests. It supports parsing a
// human-readable string describing a blob handle, synthesizing unspecified
// fields, and tracking the blob handle to support future fetches.
type BlobValues struct {
	mostRecentHandle blob.Handle
	trackedHandles   map[blob.Handle]string
}

// Fetch returns the value corresponding to the given handle.
func (bv *BlobValues) Fetch(
	ctx context.Context, handle []byte, blobFileNum base.DiskFileNum, valLen uint32, buf []byte,
) (val []byte, callerOwned bool, err error) {
	if bv.trackedHandles == nil {
		return nil, false, errors.New("no tracked handles")
	}

	handleSuffix := blob.DecodeHandleSuffix(handle)
	decodedHandle := blob.Handle{
		FileNum:       blobFileNum,
		BlockNum:      handleSuffix.BlockNum,
		OffsetInBlock: handleSuffix.OffsetInBlock,
		ValueLen:      valLen,
	}

	value, ok := bv.trackedHandles[decodedHandle]
	if !ok {
		return nil, false, errors.Newf("unknown handle %s", decodedHandle)
	}

	// If there was not an explicitly specified value, generate a random one
	// deterministically from the file number, block number and offset in block.
	if len(value) == 0 {
		rng := rand.New(rand.NewPCG((uint64(decodedHandle.FileNum)<<32)|uint64(decodedHandle.BlockNum), uint64(decodedHandle.OffsetInBlock)))
		return RandBytes(rng, int(decodedHandle.ValueLen)), false, nil
	}
	return []byte(value), false, nil
}

// ParseInternalValue parses a debug blob handle from the string, returning the
// handle as an InternalValue and recording the handle's corresponding value.
func (bv *BlobValues) ParseInternalValue(input string) (base.InternalValue, error) {
	h, err := bv.Parse(input)
	if err != nil {
		return base.InternalValue{}, err
	}

	// Encode the handle suffix to be the 'ValueOrHandle' of the InternalValue.
	handleSuffix := blob.HandleSuffix{
		BlockNum:      h.BlockNum,
		OffsetInBlock: h.OffsetInBlock,
	}
	handleSuffixBytes := make([]byte, blob.MaxInlineHandleLength)
	i := handleSuffix.Encode(handleSuffixBytes)

	return base.MakeLazyValue(base.LazyValue{
		ValueOrHandle: handleSuffixBytes[:i],
		Fetcher: &base.LazyFetcher{
			Fetcher: bv,
			Attribute: base.AttributeAndLen{
				ValueLen: h.ValueLen,
				// TODO(jackson): Support user-specified short attributes.
				ShortAttribute: base.ShortAttribute(h.ValueLen & 0x07),
			},
			BlobFileNum: h.FileNum,
		},
	}), nil
}

// Parse parses a debug blob handle from the string, returning the handle and
// recording the handle's corresponding value.
func (bv *BlobValues) Parse(input string) (h blob.Handle, err error) {
	if bv.trackedHandles == nil {
		bv.trackedHandles = make(map[blob.Handle]string)
	}

	defer func() {
		if r := recover(); r != nil {
			h, err = blob.Handle{}, errFromPanic(r)
		}
	}()
	p := makeDebugParser(input)
	p.Expect("blob")
	p.Expect("{")
	var value string
	var fileNumSet, blockNumSet, offsetSet, valueLenSet bool
	for done := false; !done; {
		if p.Done() {
			return blob.Handle{}, errors.New("unexpected end of input")
		}
		switch x := p.Next(); x {
		case "}":
			done = true
		case "fileNum":
			p.Expect("=")
			h.FileNum = p.DiskFileNum()
			fileNumSet = true
		case "blockNum":
			p.Expect("=")
			h.BlockNum = p.Uint32()
			blockNumSet = true
		case "offset":
			p.Expect("=")
			h.OffsetInBlock = p.Uint32()
			offsetSet = true
		case "valueLen":
			p.Expect("=")
			h.ValueLen = p.Uint32()
			valueLenSet = true
		case "value":
			p.Expect("=")
			value = p.Next()
			if valueLenSet && h.ValueLen != uint32(len(value)) {
				return blob.Handle{}, errors.Newf("valueLen mismatch: %d != %d", h.ValueLen, len(value))
			}
		default:
			return blob.Handle{}, errors.Newf("unknown field: %q", x)
		}
	}

	if !fileNumSet {
		h.FileNum = max(bv.mostRecentHandle.FileNum, 1)
	}
	if !blockNumSet {
		h.BlockNum = bv.mostRecentHandle.BlockNum
	}
	if !offsetSet {
		h.OffsetInBlock = bv.mostRecentHandle.OffsetInBlock + bv.mostRecentHandle.ValueLen
	}
	if !valueLenSet {
		if len(value) > 0 {
			h.ValueLen = uint32(len(value))
		} else {
			h.ValueLen = 12
		}
	}
	bv.mostRecentHandle = h
	bv.trackedHandles[h] = value
	return h, nil
}

// debugParser is a helper used to implement parsing of debug strings, like
// BlobValues.Parse.
//
// It takes a string and splits it into tokens. Tokens are separated by
// whitespace; in addition separators "_-[](){};=" are always separate tokens. For
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

const debugParserSeparators = ":-[](){};="

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

// Uint64 parses the next token as an uint64.
func (p *debugParser) Uint64() uint64 {
	x, err := strconv.ParseUint(p.Next(), 10, 64)
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return x
}

// Uint32 parses the next token as an uint32.
func (p *debugParser) Uint32() uint32 {
	x, err := strconv.ParseUint(p.Next(), 10, 32)
	if err != nil {
		p.Errf("cannot parse number: %v", err)
	}
	return uint32(x)
}

// DiskFileNum parses the next token as a DiskFileNum.
func (p *debugParser) DiskFileNum() base.DiskFileNum {
	return base.DiskFileNum(p.Uint64())
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

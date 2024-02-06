// Package datadrivenutil defines facilities to improve ergonomics around
// parsing datadriven test input.
package datadrivenutil

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Lines wraps a string, providing facilities for parsing individual lines.
type Lines string

// Next returns the string up to the next newline. The receiver is modified to
// point to the string's contents immediately after the newline.
func (l *Lines) Next() (line Line) {
	i := strings.IndexByte(string(*l), '\n')
	if i == -1 {
		line = Line(*l)
		*l = ""
	} else {
		line = Line((*l)[:i])
		*l = (*l)[i+1:]
	}
	return line
}

// A Line is a string with no newlines.
type Line string

// Fields breaks the line into fields delimited by whitespace.
func (l Line) Fields() Fields { return Fields(strings.Fields(string(l))) }

// Fields wraps a []string with facilities for parsing out values.
type Fields []string

// HasValue searches for a field that is exactly the provided string.
func (fs Fields) HasValue(value string) bool {
	for i := range fs {
		if fs[i] == value {
			return true
		}
	}
	return false
}

// KeyValue looks for a field containing a key=value pair with the provided key.
// If not found, KeyValue returns false for the second return value.
func (fs Fields) KeyValue(key string) (Field, bool) {
	for i := range fs {
		if len(fs[i]) >= len(key) && strings.HasPrefix(fs[i], key) && fs[i][len(key)] == '=' {
			return Field(fs[i][len(key)+1:]), true
		}
	}
	return "", false
}

// MustKeyValue is like KeyValue but panics if the field is not found.
func (fs Fields) MustKeyValue(key string) Field {
	f, ok := fs.KeyValue(key)
	if !ok {
		panic(fmt.Sprintf("unable to find required key-value pair %q", key))
	}
	return f
}

// HexBytes parses all the fields as hex-encoded bytes, returning the
// concatenated decoded bytes. It panics if any of the fields fail to parse as
// hex-encoded bytes.
func (fs Fields) HexBytes() []byte {
	var b []byte
	for _, f := range fs {
		b = append(b, Field(f).HexBytes()...)
	}
	return b
}

// A Field represents a single string of unknown structure.
type Field string

// Int parses the field as an int. It panics if the field fails to decode as an
// integer.
func (f Field) Int() int {
	v, err := strconv.Atoi(string(f))
	if err != nil {
		panic(err)
	}
	return v
}

// Uint64 parses the field as an uint64. It panics if the field fails to decode
// as an uint64.
func (f Field) Uint64() uint64 {
	v, err := strconv.ParseUint(string(f), 10, 64)
	if err != nil {
		panic(err)
	}
	return v
}

// HexBytes decodes the field as hex-encoded bytes. It panics if the field fails
// to decode as hex-encoded.
func (f Field) HexBytes() []byte {
	b, err := hex.DecodeString(string(f))
	if err != nil {
		panic(err)
	}
	return b
}

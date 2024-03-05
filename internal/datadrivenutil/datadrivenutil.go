// Package datadrivenutil defines facilities to improve ergonomics around
// parsing datadriven test input.
package datadrivenutil

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"
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

// Fields breaks the line into fields delimited by whitespace and any runes
// passed into the function.
func (l Line) Fields(delims ...rune) Fields {
	return Fields(strings.FieldsFunc(string(l), func(r rune) bool {
		if unicode.IsSpace(r) {
			return true
		}
		for _, delim := range delims {
			if delim == r {
				return true
			}
		}
		return false
	}))
}

// Fields wraps a []string with facilities for parsing out values.
type Fields []string

// String implements fmt.Stringer.
func (fs Fields) String() string {
	return strings.Join(fs, " ")
}

// HasValue searches for a field that is exactly the provided string.
func (fs Fields) HasValue(value string) bool {
	for i := range fs {
		if fs[i] == value {
			return true
		}
	}
	return false
}

// Index returns the field at index i, or the empty string if there are i or
// fewer fields.
func (fs Fields) Index(i int) Value {
	if len(fs) <= i {
		return ""
	}
	return Value(fs[i])
}

// KeyValue looks for a field containing a key=value pair with the provided key.
// If not found, KeyValue returns false for the second return value.
func (fs Fields) KeyValue(key string) (Value, bool) {
	for i := range fs {
		if len(fs[i]) >= len(key) && strings.HasPrefix(fs[i], key) && fs[i][len(key)] == '=' {
			return Value(fs[i][len(key)+1:]), true
		}
	}
	return "", false
}

// MustKeyValue is like KeyValue but panics if the field is not found.
func (fs Fields) MustKeyValue(key string) Value {
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
		b = append(b, Value(f).HexBytes()...)
	}
	return b
}

// A Value represents a single string of unknown structure. A Value is sometimes
// used to represent an entire element of a Fields and other times a substring
// of an individual field. This blurring of semantics is convenient for
// parsing.
type Value string

// Str returns the value as a string.
func (v Value) Str() string { return string(v) }

// Bytes returns the value as a byte slice.
func (v Value) Bytes() []byte { return []byte(v) }

// Int parses the value as an int. It panics if the value fails to decode as an
// integer.
func (v Value) Int() int {
	vi, err := strconv.Atoi(string(v))
	if err != nil {
		panic(err)
	}
	return vi
}

// Uint64 parses the value as an uint64. It panics if the value fails to decode
// as an uint64.
func (v Value) Uint64() uint64 {
	vi, err := strconv.ParseUint(string(v), 10, 64)
	if err != nil {
		panic(err)
	}
	return vi
}

// HexBytes decodes the value as hex-encoded bytes. It panics if the value fails
// to decode as hex-encoded.
func (v Value) HexBytes() []byte {
	b, err := hex.DecodeString(string(v))
	if err != nil {
		panic(err)
	}
	return b
}

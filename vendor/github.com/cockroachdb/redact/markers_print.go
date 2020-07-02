// Copyright 2020 The Cockroach Authors.
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

package redact

import (
	"fmt"
	"io"
)

// Sprint prints out the arguments and encloses unsafe bits
// between redaction markers.
// If either safe and unsafe bits of data contain the markers
// in their representation already, they are escaped first.
// If a RedactableString or RedactableBytes argument is passed,
// it is reproduced as-is without escaping.
func Sprint(args ...interface{}) RedactableString {
	annotateArgs(args)
	return RedactableString(fmt.Sprint(args...))
}

// Sprintf formats the arguments and encloses unsafe bits
// between redaction markers.
// If either safe and unsafe bits of data contain the markers
// in their representation already, they are escaped first.
// The format is always considered safe and the caller
// is responsible to ensure that the markers are not present
// in the format string.
func Sprintf(format string, args ...interface{}) RedactableString {
	annotateArgs(args)
	return RedactableString(fmt.Sprintf(format, args...))
}

// Sprintfn produces a RedactableString using the provided
// SafeFormat-alike function.
func Sprintfn(printer func(w SafePrinter)) RedactableString {
	return Sprint(printerfn{printer})
}

// StringWithoutMarkers formats the provided SafeFormatter and strips
// the redaction markers from the result. This is provided for
// convenience to facilitate the implementation of String() methods
// alongside SafeFormat() to avoid code duplication.
//
// Note: if this function is ever found to be a performance
// bottleneck, one can consider using an alternate implementation of
// Sprint() which similarly calls the SafeFormat() methods but does
// not introduce markers and instead writes to a string buffer
// directly.
func StringWithoutMarkers(f SafeFormatter) string {
	return Sprint(f).StripMarkers()
}

// Fprint is like Sprint but outputs the redactable
// string to the provided Writer.
func Fprint(w io.Writer, args ...interface{}) (int, error) {
	annotateArgs(args)
	return fmt.Fprint(w, args...)
}

// Fprintf is like Sprintf but outputs the redactable string to the
// provided Writer.
func Fprintf(w io.Writer, format string, args ...interface{}) (int, error) {
	annotateArgs(args)
	return fmt.Fprintf(w, format, args...)
}

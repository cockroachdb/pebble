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

package errbase

import (
	"fmt"

	"github.com/cockroachdb/errors/errorspb"
)

// opaqueLeaf is used when receiving an unknown leaf type.
// Its important property is that if it is communicated
// back to some network system that _does_ know about
// the type, the original object can be restored.
type opaqueLeaf struct {
	msg     string
	details errorspb.EncodedErrorDetails
}

var _ error = (*opaqueLeaf)(nil)
var _ SafeDetailer = (*opaqueLeaf)(nil)
var _ fmt.Formatter = (*opaqueLeaf)(nil)
var _ Formatter = (*opaqueLeaf)(nil)

// opaqueWrapper is used when receiving an unknown wrapper type.
// Its important property is that if it is communicated
// back to some network system that _does_ know about
// the type, the original object can be restored.
type opaqueWrapper struct {
	cause   error
	prefix  string
	details errorspb.EncodedErrorDetails
}

var _ error = (*opaqueWrapper)(nil)
var _ SafeDetailer = (*opaqueWrapper)(nil)
var _ fmt.Formatter = (*opaqueWrapper)(nil)
var _ Formatter = (*opaqueWrapper)(nil)

func (e *opaqueLeaf) Error() string { return e.msg }

func (e *opaqueWrapper) Error() string {
	if e.prefix == "" {
		return e.cause.Error()
	}
	return fmt.Sprintf("%s: %s", e.prefix, e.cause)
}

// the opaque wrapper is a wrapper.
func (e *opaqueWrapper) Cause() error  { return e.cause }
func (e *opaqueWrapper) Unwrap() error { return e.cause }

func (e *opaqueLeaf) SafeDetails() []string    { return e.details.ReportablePayload }
func (e *opaqueWrapper) SafeDetails() []string { return e.details.ReportablePayload }

func (e *opaqueLeaf) Format(s fmt.State, verb rune)    { FormatError(e, s, verb) }
func (e *opaqueWrapper) Format(s fmt.State, verb rune) { FormatError(e, s, verb) }

func (e *opaqueLeaf) FormatError(p Printer) (next error) {
	p.Print(e.msg)
	if p.Detail() {
		p.Print("\n(opaque error leaf)")
		p.Printf("\ntype name: %s", e.details.OriginalTypeName)
		for i, d := range e.details.ReportablePayload {
			p.Printf("\nreportable %d:\n%s", i, d)
		}
		if e.details.FullDetails != nil {
			p.Printf("\npayload type: %s", e.details.FullDetails.TypeUrl)
		}
	}
	return nil
}

func (e *opaqueWrapper) FormatError(p Printer) (next error) {
	p.Print(e.prefix)
	if p.Detail() {
		p.Print("\n(opaque error wrapper)")
		p.Printf("\ntype name: %s", e.details.OriginalTypeName)
		for i, d := range e.details.ReportablePayload {
			p.Printf("\nreportable %d:\n%s", i, d)
		}
		if e.details.FullDetails != nil {
			p.Printf("\npayload type: %s", e.details.FullDetails.TypeUrl)
		}
	}
	return e.cause
}

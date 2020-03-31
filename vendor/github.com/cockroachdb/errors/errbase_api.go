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

package errors

import (
	"context"

	"github.com/cockroachdb/errors/errbase"
)

// UnwrapOnce forwards a definition.
func UnwrapOnce(err error) error { return errbase.UnwrapOnce(err) }

// UnwrapAll forwards a definition.
func UnwrapAll(err error) error { return errbase.UnwrapAll(err) }

// EncodedError forwards a definition.
type EncodedError = errbase.EncodedError

// EncodeError forwards a definition.
func EncodeError(ctx context.Context, err error) EncodedError { return errbase.EncodeError(ctx, err) }

// DecodeError forwards a definition.
func DecodeError(ctx context.Context, enc EncodedError) error { return errbase.DecodeError(ctx, enc) }

// SafeDetailer forwards a definition.
type SafeDetailer = errbase.SafeDetailer

// GetAllSafeDetails forwards a definition.
func GetAllSafeDetails(err error) []SafeDetailPayload { return errbase.GetAllSafeDetails(err) }

// GetSafeDetails forwards a definition.
func GetSafeDetails(err error) (payload SafeDetailPayload) { return errbase.GetSafeDetails(err) }

// SafeDetailPayload forwards a definition.
type SafeDetailPayload = errbase.SafeDetailPayload

// RegisterLeafDecoder forwards a definition.
func RegisterLeafDecoder(typeName TypeKey, decoder LeafDecoder) {
	errbase.RegisterLeafDecoder(typeName, decoder)
}

// TypeKey forwards a definition.
type TypeKey = errbase.TypeKey

// GetTypeKey forwards a definition.
func GetTypeKey(err error) TypeKey { return errbase.GetTypeKey(err) }

// LeafDecoder forwards a definition.
type LeafDecoder = errbase.LeafDecoder

// RegisterWrapperDecoder forwards a definition.
func RegisterWrapperDecoder(typeName TypeKey, decoder WrapperDecoder) {
	errbase.RegisterWrapperDecoder(typeName, decoder)
}

// WrapperDecoder forwards a definition.
type WrapperDecoder = errbase.WrapperDecoder

// RegisterLeafEncoder forwards a definition.
func RegisterLeafEncoder(typeName TypeKey, encoder LeafEncoder) {
	errbase.RegisterLeafEncoder(typeName, encoder)
}

// LeafEncoder forwards a definition.
type LeafEncoder = errbase.LeafEncoder

// RegisterWrapperEncoder forwards a definition.
func RegisterWrapperEncoder(typeName TypeKey, encoder WrapperEncoder) {
	errbase.RegisterWrapperEncoder(typeName, encoder)
}

// WrapperEncoder forwards a definition.
type WrapperEncoder = errbase.WrapperEncoder

// SetWarningFn forwards a definition.
func SetWarningFn(fn func(context.Context, string, ...interface{})) { errbase.SetWarningFn(fn) }

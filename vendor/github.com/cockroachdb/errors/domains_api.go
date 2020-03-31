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

import "github.com/cockroachdb/errors/domains"

// Domain forwards a definition.
type Domain = domains.Domain

// NoDomain forwards a definition.
const NoDomain Domain = domains.NoDomain

// NamedDomain forwards a definition.
func NamedDomain(domainName string) Domain { return domains.NamedDomain(domainName) }

// PackageDomain forwards a definition.
func PackageDomain() Domain { return domains.PackageDomainAtDepth(1) }

// PackageDomainAtDepth forwards a definition.
func PackageDomainAtDepth(depth int) Domain { return domains.PackageDomainAtDepth(depth) }

// WithDomain forwards a definition.
func WithDomain(err error, domain Domain) error { return domains.WithDomain(err, domain) }

// NotInDomain forwards a definition.
func NotInDomain(err error, doms ...Domain) bool { return domains.NotInDomain(err, doms...) }

// EnsureNotInDomain forwards a definition.
func EnsureNotInDomain(err error, constructor DomainOverrideFn, forbiddenDomains ...Domain) error {
	return domains.EnsureNotInDomain(err, constructor, forbiddenDomains...)
}

// DomainOverrideFn forwards a definition.
type DomainOverrideFn = func(originalDomain Domain, err error) error

// HandledInDomain forwards a definition.
func HandledInDomain(err error, domain Domain) error { return domains.HandledInDomain(err, domain) }

// HandledInDomainWithMessage forwards a definition.
func HandledInDomainWithMessage(err error, domain Domain, msg string) error {
	return domains.HandledInDomainWithMessage(err, domain, msg)
}

// GetDomain forwards a definition.
func GetDomain(err error) Domain { return domains.GetDomain(err) }

// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package metamorphic

import (
	"bytes"
	"io"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/stretchr/testify/require"
)

// TestDeleteSuffixRangeOpRoundTrip verifies that hand-constructed
// deleteSuffixRangeOps survive formatOps -> parse -> formatOps.
func TestDeleteSuffixRangeOpRoundTrip(t *testing.T) {
	kf := TestkeysKeyFormat
	ops := []op{
		&initOp{dbSlots: 1},
		&deleteSuffixRangeOp{
			dbID:  makeObjID(dbTag, 1),
			start: []byte("a"),
			end:   []byte("z"),
			lower: []byte("@5"),
			upper: []byte("@9"),
		},
	}
	src := formatOps(kf, ops)
	parsed, err := parse([]byte(src), parserOpts{
		parseFormattedUserKey:       kf.ParseFormattedKey,
		parseFormattedUserKeySuffix: kf.ParseFormattedKeySuffix,
	})
	require.NoError(t, err)
	require.Equal(t, ops, parsed)
}

// TestDeleteSuffixRangeOpGeneration verifies that a generator configured to
// produce DeleteSuffixRange ops actually produces them, and that the lower
// and upper suffix bounds are non-empty and well-ordered per the comparer's
// suffix ordering. The default config currently has DSR weight 0 (see the
// comment on OpDBDeleteSuffixRange in config.go), so we explicitly bump the
// weight here to exercise the generator path.
func TestDeleteSuffixRangeOpGeneration(t *testing.T) {
	cfg := DefaultOpConfig().WithOpWeight(OpDBDeleteSuffixRange, 50)
	rng := rand.New(rand.NewPCG(0, 42))
	km := newKeyManager(1 /* numInstances */, TestkeysKeyFormat)
	g := newGenerator(rng, cfg, km)
	ops := g.generate(2000)

	cmp := TestkeysKeyFormat.Comparer.ComparePointSuffixes
	var dsrCount int
	for _, o := range ops {
		dsr, ok := o.(*deleteSuffixRangeOp)
		if !ok {
			continue
		}
		dsrCount++
		require.NotEmpty(t, dsr.lower, "DeleteSuffixRange.lower must be non-empty")
		require.NotEmpty(t, dsr.upper, "DeleteSuffixRange.upper must be non-empty")
		require.Less(t, cmp(dsr.lower, dsr.upper), 0,
			"DeleteSuffixRange suffix bounds must satisfy lower < upper per ComparePointSuffixes")
		require.Less(t, TestkeysKeyFormat.Comparer.Compare(dsr.start, dsr.end), 0,
			"DeleteSuffixRange span must satisfy start < end")
	}
	require.NotZero(t, dsrCount, "expected non-zero DSR weight to generate DeleteSuffixRange ops")
}

// TestDeleteSuffixRangeOpExecute runs a small hand-built op stream containing
// DeleteSuffixRange against a real database. The database is started at a
// format major version below FormatSuffixMask, ratcheted up to it, and then
// the DSR op is exercised. This serves as a smoke test that the op runs
// end-to-end through the metamorphic framework.
func TestDeleteSuffixRangeOpExecute(t *testing.T) {
	kf := TestkeysKeyFormat
	dbID := makeObjID(dbTag, 1)
	ops := Ops{
		&initOp{dbSlots: 1},
		// Establish a non-trivial state: write a handful of versioned keys
		// and flush so they reside in an sstable.
		&setOp{writerID: dbID, key: []byte("a@5"), value: []byte("v1")},
		&setOp{writerID: dbID, key: []byte("a@7"), value: []byte("v2")},
		&setOp{writerID: dbID, key: []byte("b@5"), value: []byte("v3")},
		&setOp{writerID: dbID, key: []byte("b@9"), value: []byte("v4")},
		&setOp{writerID: dbID, key: []byte("c"), value: []byte("v5")}, // no suffix
		&flushOp{db: dbID},
		&dbRatchetFormatMajorVersionOp{dbID: dbID, vers: pebble.FormatSuffixMask},
		&deleteSuffixRangeOp{
			dbID:  dbID,
			start: []byte("a"),
			end:   []byte("z"),
			lower: []byte("@6"),
			upper: []byte("@10"),
		},
		&flushOp{db: dbID},
		&deleteSuffixRangeOp{
			dbID:  dbID,
			start: []byte("a"),
			end:   []byte("z"),
			lower: []byte("@1"),
			upper: []byte("@2"),
		},
		&closeOp{objID: dbID},
	}

	rng := rand.New(rand.NewPCG(0, 1))
	testOpts := RandomOptions(rng, kf, RandomOptionsCfg{})
	// Force the starting FMV below FormatSuffixMask so we exercise the
	// in-stream ratchet path. (RandomOptions can pick any FMV in range.)
	// Also disable shared storage / external storage / WAL failover / and
	// any other option that requires a higher FMV than FormatMinSupported;
	// RandomOptions may have enabled them at higher FMVs and they'd fail
	// `Options.Validate` once we ratchet the FMV back down.
	testOpts.Opts.FormatMajorVersion = pebble.FormatMinSupported
	testOpts.Opts.CreateOnShared = remote.CreateOnSharedNone
	testOpts.sharedStorageEnabled = false
	testOpts.externalStorageEnabled = false
	testOpts.useSharedReplicate = false
	testOpts.useExternalReplicate = false

	var historyBuf bytes.Buffer
	test, err := New(ops, testOpts, "" /* dir */, io.MultiWriter(&historyBuf))
	require.NoError(t, err)
	require.NoError(t, Execute(test))

	// Sanity: the op should appear in the recorded history as DeleteSuffixRange.
	require.True(t, strings.Contains(historyBuf.String(), "DeleteSuffixRange"),
		"expected DeleteSuffixRange to appear in history; got:\n%s", historyBuf.String())
}

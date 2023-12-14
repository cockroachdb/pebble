package metamorphic

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/randvar"
	"github.com/stretchr/testify/require"
)

func TestObjKey(t *testing.T) {
	testCases := []struct {
		key  objKey
		want string
	}{
		{
			key:  makeObjKey(makeObjID(dbTag, 1), []byte("foo")),
			want: "db1:foo",
		},
		{
			key:  makeObjKey(makeObjID(batchTag, 1), []byte("bar")),
			want: "batch1:bar",
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tc.want, tc.key.String())
		})
	}
}

func TestKeyManager_AddKey(t *testing.T) {
	m := newKeyManager(1 /* numInstances */)
	require.Empty(t, m.globalKeys)

	k1 := []byte("foo")
	require.True(t, m.addNewKey(k1))
	require.Len(t, m.globalKeys, 1)
	require.Len(t, m.globalKeyPrefixes, 1)
	require.Contains(t, m.globalKeys, k1)
	require.Contains(t, m.globalKeyPrefixes, k1)
	require.False(t, m.addNewKey(k1))
	require.True(t, m.prefixExists([]byte("foo")))
	require.False(t, m.prefixExists([]byte("bar")))

	k2 := []byte("bar")
	require.True(t, m.addNewKey(k2))
	require.Len(t, m.globalKeys, 2)
	require.Len(t, m.globalKeyPrefixes, 2)
	require.Contains(t, m.globalKeys, k2)
	require.Contains(t, m.globalKeyPrefixes, k2)
	require.True(t, m.prefixExists([]byte("bar")))
	k3 := []byte("bax@4")
	require.True(t, m.addNewKey(k3))
	require.Len(t, m.globalKeys, 3)
	require.Len(t, m.globalKeyPrefixes, 3)
	require.Contains(t, m.globalKeys, k3)
	require.Contains(t, m.globalKeyPrefixes, []byte("bax"))
	require.True(t, m.prefixExists([]byte("bax")))
	k4 := []byte("foo@6")
	require.True(t, m.addNewKey(k4))
	require.Len(t, m.globalKeys, 4)
	require.Len(t, m.globalKeyPrefixes, 3)
	require.Contains(t, m.globalKeys, k4)
	require.True(t, m.prefixExists([]byte("foo")))

	require.Equal(t, [][]byte{
		[]byte("bar"), []byte("bax"), []byte("foo"),
	}, m.prefixes())
}

func TestKeyManager_GetOrInit(t *testing.T) {
	id := makeObjID(batchTag, 1)
	key := []byte("foo")
	o := makeObjKey(id, key)

	m := newKeyManager(1 /* numInstances */)
	require.NotContains(t, m.byObjKey, o.String())
	require.NotContains(t, m.byObj, id)
	require.Contains(t, m.byObj, makeObjID(dbTag, 1)) // Always contains the DB key.

	meta1 := m.getOrInit(id, key)
	require.Contains(t, m.byObjKey, o.String())
	require.Contains(t, m.byObj, id)

	// Idempotent.
	meta2 := m.getOrInit(id, key)
	require.Equal(t, meta1, meta2)
}

func mustParseObjID(s string) objID {
	id, err := parseObjID(s)
	if err != nil {
		panic(err)
	}
	return id
}

func printKeys(w io.Writer, keys [][]byte) {
	if len(keys) == 0 {
		fmt.Fprintln(w, "(none)")
		return
	}
	for i, key := range keys {
		if i > 0 {
			fmt.Fprint(w, ", ")
		}
		fmt.Fprintf(w, "%q", key)
	}
	fmt.Fprintln(w)
}

func TestKeyManager(t *testing.T) {
	var buf bytes.Buffer
	km := newKeyManager(1 /* numInstances */)
	datadriven.RunTest(t, "testdata/key_manager", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "reset":
			km = newKeyManager(1 /* numInstances */)
			return ""
		case "run":
			buf.Reset()
			for _, line := range strings.Split(td.Input, "\n") {
				fields := strings.Fields(line)
				switch fields[0] {
				case "add-new-key":
					if km.addNewKey([]byte(fields[1])) {
						fmt.Fprintf(&buf, "%q is new\n", fields[1])
					} else {
						fmt.Fprintf(&buf, "%q already tracked\n", fields[1])
					}
				case "bounds":
					for i := 1; i < len(fields); i++ {
						objID := mustParseObjID(fields[1])
						fmt.Fprintf(&buf, "%s: %s\n", objID, km.boundsByObj[objID])
					}
				case "keys":
					fmt.Fprintf(&buf, "keys: ")
					printKeys(&buf, km.globalKeys)
				case "singledel-keys":
					objID := mustParseObjID(fields[1])
					fmt.Fprintf(&buf, "can singledel on %s: ", objID)
					printKeys(&buf, km.eligibleSingleDeleteKeys(objID))
				case "conflicts":
					var collapsed bool
					args := fields[1:]
					if args[0] == "collapsed" {
						collapsed = true
						args = args[1:]
					}
					src := mustParseObjID(args[0])
					dst := mustParseObjID(args[1])
					fmt.Fprintf(&buf, "conflicts merging %s", src)
					if collapsed {
						fmt.Fprint(&buf, " (collapsed)")
					}
					fmt.Fprintf(&buf, " into %s: ", dst)
					printKeys(&buf, km.checkForSingleDelConflicts(src, dst, collapsed))
				case "op":
					ops, err := parse([]byte(strings.TrimPrefix(line, "op")), parserOpts{
						allowUndefinedObjs: true,
					})
					if err != nil {
						t.Fatalf("parsing line %q: %s", line, err)
					} else if len(ops) != 1 {
						t.Fatalf("expected 1 op but found %d", len(ops))
					}
					km.update(ops[0])
					fmt.Fprintf(&buf, "[%s]\n", ops[0])
				default:
					return fmt.Sprintf("unrecognized subcommand %q", fields[0])
				}
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

func TestOpWrittenKeys(t *testing.T) {
	for name, info := range methods {
		t.Run(name, func(t *testing.T) {
			// Any operations that exist in methods but are not handled in
			// opWrittenKeys will result in a panic, failing the subtest.
			opWrittenKeys(info.constructor())
		})
	}
}

func TestLoadPrecedingKeys(t *testing.T) {
	rng := randvar.NewRand()
	cfg := DefaultOpConfig()
	km := newKeyManager(1 /* numInstances */)
	ops := generate(rng, 1000, cfg, km)

	cfg2 := DefaultOpConfig()
	km2 := newKeyManager(1 /* numInstances */)
	loadPrecedingKeys(t, ops, &cfg2, km2)

	// NB: We can't assert equality, because the original run may not have
	// ever used the max of the distribution.
	require.Greater(t, cfg2.writeSuffixDist.Max(), uint64(1))

	// NB: We can't assert equality, because the original run may have generated
	// keys that it didn't end up using in operations.
	require.Subset(t, km.globalKeys, km2.globalKeys)
	require.Subset(t, km.globalKeyPrefixes, km2.globalKeyPrefixes)
}

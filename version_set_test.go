package pebble

import (
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestVersionSetCheckpoint(t *testing.T) {
	mem := vfs.NewMem()
	err := mem.MkdirAll("ext", 0755)
	if err != nil {
		t.Fatal(err)
	}

	opts := &Options{
		FS:                  mem,
		MaxManifestFileSize: 1,
	}
	d, err := Open("", opts)
	if err != nil {
		t.Fatal(err)
	}

	writeAndIngest := func(k InternalKey, v []byte, filename string) {
		path := mem.PathJoin("ext", filename)
		f, err := mem.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		w := sstable.NewWriter(f, sstable.WriterOptions{})
		if err = w.Add(k, v); err != nil {
			t.Fatal(err)
		}
		if err = w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Ingest([]string{path}); err != nil {
			t.Fatal(err)
		}
	}

	// Multiple manifest files are created such that the latest one must have a correct snapshot
	// of the preceding state for the DB to be opened correctly and see the written data.
	writeAndIngest(base.MakeInternalKey([]byte("a"), 0, InternalKeyKindSet), []byte("b"), "a")
	writeAndIngest(base.MakeInternalKey([]byte("c"), 0, InternalKeyKindSet), []byte("d"), "c")
	d.Close()
	d, err = Open("", opts)
	if err != nil {
		t.Fatal(err)
	}
	checkValue := func(k string, expected string) {
		v, err := d.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, expected, string(v))
	}
	checkValue("a", "b")
	checkValue("c", "d")
}

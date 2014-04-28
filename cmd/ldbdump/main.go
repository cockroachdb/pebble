// The ldbdump program dumps the contents of LevelDB tables (.ldb files),
// formerly known as sorted string tables (.sst files).
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"code.google.com/p/leveldb-go/leveldb/db"
	"code.google.com/p/leveldb-go/leveldb/table"
)

var (
	verifyChecksums = flag.Bool("c", false, "Verify checksums.")
	truncate        = flag.Bool("t", false, "Truncate long keys and values.")

	kBuf, vBuf bytes.Buffer
)

func main() {
	flag.Parse()
	bad := false
	for i, arg := range flag.Args() {
		if i != 0 {
			fmt.Println()
		}
		fmt.Printf("filename: %q\n", arg)
		if err := dump(arg); err != nil {
			fmt.Printf("error: %q\n", err)
			bad = true
		}
	}
	if bad {
		os.Exit(1)
	}
}

func dump(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	// No need to "defer f.Close()", as closing r will close f.
	r := table.NewReader(f, &db.Options{
		VerifyChecksums: *verifyChecksums,
	})
	defer r.Close()

	t := r.Find(nil, nil)
	for t.Next() {
		k, v := t.Key(), t.Value()
		if *truncate {
			k = trunc(&kBuf, k)
			v = trunc(&vBuf, v)
		}
		fmt.Printf("%q: %q,\n", k, v)
	}
	return t.Close()
}

func trunc(dst *bytes.Buffer, b []byte) []byte {
	if len(b) < 64 {
		return b
	}
	dst.Reset()
	fmt.Fprintf(dst, "%s...(%d bytes)...%s", b[:20], len(b)-40, b[len(b)-20:])
	return dst.Bytes()
}

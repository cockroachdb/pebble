package private

import (
	"io"

	"github.com/cockroachdb/pebble/internal/manifest"
)

// FlushExternalTable is a hook for locking the commit pipeline and ingesting
// files into L0 without assigning a global sequence number for the files,
// mimicking a series of flushes.
//
// FlushExternalTable takes a *pebble.DB as its only argument, locks its
// commit pipeline and returns a function that may be invoked with the path
// and metadata of a sstable to flush directly into L0. Its second return
// argument is a Closer than when closed will release the commit pipeline.
// Because the commit pipeline remains locked until the Closer is closed, any
// concurrent writes or manual compactions will block.
//
// Calls to flush a sstable may fail if the file's sequence numbers are not
// greater than the current commit pipeline's sequence number. On sucess the
// commit pipeline's published sequence number will be moved to the file's
// highest sequence number.
//
// This function is intended for use by the pebble bench compact tool.
var FlushExternalTable func(interface{}) (func(string, *manifest.FileMetadata) error, io.Closer, error)

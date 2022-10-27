package replay

import (
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWorkloadCaptureCleanerNotReadyToClean(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "./", base.FileTypeTable, 1)
	_, err := imfs.Create(filePath)
	require.NoError(t, err)

	captureFileHandler := DefaultWorkloadCollectorFileHandler{}
	cleaner := NewWorkloadCaptureCleaner(imfs, "./", captureFileHandler)
	err = cleaner.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)
	_, err = imfs.Stat(filePath)
	require.NoError(t, err)
}

func TestWorkloadCaptureCleanerMarkForClean(t *testing.T) {
	wg := sync.WaitGroup{}
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "./", base.FileTypeTable, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	captureFileHandler := DefaultWorkloadCollectorFileHandler{}
	cleaner := NewWorkloadCaptureCleaner(imfs, "./", captureFileHandler)
	wg.Add(1)

	go func() {
		defer wg.Done()
		cleaner.waitAndProcessFile()
	}()
	cleaner.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	wg.Wait()
	err = cleaner.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)
	_, err = imfs.Stat(filePath)
	require.Errorf(t, err, "stat 000001.sst: sourceFile does not exist")
}

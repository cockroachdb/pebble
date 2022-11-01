package replay

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWorkloadCaptureCleanerNotReadyToClean(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	captureFileHandler := DefaultWorkloadCollectorFileHandler{}
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)
	err = cleaner.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)
	_, err = imfs.Stat(filePath)
	require.NoError(t, err)
}

func TestWorkloadCaptureCleanerMarkForClean(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	captureFileHandler := DefaultWorkloadCollectorFileHandler{}
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	ch := make(chan struct{})
	go func() {
		cleaner.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	cleaner.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	cleaner.Lock()
	for len(cleaner.filesToProcess) != 0 {
		cleaner.Unlock()
		time.Sleep(time.Microsecond)
		cleaner.Lock()
	}
	cleaner.Unlock()
	cleaner.StopCollectorFileListener()
	<-ch
	err = cleaner.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)
	_, err = imfs.Stat(filePath)
	require.Errorf(t, err, "stat 000001.sst: file does not exist")
}

func TestWorkloadCaptureWatcherDeleteWhenObsolete(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	captureFileHandler := DefaultWorkloadCollectorFileHandler{}
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	err = cleaner.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)

	ch := make(chan struct{})
	go func() {
		cleaner.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	cleaner.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	cleaner.Lock()
	for len(cleaner.filesToProcess) != 0 {
		cleaner.Unlock()
		time.Sleep(time.Microsecond)
		cleaner.Lock()
	}
	cleaner.Unlock()
	cleaner.StopCollectorFileListener()
	<-ch
	_, err = imfs.Stat(filePath)
	require.Errorf(t, err, "stat 000001.sst: file does not exist")
}

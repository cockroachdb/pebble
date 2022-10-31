package replay

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func newWorkloadCollectorForTest(
	fs vfs.FS, srcDir string, cleaner base.Cleaner, fileHandler WorkloadStorage,
) *WorkloadCollector {
	collector := NewWorkloadCollector(srcDir, fileHandler)
	collector.configuration.fs = fs
	collector.configuration.cleaner = cleaner

	return collector
}

func TestWorkloadCaptureCleanerNotReadyToClean(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)
	atomic.StoreUint32(&collector.mu.enabled, 1)
	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	err = collector.Clean(imfs, base.FileTypeTable, filePath)
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

	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)

	atomic.StoreUint32(&collector.mu.enabled, 1)
	ch := make(chan struct{})
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	collector.mu.Lock()
	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()
	collector.StopCollectorFileListener()
	<-ch
	err = collector.Clean(imfs, base.FileTypeTable, filePath)
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

	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)

	collector.mu.fileState[filePath] |= readyForProcessing
	err = collector.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)

	atomic.StoreUint32(&collector.mu.enabled, 1)
	ch := make(chan struct{})
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	collector.mu.Lock()
	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()
	collector.StopCollectorFileListener()
	<-ch
	_, err = imfs.Stat(filePath)
	require.Errorf(t, err, "stat 000001.sst: file does not exist")
}

func TestManifestCollection(t *testing.T) {
	imfs := vfs.NewMem()
	manifestFilePath := base.MakeFilepath(imfs, "", base.FileTypeManifest, 1)
	manifestFile, err := imfs.Create(manifestFilePath)
	require.NoError(t, err)

	const numberOfBytesToWrite = 1024
	token := make([]byte, numberOfBytesToWrite)
	rand.Read(token)

	_, err = manifestFile.Write(token)
	require.NoError(t, err)
	require.NoError(t, manifestFile.Close())

	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)

	ch := make(chan struct{})
	atomic.StoreUint32(&collector.mu.enabled, 1)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})
	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	collector.mu.Lock()

	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()
	collector.StopCollectorFileListener()
	<-ch
	destFilepath := imfs.PathJoin("captured", imfs.PathBase(collector.mu.manifests[0].sourceFilepath))
	stat, err := imfs.Stat(destFilepath)
	require.NoError(t, err)
	require.Equal(t, int64(numberOfBytesToWrite), stat.Size())
	f, err := imfs.Open(destFilepath)
	require.NoError(t, err)
	fromOutputFile := make([]byte, numberOfBytesToWrite)
	_, err = f.Read(fromOutputFile)
	require.NoError(t, err)
	require.Equal(t, fromOutputFile, token)
}

func TestManifestCopyingWithChunks(t *testing.T) {
	imfs := vfs.NewMem()
	manifestFilePath := base.MakeFilepath(imfs, "", base.FileTypeManifest, 1)
	manifestFile, err := imfs.Create(manifestFilePath)
	require.NoError(t, err)

	const numberOfBytesToWrite = 9 << 9
	token := make([]byte, numberOfBytesToWrite)
	rand.Read(token)

	_, err = manifestFile.Write(token)
	require.NoError(t, err)
	require.NoError(t, manifestFile.Close())

	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)

	ch := make(chan struct{})
	atomic.StoreUint32(&collector.mu.enabled, 1)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})
	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})
	collector.mu.Lock()

	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()
	collector.StopCollectorFileListener()
	<-ch
	destFilepath := imfs.PathJoin("captured", imfs.PathBase(collector.mu.manifests[0].sourceFilepath))
	stat, err := imfs.Stat(destFilepath)
	require.NoError(t, err)
	require.Equal(t, int64(numberOfBytesToWrite), stat.Size())
	f, err := imfs.Open(destFilepath)
	require.NoError(t, err)
	fromOutputFile := make([]byte, numberOfBytesToWrite)
	_, err = f.Read(fromOutputFile)
	require.NoError(t, err)
	require.Equal(t, fromOutputFile, token)
}

func TestManifestCopyingWithRotation(t *testing.T) {
	imfs := vfs.NewMem()

	// Manifest 1
	manifestFilePath1 := base.MakeFilepath(imfs, "", base.FileTypeManifest, 1)
	manifestFile1, err := imfs.Create(manifestFilePath1)
	require.NoError(t, err)

	// Manifest 2
	manifestFilePath2 := base.MakeFilepath(imfs, "", base.FileTypeManifest, 2)
	manifestFile2, err := imfs.Create(manifestFilePath2)
	require.NoError(t, err)

	// Write HALF the data to Manifest 1
	manifest1DataSize := 8338
	fileInputData1 := make([]byte, manifest1DataSize)
	rand.Read(fileInputData1)
	_, err = manifestFile1.Write(fileInputData1[:manifest1DataSize/2])
	require.NoError(t, err)

	// Write all the data to Manifest 2
	manifest2DataSize := 6746
	fileInputData2 := make([]byte, manifest2DataSize)
	rand.Read(fileInputData2)
	_, err = manifestFile2.Write(fileInputData2)
	require.NoError(t, err)

	// Create table path to trigger the FileHandler
	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	// Create the collector and file handler
	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)

	ch := make(chan struct{})
	atomic.StoreUint32(&collector.mu.enabled, 1)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	// Create the first manifests
	collector.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath1,
		FileNum: 1,
	})
	// Trigger the Manifest Handler
	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})

	collector.mu.Lock()
	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()

	_, err = manifestFile1.Write(fileInputData1[manifest1DataSize/2:])
	require.NoError(t, err)

	collector.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath2,
		FileNum: 2,
	})

	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})

	collector.mu.Lock()
	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()

	collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})

	collector.mu.Lock()
	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()

	collector.StopCollectorFileListener()
	<-ch
	expectedManifestSize := []int{manifest1DataSize, manifest2DataSize}
	expectedFileData := [][]byte{fileInputData1, fileInputData2}
	require.Len(t, collector.mu.manifests, 2)
	require.Equal(t, collector.mu.manifestIndex, 1)
	for i, manifest := range collector.mu.manifests {
		destFilepath := imfs.PathJoin("captured", imfs.PathBase(manifest.sourceFilepath))
		manifestStats, err := imfs.Stat(destFilepath)
		require.NoError(t, err)
		require.Equal(t, int64(expectedManifestSize[i]), manifestStats.Size())
		f, err := imfs.Open(destFilepath)
		require.NoError(t, err)
		fromOutputFile := make([]byte, expectedManifestSize[i])
		_, err = f.Read(fromOutputFile)
		require.NoError(t, err)
		require.Equal(
			t,
			fromOutputFile,
			expectedFileData[i],
			"File contents do not match between %s and %s",
			destFilepath,
			manifest.sourceFilepath,
		)
	}
}

func TestManifestNotCleanedBeforeOpen(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "", base.FileTypeManifest, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)
	atomic.StoreUint32(&collector.mu.enabled, 1)
	collector.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    filePath,
		FileNum: 1,
	})
	err = collector.Clean(imfs, base.FileTypeManifest, filePath)
	require.NoError(t, err)
	_, err = imfs.Stat(filePath)
	require.NoError(t, err)
}

type testEventListener struct {
	copySSTableCount    int
	createManifestCount int
}

func (tel *testEventListener) CopySSTable(fs vfs.FS, path string) error {
	tel.copySSTableCount++
	return nil
}

func (tel *testEventListener) CreateManifestFile(name string) (vfs.File, error) {
	tel.createManifestCount++
	return nil, nil
}

func TestAttachCollectorToPebble(t *testing.T) {

	imfs := vfs.NewMem()
	manifestFilePath := base.MakeFilepath(imfs, "", base.FileTypeManifest, 1)
	manifestFile, err := imfs.Create(manifestFilePath)
	require.NoError(t, err)
	require.NoError(t, manifestFile.Close())

	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	tel := &testEventListener{}
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, tel)
	atomic.StoreUint32(&collector.mu.enabled, 1)
	ch := make(chan struct{})

	opts := &pebble.Options{FS: imfs}
	collector.Attach(opts)

	require.Equal(t, collector, opts.Cleaner)
	require.NotNil(t, opts.EventListener.TableIngested)
	require.NotNil(t, opts.EventListener.ManifestCreated)
	require.NotNil(t, opts.EventListener.FlushEnd)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	opts.EventListener.FlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
		FileNum: 1,
		Size:    10,
	}}})

	opts.EventListener.ManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})

	collector.mu.Lock()
	for len(collector.mu.sstablesToProcess) != 0 {
		collector.mu.Unlock()
		time.Sleep(time.Microsecond)
		collector.mu.Lock()
	}
	collector.mu.Unlock()

	collector.StopCollectorFileListener()
	<-ch

	require.Equal(t, 1, tel.copySSTableCount)
	require.Equal(t, 1, tel.createManifestCount)
}

func TestEnableDisable(t *testing.T) {
	imfs := vfs.NewMem()
	captureFileHandler := FilesystemWorkloadStorage(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{}, captureFileHandler)
	type testCase struct{ onFlushEndLength, onTableIngestLength, onManifestLength int }
	testCases := []testCase{
		{
			onFlushEndLength:    0,
			onTableIngestLength: 0,
			onManifestLength:    0,
		},
		{
			onFlushEndLength:    1,
			onTableIngestLength: 2,
			onManifestLength:    1,
		},
	}
	for _, currentTestCase := range testCases {
		collector.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
			FileNum: 1,
			Size:    10,
		}}})
		require.Len(t, collector.mu.sstablesToProcess, currentTestCase.onFlushEndLength)
		collector.OnTableIngest(pebble.TableIngestInfo{
			Tables: []struct {
				pebble.TableInfo
				Level int
			}{
				{TableInfo: pebble.TableInfo{
					FileNum: 1,
					Size:    10,
				}, Level: 0},
			},
		})
		require.Len(t, collector.mu.sstablesToProcess, currentTestCase.onTableIngestLength)
		collector.OnManifestCreated(pebble.ManifestCreateInfo{
			FileNum: 1,
		})
		require.Len(t, collector.mu.manifests, currentTestCase.onManifestLength)

		// Enable the WorkloadCollector for the second iteration
		atomic.StoreUint32(&collector.mu.enabled, 1)
	}
}

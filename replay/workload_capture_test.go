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
	fs vfs.FS, srcDir string, cleaner base.Cleaner,
) *WorkloadCollector {
	collector := NewWorkloadCollector(srcDir)
	collector.configuration.srcFS = fs
	collector.configuration.destFS = fs
	collector.configuration.destDir = "captured"
	collector.configuration.cleaner = cleaner

	return collector
}

func createCaptureDir(fs vfs.FS, destDir string) {
	if err := fs.MkdirAll(destDir, 0755); err != nil {
		panic(err)
	}
}

func TestWorkloadCaptureCleanerNotReadyToClean(t *testing.T) {
	imfs := vfs.NewMem()
	filePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	f, err := imfs.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})
	atomic.StoreUint32(&collector.enabled, 1)
	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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

	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})

	atomic.StoreUint32(&collector.enabled, 1)
	ch := make(chan struct{})
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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
	collector.Stop()
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

	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})

	collector.mu.fileState[imfs.PathBase(filePath)] |= readyForProcessing
	err = collector.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)

	atomic.StoreUint32(&collector.enabled, 1)
	ch := make(chan struct{})
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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
	collector.Stop()
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
	dataToWrite := make([]byte, numberOfBytesToWrite)
	copyOfFileInputData := make([]byte, numberOfBytesToWrite)
	rand.Read(dataToWrite)
	copy(copyOfFileInputData, dataToWrite)

	_, err = manifestFile.Write(dataToWrite)
	require.NoError(t, err)
	require.NoError(t, manifestFile.Close())

	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})

	ch := make(chan struct{})
	atomic.StoreUint32(&collector.enabled, 1)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})
	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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
	collector.Stop()
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
	require.Equal(t, fromOutputFile, copyOfFileInputData)
}

func TestManifestNumberCollectionBeforeEnable(t *testing.T) {
	imfs := vfs.NewMem()
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})
	require.Equal(t, uint64(0), collector.curManifest)
	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    "",
		FileNum: 1,
	})
	require.Equal(t, uint64(1), collector.curManifest)
}

func TestManifestCopyingWithChunks(t *testing.T) {
	imfs := vfs.NewMem()
	manifestFilePath := base.MakeFilepath(imfs, "", base.FileTypeManifest, 1)
	manifestFile, err := imfs.Create(manifestFilePath)
	require.NoError(t, err)

	const numberOfBytesToWrite = 9 << 9
	dataToWrite := make([]byte, numberOfBytesToWrite)
	copyOfDataToWrite := make([]byte, numberOfBytesToWrite)
	rand.Read(dataToWrite)
	copy(copyOfDataToWrite, dataToWrite)

	_, err = manifestFile.Write(dataToWrite)
	require.NoError(t, err)
	require.NoError(t, manifestFile.Close())

	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})

	ch := make(chan struct{})
	atomic.StoreUint32(&collector.enabled, 1)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})
	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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
	collector.Stop()
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
	require.Equal(t, fromOutputFile, copyOfDataToWrite)
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

	// Manifest 3
	manifestFilePath3 := base.MakeFilepath(imfs, "", base.FileTypeManifest, 3)
	manifestFile3, err := imfs.Create(manifestFilePath3)
	require.NoError(t, err)

	// Write HALF the data to Manifest 1
	manifest1DataSize := 8338
	fileInputData1 := make([]byte, manifest1DataSize)
	copyOfFileInputData1 := make([]byte, manifest1DataSize)
	rand.Read(fileInputData1)
	copy(copyOfFileInputData1, fileInputData1)
	_, err = manifestFile1.Write(fileInputData1[:manifest1DataSize/2])
	require.NoError(t, err)

	// Write all the data to Manifest 2
	manifest2DataSize := 6746
	fileInputData2 := make([]byte, manifest2DataSize)
	copyOfFileInputData2 := make([]byte, manifest2DataSize)
	rand.Read(fileInputData2)
	copy(copyOfFileInputData2, fileInputData2)
	_, err = manifestFile2.Write(fileInputData2)
	require.NoError(t, err)

	// Write all the data to Manifest 3
	manifest3DataSize := 4378
	fileInputData3 := make([]byte, manifest3DataSize)
	copyOfFileInputData3 := make([]byte, manifest3DataSize)
	rand.Read(fileInputData3)
	copy(copyOfFileInputData3, fileInputData3)
	_, err = manifestFile3.Write(fileInputData3)
	require.NoError(t, err)

	// Create table path to trigger the FileHandler
	tableFilePath := base.MakeFilepath(imfs, "", base.FileTypeTable, 1)
	tableFile, err := imfs.Create(tableFilePath)
	require.NoError(t, err)
	require.NoError(t, tableFile.Close())

	// Create the collector and file handler
	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})

	ch := make(chan struct{})
	atomic.StoreUint32(&collector.enabled, 1)
	go func() {
		collector.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	// Create the first manifests
	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath1,
		FileNum: 1,
	})

	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath2,
		FileNum: 2,
	})

	// Trigger the Manifest Handler
	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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

	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath3,
		FileNum: 3,
	})

	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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

	collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
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

	collector.Stop()
	<-ch
	expectedManifestSize := []int{manifest1DataSize, manifest2DataSize, manifest3DataSize}
	expectedFileData := [][]byte{copyOfFileInputData1, copyOfFileInputData2, copyOfFileInputData3}
	require.Len(t, collector.mu.manifests, 3)
	require.Equal(t, collector.mu.manifestIndex, 2)
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

	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})
	atomic.StoreUint32(&collector.enabled, 1)
	collector.onManifestCreated(pebble.ManifestCreateInfo{
		Path:    filePath,
		FileNum: 1,
	})
	err = collector.Clean(imfs, base.FileTypeManifest, filePath)
	require.NoError(t, err)
	_, err = imfs.Stat(filePath)
	require.NoError(t, err)
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

	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})
	atomic.StoreUint32(&collector.enabled, 1)

	opts := &pebble.Options{FS: imfs}
	collector.Attach(opts)

	require.Equal(t, collector, opts.Cleaner)
	require.NotNil(t, opts.EventListener.TableIngested)
	require.NotNil(t, opts.EventListener.ManifestCreated)
	require.NotNil(t, opts.EventListener.FlushEnd)
}

func TestEnableDisable(t *testing.T) {
	imfs := vfs.NewMem()
	createCaptureDir(imfs, "captured")
	collector := newWorkloadCollectorForTest(imfs, "", base.DeleteCleaner{})
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
		collector.onFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
			FileNum: 1,
			Size:    10,
		}}})
		require.Len(t, collector.mu.sstablesToProcess, currentTestCase.onFlushEndLength)
		collector.onTableIngest(pebble.TableIngestInfo{
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
		collector.onManifestCreated(pebble.ManifestCreateInfo{
			FileNum: 1,
		})
		require.Len(t, collector.mu.manifests, currentTestCase.onManifestLength)

		// Enable the WorkloadCollector for the second iteration
		atomic.StoreUint32(&collector.enabled, 1)
	}
}

func TestAtomicStartStop(t *testing.T) {
	imfs := vfs.NewMem()
	collector := NewWorkloadCollector("")
	collector.Stop()
	require.Equal(t, collector.fileListener.stopFileListener, false)
	atomic.StoreUint32(&collector.enabled, 1)
	collector.Start(imfs, "captured")
	require.NotEqual(t, imfs, collector.configuration.destFS)
}

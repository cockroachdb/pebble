package replay

import (
	"math/rand"
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

	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
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

	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	cleaner.enabled.Store(true)
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

	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	err = cleaner.Clean(imfs, base.FileTypeTable, filePath)
	require.NoError(t, err)

	cleaner.enabled.Store(true)
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

	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	ch := make(chan struct{})
	cleaner.enabled.Store(true)
	go func() {
		cleaner.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	cleaner.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})
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
	stat, err := imfs.Stat(cleaner.manifest[0].destFilepath)
	require.NoError(t, err)
	require.Equal(t, int64(numberOfBytesToWrite), stat.Size())
	f, err := imfs.Open(cleaner.manifest[0].destFilepath)
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

	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	ch := make(chan struct{})
	cleaner.enabled.Store(true)
	go func() {
		cleaner.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	cleaner.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath,
		FileNum: 1,
	})
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
	stat, err := imfs.Stat(cleaner.manifest[0].destFilepath)
	require.NoError(t, err)
	require.Equal(t, int64(numberOfBytesToWrite), stat.Size())
	f, err := imfs.Open(cleaner.manifest[0].destFilepath)
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
	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)

	ch := make(chan struct{})
	cleaner.enabled.Store(true)
	go func() {
		cleaner.filesToProcessWatcher()
		ch <- struct{}{}
	}()

	// Create the first manifest
	cleaner.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath1,
		FileNum: 1,
	})
	// Trigger the Manifest Handler
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

	_, err = manifestFile1.Write(fileInputData1[manifest1DataSize/2:])
	require.NoError(t, err)

	cleaner.OnManifestCreated(pebble.ManifestCreateInfo{
		Path:    manifestFilePath2,
		FileNum: 2,
	})

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
	expectedManifestSize := []int{manifest1DataSize, manifest2DataSize}
	expectedFileData := [][]byte{fileInputData1, fileInputData2}
	require.Len(t, cleaner.manifest, 2)
	require.Equal(t, cleaner.manifestIndex, 1)
	for i, manifest := range cleaner.manifest {
		manifestStats, err := imfs.Stat(manifest.destFilepath)
		require.NoError(t, err)
		require.Equal(t, int64(expectedManifestSize[i]), manifestStats.Size())
		f, err := imfs.Open(manifest.destFilepath)
		require.NoError(t, err)
		fromOutputFile := make([]byte, expectedManifestSize[i])
		_, err = f.Read(fromOutputFile)
		require.NoError(t, err)
		require.Equal(
			t,
			fromOutputFile,
			expectedFileData[i],
			"File contents do not match between %s and %s",
			manifest.destFilepath,
			manifest.sourceFilepath,
		)
	}
}

func TestEnableDisable(t *testing.T) {
	imfs := vfs.NewMem()
	captureFileHandler := NewDefaultWorkloadCollectorFileHandler("captured")
	cleaner := NewWorkloadCaptureCleaner(imfs, "", captureFileHandler)
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
		cleaner.OnFlushEnd(pebble.FlushInfo{Output: []pebble.TableInfo{{
			FileNum: 1,
			Size:    10,
		}}})
		require.Len(t, cleaner.filesToProcess, currentTestCase.onFlushEndLength)
		cleaner.OnTableIngest(pebble.TableIngestInfo{
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
		require.Len(t, cleaner.filesToProcess, currentTestCase.onTableIngestLength)
		cleaner.OnManifestCreated(pebble.ManifestCreateInfo{
			FileNum: 1,
		})
		require.Len(t, cleaner.manifest, currentTestCase.onManifestLength)

		// Enable the WorkloadCollector for the second iteration
		cleaner.enabled.Store(true)
	}
}

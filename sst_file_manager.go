package pebble

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

// SstFileManager is used to track SST files in the DB and Set a maximum allowed
// space limit for SST files that when reached the DB won't do any further
// flushes or compactions and will set the background error.
//
// All SstFileManager public functions are thread-safe.
//
// This seems to be done only on a best-effort basis. We don't fail for example
// if any of the onAddFile calls fail.
//
// Note: Doesn't implement everything that the RocksDB one does.
// For example controlling the deletion rate.
type SstFileManager struct {
	// Requires FS to query for file sizes.
	fs vfs.FS

	// TODO: Doc on what it protects.
	sync.Mutex

	// The maximum allowed space (in bytes) for sst files.
	maxAllowedSpace uint64

	// Compactions should only execute if they can leave at least
	// this amount of buffer space for logs and flushes.
	compactionBufferSize uint64

	// The summation of all output files of in-progress compactions.
	inProgressFilesSize uint64

	// A set of files belonging to in-progress compactions.
	inProgressFiles map[string]bool

	// The summation of the sizes of all files in trackedFiles map.
	totalFilesSize uint64

	// A map containing all tracked files and their sizes.
	//  file_path => file_size
	trackedFiles map[string]uint64

	// Estimated size of the current ongoing compactions.
	currentCompactionsReservedSize uint64
}

// TODO: Use Options since the list of ctor params may grow.
func NewSstFileManager(fs vfs.FS) *SstFileManager {
	return &SstFileManager{
		fs: fs,
	}
}

// Update the maximum allowed space that should be used by Pebble. If
// the total size of the SST files exceeds maxAllowedSpace, writes to
// Pebble will fail.
//
// Setting maxAllowedSpace to 0 will disable this feature i.e. maximum allowed
// space will be infinite. This is the default value.
//
// Thread-safe.
func (s *SstFileManager) SetMaxAllowedSpaceUsage(maxAllowedSpace uint64) {
	s.Lock()
	defer s.Unlock()
	s.maxAllowedSpace = maxAllowedSpace
}

// Set the amount of buffer room each compaction should be able to leave.
// In other words, at its maximum disk space consumption, the compaction
// should still leave compactionBufferSize available on the disk so that
// other background functions may continue, such as logging and flushing.

// Thread-safe.
func (s *SstFileManager) SetCompactionBufferSize(compactionBufferSize uint64) {
	s.Lock()
	defer s.Unlock()
	s.compactionBufferSize = compactionBufferSize
}

// Return true if the total size of SST files exceeded the maximum allowed
// space usage.
//
// Thread-safe.
func (s *SstFileManager) IsMaxAllowedSpaceReached() bool {
	s.Lock()
	defer s.Unlock()
	if s.maxAllowedSpace <= 0 {
		return false
	}
	return s.totalFilesSize >= s.maxAllowedSpace
}

// Returns true if the total size of SST files as well as estimated size
// of ongoing compactions exceeds the maximum allowed space usage.
//
// Thread-safe.
func (s *SstFileManager) IsMaxAllowedSpaceReachedIncludingCompactions() bool {
	s.Lock()
	defer s.Unlock()
	if s.maxAllowedSpace <= 0 {
		return false
	}
	return s.totalFilesSize+s.currentCompactionsReservedSize >= s.maxAllowedSpace
}

// Return the total size of all tracked files.
//
// Thread-safe.
func (s *SstFileManager) GetTotalSize() uint64 {
	s.Lock()
	defer s.Unlock()
	return s.totalFilesSize
}

// Return a map containing all tracked files and their corresponding sizes.
//
// Thread-safe.
func (s *SstFileManager) GetTrackedFiles() map[string]uint64 {
	s.Lock()
	defer s.Unlock()
	// QUES: Does this include the in progress compaction files?
	m := make(map[string]uint64, len(s.trackedFiles))
	for k, v := range s.trackedFiles {
		m[k] = v
	}
	return m
}

// Bookkeeping so total_file_sizes_ goes back to normal after compaction
// finishes
func (s *SstFileManager) onCompactionCompletion(c *compaction) {
}

// onAddFile is called whenever a new SST file is added as a result of
// flush, compaction or opening an existing DB.
// QUES: What about file ingestion?
// TODO: RocksDB ignores result of this function. So return void?
// And hence document that most APIs of this struct are best-effort only?
// TODO: Compaction never set to true. Why?
func (s *SstFileManager) onAddFile(path string, compaction bool) error {
	info, err := s.fs.Stat(path)
	if err != nil {
		return err
	}
	size := uint64(info.Size())
	s.Lock()
	defer s.Unlock()
	trackedFileSize, ok := s.trackedFiles[path]
	if ok {
		// assert(!compaction); Why compaction is false in this case?
		// And why would a file be found?
		// After finishing the compaction?
		s.totalFilesSize = s.totalFilesSize - trackedFileSize + size
		// We touch this? means compaction ended? and hence we re-add the same file?
		s.currentCompactionsReservedSize -= size
	} else {
		s.totalFilesSize += size
		if compaction {
			// why don't we add to currentCompactionsReservedSize here?
			s.inProgressFilesSize += size
			s.inProgressFiles[path] = true
		}
	}
	s.trackedFiles[path] = size
	return nil
}

func (s *SstFileManager) onDeleteFile(path string) {
	s.Lock()
	defer s.Unlock()
	trackedFileSize, ok := s.trackedFiles[path]
	if !ok {
		_, ok := s.inProgressFiles[path]
		if ok {
			panic(fmt.Sprintf("pebble: file %s tracked by inProgressFiles but not by trackedFiles", path))
		}
	}
	s.totalFilesSize -= trackedFileSize
	delete(s.trackedFiles, path)
	// Check if it belonged to an in-progress compaction.
	if _, ok := s.inProgressFiles[path]; ok {
		s.inProgressFilesSize -= trackedFileSize
		delete(s.inProgressFiles, path)
	}
}

func (s *SstFileManager) enoughRoomForCompaction() {

}

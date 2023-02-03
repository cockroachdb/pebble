package base

import (
	"fmt"
	"strconv"
	"strings"
)

// FileNum is an internal DB identifier for a file.
type FileNum uint64

// String returns a string representation of the file number.
func (fn FileNum) String() string { return fmt.Sprintf("%06d", fn) }

// FileType enumerates the types of files found in a DB.
type FileType int

// The FileType enumeration.
const (
	FileTypeLog FileType = iota
	FileTypeLock
	FileTypeTable
	FileTypeManifest
	FileTypeCurrent
	FileTypeOptions
	FileTypeOldTemp
	FileTypeTemp
)

// MakeFilename builds a filename from components.
func MakeFilename(fileType FileType, fileNum FileNum) string {
	switch fileType {
	case FileTypeLog:
		return fmt.Sprintf("%s.log", fileNum)
	case FileTypeLock:
		return "LOCK"
	case FileTypeTable:
		return fmt.Sprintf("%s.sst", fileNum)
	case FileTypeManifest:
		return fmt.Sprintf("MANIFEST-%s", fileNum)
	case FileTypeCurrent:
		return "CURRENT"
	case FileTypeOptions:
		return fmt.Sprintf("OPTIONS-%s", fileNum)
	case FileTypeOldTemp:
		return fmt.Sprintf("CURRENT.%s.dbtmp", fileNum)
	case FileTypeTemp:
		return fmt.Sprintf("temporary.%s.dbtmp", fileNum)
	}
	panic("unreachable")
}

// ParseFilename parses the components from a filename.
func ParseFilename(filename string) (fileType FileType, fileNum FileNum, ok bool) {
	switch {
	case filename == "CURRENT":
		return FileTypeCurrent, 0, true
	case filename == "LOCK":
		return FileTypeLock, 0, true
	case strings.HasPrefix(filename, "MANIFEST-"):
		fileNum, ok = parseFileNum(filename[len("MANIFEST-"):])
		if !ok {
			break
		}
		return FileTypeManifest, fileNum, true
	case strings.HasPrefix(filename, "OPTIONS-"):
		fileNum, ok = parseFileNum(filename[len("OPTIONS-"):])
		if !ok {
			break
		}
		return FileTypeOptions, fileNum, ok
	case strings.HasPrefix(filename, "CURRENT.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("CURRENT."):], ".dbtmp")
		fileNum, ok = parseFileNum(s)
		if !ok {
			break
		}
		return FileTypeOldTemp, fileNum, ok
	case strings.HasPrefix(filename, "temporary.") && strings.HasSuffix(filename, ".dbtmp"):
		s := strings.TrimSuffix(filename[len("temporary."):], ".dbtmp")
		fileNum, ok = parseFileNum(s)
		if !ok {
			break
		}
		return FileTypeTemp, fileNum, ok
	default:
		i := strings.IndexByte(filename, '.')
		if i < 0 {
			break
		}
		fileNum, ok = parseFileNum(filename[:i])
		if !ok {
			break
		}
		switch filename[i+1:] {
		case "sst":
			return FileTypeTable, fileNum, true
		case "log":
			return FileTypeLog, fileNum, true
		}
	}
	return 0, fileNum, false
}

func parseFileNum(s string) (fileNum FileNum, ok bool) {
	u, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fileNum, false
	}
	return FileNum(u), true
}

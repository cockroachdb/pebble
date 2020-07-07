package errors

// CorruptionError wraps errors due to internal constraint violations
// that lead to placing the DB in read only mode.
// Might also signify corruption.
// TODO: rename to Corruption. because the invariants are always around the ssts.
// all invariants that could only break if data in sst is corrupted or
// isn't what we expect.
// not for programmatic errors/in-memory logic errors.
// for example ?
// expected locking. concurrent compactions, picking the same file.
//
// it is all about data in ssts.
// rename to sstcorruptionerror?
//
// rocksDB tags manifest errors as corruption as well.
// so maybe anything that is on disk?
// check if they do this for WAL as well.
// check what errors they tag in version_set.cc
// could mean in memory corruption as well, for example in batch.go
type CorruptionError struct {
	Err error
}

// Unwrap the wrapped descriptive error that describes the constraint that got
// violated.
func (i CorruptionError) Unwrap() error {
	return i.Err
}

func (i CorruptionError) Error() string {
	return i.Err.Error()
}

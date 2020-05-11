package errors

import "errors"

var (
	ErrNotSupported = errors.New("pebble: not supported")
)

// This is equivalent to RocksDB's Status::Corruption.
type InvariantError struct {
	Err error
}

// Unwrap the wrapped descriptive error that describes the constraint that got violated.
func (i InvariantError) Unwrap() error {
	return i.Err
}

func (i InvariantError) Error() string {
	return i.Err.Error()
}

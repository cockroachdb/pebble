package errors

import "errors"

var (
	ErrNotSupported = errors.New("pebble: not supported")
)

// Marker error for whenever an invariant is violated. Maps to
// Status::Corruption. Such errors are unrecoverable.
type InvariantError struct {
	Err error
}

func (i InvariantError) Unwrap() error {
	return i.Err
}

func (i InvariantError) Error() string {
	return i.Err.Error()
}

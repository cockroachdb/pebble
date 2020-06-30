package errors

// InvariantError wraps errors due to internal constraint violations
// that lead to placing the DB in read only mode.
type InvariantError struct {
	Err error
}

// Unwrap the wrapped descriptive error that describes the constraint that got
// violated.
func (i InvariantError) Unwrap() error {
	return i.Err
}

func (i InvariantError) Error() string {
	return i.Err.Error()
}

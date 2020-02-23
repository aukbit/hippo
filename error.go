package hippo

// Params errors.
const (
	ErrParamsIDRequired = Error("parameter id required")
)

// Event errors.
const (
	ErrConcurrencyException = Error("concurrency exception")
	ErrEventFormatIsInvalid = Error("event data is not encoded in the right format")
)

// Error represents a HIPPO error.
type Error string

// Error returns the error message.
func (e Error) Error() string { return string(e) }

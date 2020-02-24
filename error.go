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

// Cache errors.
const (
	ErrKeyDoesNotExist          = Error("key does not exist")
	ErrVersionFieldDoesNotExist = Error("invalid aggregate key - version field does not exist")
	ErrStateFieldDoesNotExist   = Error("invalid aggregate key - state field does not exist")
)

// Error represents a HIPPO error.
type Error string

// Error returns the error message.
func (e Error) Error() string { return string(e) }

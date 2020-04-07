package hippo

// Params errors.
const (
	ErrParamsIDRequired = Error("parameter id required")
	ErrNotImplemented   = Error("feature not implemented")
)

// Event errors.
const (
	ErrAggregateIDCanNotBeEmpty = Error("aggregateID can not be empty")
	ErrBufferCanNotBeNil        = Error("buffer can not be nil")
	ErrFormatNotProvided        = Error("format is not provided")
	ErrConcurrencyException     = Error("concurrency exception")
	ErrAggregateIDWithoutEvents = Error("aggregateID without events")
	ErrEmptyState               = Error("aggregate with empty state")
	ErrInvalidEventFormat       = Error("event data is not encoded in the right format")
	ErrInvalidSchema            = Error("invalid schema schema to decode event data")
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

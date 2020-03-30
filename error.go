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
	ErrEventFormatIsInvalid     = Error("event data is not encoded in the right format")
	ErrSchemaProvidedIsInvalid  = Error("schema provided to decode event data is invalid")
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

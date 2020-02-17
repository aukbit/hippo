package hippo

import (
	"context"

	"github.com/golang/protobuf/proto"
)

// StoreService represents a service for managing an aggregate store.
type StoreService interface {
	Dispatch(ctx context.Context, msg Message, fns ...HookFn) (interface{}, error)
	Load(ctx context.Context, id string, fromVersion, toVersion int64) error
}

// Store represents an aggregator state at a specific version
type Store struct {
	State         interface{}
	Version       int64
	BusinessRules BusinessRulesFn
}

// State represents holds aggregator data to a specific version
type State struct {
	Data    interface{}
	Version int64
}

// Message to be dispatched and marshaled into an event
type Message struct {
	ID       string
	Topic    string
	Data     proto.Message
	Metadata map[string]string
}

// HookFn represents a function type that will be called after the store is loaded.
// Please note that the new event is not yet dispatched / persisted when the HookFn is called.
type HookFn func(*Store) error

// BusinessRulesFn represents a function type for business rules. Business rules define the ways
// the state of a specific aggregate change for a determine event.
type BusinessRulesFn func(e *Event, state interface{}) (interface{}, error)

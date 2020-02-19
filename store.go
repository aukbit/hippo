package hippo

import (
	"context"
)

// StoreService represents a service for managing an aggregate store.
type StoreService interface {
	CreateEvent(ctx context.Context, e *Event) error
	ListEvents(ctx context.Context, p Params) ([]*Event, error)
	Dispatch(ctx context.Context, msg Message, rules DomainRulesFn, hooks ...HookFn) (*Store, error)
}

// Message represents a ID and event to be dispatched
type Message struct {
	ID    string
	Event *Event
}

// Params represents parameters to load a store
type Params struct {
	// ID is required
	ID string
	// FromVersion (optional)
	FromVersion int64
	// ToVersion (optional)
	ToVersion int64
}

// Store represents an aggregator state at a specific version
type Store struct {
	State   interface{}
	Version int64
}

// Load takes a list of events and apply them to the store
func (s *Store) Load(events []*Event, rules DomainRulesFn) error {

	for _, e := range events {
		if err := s.Apply(e, rules); err != nil {
			return err
		}
	}

	return nil
}

// Apply applies changes to the current state based on the domain rules defined for the
// respective event topic
func (s *Store) Apply(e *Event, fn DomainRulesFn) error {
	n, err := fn(e, s.State)
	if err != nil {
		return err
	}
	// set changes to the state
	s.State = n
	// set state version the same as the aggregator
	s.Version = e.Version
	return nil
}

// HookFn represents a function type that will be called after the store is loaded.
// Please note that the new event is not yet dispatched / persisted when the HookFn is called.
type HookFn func(*Store) error

// DomainRulesFn represents a function type for domain rules. Domain rules define the ways
// the state of a specific aggregate change for a determine event.
type DomainRulesFn func(e *Event, state interface{}) (interface{}, error)

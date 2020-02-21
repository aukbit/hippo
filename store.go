package hippo

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

// HookFn represents a function type that will be called after the store is loaded.
// Please note that the new event is not yet dispatched / persisted when the HookFn is called.
type HookFn func(*Store) error

// DomainRulesFn represents a function type for domain rules. Domain rules define the ways
// the state of a specific aggregate change for a determine event.
type DomainRulesFn func(e *Event, state interface{}) (interface{}, error)

// Store represents an aggregator state at a specific version
type Store struct {
	State   interface{}
	Version int64
}

// load takes a list of events and apply them to the store
func (s *Store) load(events []*Event, rules DomainRulesFn) error {

	for _, e := range events {
		if err := s.apply(e, rules); err != nil {
			return err
		}
	}

	return nil
}

// apply applies changes to the current state based on the domain rules defined for the
// respective event topic
func (s *Store) apply(e *Event, fn DomainRulesFn) error {
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

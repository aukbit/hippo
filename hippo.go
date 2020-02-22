package hippo

import "context"

// StoreService represents a service for managing an aggregate store.
type StoreService interface {
	EventService() EventService
}

// Message represents ID and event to be dispatched
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
type HookFn func(*Aggregate) error

// DomainRulesFn represents a function type for domain rules. Domain rules define the ways
// the state of a specific aggregate change for a determine event.
type DomainRulesFn func(e *Event, state interface{}) (interface{}, error)

// Aggregate represents an aggregator state and respective version
type Aggregate struct {
	State   interface{}
	Version int64
}

// load takes a list of events and apply them to the aggregate
func (a *Aggregate) load(events []*Event, rules DomainRulesFn) error {

	for _, e := range events {
		if err := a.apply(e, rules); err != nil {
			return err
		}
	}

	return nil
}

// apply applies changes to the current state based on the domain rules defined for the
// respective event topic
func (a *Aggregate) apply(e *Event, fn DomainRulesFn) error {
	n, err := fn(e, a.State)
	if err != nil {
		return err
	}
	// set changes to the state
	a.State = n
	// set state version the same as the aggregator
	a.Version = e.Version
	return nil
}

// Client to hold store service implementation
type Client struct {
	store StoreService
}

// NewClient return client struct
func NewClient(s StoreService) *Client {
	return &Client{
		store: s,
	}
}

// Dispatch returns an aggregate based on a event message and domain rules
func (c *Client) Dispatch(ctx context.Context, msg Message,
	rules DomainRulesFn, hooks ...HookFn) (*Aggregate, error) {

	// Fetch events from datastore
	events, err := c.store.EventService().List(ctx, Params{ID: msg.ID})
	if err != nil {
		return nil, err
	}

	// Create new aggregate
	agg := &Aggregate{}

	// Load events into the aggregate
	if err := agg.load(events, rules); err != nil {
		return nil, err
	}

	// Run hooks
	for _, h := range hooks {
		if err := h(agg); err != nil {
			return nil, err
		}
	}

	// Do an optimistic concurrency test on the data coming in,
	// if the expected version does not match the actual aggregate version
	// it will raise a concurrency exception
	v, err := c.store.EventService().GetLastVersion(ctx, msg.ID)
	if err != nil {
		return nil, err
	}

	if agg.Version != v {
		return nil, ErrConcurrencyException
	}
	// Increment version by one and assign it to the new event
	msg.Event.SetVersion(agg.Version + 1)

	// Persist event to datastore
	if err := c.store.EventService().Create(ctx, msg.Event); err != nil {
		return nil, err
	}

	// Apply last event to the aggregator store
	if err := agg.apply(msg.Event, rules); err != nil {
		return nil, err
	}

	return agg, nil
}

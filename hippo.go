package hippo

import (
	"context"
)

// StoreService represents a service for managing an aggregate store.
type StoreService interface {
	EventService() EventService
}

// CacheService represents a service for managing cache.
type CacheService interface {
	Get(ctx context.Context, aggregateID string, out *Aggregate) error
	Set(ctx context.Context, aggregateID string, in *Aggregate) error
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

// Domain ..
type Domain struct {
	NextState Data
	Rules     DomainRulesFn
}

// DomainRulesFn represents a function type for domain rules. Domain rules define the ways
// the state of a specific aggregate change for a determine event.
type DomainRulesFn func(e *Event, currentState, nextState Data) error

// Data is a replica of proto.Message which is implemented by generated protocol buffer messages.
// We find using protocol buffers quite useful to marshal and unmarshal objects
type Data interface {
	Reset()
	String() string
	ProtoMessage()
}

// Aggregate represents an aggregator state and respective version
type Aggregate struct {
	State   Data
	Version int64
}

// load takes a list of events and apply them to the aggregate
func (a *Aggregate) load(events []*Event, domain Domain) error {

	for _, e := range events {
		if err := a.apply(e, domain); err != nil {
			return err
		}
	}

	return nil
}

// apply applies changes to the current state based on the domain rules defined for the
// respective event topic
func (a *Aggregate) apply(e *Event, domain Domain) error {
	if err := domain.Rules(e, a.State, domain.NextState); err != nil {
		return err
	}
	// set changes to the state
	a.State = domain.NextState
	// set state version the same as the aggregator
	a.Version = e.Version
	return nil
}

// Client to hold store service implementation
type Client struct {
	store StoreService
	cache CacheService
}

// NewClient return client struct
func NewClient(s StoreService) *Client {
	return &Client{
		store: s,
	}
}

// SetCacheService assigns a cache service to the client store
func (c *Client) SetCacheService(cache CacheService) {
	c.cache = cache
}

// Dispatch returns an aggregate based on a event message and domain rules
func (c *Client) Dispatch(ctx context.Context, msg Message, domain Domain, hooks ...HookFn) (*Aggregate, error) {

	// Fetch aggregate.
	agg, err := c.fetch(ctx, msg, domain)
	if err != nil {
		return nil, err
	}

	// Run hooks
	for _, h := range hooks {
		if err := h(agg); err != nil {
			return nil, err
		}
	}

	// Increment version by one and assign it to the new event
	msg.Event.SetVersion(agg.Version + 1)

	// Persist event to datastore
	if err := c.store.EventService().Create(ctx, msg.Event); err != nil {
		return nil, err
	}

	// Apply last event to the aggregator store
	if err := agg.apply(msg.Event, domain); err != nil {
		return nil, err
	}

	// If CacheService is defined store aggregate in cache.
	if c.cache != nil {
		if err := c.cache.Set(ctx, msg.ID, agg); err != nil {
			return nil, err
		}
	}

	return agg, nil
}

func (c *Client) fetch(ctx context.Context, msg Message, domain Domain) (*Aggregate, error) {

	// Get last aggregate version to do a optimistic concurrency test
	// on the data coming in.
	v, err := c.store.EventService().GetLastVersion(ctx, msg.ID)
	if err != nil {
		return nil, err
	}

	// If CacheService is defined check in Cache first.
	if c.cache != nil {
		agg := &Aggregate{
			State: domain.NextState,
		}
		if err := c.cache.Get(ctx, msg.ID, agg); err == nil {
			// Check if verssion from cache is the version expected.
			if agg.Version == v {
				return agg, nil
			}
		}
	}

	// Create new aggregate
	agg := &Aggregate{}

	// Fetch events from datastore
	events, err := c.store.EventService().List(ctx, Params{ID: msg.ID})
	if err != nil {
		return nil, err
	}

	// Load events into the aggregate
	if err := agg.load(events, domain); err != nil {
		return nil, err
	}

	// If the expected version does not match the actual aggregate version
	// it will raise a concurrency exception
	if agg.Version != v {
		return nil, ErrConcurrencyException
	}

	return agg, nil
}

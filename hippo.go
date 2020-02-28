package hippo

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
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
	Output interface{}
	Rules  DomainRulesFn
}

// DomainRulesFn represents a function type for domain rules.
// Domain rules define how the new data received changes the aggregate state.
type DomainRulesFn func(topic string, old, new interface{}) (next interface{})

// Data is a replica of proto.Message which is implemented by generated protocol buffer messages.
// We find using protocol buffers quite useful to marshal and unmarshal objects
type Data interface {
	Reset()
	String() string
	ProtoMessage()
}

// Aggregate represents an aggregator state and respective version
type Aggregate struct {
	State   interface{}
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

	switch e.Format {
	case PROTOBUF:
		if e.Schema != fmt.Sprintf("%T", domain.Output) {
			return ErrSchemaProvidedIsInvalid
		}
		if err := e.UnmarshalProto(domain.Output.(proto.Message)); err != nil {
			return err
		}
	case JSON:
		// TODO
		return ErrNotImplemented
	case STRING:
		// TODO
		return ErrNotImplemented
	}

	// run rules for the event topic and update aggregate state accordingly
	a.State = domain.Rules(e.Topic, a.State, domain.Output)
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

// Dispatch returns an aggregate resource based on the event and domain rules defined
func (c *Client) Dispatch(ctx context.Context, event *Event, domain Domain, hooks ...HookFn) (*Aggregate, error) {

	if event.AggregateID == "" {
		return nil, ErrAggregateIDCanNotBeEmpty
	}

	// Fetch aggregate.
	agg, err := c.fetch(ctx, event.AggregateID, domain)
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
	event.SetVersion(agg.Version + 1)

	// Persist event to datastore
	if err := c.store.EventService().Create(ctx, event); err != nil {
		return nil, err
	}

	// Apply last event to the aggregator store
	if err := agg.apply(event, domain); err != nil {
		return nil, err
	}

	// If CacheService is defined store aggregate in cache.
	if c.cache != nil {
		if err := c.cache.Set(ctx, event.AggregateID, agg); err != nil {
			return nil, err
		}
	}

	return agg, nil
}

func (c *Client) fetch(ctx context.Context, aggregateID string, domain Domain) (*Aggregate, error) {

	// Get last aggregate version to do a optimistic concurrency test
	// on the data coming in.
	v, err := c.store.EventService().GetLastVersion(ctx, aggregateID)
	if err != nil {
		return nil, err
	}

	// If CacheService is defined check in Cache first.
	if c.cache != nil {
		agg := &Aggregate{
			State: domain.Output,
		}
		if err := c.cache.Get(ctx, aggregateID, agg); err == nil {
			// Check if verssion from cache is the version expected.
			if agg.Version == v {
				return agg, nil
			}
		}
	}

	// Create new aggregate
	agg := &Aggregate{}

	// Fetch events from datastore
	events, err := c.store.EventService().List(ctx, Params{ID: aggregateID})
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

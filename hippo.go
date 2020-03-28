package hippo

import (
	"context"
	"fmt"
	"log"

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
	DB() interface{}
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

// DomainTypeRulesFn represents a function type to define how data in buffer could
// change the previous State into the next State for a specific topic.
type DomainTypeRulesFn func(topic string, buffer, previous interface{}) (next interface{})

// DomainTypeRulesMap a map from domain type names to map domain type rules function
type DomainTypeRulesMap map[string]DomainTypeRulesFn

// Version type.
type Version int64

// Aggregate represents an aggregator state and respective version
type Aggregate struct {
	State   interface{}
	Version int64
}

// load takes a list of events and apply them to the aggregate
func (a *Aggregate) load(events []*Event, buffer interface{}, fn DomainTypeRulesFn) error {

	for _, e := range events {
		if err := a.apply(e, buffer, fn); err != nil {
			return err
		}
	}

	return nil
}

// apply applies changes to the current state based on the domain rules defined for the
// respective event topic
func (a *Aggregate) apply(e *Event, buffer interface{}, fn DomainTypeRulesFn) error {
	switch e.Format {
	case PROTOBUF:
		if e.Schema != fmt.Sprintf("%T", buffer) {
			return ErrSchemaProvidedIsInvalid
		}
		if err := e.UnmarshalProto(buffer.(proto.Message)); err != nil {
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
	a.State = fn(e.Topic, buffer, a.State)
	// set state version the same as the aggregator
	a.Version = e.Version
	return nil
}

// Client to hold store service implementation
type Client struct {
	store         StoreService
	cache         CacheService
	rulesRegistry map[string]DomainTypeRulesFn // a map from domain type names to map functions
}

// NewClient return client struct
func NewClient(s StoreService) *Client {
	return &Client{
		store:         s,
		rulesRegistry: make(DomainTypeRulesMap),
	}
}

// RegisterDomainRules assigns a cache service to the client store
func (c *Client) RegisterDomainRules(fn DomainTypeRulesFn, domainType interface{}) {
	name := fmt.Sprintf("%T", domainType)
	if _, ok := c.rulesRegistry[name]; ok {
		log.Printf("duplicate domain type registered: %s", name)
		return
	}
	c.rulesRegistry[name] = fn
}

// Rules returns domain function rules for a specific domain type
// if domain types not previously registered log and return template function
func (c *Client) Rules(domainType interface{}) DomainTypeRulesFn {
	name := fmt.Sprintf("%T", domainType)
	if _, ok := c.rulesRegistry[name]; !ok {
		log.Printf("domain type NOT registered: %s", name)
		return func(topic string, buffer, previous interface{}) (next interface{}) { return buffer }
	}
	return c.rulesRegistry[name]
}

// RegisterCacheService assigns a cache service to the client store
func (c *Client) RegisterCacheService(cache CacheService) {
	c.cache = cache
}

// CacheService assigns a cache service to the client store
func (c *Client) CacheService() CacheService {
	return c.cache
}

// Dispatch returns an aggregate resource based on the event and domain rules defined
func (c *Client) Dispatch(ctx context.Context, event *Event, buffer interface{}, hooks ...HookFn) (*Aggregate, error) {

	if event.AggregateID == "" {
		return nil, ErrAggregateIDCanNotBeEmpty
	}

	// Fetch aggregate.
	agg, err := c.fetch(ctx, event.AggregateID, buffer)
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
	if err := agg.apply(event, buffer, c.Rules(buffer)); err != nil {
		return nil, err
	}

	// If CacheService is defined store aggregate in cache.
	if c.cache != nil {
		if err := c.cache.Set(ctx, event.AggregateID, agg); err != nil {
			return nil, err
		}
	}

	// Publish event to subscribers
	publish(event)

	return agg, nil
}

func (c *Client) fetch(ctx context.Context, aggregateID string, buffer interface{}) (*Aggregate, error) {

	// Get last aggregate version to do a optimistic concurrency test
	// on the data coming in.
	v, err := c.store.EventService().GetLastVersion(ctx, aggregateID)
	if err != nil {
		return nil, err
	}

	// If CacheService is defined check in Cache first.
	if c.cache != nil {
		agg := &Aggregate{
			State: buffer,
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
	if err := agg.load(events, buffer, c.Rules(buffer)); err != nil {
		return nil, err
	}

	// If the expected version does not match the actual aggregate version
	// it will raise a concurrency exception
	if agg.Version != v {
		return nil, ErrConcurrencyException
	}

	return agg, nil
}

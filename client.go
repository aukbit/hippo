package hippo

import (
	"context"
)

// Client represents a service for managing an aggregate store.
type Client interface {
	Dispatch(ctx context.Context, msg Message,
		rules DomainRulesFn, hooks ...HookFn) (*Store, error)
}

// Dispatch ..
func Dispatch(ctx context.Context, service EventService, msg Message,
	rules DomainRulesFn, hooks ...HookFn) (*Store, error) {

	// Fetch events from datastore
	events, err := service.List(ctx, Params{ID: msg.ID})
	if err != nil {
		return nil, err
	}

	// Create new Hippo store
	store := &Store{}

	// Load events into the store
	if err := store.load(events, rules); err != nil {
		return nil, err
	}

	// Run hooks
	for _, h := range hooks {
		if err := h(store); err != nil {
			return nil, err
		}
	}

	// Do an optimistic concurrency test on the data coming in,
	// if the expected version does not match the actual store version
	// it will raise a concurrency exception
	v, err := service.GetLastVersion(ctx, msg.ID)
	if err != nil {
		return nil, err
	}

	if store.Version != v {
		return nil, ErrConcurrencyException
	}
	// Increment version by one and assign it to the new event
	msg.Event.SetVersion(store.Version + 1)

	// Persist event to datastore
	if err := service.Create(ctx, msg.Event); err != nil {
		return nil, err
	}

	// Apply last event to the aggregator store
	if err := store.apply(msg.Event, rules); err != nil {
		return nil, err
	}

	return store, nil
}

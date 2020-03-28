package hippo

import "sync"

// Topic string name
type Topic string

type handler struct {
	topics map[Topic]bool
}

func (h *handler) valid(t Topic) bool {
	_, ok := h.topics[t]
	return ok
}

func (h *handler) set(t Topic) {
	h.topics[t] = true
}

func (h *handler) clear(t Topic) {
	delete(h.topics, t)
}

// handlers is a collection of events.
var handlers struct {
	sync.Mutex
	m   map[chan<- *Event]*handler
	ref map[Topic]int64
}

// Subscribe causes package pubsub to relay incoming events to c.
// If no events are provided, all incoming events will be relayed to c.
// Otherwise, just the provided events will.
//
// Package pubsub will not block sending to c: the caller must ensure
// that c has sufficient buffer space to keep up with the expected
// event rate. For a channel used for notification of just one event value,
// a buffer of size 1 is sufficient.
//
// It is allowed to call Subscribe multiple times with the same channel:
// each call expands the set of events sent to that channel.
//
// It is allowed to call Subscribe multiple times with different channels
// and the same events: each channel receives copies of incoming
// events independently.
func Subscribe(c chan<- *Event, topics ...Topic) {
	if c == nil {
		panic("pubsub: subscribe using nil channel")
	}

	handlers.Lock()
	defer handlers.Unlock()

	h, ok := handlers.m[c]
	if !ok {
		if handlers.m == nil {
			handlers.m = make(map[chan<- *Event]*handler)
		}
		if handlers.ref == nil {
			handlers.ref = make(map[Topic]int64)
		}
		h = &handler{make(map[Topic]bool)}
		handlers.m[c] = h
	}

	add := func(t Topic) {
		if t == "" {
			return
		}
		if !h.valid(t) {
			h.set(t)
			handlers.ref[t]++
		}
	}

	for _, t := range topics {
		add(t)
	}
}

// Publish publishes an event on the registered subscriber channels.
func publish(e *Event) {
	if e == nil || e.Topic == "" {
		return
	}

	handlers.Lock()
	defer handlers.Unlock()

	for c, h := range handlers.m {
		if h.valid(Topic(e.Topic)) {
			// send but do not block for it
			select {
			case c <- e:
			default:
			}
		}
	}
}

// Unsubscribe remove events from the map.
func Unsubscribe(c chan<- *Event) {
	handlers.Lock()
	defer handlers.Unlock()

	h, ok := handlers.m[c]
	if !ok {
		return
	}

	remove := func(t Topic) {
		if t == "" {
			return
		}
		if h.valid(t) {
			handlers.ref[t]--
			if handlers.ref[t] == 0 {
				delete(handlers.ref, t)
			}
			h.clear(t)
		}
	}

	for t := range h.topics {
		remove(t)
	}

	delete(handlers.m, c)
}

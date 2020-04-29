package hippo

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Topic string name
type Topic string

// ActionFn signature of an action func
type ActionFn func(context.Context, *Event) error

// ActionTopics map between a topic and respective action func
type ActionTopics map[Topic][]ActionFn

type handler struct {
	topics ActionTopics
}

func (h *handler) valid(t Topic) bool {
	_, ok := h.topics[t]
	return ok
}

func (h *handler) set(t Topic, a []ActionFn) {
	h.topics[t] = a
}

func (h *handler) get(t Topic) []ActionFn {
	a, ok := h.topics[t]
	if !ok {
		return []ActionFn{}
	}
	return a
}

func (h *handler) clear(t Topic) {
	delete(h.topics, t)
}

// handlers is a collection of events.
var handlers struct {
	sync.Mutex
	m   map[chan *Event]*handler
	ref map[Topic]int64
}

// Subscribe causes package pubsub to relay incoming events to c.
// If no events are provided, all incoming events will be relayed to c.
// Otherwise, just the provided events will.
//
// Package pubsub will block sending to c:
// For a channel used for notification of just one event value,
// a buffer of size 1 is sufficient.
//
// NOTE: Nice article that clear shows how to achieve delayed guaratee with a
// buffer of size 1
// https://www.ardanlabs.com/blog/2017/10/the-behavior-of-channels.html
//
// It is allowed to call Subscribe multiple times with the same channel:
// each call expands the set of events sent to that channel.
//
// It is allowed to call Subscribe multiple times with different channels
// and the same events: each channel receives copies of incoming
// events independently.
func Subscribe(c chan *Event, topics ActionTopics) {
	if c == nil {
		panic("pubsub: subscribe using nil channel")
	}

	handlers.Lock()
	defer handlers.Unlock()

	h, ok := handlers.m[c]
	if !ok {
		if handlers.m == nil {
			handlers.m = make(map[chan *Event]*handler)
		}
		if handlers.ref == nil {
			handlers.ref = make(map[Topic]int64)
		}
		h = &handler{make(ActionTopics)}
		handlers.m[c] = h
	}

	add := func(t Topic, a []ActionFn) {
		if t == "" {
			return
		}
		if !h.valid(t) {
			h.set(t, a)
			handlers.ref[t]++
		}
	}

	for t, actions := range topics {
		add(t, actions)
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
			// NOTE: block sending to c if buffer is full
			c <- e
		}
	}
}

// Unsubscribe remove events from the map.
func Unsubscribe(c chan *Event) {

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

// Worker waits for events from a subscribed channel and run respective action functions
func Worker(ctx context.Context, c chan *Event) {
	if c == nil {
		panic("pubsub: subscribe using nil channel")
	}

	h, ok := handlers.m[c]
	if !ok {
		return
	}

	//  Stop also in case of any host signal
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGTERM, syscall.SIGINT)
outer:
	for {
		select {
		case evt := <-c:
			if h.valid(evt.GetTopic()) {
				actions := h.get(evt.GetTopic())
				for _, a := range actions {
					err := a(ctx, evt)
					if err != nil {
						// TODO: retry running the func in an exponential way
						log.Printf("actionFn %v for evt %v failed > error %v", a, evt, err)
						continue
					}
				}
			}
		case <-sigch:
			Unsubscribe(c)
			break outer
		default:
			// keep on looping, non-blocking channel operations
		}
	}
}

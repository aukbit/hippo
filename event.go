package hippo

import (
	"time"
)

// EventService represents a service for managing events.
type EventService interface {
	CreateEvent(e *Event) error
	GetEvent(aggregateID string, version int64) (*Event, error)
	ListEvents(aggregateID string, lowestVersion, highestVersion int64) error
}

// Format enumerator
type Format int32

const (
	PROTOBUF Format = 0
	JSON     Format = 1
	STRING   Format = 2
)

// Event resource.
type Event struct {
	// Topic (name) of the event. These should be written in the past tense (event_created)
	Topic string
	// Aggregate ID is the primary key of the aggregate to which the event refers to.
	AggregateID string
	// Version of the aggregate, useful when using concurrency writes.
	Version int64
	// Schema of the aggregate.
	Schema string
	// Format of the encoded type of the aggregate data
	Format Format
	// Data raw object data.
	Data []byte
	// Priority of the event, where 0 is the highest priority.
	Priority int32
	// Signature includes SHA1 signature computed against it's contents and signature of the previous event.
	Signature string
	// Origin of the event. e.g. service name.
	OriginName string
	// Origin of the event. e.g. service ip address / browser.
	OriginIP string
	// Metadata
	Metadata map[string]string
	// Created has the identification of which service has created the event and
	// respective timestamp at which the event ocurred
	CreateTime time.Time
}

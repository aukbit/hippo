package hippo

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
)

// EventService represents a service for managing an aggregate store.
type EventService interface {
	Create(ctx context.Context, e *Event) error
	GetLastVersion(ctx context.Context, aggregateID string) (int64, error)
	List(ctx context.Context, p Params) ([]*Event, error)
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
	// Version of the aggregate, useful when using concurrency writes. (read-only)
	Version int64
	// Schema of the aggregate.
	Schema string
	// Format of the encoded type of the aggregate data
	Format Format
	// Data raw object data.
	Data []byte
	// Priority of the event, where 0 is the highest priority.
	Priority int32
	// TODO: Signature includes SHA1 signature computed against it's contents and signature
	// of the previous event. (not implemented yet)
	Signature string
	// TODO: Origin of the event. e.g. service name. (not implemented yet)
	OriginName string
	// TODO: Origin of the event. e.g. service ip address / browser. (not implemented yet)
	OriginIP string
	// Metadata
	Metadata map[string]string
	// CreateTime timestamp when event ocurred, location should be set to UTC.
	CreateTime time.Time
}

// GetTopic returns event topic casted as Topic
func (e *Event) GetTopic() Topic {
	return Topic(e.Topic)
}

// NewEvent returns an event resource.
func NewEvent(topic, aggregateID string) *Event {
	return &Event{
		Topic:       topic,
		AggregateID: aggregateID,
		CreateTime:  time.Now().UTC(),
	}
}

// NewEventProto returns an event resource.
func NewEventProto(topic, aggregateID string, data proto.Message) *Event {
	e := &Event{
		Topic:       topic,
		AggregateID: aggregateID,
		CreateTime:  time.Now().UTC(),
	}
	// Marshal data and assign it to event
	e.MarshalProto(data)
	return e
}

// NewEventWithMetadata returns an event resource.
func NewEventWithMetadata(topic, aggregateID string, metadata map[string]string) *Event {
	return &Event{
		Topic:       topic,
		AggregateID: aggregateID,
		Metadata:    metadata,
		CreateTime:  time.Now().UTC(),
	}
}

// MarshalProto takes a protocol buffer message
// and encodes it into the wire format.
// Also sets the event schema as the underlying proto message type and encoded format
func (e *Event) MarshalProto(pb proto.Message) error {
	// Encodes proto message
	data, err := proto.Marshal(pb)
	if err != nil {
		return err
	}
	e.Schema = fmt.Sprintf("%T", pb)
	e.Format = PROTOBUF
	e.Data = data
	return nil
}

// UnmarshalProto parses the protocol buffer representation in buf and places the
// decoded result in pb.  If the struct underlying pb does not match
// the data in buf, the results can be unpredictable.
func (e *Event) UnmarshalProto(pb proto.Message) error {
	if e.Format != PROTOBUF {
		return ErrEventFormatIsInvalid
	}
	err := proto.Unmarshal(e.Data, pb)
	if err != nil {
		return err
	}
	return nil
}

// Marshal data to event data
func (e *Event) Marshal(in interface{}) error {
	//
	switch e.Format {
	case PROTOBUF:
		if err := e.MarshalProto(in.(proto.Message)); err != nil {
			return err
		}
	case JSON:
		// TODO
		return ErrNotImplemented
	case STRING:
		// TODO
		return ErrNotImplemented
	default:
		return ErrFormatNotProvided
	}
	return nil
}

// Unmarshal event data to out
func (e *Event) Unmarshal(out interface{}) error {
	//
	switch e.Format {
	case PROTOBUF:
		if e.Schema != fmt.Sprintf("%T", out) {
			return ErrSchemaProvidedIsInvalid
		}
		if err := e.UnmarshalProto(out.(proto.Message)); err != nil {
			return err
		}
	case JSON:
		// TODO
		return ErrNotImplemented
	case STRING:
		// TODO
		return ErrNotImplemented
	default:
		return ErrFormatNotProvided
	}
	return nil
}

// SetVersion assign event version
func (e *Event) SetVersion(version int64) {
	e.Version = version
}

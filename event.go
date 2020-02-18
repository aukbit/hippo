package hippo

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
)

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

// NewEvent intantiates hippo event
func NewEvent(id, topic string, metadata map[string]string) *Event {
	return &Event{
		Topic:       topic,
		AggregateID: id,
		Metadata:    metadata,
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

// SetVersion set event version
func (e *Event) SetVersion(version int64) {
	e.Version = version
}

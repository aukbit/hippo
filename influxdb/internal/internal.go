package internal

import (
	"github.com/aukbit/hippo"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

// go:generate protoc --go_out=plugins=grpc,paths=source_relative:. influxdb/internal/internal.proto

func mapHippoToProto(e *hippo.Event) (*Event, error) {
	t, err := ptypes.TimestampProto(e.CreateTime)
	if err != nil {
		return nil, err
	}
	return &Event{
		Topic:       e.Topic,
		AggregateId: e.AggregateID,
		Version:     e.Version,
		Schema:      e.Schema,
		Format:      Format(e.Format),
		Data:        e.Data,
		Priority:    e.Priority,
		Signature:   e.Signature,
		OriginName:  e.OriginName,
		OriginIp:    e.OriginIP,
		Metadata:    e.Metadata,
		CreateTime:  t,
	}, nil
}

func mapProtoToHippo(pb *Event, e *hippo.Event) error {
	t, err := ptypes.Timestamp(pb.GetCreateTime())
	if err != nil {
		return err
	}
	e.Topic = pb.GetTopic()
	e.AggregateID = pb.GetAggregateId()
	e.Version = pb.GetVersion()
	e.Schema = pb.GetSchema()
	e.Format = hippo.Format(pb.GetFormat())
	e.Data = pb.GetData()
	e.Priority = pb.GetPriority()
	e.Signature = pb.GetSignature()
	e.OriginName = pb.GetOriginName()
	e.OriginIP = pb.GetOriginIp()
	e.Metadata = pb.GetMetadata()
	e.CreateTime = t
	return nil
}

// MarshalEvent encodes a event to binary format.
func MarshalEvent(e *hippo.Event) ([]byte, error) {
	m, err := mapHippoToProto(e)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(m)
}

// UnmarshalEvent decodes a event from a binary data.
func UnmarshalEvent(data []byte, e *hippo.Event) error {
	var pb Event
	if err := proto.Unmarshal(data, &pb); err != nil {
		return err
	}
	return mapProtoToHippo(&pb, e)
}

// MarshalEventText encodes a event to compact string format.
func MarshalEventText(e *hippo.Event) (string, error) {
	m, err := mapHippoToProto(e)
	if err != nil {
		return "", err
	}
	return proto.CompactTextString(m), nil
}

// UnmarshalEventText decodes a event from a string format.
func UnmarshalEventText(s string, e *hippo.Event) error {
	var pb Event
	if err := proto.UnmarshalText(s, &pb); err != nil {
		return err
	}
	return mapProtoToHippo(&pb, e)
}

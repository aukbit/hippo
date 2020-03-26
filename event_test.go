package hippo

import (
	"testing"

	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
	"github.com/golang/protobuf/proto"
)

// Ensure event data can be marshaled and unmarshaled.
func TestEvent_MarshalProto(t *testing.T) {

	// Define proto message
	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	event := NewEvent("event_created", "123ABC")

	var other pb.User
	if err := event.MarshalProto(&user); err != nil {
		t.Fatal(err)
	} else if err := event.UnmarshalProto(&other); err != nil {
		t.Fatal(err)
	} else if !proto.Equal(&user, &other) {
		t.Fatalf("unexpected copy: %#v != %#v", user, other)
	}
}

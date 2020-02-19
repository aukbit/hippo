package influxdb_test

import (
	"context"
	"testing"

	"github.com/aukbit/hippo"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
)

// Ensure event can be created.
func TestStoreService_CreateEvent(t *testing.T) {
	c := MustConnectClient()
	defer c.Close()

	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	event := hippo.NewEvent("event_created", user.GetId(), nil)
	if err := event.MarshalProto(&user); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	s := c.StoreService()

	// Create new event.
	if err := s.CreateEvent(ctx, event); err != nil {
		t.Fatal(err)
	}

}

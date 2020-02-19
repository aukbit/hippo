package influxdb_test

import (
	"context"
	"testing"

	"github.com/aukbit/hippo"
	pb "github.com/aukbit/hippo/influxdb/test/proto"
	"github.com/aukbit/rand"
)

// Ensure event can be created.
func TestStoreService_CreateEvent(t *testing.T) {
	c := MustConnectClient()
	defer c.Close()
	s := c.StoreService()

	user := &pb.User{
		Id:    "123",
		Name:  "test",
		Email: "test@email.com",
	}
	event := hippo.NewEvent(rand.String(10), "event_created", nil)
	if err := event.MarshalProto(user); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Create new event.
	if err := s.CreateEvent(ctx, event); err != nil {
		t.Fatal(err)
	}

}

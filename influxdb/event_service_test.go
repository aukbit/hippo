package influxdb_test

import (
	"context"
	"testing"

	"github.com/aukbit/hippo"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
)

// Ensure event can be created.
func TestEventService_Create(t *testing.T) {
	c := MustConnectStore()
	defer c.Close()

	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	// Create new event for user_created topic.
	event := hippo.NewEvent("user_created", user.GetId())
	// Marshal user proto and assign it to event data
	if err := event.MarshalProto(&user); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create event in store.
	if err := c.EventService().Create(ctx, event); err != nil {
		t.Fatal(err)
	}

}

func TestEventService_GetLastVersion(t *testing.T) {
	c := MustConnectStore()
	defer c.Close()

	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	// Create new event for user_created topic.
	event := hippo.NewEvent("user_created", user.GetId())
	// Marshal user proto and assign it to event data
	if err := event.MarshalProto(&user); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create event in store.
	if err := c.EventService().Create(ctx, event); err != nil {
		t.Fatal(err)
	}

	// Get last event version from store.
	if n, err := c.EventService().GetLastVersion(ctx, user.GetId()); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Fatalf("unexpected version: %#v != 0", n)
	}

}

func TestEventService_ListEvents(t *testing.T) {
	c := MustConnectStore()
	defer c.Close()

	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	// Create new event for user_created topic.
	ev1 := hippo.NewEvent("user_created", user.GetId())
	// Marshal user proto and assign it to event data
	if err := ev1.MarshalProto(&user); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Create event 1 in store.
	if err := c.EventService().Create(ctx, ev1); err != nil {
		t.Fatal(err)
	}

	// Update user details.
	user.Name = "my name changed to something else"
	// Create new event for user_created topic.
	ev2 := hippo.NewEvent("user_updated", user.GetId())
	// Marshal user proto and assign it to event data
	if err := ev2.MarshalProto(&user); err != nil {
		t.Fatal(err)
	}
	// Increase event aggregate version to avoid concurrency exception
	ev2.Version = 1

	// Create event 2 in store.
	if err := c.EventService().Create(ctx, ev2); err != nil {
		t.Fatal(err)
	}

	// Define query parameters
	p := hippo.Params{
		ID: user.GetId(),
	}
	// List events
	if events, err := c.EventService().List(ctx, p); err != nil {
		t.Fatal(err)
	} else if len(events) != 2 {
		t.Fatalf("unexpected number of events: %#v != 2", len(events))
	}

}

package influxdb_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/influxdb"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
)

// Client is a test wrapper for influxdb.Client.
type Client struct {
	*influxdb.Client
}

// NewClient returns a new instance of Client
func NewClient() *Client {

	// Create client wrapper.
	c := &Client{
		Client: influxdb.NewClient(),
	}
	return c
}

// MustConnectClient returns a new and available Client.
func MustConnectClient() *Client {
	c := NewClient()
	if err := c.Connect(influxdb.Config{
		Database: "hippo_db_test",
	}); err != nil {
		panic(err)
	}
	return c
}

// Close closes the client and removes the underlying database.
func (c *Client) Close() error {
	return c.Client.Close()
}

func TestClient_Dispatch(t *testing.T) {
	c := MustConnectClient()
	defer c.Close()

	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	// Create new event for user_created topic.
	ev1 := hippo.NewEvent("user_created", user.GetId(), nil)
	// Marshal user proto and assign it to event data
	if err := ev1.MarshalProto(&user); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	msg := hippo.Message{
		ID:    user.GetId(),
		Event: ev1,
	}

	rules := func(e *hippo.Event, state interface{}) (interface{}, error) {
		n := pb.User{}
		if e.Schema == fmt.Sprintf("%T", &pb.User{}) {
			if err := e.UnmarshalProto(&n); err != nil {
				return nil, err
			}
		}
		switch e.Topic {
		default:
			return state, nil
		case "user_created":
			return &n, nil
		}
		return state, nil
	}

	// Create event 1 in store.
	if store, err := c.Dispatch(ctx, msg, rules); err != nil {
		t.Fatal(err)
	} else if _, ok := store.State.(*pb.User); !ok {
		t.Fatalf("unexpected store state type: %T ", store.State)
	} else if store.Version != 1 {
		t.Fatalf("unexpected store version: %d ", store.Version)
	}

}

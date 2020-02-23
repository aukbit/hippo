package hippo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/mock"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
)

func TestStore_Dispatch(t *testing.T) {

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

	msg := hippo.Message{
		ID:    user.GetId(),
		Event: ev1,
	}

	var ss mock.StoreService
	var es mock.EventService

	// Mock our EventService() call.
	ss.EventServiceFn = func() *mock.EventService {
		return &es
	}

	// Mock our List()
	es.ListFn = func(ctx context.Context, p hippo.Params) ([]*hippo.Event, error) {
		return []*hippo.Event{ev1}, nil
	}

	// Mock our GetLastVersion()
	es.GetLastVersionFn = func(ctx context.Context, aggregateID string) (int64, error) {
		return 0, nil
	}

	// Mock our GetLastVersion()
	es.CreateFn = func(ctx context.Context, e *hippo.Event) error {
		return nil
	}

	clt := hippo.NewClient(&ss)

	ctx := context.Background()

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
	}

	// Create event 1 in store.
	if store, err := clt.Dispatch(ctx, msg, rules); err != nil {
		t.Fatal(err)
	} else if !ss.EventServiceInvoked {
		t.Fatal("expected EventService() to be invoked")
	} else if !es.CreateInvoked {
		t.Fatal("expected Create() to be invoked")
	} else if !es.GetLastVersionInvoked {
		t.Fatal("expected GetLastVersion() to be invoked")
	} else if !es.ListInvoked {
		t.Fatal("expected List() to be invoked")
	} else if _, ok := store.State.(*pb.User); !ok {
		t.Fatalf("unexpected store state type: %T ", store.State)
	} else if store.Version != 1 {
		t.Fatalf("unexpected store version: %d ", store.Version)
	}

}

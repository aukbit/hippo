package hippo_test

import (
	"context"
	"testing"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/mock"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
	"github.com/paulormart/assert"
)

func TestStore_Dispatch(t *testing.T) {

	user := pb.User{
		Id:    rand.String(10),
		Name:  "Luke",
		Email: "luke@email.com",
	}

	// Create new event for user_created topic.
	ev1 := hippo.NewEvent("user_created", user.GetId())
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

	// Mock StoreService.EventService() call.
	ss.EventServiceFn = func() *mock.EventService {
		return &es
	}

	// Mock EventService.List()
	es.ListFn = func(ctx context.Context, p hippo.Params) ([]*hippo.Event, error) {
		return []*hippo.Event{ev1}, nil
	}

	// Mock EventService.GetLastVersion()
	es.GetLastVersionFn = func(ctx context.Context, aggregateID string) (int64, error) {
		return 0, nil
	}

	// Mock EventService.GetLastVersion()
	es.CreateFn = func(ctx context.Context, e *hippo.Event) error {
		return nil
	}

	clt := hippo.NewClient(&ss)

	ctx := context.Background()

	rules := func(topic string, state, new interface{}) interface{} {
		switch topic {
		default:
			return state
		case "user_created":
			return new
		}
	}

	var next pb.User
	domain := hippo.Domain{Output: &next, Rules: rules}

	// Create event 1 in store.
	if store, err := clt.Dispatch(ctx, msg, domain); err != nil {
		t.Fatal(err)
	} else if _, ok := store.State.(*pb.User); !ok {
		t.Fatalf("unexpected store state type: %T ", store.State)
	} else if store.Version != 1 {
		t.Fatalf("unexpected store version: %d ", store.Version)
	}

	assert.Equal(t, true, ss.EventServiceInvoked)
	assert.Equal(t, true, es.CreateInvoked)
	assert.Equal(t, true, es.GetLastVersionInvoked)
	assert.Equal(t, true, es.ListInvoked)

}

func TestStoreWithCache_Dispatch(t *testing.T) {

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

	msg := hippo.Message{
		ID:    user.GetId(),
		Event: ev1,
	}

	var ss mock.StoreService
	var es mock.EventService
	var cs mock.CacheService

	// Mock StoreService.EventService() call.
	ss.EventServiceFn = func() *mock.EventService {
		return &es
	}

	// Mock EventService.GetLastVersion()
	es.GetLastVersionFn = func(ctx context.Context, aggregateID string) (int64, error) {
		return 0, nil
	}

	// Mock EventService.GetLastVersion()
	es.CreateFn = func(ctx context.Context, e *hippo.Event) error {
		return nil
	}

	// Mock CacheService.Get() call.
	cs.GetFn = func(ctx context.Context, aggregateID string, out *hippo.Aggregate) error {
		return nil
	}

	cs.SetFn = func(ctx context.Context, aggregateID string, in *hippo.Aggregate) error {
		return nil
	}

	clt := hippo.NewClient(&ss)

	//
	clt.SetCacheService(&cs)

	ctx := context.Background()

	rules := func(topic string, old, new interface{}) interface{} {
		o := old.(*pb.User)
		switch topic {
		default:
			return old
		case "user_created":
			return new
		case "user_updated":
			n := new.(*pb.User)
			if n.GetName() != "" && o.GetName() != n.GetName() {
				o.Name = n.GetName()
			}
			return o
		}
	}

	domain := hippo.Domain{Output: &pb.User{}, Rules: rules}

	// Create event 1 in store.
	if store, err := clt.Dispatch(ctx, msg, domain); err != nil {
		t.Fatal(err)
	} else if _, ok := store.State.(*pb.User); !ok {
		t.Fatalf("unexpected store state type: %T ", store.State)
	} else if store.Version != 1 {
		t.Fatalf("unexpected store version: %d ", store.Version)
	}

	assert.Equal(t, true, ss.EventServiceInvoked)
	assert.Equal(t, true, cs.GetInvoked)
	assert.Equal(t, true, cs.SetInvoked)
	assert.Equal(t, true, es.CreateInvoked)
	assert.Equal(t, true, es.GetLastVersionInvoked)

}

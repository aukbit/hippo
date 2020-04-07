package hippo_test

import (
	"context"
	"sync"
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
	ev1 := hippo.NewEventProto("user_created", user.GetId(), &user)

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

	// Domain Type Rules
	rules := func(topic string, buffer, previous interface{}) (next interface{}) {
		switch topic {
		default:
			return previous
		case "user_created":
			return buffer
		}
	}

	clt := hippo.NewClient(&ss)
	clt.RegisterDomainRules(rules, &pb.User{})

	ctx := context.Background()

	// Create event 1 in store.
	if store, err := clt.Dispatch(ctx, ev1, &user); err != nil {
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

	// Create event 2 for nothing_changed topic.
	user2 := pb.User{
		Id: user.GetId(),
	}
	ev2 := hippo.NewEventProto("nothing_changed", user.GetId(), &user2)

	// Mock EventService.List()
	es.ListFn = func(ctx context.Context, p hippo.Params) ([]*hippo.Event, error) {
		return []*hippo.Event{ev1, ev2}, nil
	}

	// Create event 2 in store.
	if store, err := clt.Dispatch(ctx, ev2, &user2); err != nil {
		t.Fatal(err)
	} else if u, ok := store.State.(*pb.User); !ok {
		t.Fatalf("unexpected store state type: %T ", store.State)
	} else if u.GetId() != user.GetId() {
		t.Fatalf("unexpected id %v ", u.GetId())
	} else if u.GetName() != user.GetName() {
		t.Fatalf("unexpected name %v ", u.GetName())
	} else if u.GetEmail() != user.GetEmail() {
		t.Fatalf("unexpected email %v ", u.GetEmail())
	}
}

func TestStore_FetchNoEvents(t *testing.T) {

	var ss mock.StoreService
	var es mock.EventService

	// Mock StoreService.EventService() call.
	ss.EventServiceFn = func() *mock.EventService {
		return &es
	}

	// Mock EventService.List()
	es.ListFn = func(ctx context.Context, p hippo.Params) ([]*hippo.Event, error) {
		return []*hippo.Event{}, nil
	}

	// Mock EventService.GetLastVersion()
	es.GetLastVersionFn = func(ctx context.Context, aggregateID string) (int64, error) {
		return 0, nil
	}

	// Mock EventService.GetLastVersion()
	es.CreateFn = func(ctx context.Context, e *hippo.Event) error {
		return nil
	}

	// Domain Type Rules
	rules := func(topic string, buffer, previous interface{}) (next interface{}) {
		switch topic {
		default:
			return previous
		case "user_created":
			return buffer
		}
	}

	clt := hippo.NewClient(&ss)
	clt.RegisterDomainRules(rules, &pb.User{})

	ctx := context.Background()

	_, err := clt.Fetch(ctx, "someid", &pb.User{})
	assert.Equal(t, hippo.ErrNoEventsToBuildState, err)

}

func TestStore_WithCache(t *testing.T) {

	user := pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	// Create new event for user_created topic.
	ev1 := hippo.NewEventProto("user_created", user.GetId(), &user)

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

	// Domain Type Rules
	rules := func(topic string, buffer, previous interface{}) (next interface{}) {
		p := previous.(*pb.User)
		switch topic {
		default:
			return previous
		case "user_created":
			return buffer
		case "user_updated":
			b := buffer.(*pb.User)
			if b.GetName() != "" && p.GetName() != b.GetName() {
				p.Name = b.GetName()
			}
			return p
		}
	}

	clt := hippo.NewClient(&ss)
	clt.RegisterDomainRules(rules, &pb.User{})

	//
	clt.RegisterCacheService(&cs)

	ctx := context.Background()

	// Create event 1 in store.
	if store, err := clt.Dispatch(ctx, ev1, &pb.User{}); err != nil {
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

func TestStore_WithSubscribers(t *testing.T) {
	wg := &sync.WaitGroup{}
	user := pb.User{
		Id:    rand.String(10),
		Name:  "Luke",
		Email: "luke@email.com",
	}

	// Create new event for user_created topic.
	ev1 := hippo.NewEventProto("user_created", user.GetId(), &user)

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

	// Domain Type Rules
	rules := func(topic string, buffer, previous interface{}) (next interface{}) {
		switch topic {
		default:
			return previous
		case "user_created":
			return buffer
		}
	}

	clt := hippo.NewClient(&ss)
	clt.RegisterDomainRules(rules, &pb.User{})

	ctx := context.Background()

	// Subscribe
	wg.Add(1)
	c1 := make(chan *hippo.Event, 1)
	hippo.Subscribe(c1, ev1.GetTopic())
	go func() {
		defer wg.Done()
		ev1 := <-c1
		assert.Equal(t, "user_created", ev1.Topic)
	}()

	// Create event 1 in store.
	if store, err := clt.Dispatch(ctx, ev1, &pb.User{}); err != nil {
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
	// Wait and unsubscribe
	wg.Wait()
	hippo.Unsubscribe(c1)
}

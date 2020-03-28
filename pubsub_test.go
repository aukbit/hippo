package hippo

import (
	"sync"
	"testing"

	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
	"github.com/paulormart/assert"
)

func TestPubSub_MultiEventsSingleChannel(t *testing.T) {
	u1 := pb.User{
		Id:    rand.String(10),
		Name:  "Luke",
		Email: "luke@email.com",
	}
	ev1 := NewEventProto("user_created", u1.GetId(), &u1)
	u1.Name = "Luke Skywalker"
	ev2 := NewEventProto("user_updated", u1.GetId(), &u1)

	wg := &sync.WaitGroup{}
	c1 := make(chan *Event, 1)
	assert.Equal(t, 0, len(handlers.m))
	assert.Equal(t, 0, len(handlers.ref))
	Subscribe(c1, "user_created")
	assert.Equal(t, 1, len(handlers.m))
	assert.Equal(t, int64(1), handlers.ref[ev1.GetTopic()])
	assert.Equal(t, int64(0), handlers.ref[ev2.GetTopic()])
	wg.Add(1)
	go func() {
		defer wg.Done()
		evc1 := <-c1
		assert.Equal(t, true, evc1.GetTopic() == "user_created")
	}()
	publish(ev1)
	wg.Wait()
	Unsubscribe(c1)
	assert.Equal(t, 0, len(handlers.m))
	assert.Equal(t, 0, len(handlers.ref))
}

func TestPubSub_MultiEventsDifferentChannels(t *testing.T) {
	u1 := pb.User{
		Id:    rand.String(10),
		Name:  "Luke",
		Email: "luke@email.com",
	}
	ev1 := NewEventProto("user_created", u1.GetId(), &u1)
	u1.Name = "Luke Skywalker"
	ev2 := NewEventProto("user_updated", u1.GetId(), &u1)

	wg := &sync.WaitGroup{}
	c1 := make(chan *Event, 1)
	c2 := make(chan *Event, 1)
	Subscribe(c1, ev1.GetTopic())
	assert.Equal(t, 1, len(handlers.m))
	assert.Equal(t, int64(1), handlers.ref[ev1.GetTopic()])
	assert.Equal(t, int64(0), handlers.ref[ev2.GetTopic()])
	Subscribe(c2, ev2.GetTopic())
	assert.Equal(t, 2, len(handlers.m))
	assert.Equal(t, int64(1), handlers.ref[ev1.GetTopic()])
	assert.Equal(t, int64(1), handlers.ref[ev2.GetTopic()])
	wg.Add(2)
	go func() {
		defer wg.Done()
		ev1 := <-c1
		assert.Equal(t, "user_created", ev1.Topic)
	}()
	go func() {
		defer wg.Done()
		ev2 := <-c2
		assert.Equal(t, "user_updated", ev2.Topic)
	}()
	// Publish
	publish(ev1)
	publish(ev2)
	wg.Wait()
	// Unsubscribe
	Unsubscribe(c1)
	assert.Equal(t, int64(0), handlers.ref[ev1.GetTopic()])
	assert.Equal(t, int64(1), handlers.ref[ev2.GetTopic()])
	Unsubscribe(c2)
	assert.Equal(t, int64(0), handlers.ref[ev2.GetTopic()])
}

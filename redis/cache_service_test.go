package redis_test

import (
	"context"
	"testing"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/redis"
	pb "github.com/aukbit/hippo/test/proto"
	"github.com/aukbit/rand"
	"github.com/paulormart/assert"
)

// Cache is a test wrapper for redis.CacheService.
type Cache struct {
	*redis.CacheService
}

// NewCache returns a new instance of Cache
func NewCache() *Cache {

	// Create cache wrapper.
	s := &Cache{
		CacheService: redis.NewCacheService(),
	}
	return s
}

// MustLinkCache returns a new and available Store.
func MustLinkCache() *Cache {
	s := NewCache()
	if err := s.CacheService.Link(redis.Config{
		Database: 5,
	}); err != nil {
		panic(err)
	}
	return s
}

// Close closes the store connection.
func (s *Cache) Close() error {
	return s.CacheService.Close()
}

// Ensure event can be created.
func TestCacheService_Set(t *testing.T) {
	c := MustLinkCache()
	defer c.Close()

	user := &pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	in := &hippo.Aggregate{
		State:   user,
		Version: 1,
	}

	ctx := context.Background()

	// Set aggregator in cache.
	if err := c.Set(ctx, user.GetId(), in); err != nil {
		t.Fatal(err)
	}

}

func TestCacheService_Get(t *testing.T) {
	c := MustLinkCache()
	defer c.Close()

	user := &pb.User{
		Id:    rand.String(10),
		Name:  "test",
		Email: "test@email.com",
	}

	in := &hippo.Aggregate{
		State:   user,
		Version: 1,
	}

	ctx := context.Background()

	// Set aggregator in cache.
	if err := c.Set(ctx, user.GetId(), in); err != nil {
		t.Fatal(err)
	}

	out := &hippo.Aggregate{
		State: &pb.User{},
	}

	// Get aggregator from cache.
	if err := c.Get(ctx, user.GetId(), out); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, user.GetId(), out.State.(*pb.User).GetId())
	assert.Equal(t, user.GetName(), out.State.(*pb.User).GetName())
	assert.Equal(t, user.GetEmail(), out.State.(*pb.User).GetEmail())
	assert.Equal(t, int64(1), out.Version)
	assert.Equal(t, in, out)

}

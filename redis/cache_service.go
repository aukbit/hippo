package redis

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aukbit/hippo"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
)

var _ hippo.CacheService = &CacheService{}

// CacheService holds aggregates in Redis.
type CacheService struct {

	// client to connect to Redis
	db *redis.Client
}

// Config represents a configuration to initialize a new Redis
type Config struct {
	// Addr should be of the form "host:port", defaults to "localhost:6379"
	Addr string

	// Password is the Redis password, optional.
	Password string

	// Database to be used in Redis, defaults to 0
	Database int
}

// NewCacheService creates a new CacheService
func NewCacheService() *CacheService {
	return &CacheService{}
}

// Link connects and pings the Redis database.
func (s *CacheService) Link(conf Config) error {
	if conf.Addr == "" {
		conf.Addr = "localhost:6379"
	}

	opt := &redis.Options{
		Addr:     conf.Addr,
		DB:       conf.Database,
		Password: conf.Password,
	}

	// Initialize Redis client
	clt := redis.NewClient(opt)
	s.db = clt

	// Ping checks Redis status
	cmd := s.db.Ping()
	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

// Close closes then underlying Redis database.
func (s *CacheService) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Get returns aggregate data for the respective aggregateID.
// State is unmarshaled to it's original form.
func (s *CacheService) Get(ctx context.Context, aggregateID string, out *hippo.Aggregate) error {

	// Get version and state fields from aggregate hash key in Redis.
	cmd := s.db.HGetAll(aggregateID)

	if cmd.Err() != nil && cmd.Err() == redis.Nil {
		return hippo.ErrKeyDoesNotExist
	} else if cmd.Err() != nil {
		return cmd.Err()
	}

	if v, ok := cmd.Val()["version"]; !ok {
		return hippo.ErrVersionFieldDoesNotExist
	} else if v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err != nil {
			return err
		} else if n != 0 {
			out.Version = n
		}
	}

	if s, ok := cmd.Val()["state"]; !ok {
		return hippo.ErrStateFieldDoesNotExist
	} else if s != "" {
		// Decodes data string into a proto message
		if err := proto.UnmarshalText(s, out.State.(proto.Message)); err != nil {
			return err
		}
	}

	return nil
}

// Set stores aggregate has a form of hash key in Redis. Aggregate State is marshaled into string.
func (s *CacheService) Set(ctx context.Context, aggregateID string, in *hippo.Aggregate) error {

	fmt.Printf("%v \n", in)

	fields := make(map[string]interface{})
	fields["version"] = strconv.FormatInt(in.Version, 10)
	fields["schema"] = fmt.Sprintf("%T", in.State)
	fields["state"] = proto.CompactTextString(in.State.(proto.Message))

	if cmd := s.db.HMSet(aggregateID, fields); cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

package influxdb

import (
	"context"

	"github.com/aukbit/hippo"
)

// Ensure StoreService implements hippo.StoreService.
var _ hippo.StoreService = &StoreService{}

type StoreService struct {
	client *Client
}

func (s *StoreService) Dispatch(ctx context.Context, msg hippo.Message, fns ...hippo.HookFn) (interface{}, error) {
	return nil, nil
}

func (s *StoreService) Load(ctx context.Context, id string, fromVersion, toVersion int64) error {
	return nil
}

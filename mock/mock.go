package mock

import (
	"context"

	"github.com/aukbit/hippo"
)

type EventService struct {
	CreateFn      func(ctx context.Context, e *hippo.Event) error
	CreateInvoked bool

	GetLastVersionFn      func(ctx context.Context, aggregateID string) (int64, error)
	GetLastVersionInvoked bool

	ListFn      func(ctx context.Context, p hippo.Params) ([]*hippo.Event, error)
	ListInvoked bool
}

func (s *EventService) Create(ctx context.Context, e *hippo.Event) error {
	s.CreateInvoked = true
	return s.CreateFn(ctx, e)
}
func (s *EventService) GetLastVersion(ctx context.Context, aggregateID string) (int64, error) {
	s.GetLastVersionInvoked = true
	return s.GetLastVersionFn(ctx, aggregateID)
}
func (s *EventService) List(ctx context.Context, p hippo.Params) ([]*hippo.Event, error) {
	s.ListInvoked = true
	return s.ListFn(ctx, p)
}

type StoreService struct {
	EventServiceFn      func() *EventService
	EventServiceInvoked bool
}

func (s *StoreService) EventService() hippo.EventService {
	s.EventServiceInvoked = true
	return s.EventServiceFn()
}

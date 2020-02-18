package influxdb

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/aukbit/hippo"
	"github.com/aukbit/hippo/influxdb/internal"
)

// Ensure StoreService implements hippo.StoreService.
var _ hippo.StoreService = &StoreService{}

// StoreService represents a service for managing an aggregate store
// with a InfluxDB client connected.
type StoreService struct {
	client *Client
}

// Dispatch ...
func (s *StoreService) Dispatch(ctx context.Context, msg hippo.Message, rules hippo.DomainRulesFn,
	hooks ...hippo.HookFn) (*hippo.Store, error) {

	// Fetch events from InfluxDB
	events, err := s.ListEvents(ctx, hippo.Params{ID: msg.ID})
	if err != nil {
		return nil, err
	}

	// Create new Hippo store
	store := &hippo.Store{}

	// Load events into the store
	if err := store.Load(events, rules); err != nil {
		return nil, err
	}

	// Run hooks
	for _, h := range hooks {
		if err := h(store); err != nil {
			return nil, err
		}
	}

	// Persist event into InfluxDB
	if err := s.CreateEvent(ctx, msg.Event); err != nil {
		return nil, err
	}

	// Apply last event to the aggregator store
	if err := store.Apply(msg.Event, rules); err != nil {
		return nil, err
	}

	return store, nil
}

// CreateEvent ..
func (s *StoreService) CreateEvent(ctx context.Context, e *hippo.Event) error {
	start := time.Now()

	v, err := s.fetchLastVersion(ctx, e.AggregateID)
	if err != nil {
		return err
	}

	// Do an optimistic concurrency test on the data coming in,
	// if the expected version does not match the actual version it will raise a concurrency exception
	if e.Version != v {
		return hippo.ErrConcurrencyException
	}
	// Increment aggregate version by one.
	e.Version++

	// NOTE: on migration events already bring timestamp otherwise timestamp
	// should be set here
	// if e.CreateTime == nil {
	// 	e.CreateTime = ptypes.TimestampNow()
	// }

	// Create a new batch points
	bp, err := s.client.BatchPoints()
	if err != nil {
		return err
	}

	// Create a point and add to batch
	tags := map[string]string{
		"aggregate_id": e.AggregateID,
		"topic":        e.Topic,
	}

	// Encode event
	data, err := internal.MarshalEvent(e)
	if err != nil {
		return err
	}
	fields := map[string]interface{}{
		"data":    data,
		"version": e.Version,
	}
	pt, err := s.client.NewPoint("events", tags, fields, e.CreateTime)
	if err != nil {
		return err
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := s.client.db.Write(bp); err != nil {
		return err
	}

	log.Printf("event %s with aggregate %s version %d created - duration: %v", e.Topic, e.AggregateID, e.Version, time.Now().Sub(start))
	return nil
}

func (s *StoreService) fetchLastVersion(ctx context.Context, id string) (int64, error) {
	cmd := fmt.Sprintf("select last(version) from events where aggregate_id='%s'", id)
	response, err := s.client.db.Query(s.client.Query(cmd))
	if err != nil {
		return 0, err
	}
	for _, res := range response.Results {
		for _, ser := range res.Series {
			for _, val := range ser.Values {
				n, err := val[1].(json.Number).Int64()
				if err != nil {
					return 0, err
				}
				log.Printf("%s --> %d", cmd, n)
				return n, nil
			}
		}
	}
	return 0, nil
}

// ListEvents fetches events from InfluxDB filtered by parameters
func (s *StoreService) ListEvents(ctx context.Context, params hippo.Params) ([]*hippo.Event, error) {
	start := time.Now()
	var events []*hippo.Event

	// Require p.ID.
	if params.ID == "" {
		return nil, hippo.ErrParamsIDRequired
	}

	var cmd string
	// Load all events between versions
	if params.FromVersion >= 0 && params.ToVersion > 0 {
		cmd = fmt.Sprintf("select data from events where aggregate_id='%s' and version>=%d and version <=%d", params.ID, params.FromVersion, params.ToVersion)
	} else {
		cmd = fmt.Sprintf("select data from events where aggregate_id='%s'", params.ID)
	}
	response, err := s.client.db.Query(s.client.Query(cmd))
	if err != nil {
		return nil, err
	}
	if response.Error() != nil {
		return nil, response.Error()
	}
	for _, res := range response.Results {
		for _, ser := range res.Series {
			for _, val := range ser.Values {
				e := &hippo.Event{}
				if err := internal.UnmarshalEvent(val[1].([]byte), e); err != nil {
					return nil, err
				}
				events = append(events, e)
			}
			log.Printf("%v --> %v events fetched - duration: %v", cmd, len(ser.Values), time.Now().Sub(start))
			return events, nil
		}
	}

	log.Printf("%v --> no events to fetch - duration: %v", cmd, time.Now().Sub(start))
	return events, nil
}

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

// Ensure EventService implements hippo.EventService.
var _ hippo.EventService = &EventService{}

// EventService represents a service for managing an aggregate store
// with a InfluxDB client connected.
type EventService struct {
	// db client
	client *Client
}

// Create persists the event.
func (s *EventService) Create(ctx context.Context, e *hippo.Event) error {
	start := time.Now()

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
	data, err := internal.MarshalEventText(e)
	if err != nil {
		return err
	}
	fields := map[string]interface{}{
		"data":    string(data),
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

// GetLastVersion fetches the last version for the aggregate
func (s *EventService) GetLastVersion(ctx context.Context, aggregateID string) (int64, error) {
	start := time.Now()
	cmd := fmt.Sprintf("select last(version) from events where aggregate_id='%s'", aggregateID)
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
				log.Printf("%s --> version %d - duration: %v", cmd, n, time.Now().Sub(start))
				return n, nil
			}
		}
	}
	log.Printf("%s --> no events to fetch - duration: %v", cmd, time.Now().Sub(start))
	return 0, nil
}

// List fetches events filtered by parameters
func (s *EventService) List(ctx context.Context, params hippo.Params) ([]*hippo.Event, error) {
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
				if err := internal.UnmarshalEventText(val[1].(string), e); err != nil {
					return nil, err
				}
				events = append(events, e)
			}
			log.Printf("%s --> %d events fetched - duration: %v", cmd, len(ser.Values), time.Now().Sub(start))
			return events, nil
		}
	}

	log.Printf("%s --> no events to fetch - duration: %v", cmd, time.Now().Sub(start))
	return events, nil
}

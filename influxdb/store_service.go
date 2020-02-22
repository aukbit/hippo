package influxdb

import (
	"fmt"
	"net/url"
	"time"

	"github.com/aukbit/hippo"
	db "github.com/influxdata/influxdb1-client/v2"
)

var _ hippo.StoreService = &StoreService{}

// StoreService holds event service and InfluxDB client connection.
type StoreService struct {
	// Services
	eventService EventService

	// client to connect to InfluxDB
	db db.Client

	// database to be used in InfluxDB
	database string
}

// Config represents a configuration to initialize a new InfluxDB
type Config struct {
	// Addr should be of the form "host:port", defaults to "localhost:8086"
	Addr string

	// Username is the influxdb username, optional.
	Username string

	// Password is the influxdb password, optional.
	Password string

	// Database is the influxdb to be used, defaults to "hippodb"
	Database string
}

// NewStoreService creates a new StoreService
func NewStoreService() *StoreService {
	s := &StoreService{}
	s.eventService.store = s
	return s
}

// Connect connects and pings the InfluxDB database.
func (s *StoreService) Connect(conf Config) error {
	if conf.Addr == "" {
		conf.Addr = "localhost:8086"
	}
	host, err := url.Parse(fmt.Sprintf("http://%s", conf.Addr))
	if err != nil {
		return err
	}
	// NewHTTPClient returns a new InfluxDB Client from the provided config.
	clt, err := db.NewHTTPClient(db.HTTPConfig{
		Addr:     host.String(),
		Username: conf.Username,
		Password: conf.Password,
		Timeout:  1 * time.Second,
	})
	if err != nil {
		return err
	}
	s.db = clt

	// Ping checks InfluxDB status
	_, _, err = s.db.Ping(1 * time.Second)
	if err != nil {
		return err
	}

	if conf.Database == "" {
		conf.Database = "hippo_db"
	}
	s.database = conf.Database

	// Note: If you attempt to create a database that already exists,
	// InfluxDB does nothing and does not return an error.
	if _, err := s.db.Query(s.Query(fmt.Sprintf("create database %s", s.database))); err != nil {
		return err
	}
	return nil
}

// Close closes then underlying InfluxDB database.
func (s *StoreService) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Query returns Query struct with command instruction to performed at InfluxDB
func (s *StoreService) Query(command string) db.Query {
	return db.NewQuery(command, s.database, "")
}

// BatchPoints returns a BatchPoints interface based on the given config.
func (s *StoreService) BatchPoints() (db.BatchPoints, error) {
	bp, err := db.NewBatchPoints(db.BatchPointsConfig{Database: s.database})
	if err != nil {
		return nil, err
	}
	return bp, nil
}

// NewPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended to
// send data with a timestamp.
func (s *StoreService) NewPoint(name string, tags map[string]string,
	fields map[string]interface{}, t ...time.Time) (*db.Point, error) {
	return db.NewPoint(name, tags, fields, t...)
}

// EventService returns the event service associated with the client.
func (s *StoreService) EventService() hippo.EventService { return &s.eventService }

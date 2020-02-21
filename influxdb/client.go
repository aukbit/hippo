package influxdb

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/aukbit/hippo"
	db "github.com/influxdata/influxdb1-client/v2"
)

var _ hippo.Client = &Client{}

// Client represents a client to the underlying InfluxDB data store.
type Client struct {
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

// NewClient creates a new client
func NewClient() *Client {
	c := &Client{}
	c.eventService.client = c
	return c
}

// Connect connects and pings the InfluxDB database.
func (c *Client) Connect(conf Config) error {
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
	c.db = clt

	// Ping checks InfluxDB status
	_, _, err = c.db.Ping(1 * time.Second)
	if err != nil {
		return err
	}

	if conf.Database == "" {
		conf.Database = "hippo_db"
	}
	c.database = conf.Database

	// Note: If you attempt to create a database that already exists,
	// InfluxDB does nothing and does not return an error.
	if _, err := c.db.Query(c.Query(fmt.Sprintf("create database %s", c.database))); err != nil {
		return err
	}
	return nil
}

// Close closes then underlying InfluxDB database.
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Query returns Query struct with command instruction to performed at InfluxDB
func (c *Client) Query(command string) db.Query {
	return db.NewQuery(command, c.database, "")
}

// BatchPoints returns a BatchPoints interface based on the given config.
func (c *Client) BatchPoints() (db.BatchPoints, error) {
	bp, err := db.NewBatchPoints(db.BatchPointsConfig{Database: c.database})
	if err != nil {
		return nil, err
	}
	return bp, nil
}

// NewPoint returns a point with the given timestamp. If a timestamp is not
// given, then data is sent to the database without a timestamp, in which case
// the server will assign local time upon reception. NOTE: it is recommended to
// send data with a timestamp.
func (c *Client) NewPoint(name string, tags map[string]string, fields map[string]interface{}, t ...time.Time) (*db.Point, error) {
	return db.NewPoint(name, tags, fields, t...)
}

// EventService returns the event service associated with the client.
func (c *Client) EventService() hippo.EventService { return &c.eventService }

// Dispatch ..
func (c *Client) Dispatch(ctx context.Context, msg hippo.Message,
	rules hippo.DomainRulesFn, hooks ...hippo.HookFn) (*hippo.Store, error) {
	return hippo.Dispatch(ctx, c.EventService(), msg, rules, hooks...)
}

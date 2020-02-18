package influxdb

import (
	"fmt"
	"net/url"
	"time"

	"github.com/aukbit/hippo"
	db "github.com/influxdata/influxdb1-client/v2"
)

// Client represents a client to the underlying InfluxDB data store.
type Client struct {
	// Services
	storeService StoreService

	// client to connect to InfluxDB
	db db.Client

	// database to be used in InfluxDB
	database string
}

// Config represents a configuration to initialize a new
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
	c.storeService.client = c
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
		conf.Database = "hippodb"
	}
	// Create database
	// Note: If you attempt to create a database that already exists,
	// InfluxDB does nothing and does not return an error.
	q := db.Query{
		Command:  "create database",
		Database: conf.Database,
	}
	if _, err := c.db.Query(q); err != nil {
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

// StoreService returns the store service associated with the client.
func (c *Client) StoreService() hippo.StoreService { return &c.storeService }

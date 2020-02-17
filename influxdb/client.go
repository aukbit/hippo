package influxdb

import (
	db "github.com/influxdata/influxdb1-client/v2"
)

// Client represents a client to the underlying BoltDB data store.
type Client struct {
	// Services
	storeService StoreService

	// Database name to the InfluxDB database.
	Database string

	db db.Client
}

func NewClient() *Client {
	c := &Client{}
	c.storeService.client = c
	return c
}

package influxdb_test

import (
	"github.com/aukbit/hippo/influxdb"
)

// Client is a test wrapper for bolt.Client.
type Client struct {
	*influxdb.Client
}

// NewClient returns a new instance of Client
func NewClient() *Client {

	// Create client wrapper.
	c := &Client{
		Client: influxdb.NewClient(),
	}

	return c
}

// MustConnectClient returns a new and available Client.
func MustConnectClient() *Client {
	c := NewClient()
	if err := c.Connect(influxdb.Config{
		Database: "hippo_db_test",
	}); err != nil {
		panic(err)
	}
	return c
}

// Close closes the client and removes the underlying database.
func (c *Client) Close() error {
	return c.Client.Close()
}

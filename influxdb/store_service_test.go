package influxdb_test

import (
	"github.com/aukbit/hippo/influxdb"
)

// Store is a test wrapper for influxdb.StoreService.
type Store struct {
	*influxdb.StoreService
}

// NewStore returns a new instance of Store
func NewStore() *Store {

	// Create store wrapper.
	s := &Store{
		StoreService: influxdb.NewStoreService(),
	}
	return s
}

// MustConnectStore returns a new and available Store.
func MustConnectStore() *Store {
	s := NewStore()
	if err := s.Connect(influxdb.Config{
		Database: "hippo_db_test",
	}); err != nil {
		panic(err)
	}
	return s
}

// Close closes the store connection.
func (s *Store) Close() error {
	return s.StoreService.Close()
}

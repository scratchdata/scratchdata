package destinations

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"github.com/scratchdata/scratchdata/pkg/destinations/memory"
)

var (
	destinations = &destinationsCache{}
)

type destinationsCache struct {
	// the number of different db connections should be small
	// and they are openend only once, so there shouldn't be much, if any contention
	//
	// if there is a performance issue, a slightly more complex locking scheme might be required:
	// an exclusive lock around the whole cache blocks every caller to GetDestinations
	// until the connection is opened (and pinged)
	// so we'd instead need to lock only the relevant cache key.
	mu sync.Mutex
	m  map[string]DatabaseServer
}

func (dc *destinationsCache) Get(dbConfig models.DatabaseConnection) (DatabaseServer, error) {
	key := dc.deriveKey(dbConfig)

	dc.mu.Lock()
	defer dc.mu.Unlock()

	if db, ok := dc.m[key]; ok {
		return db, nil
	}

	db, err := dc.openServer(dbConfig.Type, dbConfig.ConnectionSettings)
	if err != nil {
		return nil, fmt.Errorf("GetDestination: %s: %w", dbConfig.Type, err)
	}

	if dc.m == nil {
		dc.m = map[string]DatabaseServer{}
	}
	dc.m[key] = db
	return db, nil
}

func (dc *destinationsCache) Clear() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	var errs []error
	for _, db := range dc.m {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	clear(dc.m)
	return errors.Join(errs...)

}

func (dc *destinationsCache) deriveKey(dbConfig models.DatabaseConnection) string {
	// if ID isn't enough, we can also marshal dbConfig using a canonical/deterministic JSON encoder
	return dbConfig.ID
}

func (dc *destinationsCache) openServer(dbType string, settings map[string]any) (DatabaseServer, error) {
	switch dbType {
	case "duckdb":
		return duckdb.OpenServer(settings)
	case "clickhouse":
		return clickhouse.OpenServer(settings)
	case "memory":
		return memory.OpenServer(settings), nil
	default:
		return nil, fmt.Errorf("GetDestination: Unsupported database type: %s", dbType)
	}
}

// GetDestination returns a cached Destination corresponding to dbConfig
func GetDestination(dbConfig models.DatabaseConnection) (DatabaseServer, error) {
	return destinations.Get(dbConfig)
}

// ClearCache closes all cached Destinations and clears the cache.
//
// A combined error is returned for all destinations for which close fails.
func ClearCache() error {
	return destinations.Clear()
}

type DatabaseServer interface {
	InsertBatchFromNDJson(table string, input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error

	// Close closes the database server and prevents new operations from starting.
	// If there are on-going operations, Close waits for them to complete before returning.
	Close() error
}

type Destination interface {
	QueryJSON(query string, writer io.Writer) error
}

func NewDestinationManager(storage *models.StorageServices) *DestinationManager {
	rc := DestinationManager{
		storage: storage,
	}
	return &rc
}

type DestinationManager struct {
	storage *models.StorageServices
}

func (m *DestinationManager) Destination(databaseID int64) (Destination, error) {
	creds, err := m.storage.Database.GetDestinationCredentials(databaseID)
	if err != nil {
		return nil, err
	}

	switch creds.Type {
	case "duckdb":
		return duckdb.OpenServer(creds.Settings)
	case "clickhouse":
		return clickhouse.OpenServer(creds.Settings)
	}
	// TODO cache connection

	return nil, errors.New("Unrecognized database type: " + creds.Type)
}

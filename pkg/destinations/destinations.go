package destinations

import (
	"fmt"
	"io"
	"scratchdata/models"
	"scratchdata/pkg/destinations/clickhouse"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/memory"
	"sync"
)

var (
	destinationCache = struct {
		sync.RWMutex
		m map[string]DatabaseServer
	}{
		m: map[string]DatabaseServer{},
	}
)

func deriveDatabaseConnectionKey(dbConfig models.DatabaseConnection) string {
	// if ID isn't enough, we can also marshal dbConfig using a canonical/deterministic JSON encoder
	return dbConfig.ID
}

func openDBServer(dbType string, settings map[string]any) (DatabaseServer, error) {
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

func GetDestination(dbConfig models.DatabaseConnection) (DatabaseServer, error) {
	key := deriveDatabaseConnectionKey(dbConfig)

	destinationCache.RLock()
	db, ok := destinationCache.m[key]
	destinationCache.RUnlock()
	if ok {
		return db, nil
	}

	destinationCache.Lock()
	defer destinationCache.Unlock()

	if db, ok := destinationCache.m[key]; ok {
		return db, nil
	}

	db, err := openDBServer(dbConfig.Type, dbConfig.ConnectionSettings)
	if err != nil {
		return nil, fmt.Errorf("GetDestination: %s: %w", dbConfig.Type, err)
	}

	destinationCache.m[key] = db
	return db, nil
}

type DatabaseServer interface {
	InsertBatchFromNDJson(table string, input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
}

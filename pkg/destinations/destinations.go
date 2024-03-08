package destinations

import (
	"errors"
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"io"
)

type Destination interface {
	QueryJSON(query string, writer io.Writer) error

	CreateEmptyTable(name string) error
	CreateColumns(table string, filePath string) error
	InsertFromNDJsonFile(table string, filePath string) error
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

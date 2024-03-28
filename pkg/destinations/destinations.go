package destinations

import (
	"context"
	"errors"
	"io"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage"

	"github.com/EagleChen/mapmutex"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/destinations/bigquery"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"github.com/scratchdata/scratchdata/pkg/destinations/redshift"
)

type DestinationManager struct {
	storage *storage.Services
	pool    map[int64]Destination
	mux     *mapmutex.Mutex
}

type Destination interface {
	QueryJSON(query string, writer io.Writer) error
	QueryCSV(query string, writer io.Writer) error

	Tables() ([]string, error)
	Columns(table string) ([]models.Column, error)

	CreateEmptyTable(name string) error
	CreateColumns(table string, filePath string) error
	InsertFromNDJsonFile(table string, filePath string) error

	Close() error
}

func NewDestinationManager(storage *storage.Services) *DestinationManager {
	mux := mapmutex.NewMapMutex()
	rc := DestinationManager{
		storage: storage,
		pool:    map[int64]Destination{},
		mux:     mux,
	}

	return &rc
}

func (m *DestinationManager) CloseAll() {
	for id, dest := range m.pool {
		// TODO: context timeout on close
		err := dest.Close()
		if err != nil {
			log.Error().Err(err).Int64("destination_id", id).Msg("Unable to close destination")
		}
	}
}

func (m *DestinationManager) TestCredentials(creds config.Destination) error {
	var dest Destination
	var err error
	switch creds.Type {
	case "duckdb":
		dest, err = duckdb.OpenServer(creds.Settings)
	case "clickhouse":
		dest, err = clickhouse.OpenServer(creds.Settings)
	case "redshift":
		dest, err = redshift.OpenServer(creds.Settings)
	case "bigquery":
		dest, err = bigquery.OpenServer(creds.Settings)
	default:
		err = errors.New("Invalid destination type")
	}

	if err != nil {
		return err
	}

	dest.Close()
	return nil
}

func (m *DestinationManager) Destination(ctx context.Context, databaseID int64) (Destination, error) {

	if m.mux.TryLock(databaseID) {
		defer m.mux.Unlock(databaseID)

		var dest Destination

		dest, ok := m.pool[databaseID]
		if ok {
			return dest, nil
		}

		creds, err := m.storage.Database.GetDestinationCredentials(ctx, databaseID)
		if err != nil {
			return nil, err
		}

		switch creds.Type {
		case "duckdb":
			dest, err = duckdb.OpenServer(creds.Settings)
		case "clickhouse":
			dest, err = clickhouse.OpenServer(creds.Settings)
		case "redshift":
			dest, err = redshift.OpenServer(creds.Settings)
		case "bigquery":
			dest, err = bigquery.OpenServer(creds.Settings)
		}

		if err != nil {
			return nil, err
		}

		if dest != nil {
			m.pool[databaseID] = dest
			return dest, nil
		} else {
			return nil, errors.New("Unrecognized destination type " + creds.Type)
		}
	}

	return nil, errors.New("unable to acquire destination lock")
}

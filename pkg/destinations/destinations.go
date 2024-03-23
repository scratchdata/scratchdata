package destinations

import (
	"encoding/json"
	"errors"
	"io"
	"strconv"

	"github.com/EagleChen/mapmutex"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"github.com/scratchdata/scratchdata/pkg/destinations/redshift"
)

type DestinationManager struct {
	storage *models.StorageServices
	pool    map[int64]Destination
	mux     *mapmutex.Mutex
}

type Destination interface {
	QueryJSON(query string, writer io.Writer) error
	QueryCSV(query string, writer io.Writer) error

	CreateEmptyTable(name string) error
	CreateColumns(table string, filePath string) error
	InsertFromNDJsonFile(table string, filePath string) error

	Close() error
}

type Creds struct {
	ID       int64          `yaml:"id" json:"id"`
	Type     string         `yaml:"type" json:"type"`
	Name     string         `yaml:"name" json:"name"`
	Settings map[string]any `yaml:"settings" json:"settings"`
	APIKeys  []string       `yaml:"api_keys" json:"api_keys"`
}

func NewDestinationManager(storage *models.StorageServices) *DestinationManager {
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

func (m *DestinationManager) Destination(databaseID int64) (Destination, error) {

	if m.mux.TryLock(databaseID) {
		defer m.mux.Unlock(databaseID)

		var dest Destination
		var creds Creds

		dest, ok := m.pool[databaseID]
		if ok {
			return dest, nil
		}

		jsonCreds, err := m.storage.Vault.GetCredential(strconv.Itoa(int(databaseID)))
		if err != nil {
			return nil, err
		}

		credsBytes := []byte(jsonCreds)
		err = json.Unmarshal(credsBytes, &creds)

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

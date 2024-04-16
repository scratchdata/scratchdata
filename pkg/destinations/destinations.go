package destinations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage"

	"github.com/EagleChen/mapmutex"
	"github.com/gosimple/slug"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/destinations/bigquery"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"github.com/scratchdata/scratchdata/pkg/destinations/redshift"
	dmodels "github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

type DestinationManager struct {
	storage *storage.Services
	pool    map[uint]Destination
	mux     *mapmutex.Mutex
}

type Destination interface {
	QueryNDJson(query string, writer io.Writer) error
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
		pool:    map[uint]Destination{},
		mux:     mux,
	}

	return &rc
}

func (m *DestinationManager) CloseAll() {
	for id, dest := range m.pool {
		// TODO: context timeout on close
		err := dest.Close()
		if err != nil {
			log.Error().Err(err).Uint("destination_id", id).Msg("Unable to close destination")
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

func (m *DestinationManager) UpdateDestination(ctx context.Context, dest dmodels.Destination, creds map[string]any) error {
	err := m.storage.Database.UpdateDestination(ctx, dest)
	if err != nil {
		return err
	}

	ds, err := json.Marshal(creds)
	if err != nil {
		return err
	}

	cn := credentialName(dest.TeamID, dest.ID, dest.Name)
	err = m.storage.Vault.SetCredential(cn, string(ds))
	if err != nil {
		return err
	}
	return nil
}

func (m *DestinationManager) CreateDestination(ctx context.Context, teamID uint, dest config.Destination) (uint, error) {
	// TODO breadchris create a destination in the database and store the credentials in the vault
	d, err := m.storage.Database.CreateDestination(ctx, teamID, dest.Name, dest.Type)
	if err != nil {
		return 0, err
	}

	ds, err := json.Marshal(dest.Settings)
	if err != nil {
		return 0, err
	}

	cn := credentialName(teamID, d.ID, dest.Name)
	err = m.storage.Vault.SetCredential(cn, string(ds))
	if err != nil {
		return 0, err
	}
	return d.ID, nil
}

func (m *DestinationManager) Destination(ctx context.Context, databaseID uint) (Destination, error) {
	if m.mux.TryLock(databaseID) {
		defer m.mux.Unlock(databaseID)

		var (
			dest     Destination
			settings map[string]any
		)

		dest, ok := m.pool[databaseID]
		if ok {
			return dest, nil
		}

		// XXX breadchris does the caller verify existence and authz for this database?
		// is this call needed?
		creds, err := m.storage.Database.GetDestinationCredentials(ctx, databaseID)
		if err != nil {
			return nil, err
		}

		cn := credentialName(creds.TeamID, databaseID, creds.Name)
		jsonDestSettings, err := m.storage.Vault.GetCredential(cn)
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal([]byte(jsonDestSettings), &settings); err != nil {
			return nil, err
		}

		switch creds.Type {
		case "duckdb":
			dest, err = duckdb.OpenServer(settings)
		case "clickhouse":
			dest, err = clickhouse.OpenServer(settings)
		case "redshift":
			dest, err = redshift.OpenServer(settings)
		case "bigquery":
			dest, err = bigquery.OpenServer(settings)
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

func credentialName(teamID, destID uint, name string) string {
	return fmt.Sprintf("%d-%d-%s", teamID, destID, slug.Make(name))
}

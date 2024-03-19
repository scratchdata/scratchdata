package database

import (
	"time"

	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/memory"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

type Database interface {
	VerifyAdminAPIKey(hashedAPIKey string) bool

	GetDestinations() []config.Destination
	CreateDestination(destType string, settings map[string]any) (config.Destination, error)

	AddAPIKey(destId int64, hashedAPIKey string) error
	GetAPIKeyDetails(hashedAPIKey string) (models.APIKey, error)

	CreateShareQuery(destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error)
	GetShareQuery(queryId uuid.UUID) (models.SharedQuery, bool)

	Hash(s string) string
}

func NewDatabaseConnection(conf config.Database, destinations []config.Destination, adminKeys []config.APIKey) Database {
	switch conf.Type {
	case "memory":
		return memory.NewMemoryDatabase(conf, destinations, adminKeys)
	}

	return nil
}

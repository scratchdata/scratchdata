package database

import (
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/memory"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

type Database interface {
	VerifyAdminAPIKey(apiKey string) bool
	GetAPIKeyDetails(apiKey string) (models.APIKey, error)

	GetDestinationCredentials(dbID int64) (config.Destination, error)
	CreateDestination(destType string, settings map[string]any) (config.Destination, error)
	AddAPIKey(destId int64, key string) error
	GetDestinations() []config.Destination
}

func NewDatabaseConnection(conf config.Database, destinations []config.Destination, adminKeys []config.APIKey) Database {
	switch conf.Type {
	case "memory":
		return memory.NewMemoryDatabase(conf, destinations, adminKeys)
	}

	return nil
}

package memory

import (
	"errors"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type MemoryDatabase struct {
	conf                config.Database
	destinations        []config.Destination
	apiKeyToDestination map[string]int64
	adminAPIKeys        []config.APIKey

	sqlite *gorm.DB
}

func (db *MemoryDatabase) VerifyAdminAPIKey(apiKey string) bool {
	for _, key := range db.adminAPIKeys {
		if key.Key == apiKey {
			return true
		}
	}

	return false
}

func NewMemoryDatabase(conf config.Database, destinations []config.Destination, apiKeys []config.APIKey) *MemoryDatabase {
	rc := MemoryDatabase{
		conf:                conf,
		destinations:        destinations,
		apiKeyToDestination: map[string]int64{},
		adminAPIKeys:        apiKeys,
	}

	for i, destination := range destinations {
		for _, apiKey := range destination.APIKeys {
			rc.apiKeyToDestination[apiKey] = int64(i)
		}
	}
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		log.Error().Err(err).Msg("Unable to connect to sqlite")
		return nil
	}

	rc.sqlite = db

	err = db.AutoMigrate(&ShareLink{})
	if err != nil {
		log.Error().Err(err).Msg("Unable to create sqlite tables")
		return nil
	}

	return &rc
}

func (db *MemoryDatabase) GetAPIKeyDetails(apiKey string) (models.APIKey, error) {
	dbId, ok := db.apiKeyToDestination[apiKey]
	if !ok {
		return models.APIKey{}, errors.New("invalid API key")
	}
	rc := models.APIKey{
		DestinationID: dbId,
	}
	return rc, nil
}

func (db *MemoryDatabase) GetDestinationCredentials(dbID int64) (config.Destination, error) {
	return db.destinations[dbID], nil
}

package static

import (
	"errors"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"

	"github.com/scratchdata/scratchdata/config"
)

type StaticDatabase struct {
	conf         config.Database
	destinations []config.Destination

	apiKeyToDestination map[string]int64
}

func NewStaticDatabase(conf config.Database, destinations []config.Destination) *StaticDatabase {
	rc := StaticDatabase{
		conf:                conf,
		destinations:        destinations,
		apiKeyToDestination: map[string]int64{},
	}

	for i, destination := range destinations {
		for _, apiKey := range destination.APIKeys {
			rc.apiKeyToDestination[apiKey] = int64(i)
		}
	}

	return &rc
}

func (db *StaticDatabase) GetAPIKeyDetails(apiKey string) (models.APIKey, error) {
	dbId, ok := db.apiKeyToDestination[apiKey]
	if !ok {
		return models.APIKey{}, errors.New("invalid API key")
	}
	rc := models.APIKey{
		DestinationID: dbId,
	}
	return rc, nil
}

func (db *StaticDatabase) GetDestinationCredentials(dbID int64) (config.Destination, error) {
	return db.destinations[dbID], nil
}

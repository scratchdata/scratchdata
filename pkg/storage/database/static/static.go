package static

import (
	"errors"

	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
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

func (db *StaticDatabase) GetAPIKeyDetails(apiKey string) (database.APIKey, error) {
	dbId, ok := db.apiKeyToDestination[apiKey]
	if !ok {
		return database.APIKey{}, errors.New("invalid API key")
	}
	rc := database.APIKey{
		DatabaseID: dbId,
	}
	return rc, nil
}

func (db *StaticDatabase) GetDestinationCredentials(dbID int64) (config.Destination, error) {
	return db.destinations[dbID], nil
}

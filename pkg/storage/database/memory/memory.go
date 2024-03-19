package memory

import (
	"errors"
	"time"

	"github.com/google/uuid"
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

func (db *MemoryDatabase) Hash(s string) string {
	return s
}

func (db *MemoryDatabase) GetDestinations() []config.Destination {
	return db.destinations
}

func (db *MemoryDatabase) AddAPIKey(destId int64, key string) error {
	return errors.New("Cannot add API key to memory-based database. Update config file instead.")
}

func (db *MemoryDatabase) CreateDestination(destType string, settings map[string]any) (config.Destination, error) {
	return config.Destination{}, errors.New("Cannot add new destination to memory-based database. Update config file instead.")
}

func (db *MemoryDatabase) VerifyAdminAPIKey(apiKey string) bool {
	for _, key := range db.adminAPIKeys {
		if key.Key == apiKey {
			return true
		}
	}

	return false
}

func (db *MemoryDatabase) CreateShareQuery(destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error) {
	id := uuid.New()
	link := ShareLink{
		UUID:          id.String(),
		DestinationID: destId,
		Query:         query,
		ExpiresAt:     time.Now().Add(expires),
	}

	log.Print(link)
	log.Print(time.Now())

	res := db.sqlite.Create(&link)
	if res.Error != nil {
		return uuid.Nil, res.Error
	}

	return id, nil
}

func (db *MemoryDatabase) GetShareQuery(queryId uuid.UUID) (models.SharedQuery, bool) {
	var link ShareLink
	res := db.sqlite.First(&link, "uuid = ? AND expires_at > ?", queryId.String(), time.Now())
	if res.Error != nil {
		if !errors.Is(res.Error, gorm.ErrRecordNotFound) {
			log.Error().Err(res.Error).Str("query_id", queryId.String()).Msg("Unable to find shared query")
		}

		return models.SharedQuery{}, false
	}

	rc := models.SharedQuery{
		ID:            link.UUID,
		Query:         link.Query,
		ExpiresAt:     link.ExpiresAt,
		DestinationID: link.DestinationID,
	}

	return rc, true
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

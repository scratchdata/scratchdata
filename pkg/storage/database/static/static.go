package static

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/gorm"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

var StaticDBError = errors.New("Cannot make changes when using static config. Update config file or use a database instead.")

type StaticDatabase struct {
	conf                config.Database
	destinations        []config.Destination
	apiKeyToDestination map[string]uint
	adminAPIKeys        []config.APIKey
}

func NewStaticDatabase(destinations []config.Destination, apiKeys []config.APIKey) (*gorm.Gorm, error) {
	ctx := context.TODO()

	conf := config.Database{
		Type: "sqlite",
		Settings: map[string]any{
			"dsn": "file::memory:?cache=shared",
		},
	}

	rc, err := gorm.NewGorm(conf)
	if err != nil {
		return nil, err
	}

	team, err := rc.CreateTeam("Team Scratch")
	if err != nil {
		return nil, err
	}

	_, err = rc.CreateUser("scratch@example.com", "static", "", int64(team.ID))
	if err != nil {
		return nil, err
	}

	for _, destination := range destinations {
		dest, err := rc.CreateDestination(ctx, team.ID, destination.Type, destination.Settings)
		if err != nil {
			return nil, err
		}

		for _, apiKey := range dest.APIKeys {
			err = rc.AddAPIKey(ctx, dest.ID, rc.Hash(apiKey))
			if err != nil {
				return nil, err
			}
		}
	}

	// rc := StaticDatabase{
	// 	conf:                conf,
	// 	destinations:        destinations,
	// 	apiKeyToDestination: map[string]uint{},
	// 	adminAPIKeys:        apiKeys,
	// }

	// for i, destination := range destinations {
	// 	for _, apiKey := range destination.APIKeys {
	// 		rc.apiKeyToDestination[apiKey] = uint(i)
	// 	}
	// }

	return rc, nil
}

func (db *StaticDatabase) Hash(s string) string {
	return s
}

func (db *StaticDatabase) GetDestinations(ctx context.Context, teamId uint) []config.Destination {
	return db.destinations
}

func (db *StaticDatabase) AddAPIKey(ctx context.Context, destId int64, key string) error {
	return StaticDBError
}

func (db *StaticDatabase) CreateDestination(ctx context.Context, teamId uint, destType string, settings map[string]any) (config.Destination, error) {
	return config.Destination{}, StaticDBError
}

func (db *StaticDatabase) VerifyAdminAPIKey(ctx context.Context, apiKey string) bool {
	for _, key := range db.adminAPIKeys {
		if key.Key == apiKey {
			return true
		}
	}

	return false
}

func (db *StaticDatabase) CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error) {
	return uuid.Nil, StaticDBError
}

func (db *StaticDatabase) GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.SharedQuery, bool) {
	return models.SharedQuery{}, false
}

func (db *StaticDatabase) GetAPIKeyDetails(ctx context.Context, apiKey string) (models.APIKey, error) {
	dbId, ok := db.apiKeyToDestination[apiKey]
	if !ok {
		return models.APIKey{}, errors.New("invalid API key")
	}
	rc := models.APIKey{
		DestinationID: dbId,
	}
	return rc, nil
}

func (db *StaticDatabase) GetDestinationCredentials(ctx context.Context, dbID int64) (config.Destination, error) {
	return db.destinations[dbID], nil
}

func (db *StaticDatabase) CreateUser(email string, source string, details string) (*models.User, error) {
	user := &models.User{
		Email:    "scratchdata@example.com",
		AuthType: "static",
	}
	user.ID = 1
	return user, nil
}

func (db *StaticDatabase) GetUser(int64) *models.User {
	user := &models.User{
		Email:    "scratchdata@example.com",
		AuthType: "static",
	}
	user.ID = 1
	return user
}

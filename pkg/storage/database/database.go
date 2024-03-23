package database

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/memory"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

type Database interface {
	VerifyAdminAPIKey(ctx context.Context, hashedAPIKey string) bool

	GetDestinations(ctx context.Context) []config.Destination
	CreateDestination(ctx context.Context, destType string, settings map[string]any) (config.Destination, error)

	AddAPIKey(ctx context.Context, destId int64, hashedAPIKey string) error
	GetAPIKeyDetails(ctx context.Context, hashedAPIKey string) (models.APIKey, error)

	CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error)
	GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.SharedQuery, bool)

	Hash(s string) string
}

func NewDatabaseConnection(conf config.Database, destinations []config.Destination, adminKeys []config.APIKey) Database {
	switch conf.Type {
	case "memory":
		return memory.NewMemoryDatabase(conf, destinations, adminKeys)
	}

	return nil
}

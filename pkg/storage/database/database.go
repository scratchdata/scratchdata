package database

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/gorm"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/storage/database/static"
)

type Database interface {
	VerifyAdminAPIKey(ctx context.Context, hashedAPIKey string) bool

	GetDestinations(ctx context.Context, teamId uint) []config.Destination
	CreateDestination(ctx context.Context, teamId uint, destType string, settings map[string]any) (config.Destination, error)
	GetDestinationCredentials(ctx context.Context, dbID int64) (config.Destination, error)

	AddAPIKey(ctx context.Context, destId int64, hashedAPIKey string) error
	GetAPIKeyDetails(ctx context.Context, hashedAPIKey string) (models.APIKey, error)

	CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error)
	GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.SharedQuery, bool)

	GetUser(int64) *models.User
	CreateUser(email string, source string, details string) (*models.User, error)

	Hash(s string) string

	Enqueue(messageType models.MessageType, message any) (*models.Message, error)
	Dequeue(messageType models.MessageType, claimedBy string) (*models.Message, bool)
}

func NewConnection(conf config.Database, destinations []config.Destination, adminKeys []config.APIKey) (Database, error) {
	switch conf.Type {
	case "static":
		return static.NewStaticDatabase(conf, destinations, adminKeys)
	case "sqlite":
		return gorm.NewGorm(conf)
	case "postgres":
		return gorm.NewGorm(conf)
	}

	return nil, errors.New("Unable to connect to any database")
}

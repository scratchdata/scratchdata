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

	GetDestinations(ctx context.Context, teamId uint) ([]models.Destination, error)
	GetDestination(ctx context.Context, teamId, destId uint) (models.Destination, error)
	CreateDestination(ctx context.Context, teamId uint, name string, destType string, settings map[string]any) (models.Destination, error)
	DeleteDestination(ctx context.Context, teamId uint, destId int64) error
	UpdateDestination(ctx context.Context, dest models.Destination) error
	GetDestinationCredentials(ctx context.Context, dbID int64) (models.Destination, error)

	CreateConnectionRequest(ctx context.Context, dest models.Destination) (models.ConnectionRequest, error)
	GetConnectionRequest(ctx context.Context, requestId uuid.UUID) (models.ConnectionRequest, error)
	DeleteConnectionRequest(ctx context.Context, id uint) error

	AddAPIKey(ctx context.Context, destId int64, hashedAPIKey string) error
	GetAPIKeyDetails(ctx context.Context, hashedAPIKey string) (models.APIKey, error)

	CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error)
	GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.SharedQuery, bool)

	CreateTeam(name string) (*models.Team, error)
	AddUserToTeam(userId uint, teamId uint) error

	GetUser(uint) *models.User
	GetTeamId(userId uint) (uint, error)
	CreateUser(email string, source string, details string) (*models.User, error)

	Hash(s string) string

	Enqueue(messageType models.MessageType, message any) (*models.Message, error)
	Dequeue(messageType models.MessageType, claimedBy string) (*models.Message, bool)
	Delete(id uint) error
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

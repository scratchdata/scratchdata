package database

import (
	"context"
	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/config"
	"time"
)

type Database interface {
	VerifyAdminAPIKey(ctx context.Context, hashedAPIKey string) bool

	GetDestinations(ctx context.Context, userId uint) ([]config.Destination, error)
	CreateDestination(ctx context.Context, userId uint, name string, destType string, settings map[string]any) (config.Destination, error)
	DeleteDestination(ctx context.Context, userId uint, destId int64) error
	GetDestinationCredentials(ctx context.Context, dbID int64) (config.Destination, error)

	AddAPIKey(ctx context.Context, destId int64, hashedAPIKey string) error
	GetAPIKeyDetails(ctx context.Context, hashedAPIKey string) (APIKey, error)

	CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error)
	GetShareQuery(ctx context.Context, queryId uuid.UUID) (SharedQuery, bool)

	GetUser(int64) *User
	CreateUser(email string, source string, details string) (*User, error)

	Hash(s string) string
}

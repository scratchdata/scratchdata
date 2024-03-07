package database

import "github.com/scratchdata/scratchdata/config"

type APIKey struct {
	DatabaseID int64
}

type Database interface {
	GetAPIKeyDetails(apiKey string) (APIKey, error)
	GetDestinationCredentials(dbID int64) (config.Destination, error)
}

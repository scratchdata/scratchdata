package database

import (
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/storage/database/static"
)

//type APIKey struct {
//	DatabaseID int64
//}

type Database interface {
	GetAPIKeyDetails(apiKey string) (models.APIKey, error)
	GetDestinationCredentials(dbID int64) (config.Destination, error)
}

func NewDatabaseConnection(conf config.Database, destinations []config.Destination) Database {
	switch conf.Type {
	default:
		return static.NewStaticDatabase(conf, destinations)
	}
}

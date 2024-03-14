package database

import (
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
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
	case "static":
		return static.NewStaticDatabase(conf, destinations)
	}

	return nil
}

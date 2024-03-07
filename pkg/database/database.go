package database

import (
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/database/static"
	models2 "github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/util"
)

type Database interface {
	Open() error
	Close() error

	Hash(input string) string

	GetAPIKeyDetails(hashedAPIKey string) models2.APIKey
	GetAccount(id string) models.Account
	GetDatabaseConnection(connectionID string) models.DatabaseConnection

	HealthCheck() error
}

func GetDB(config map[string]interface{}) Database {
	configType := config["type"]

	switch configType {
	case "static":
		return util.ConfigToStruct[static.StaticDB](config)
	default:
		return nil
	}
}

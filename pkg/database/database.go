package database

import (
	"scratchdata/models"
	"scratchdata/pkg/database/static"
	"scratchdata/util"
)

type Database interface {
	Open() error
	Close() error

	Hash(input string) string

	GetAPIKeyDetails(hashedAPIKey string) models.APIKey
	GetAccount(id string) models.Account
	GetDatabaseConnection(connectionID string) models.DatabaseConnection
}

func GetDB(config map[string]interface{}) Database {
	configType := config["type"]

	switch configType {
	case "static":
		return util.ConfigToStruct[*static.StaticDB](config)
	default:
		return nil
	}
}

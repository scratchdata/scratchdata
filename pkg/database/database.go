package database

import (
	"scratchdata/models"
	"scratchdata/pkg/database/static"
	"scratchdata/util"
)

type Database interface {
	Open() error
	Close() error

	GetAccount(id string) models.Account
	GetUsers(accountID string) []models.User
	GetAPIKeys(accountID string) []models.APIKey
	GetDatabaseConnections(accountID string) []models.DatabaseConnection
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

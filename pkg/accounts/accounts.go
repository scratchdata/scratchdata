package accounts

import (
	"scratchdata/models"
	"scratchdata/pkg/destinations"
)

// AccountManagement interface
type AccountManager interface {
	GetAccount(id string) models.Account
	GetUsers(accountID string) []models.User
	GetAPIKeys(accountID string) []models.APIKey
	GetDatabaseConnections(accountID string) []destinations.DatabaseServer
}

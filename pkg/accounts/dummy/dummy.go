package dummy

import (
	"scratchdata/models"
	"scratchdata/pkg/destinations"
)

type DummyAccountManager struct {
	ConfigFile string           `mapstructure:"filename"`
	accounts   []models.Account `mapstructure:"accounts"`
	users      []models.User    `mapstructure:"users"`
	apiKeys    []models.APIKey  `mapstructure:"api_keys"`
}

func (d DummyAccountManager) GetAccount(id string) models.Account {
	return models.Account{ID: "dummy-account"}
}

func (d DummyAccountManager) GetUsers(accountID string) []models.User {
	return []models.User{{ID: "dummy-user", AccountID: "dummy-account"}}
}

func (d DummyAccountManager) GetAPIKeys(accountID string) []models.APIKey {
	return []models.APIKey{{ID: "dummy-api-key", AccountID: "dummy-account", Permissions: []models.Permission{models.Read, models.Write}}}
}

func (d DummyAccountManager) GetDatabaseConnections(accountID string) []destinations.DatabaseServer {
	c := map[string]interface{}{
		"type":     "duckdb",
		"token":    "x",
		"database": "sample_data",
	}
	return []destinations.DatabaseServer{
		destinations.GetDestination(c),
	}
}

package dummy

import "scratchdata/pkg/accounts"

type DummyAccountManager struct{}

func (d DummyAccountManager) GetAccount(id string) accounts.Account {
	return accounts.Account{ID: "dummy-account"}
}

func (d DummyAccountManager) GetUsers(accountID string) []accounts.User {
	return []accounts.User{{ID: "dummy-user", AccountID: "dummy-account"}}
}

func (d DummyAccountManager) GetAPIKeys(accountID string) []accounts.APIKey {
	return []accounts.APIKey{{ID: "dummy-api-key", AccountID: "dummy-account", Permissions: []accounts.Permission{accounts.Read, accounts.Write}}}
}

func (d DummyAccountManager) GetDatabaseConnections(accountID string) []accounts.DatabaseConnection {
	return []accounts.DatabaseConnection{{ID: "dummy-db-connection", AccountID: "dummy-account", Permissions: []accounts.Permission{accounts.Read}}}
}

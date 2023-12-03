package accounts

import "scratchdata/pkg/destinations"

type Permission string

const (
	Read  Permission = "read"
	Write Permission = "write"
)

// Account struct
type Account struct {
	ID string
}

// User struct belonging to an account
type User struct {
	ID        string
	AccountID string
}

// APIKey struct belonging to an account
type APIKey struct {
	ID          string
	AccountID   string
	Permissions []Permission
}

// DatabaseConnection struct belonging to an account
type DatabaseConnection struct {
	ID                 string
	AccountID          string
	Permissions        []Permission
	Type               string
	ConnectionSettings map[string]interface{}
	// Connection         destinations.DatabaseServer
}

func (c DatabaseConnection) Connection() destinations.DatabaseServer {
	return destinations.GetDestination(c.ConnectionSettings)
}

// AccountManagement interface
type AccountManagement interface {
	GetAccount(id string) Account
	GetUsers(accountID string) []User
	GetAPIKeys(accountID string) []APIKey
	GetDatabaseConnections(accountID string) []destinations.DatabaseServer
}

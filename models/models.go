package models

type Permission string

const (
	Read  Permission = "read"
	Write Permission = "write"
)

type Account struct {
	ID string `toml:"id"`
}

// User struct belonging to an account
type User struct {
	ID         string   `toml:"id"`
	AccountIDs []string `toml:"accounts"`
}

// APIKey struct belonging to an account
type APIKey struct {
	ID          string       `toml:"id"`
	AccountID   string       `toml:"account_id"`
	Permissions []Permission `toml:"permissions"`
}

// DatabaseConnection struct belonging to an account
type DatabaseConnection struct {
	ID                 string                 `toml:"id"`
	AccountID          string                 `toml:"account_id"`
	Permissions        []Permission           `toml:"permissions"`
	Type               string                 `toml:"type"`
	ConnectionSettings map[string]interface{} `toml:"settings"`
}

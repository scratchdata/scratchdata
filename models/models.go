package models

type Permission string

const (
	Read  Permission = "read"
	Write Permission = "write"
)

type Account struct {
	ID string `toml:"id"`
}

type User struct {
	ID         string   `toml:"id"`
	AccountIDs []string `toml:"accounts"`
}

type APIKey struct {
	ID            string       `toml:"id"`
	AccountID     string       `toml:"account_id"`
	DestinationID string       `toml:"destination_id"`
	HashedAPIKey  string       `toml:"hashed_api_key"`
	Permissions   []Permission `toml:"permissions"`
}

type DatabaseConnection struct {
	ID                 string                 `toml:"id"`
	AccountID          string                 `toml:"account_id"`
	Permissions        []Permission           `toml:"permissions"`
	Type               string                 `toml:"type"`
	ConnectionSettings map[string]interface{} `toml:"settings"`
}

type FileUploadMessage struct {
	Key  string `json:"key"`
	Path string `json:"path"`
}

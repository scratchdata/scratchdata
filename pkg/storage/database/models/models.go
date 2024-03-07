package models

type APIKey struct {
	ID            string `toml:"id"`
	AccountID     string `toml:"account_id"`
	DestinationID int64  `toml:"destination_id"`
	HashedAPIKey  string `toml:"hashed_api_key"`
	//Permissions   []models.Permission `toml:"permissions"`
}
